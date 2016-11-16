#include "stdafx.h"

#include "UtilitiesWin.hpp"
#include "ETWTracing.h"


#include "Tracer.hpp"

#include "Scheduler.hpp"


#include <objbase.h>
#include <random>
#include <limits>
#include <unordered_map>
#include <sstream>
#include <memory>


namespace  // private to this source file
{

	class NoneZeroRandom32 {
	private:
		std::random_device rd;
		std::mt19937_64 rng;
		std::tr1::uniform_int<uint32_t> unif;
	public:
		NoneZeroRandom32() : rng(rd()), unif(1, 0xffffffff)
		{
		}

		uint32_t Get() {
			return unif(rng);
		}
	};

	NoneZeroRandom32 g_noneZeroRandom;


	// Data structure for enabling ETW tracing
	//
#define DEFAULT_LOG_PATH L".\\ExabytesTracing.etl"
#define SESSION_NAME L"SessionExabytesTracing"

	struct ETWControl {
		// {314B2974-635B-46A1-9E3E-64751E038EFF}
		EVENT_TRACE_PROPERTIES* m_pSessionProperties = NULL;
		uint64_t m_sessionHandle = 0;
		bool m_traceOn = false;
	}  g_etwControl;

	const GUID SessionGuid = { 0x314B2974, 0x635B, 0x46A1, { 0x9E, 0x3E, 0x64, 0x75, 0x1E, 0x03, 0x8E, 0xFF } };
}

namespace Schedulers
{
	extern __declspec(thread) const Schedulers::Work* threadActiveJob;
}
namespace Tracer
{
	using namespace Utilities;
	using namespace std;

	struct ActivityInfo
	{
		uint64_t NumberOfIO = 0;
		uint64_t ActionStartTime[(size_t)EBCounter::MaxActions - 1];
	};

	class ActionTracker
	{
		std::unordered_map<TraceRec, ActivityInfo> m_activityHashMap;
		SRWLOCK m_mapLock;

		// This is used for everytime creating a new Activity Info struct with empty data
		ActivityInfo m_emptyActivityInfo;

	public:
		ActionTracker()
		{
			InitializeSRWLock(&m_mapLock);
			ZeroMemory(&m_emptyActivityInfo, sizeof(ActivityInfo));
		}

		// Adds a new entry to activity map with the given traceID
		void CreateActivityStack(const TraceRec* span)
		{
			{
				Utilities::Exclude<SRWLOCK> guard{ m_mapLock };
				auto iter = m_activityHashMap.insert(std::make_pair(*span, m_emptyActivityInfo));
			}
			Tracer::LogCounterValue(EBCounter::TracerMapSize, L"", m_activityHashMap.size());
		}

		// Adds new entry to the activity stack located at the given traceID
		// This should be called every time TimerStart event for a new action is called
		void AddNewAction(const TraceRec* span, EBCounter counter, int64_t startTime)
		{
			StatusCode status = StatusCode::InvalidState;

			Audit::Assert(counter > EBCounter::NonCounter && counter < EBCounter::MaxActions,
				"Unexpected counter type.");

			ActivityInfo* activityInfoPtr = nullptr;
			{
				Utilities::Share<SRWLOCK> guard{ m_mapLock };
				auto entry = m_activityHashMap.find(*span);
				if (entry != m_activityHashMap.end())
				{
					activityInfoPtr = &entry->second;
				}
			}
			if (activityInfoPtr != nullptr)
			{
				activityInfoPtr->ActionStartTime[(size_t)counter - 1] = startTime;
				if (counter == EBCounter::DiskRead)
				{
					++activityInfoPtr->NumberOfIO;
				}
				status = StatusCode::OK;
			}


			if (status != StatusCode::OK)
			{
				Tracer::LogWarning(status, L"Activity Stack not found so creating a new one from AddNewAction", span);
			}
		}

		// Get the start time of the requested activity. 
		// This should be called every time TimerEnd event is encountered for any action. 
		// If it finds orphaned activites then it reports error and returns 0 as start time
		int64_t GetActionStartTime(const TraceRec* span, EBCounter currentAction)
		{
			int64_t returnVal = 0;

			Audit::Assert(currentAction > EBCounter::NonCounter
				&& currentAction < EBCounter::MaxActions,
				"Logger was given invalid counter");

			StatusCode status = StatusCode::InvalidState;
			ActivityInfo* activityInfoPtr = nullptr;
			wchar_t errMsg[256] = L"";
			{
				Utilities::Share<SRWLOCK> guard{ m_mapLock };
				auto entry = m_activityHashMap.find(*span);
				if (entry != m_activityHashMap.end())
				{
					activityInfoPtr = &entry->second;
				}
			}

			if (activityInfoPtr != nullptr)
			{
				status = StatusCode::OK;

				if (activityInfoPtr->ActionStartTime[(size_t)currentAction - 1] != 0)
				{
					returnVal = activityInfoPtr->ActionStartTime[(size_t)currentAction - 1];
					activityInfoPtr->ActionStartTime[(size_t)currentAction - 1] = 0;
				}
				else
				{
					status = StatusCode::InvalidState;
					swprintf_s(errMsg, L"Cannot find current action on stack: %s ", g_EBCounterNames[static_cast<uint32_t>(currentAction)]);
				}
			}
			else
			{
				swprintf_s(errMsg, L"Action stack is empty. Current Counter: %s", g_EBCounterNames[static_cast<uint32_t>(currentAction)]);
			}

			// if returnVal was not set then log error
			if (status != StatusCode::OK)
			{
				Tracer::LogError(StatusCode::InvalidState, errMsg);
			}
			return returnVal;
		}

		// Removes stack associated with the requested trace id
		// This is called everytime a SpanEnd event is encountered
		// This method checks the corresponding activity stack is empty
		// If not then it generates an error
		void DeleteActivityStack(const TraceRec* span)
		{
			StatusCode status = StatusCode::OK;
			ActivityInfo activityInfo;
			wchar_t errMsg[256] = L"";
			{
				Utilities::Exclude<SRWLOCK> guard{ m_mapLock };
				auto iter = m_activityHashMap.find(*span);
				if (iter == m_activityHashMap.end())
				{
					status = StatusCode::InvalidState;
				}
				else
				{
					// move the ptr so that we can take care of it outside the critical secion
					// and delete the hash entry for this trace id
					activityInfo = iter->second;
					m_activityHashMap.erase(*span);
				}
			}

			if (status == StatusCode::OK)
			{
				Tracer::LogCounterValue(EBCounter::NumbersOfIO, L"", activityInfo.NumberOfIO, span);

				// Log Error for each orphaned activity encountered
				for (auto i = 0; i < (size_t)EBCounter::MaxActions - 1; i++)
				{
					if (activityInfo.ActionStartTime[i] != 0)
					{
						swprintf_s(errMsg,
							L"%s orphaned action found in the action latenceny tracker stack",
							g_EBCounterNames[static_cast<uint32_t>(i)]);

						Tracer::LogError(StatusCode::InvalidState, errMsg);
					}
				}

			}

			if (status != StatusCode::OK)
			{
				Tracer::LogError(status, L"Current activity stack already deleted");
			}
		}
	};

	std::unique_ptr<ActionTracker> g_pActionTracker = std::make_unique<ActionTracker>();

	const TraceRec* GetCurrentTraceHeader()
	{
		if (Schedulers::threadActiveJob == nullptr)
			return nullptr;
		return Schedulers::threadActiveJob->GetTracer();
	}

	TraceId::TraceId(bool mustLog)
	{
		Audit::Assert(S_OK == CoCreateGuid(reinterpret_cast<GUID*>(&m_id)), "Failed to create new GUID");
		if (mustLog)
		{
			uint32_t* value = reinterpret_cast<uint32_t*>(&m_id);
			value[3] &= ~TRACE_SAMPLE_MASK;
		}
	}

	TraceRec::TraceRec(const wchar_t* name, bool mustlog)
		: m_traceId(mustlog)
		, m_spanId(g_noneZeroRandom.Get())
		, m_parentSpanId(0)
	{
		if (m_traceId.ShouldSample())
		{
			g_pActionTracker->CreateActivityStack(this);
			EventWriteSpanStart(reinterpret_cast<const GUID*>(&(m_traceId)), m_parentSpanId, m_spanId,
				static_cast<uint32_t>(EBCounter::NonCounter), 0, 0, name);
		}
	}

	TraceRec::TraceRec(const wchar_t* name, const TraceRec& parent)
		: m_traceId(parent.m_traceId)
		, m_parentSpanId(parent.m_spanId)
		, m_spanId(g_noneZeroRandom.Get())
	{
		if (m_traceId.ShouldSample())
		{
			g_pActionTracker->CreateActivityStack(this);
			EventWriteSpanStart(reinterpret_cast<const GUID*>(&(m_traceId)), m_parentSpanId, m_spanId,
				static_cast<uint32_t>(EBCounter::NonCounter), 0, 0, name);
		}
	}

	TraceRec::TraceRec(uint64_t low, uint64_t high, uint32_t parentSpan, uint32_t spanId)
		: m_traceId(low, high)
		, m_parentSpanId(parentSpan)
		, m_spanId(spanId == 0 ? g_noneZeroRandom.Get() : spanId)
	{
	}

	bool TraceRec::ShouldSample() const
	{
		return  m_traceId.ShouldSample();
	}

	void InitializeLogging(const wchar_t * logFilePath)
	{
		if (logFilePath == NULL)
		{
			logFilePath = DEFAULT_LOG_PATH;
		}

		auto status = StatusCode::OK;
		uint32_t BufferSize = 0;

		// Allocate memory for the session properties. The memory must
		// be large enough to include the log file name and session name,
		// which get appended to the end of the session properties structure.

		uint32_t logPathSize = (uint32_t)wcslen(logFilePath);
		Audit::Assert(logPathSize > 0, "Invalide log file path!");
		logPathSize = sizeof(wchar_t)*(logPathSize + 1);

		BufferSize = sizeof(EVENT_TRACE_PROPERTIES)+logPathSize + sizeof(SESSION_NAME);
		g_etwControl.m_pSessionProperties = (EVENT_TRACE_PROPERTIES*)malloc(BufferSize);

		Audit::Assert(NULL != g_etwControl.m_pSessionProperties, "Unable to allocate memory for ETW tracing session!");

		// Set the session properties. You only append the log file name
		// to the properties structure; the StartTrace function appends
		// the session name for you.

		ZeroMemory(g_etwControl.m_pSessionProperties, BufferSize);
		g_etwControl.m_pSessionProperties->Wnode.BufferSize = BufferSize;
		g_etwControl.m_pSessionProperties->Wnode.Flags = WNODE_FLAG_TRACED_GUID;
		g_etwControl.m_pSessionProperties->Wnode.ClientContext = 1; //QPC clock resolution
		g_etwControl.m_pSessionProperties->Wnode.Guid = SessionGuid;
		g_etwControl.m_pSessionProperties->LogFileMode = EVENT_TRACE_REAL_TIME_MODE;
		g_etwControl.m_pSessionProperties->MaximumFileSize = 100;  // 100 MB
		g_etwControl.m_pSessionProperties->LoggerNameOffset = sizeof(EVENT_TRACE_PROPERTIES);
		g_etwControl.m_pSessionProperties->LogFileNameOffset = sizeof(EVENT_TRACE_PROPERTIES)+sizeof(SESSION_NAME);
		memcpy((void *)((char*)g_etwControl.m_pSessionProperties + g_etwControl.m_pSessionProperties->LogFileNameOffset), logFilePath, logPathSize);


		// Create the trace session.

		status = (StatusCode)StartTrace((PTRACEHANDLE)&g_etwControl.m_sessionHandle, SESSION_NAME, g_etwControl.m_pSessionProperties);
		if (StatusCode::OK != status && StatusCode::FileExists != status)
		{
			DisposeLogging();
			Audit::OutOfLine::Fail((StatusCode)status, "Failed to start ETW Tracing!");
		}

		// Enable the providers that you want to log events to your session.

		if (status == StatusCode::OK)
		{
			status = (StatusCode)EnableTraceEx2(
				g_etwControl.m_sessionHandle,
				&MicrosoftBingExabytes,
				EVENT_CONTROL_CODE_ENABLE_PROVIDER,
				TRACE_LEVEL_INFORMATION,
				0,
				0,
				0,
				NULL
				);
			if (StatusCode::OK != status)
			{
				DisposeLogging();
				Audit::OutOfLine::Fail((StatusCode)status, "Failed to enable ETW tracing!");
			}
		}

		status = (StatusCode)EventRegisterMicrosoft_Bing_Exabytes();
		if (StatusCode::OK != status)
		{
			DisposeLogging();
			Audit::OutOfLine::Fail((StatusCode)status, "Failed to register as ETW provider!");
		}
		g_etwControl.m_traceOn = true;
	}

	void DisposeLogging()
	{
		//       if (g_etwControl.m_traceOn)
		{
			EventUnregisterMicrosoft_Bing_Exabytes(); // ignore errors
			g_etwControl.m_traceOn = false;
		}

		//        if (g_etwControl.m_sessionHandle)
		{
		EnableTraceEx2(
			g_etwControl.m_sessionHandle,
			&MicrosoftBingExabytes,
			EVENT_CONTROL_CODE_DISABLE_PROVIDER,
			TRACE_LEVEL_INFORMATION,
			0,
			0,
			0,
			NULL
			);

		ControlTrace(g_etwControl.m_sessionHandle, SESSION_NAME, g_etwControl.m_pSessionProperties, EVENT_TRACE_CONTROL_STOP);
		g_etwControl.m_sessionHandle = 0;
	}

		//        if (g_etwControl.m_pSessionProperties)
		{
			free(g_etwControl.m_pSessionProperties);
			g_etwControl.m_pSessionProperties = NULL;
		}
	}


	const TraceRec* ChooseDefaultIfNULL(const TraceRec* span)
	{
		if (span == NULL)
		{
			span = GetCurrentTraceHeader();
		}
		return span;
	}

	const TraceRec* ChooseDefaultEmptyIfNULL(const TraceRec* span)
	{
		span = ChooseDefaultIfNULL(span);
		if (span == NULL)
		{
			span = &g_emptySpan;
		}
		return span;
	}

	void EndSpan(const TraceRec* span)
	{
		span = ChooseDefaultIfNULL(span);
		Audit::Assert(span != NULL, "Can not locate tracing header!");
		if (span->ShouldSample())
		{
			g_pActionTracker->DeleteActivityStack(span);
			EventWriteSpanEnd(reinterpret_cast<const GUID*>(&(span->m_traceId)), span->m_parentSpanId, span->m_spanId,
				static_cast<uint32_t>(EBCounter::NonCounter), 0, 0, L"");
		}
	}

	void RPCClientSend(const TraceRec* span)
	{
		span = ChooseDefaultIfNULL(span);
		Audit::Assert(span != NULL, "Can not locate tracing header!");
		if (span->ShouldSample())
		{
			EventWriteClientSend(reinterpret_cast<const GUID*>(&(span->m_traceId)), span->m_parentSpanId, span->m_spanId,
				static_cast<uint32_t>(EBCounter::NonCounter), 0, 0, L"");
		}
	}

	void RPCClientReceive(const TraceRec* span)
	{
		span = ChooseDefaultIfNULL(span);
		Audit::Assert(span != NULL, "Can not locate tracing header!");
		if (span->ShouldSample())
		{
			EventWriteClientReceive(reinterpret_cast<const GUID*>(&(span->m_traceId)), span->m_parentSpanId, span->m_spanId,
				static_cast<uint32_t>(EBCounter::NonCounter), 0, 0, L"");
		}
	}

	void RPCClientTimeout(const TraceRec* span)
	{
		span = ChooseDefaultIfNULL(span);
		Audit::Assert(span != NULL, "Can not locate tracing header!");
		if (span->ShouldSample())
			EventWriteClientTimeout(reinterpret_cast<const GUID*>(&(span->m_traceId)), span->m_parentSpanId, span->m_spanId,
			static_cast<uint32_t>(EBCounter::NonCounter), 0, 0, L"");
	}

	void RPCServerReceive(const TraceRec* span)
	{
		span = ChooseDefaultIfNULL(span);
		Audit::Assert(span != NULL, "Can not locate tracing header!");
		if (span->ShouldSample())
		{
			EventWriteServerReceive(reinterpret_cast<const GUID*>(&(span->m_traceId)), span->m_parentSpanId, span->m_spanId,
				static_cast<uint32_t>(EBCounter::NonCounter), 0, 0, L"");
			g_pActionTracker->CreateActivityStack(span);
		}
	}

	void RPCServerSend(const TraceRec* span)
	{
		span = ChooseDefaultIfNULL(span);
		Audit::Assert(span != NULL, "Can not locate tracing header!");
		if (span->ShouldSample())
			EventWriteServerSend(reinterpret_cast<const GUID*>(&(span->m_traceId)), span->m_parentSpanId, span->m_spanId,
			static_cast<uint32_t>(EBCounter::NonCounter), 0, 0, L"");
	}

	void LogError(StatusCode error, const wchar_t * msg, const TraceRec* span)
	{
		span = ChooseDefaultEmptyIfNULL(span);
		if (!span->ShouldSample())
		{
			return;
		}
		wchar_t* detail = (msg == nullptr) ? L"" : msg;
		EventWriteSpanException(reinterpret_cast<const GUID*>(&(span->m_traceId)), span->m_parentSpanId, span->m_spanId,
			static_cast<uint32_t>(EBCounter::NonCounter), (int)error, 0, detail);
	}

	void LogWarning(StatusCode error, const wchar_t * msg, const TraceRec* span)
	{
		span = ChooseDefaultEmptyIfNULL(span);
		if (!span->ShouldSample())
		{
			return;
		}
		EventWriteSpanWarning(reinterpret_cast<const GUID*>(&(span->m_traceId)), span->m_parentSpanId, span->m_spanId,
			static_cast<uint32_t>(EBCounter::NonCounter), (int)error, 0,msg);
	}

	void LogInfo(StatusCode error, const wchar_t * msg, const TraceRec* span)
	{
		span = ChooseDefaultEmptyIfNULL(span);
		if (!span->ShouldSample())
		{
			return;
		}
		EventWriteSpanInfo(reinterpret_cast<const GUID*>(&(span->m_traceId)), span->m_parentSpanId, span->m_spanId,
			static_cast<uint32_t>(EBCounter::NonCounter), (int)error, 0, msg);
	}

	void LogDebug(StatusCode error, const wchar_t * msg, const TraceRec* span)
	{
		span = ChooseDefaultEmptyIfNULL(span);
		if (!span->ShouldSample())
		{
			return;
		}
		EventWriteSpanDebug(reinterpret_cast<const GUID*>(&(span->m_traceId)), span->m_parentSpanId, span->m_spanId,
			static_cast<uint32_t>(EBCounter::NonCounter), (int)error, 0, msg);
	}

	void LogActionStart(EBCounter counter, const wchar_t* counterInstance, const TraceRec* span)
	{
		span = ChooseDefaultEmptyIfNULL(span);
		if (span->ShouldSample())
		{
			LARGE_INTEGER ticksNow;
			Audit::Assert(0 != QueryPerformanceCounter(&ticksNow),
				"QueryPerformanceCounter failed");

			// push new action on the current activity stack to track its start time
			g_pActionTracker->AddNewAction(span, counter, ticksNow.QuadPart);
			EventWriteTimerStart(
				reinterpret_cast<const GUID*>(&(span->m_traceId)),
				span->m_parentSpanId, span->m_spanId, static_cast<uint32_t>(counter),
				0, 0, counterInstance);
		}
	}

	void LogActionEnd(EBCounter counter, StatusCode error, const TraceRec* span)
	{
		span = ChooseDefaultEmptyIfNULL(span);
		if (span->ShouldSample())
		{
			LARGE_INTEGER ticksNow;
			Audit::Assert(0 != QueryPerformanceCounter(&ticksNow),
				"QueryPerformanceCounter failed");

			// calculate the latency for this action
			int64_t actionStartTime = g_pActionTracker->GetActionStartTime(
				span, counter);


			uint64_t operationLatency = actionStartTime == 0 ? 0
				: static_cast<uint64_t>((ticksNow.QuadPart - actionStartTime) / (Schedulers::g_frequency.TicksPerMicrosecond()));

			EventWriteTimerEnd(
				reinterpret_cast<const GUID*>(&(span->m_traceId)),
				span->m_parentSpanId, span->m_spanId, static_cast<uint32_t>(counter),
				(int)error, operationLatency, L"");
		}
	}

	void LogCounterValue(EBCounter counter, const wchar_t* counterInstance, uint64_t value, const TraceRec* span)
	{
		span = ChooseDefaultEmptyIfNULL(span);
		if (span->ShouldSample())
		{
			EventWriteCounterValue(
				reinterpret_cast<const GUID*>(&(span->m_traceId)),
				span->m_parentSpanId, span->m_spanId,
				static_cast<uint32_t>(counter), 0,
				value, counterInstance);
		}
	}
}
