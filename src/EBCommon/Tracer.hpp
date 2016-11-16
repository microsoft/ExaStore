// A bootstrap for Google Dapper style trace collection.
#pragma once

#include "stdafx.h"

#include <memory>

#include "Utilities.hpp"
#include "Counters.hpp"

namespace Tracer
{
    using namespace Utilities;
    
    /* Tracer  definitions,
    Bootstrap for a Dapper style distributed tracing infrastructure
    */

    // Trace Id, most Bing services use GUID
    // Maybe should reuse Key128. different in reserved values only.
    //
    struct TraceId
    {
        uint64_t m_id[2];

        // Default constructor generata a random GUID
        //
        TraceId(bool mustLog = false);

        TraceId(uint64_t low, uint64_t high)
        {
            m_id[0] = low;
            m_id[1] = high;
        }

        // a naive sampling aproach, using bitwise and on traceId's highest 4 bytes
        // Currently set for sampling everything.
        // should set to // 0x3FF (1/1024) for production
        static const uint32_t TRACE_SAMPLE_MASK = 0x00000000UL;

        bool ShouldSample() const
        {
            const uint32_t * const value = reinterpret_cast<const uint32_t * const>(&m_id);
            return (TRACE_SAMPLE_MASK & value[3]) == 0;
        }

        bool IsEmpty() const
        {
            return m_id[0] == 0 && m_id[1] == 0;
        }
    };


    // A Dapper style trace record aiming to trace a single request across multiple machines.
    // In Bing, default Trace ID is a GUID, basically an 128 bit key
    // Trace ID is a random 32 bit number, 0 is invalid
    // The record is immutable
    //
    // Usually one span corresponds to one RPC call. Next step is to enable finer grain tracing,
    // where programmers can start and end span like a function call. The challenge is that trace
    // records has to be organized as stacks if we want to support that.
    //
    struct TraceRec
    {
        TraceId     m_traceId;
        uint32_t    m_spanId;
        uint32_t    m_parentSpanId;

        // Create a root span with automatically generated trace id and span id.
        // Span name, trace/span id, creation time will be write to log if trace is sampled.
        //
        TraceRec(const wchar_t* name, bool mustlog = false);

        // Create a sub span for the span specified in the parameter <code>parent</code>
        // Span name, trace/span id, creation time will be write to log if trace is sampled.
        // 
        TraceRec(const wchar_t* name, const TraceRec& parent);

        // Create trace record from raw data. Possible usage
        // includes our ServiceBroker implementation. Network request should carry trace Id
        // and span Id. ServiceBroker should construct TraceRec from these IDs and set it
        // in the Activity instances processing these requests.
        //
        TraceRec(uint64_t low, uint64_t high, uint32_t parentSpan, uint32_t spanId);

        bool ShouldSample() const;

        bool operator==(const TraceRec& y) const {
            return (m_traceId.m_id[0] == y.m_traceId.m_id[0]
                && m_traceId.m_id[1] == y.m_traceId.m_id[1]
                && m_parentSpanId == y.m_parentSpanId
                && m_spanId == y.m_spanId);
        }

        bool operator!=(const TraceRec& y) const {
            return !(operator==(y));
        }
    };

    // empty span for all jobs that does not have a trace ID. random spanID will be ignored.
    const struct Tracer::TraceRec g_emptySpan(0, 0, 0, 0);

    // Sample based Logger module using ETW. 

    // TODO Add sampling

    // Initialize Logging module by providing path to log file,
    // NOT THREAD SAFE, intented to be called at beginning of application.
    //
    void InitializeLogging(const wchar_t * logFilePath = nullptr);

    // Properly release logging related resources,
    //
    void DisposeLogging();

    // Mark the end of a span in the log if sampled. Caller should reclaim the memory for span.
    //
    void EndSpan(const TraceRec* span = nullptr);

    // Mark the event when client send an RPC request
    void RPCClientSend(const TraceRec* span = nullptr);

    // Mark the event when client received ack of an RPC
    void RPCClientReceive(const TraceRec* span = nullptr);

    // Mark the event when RPC timedout detected in client
    void RPCClientTimeout(const TraceRec* span = nullptr);

    // Mark the event when server received an RPC request
    void RPCServerReceive(const TraceRec* span = nullptr);

    // Mark the event when server finished processing an RPC and sending result back
    // Caller should reclaim memory used by span
    //
    void RPCServerSend(const TraceRec* span = NULL);

    // Logging function, parameter span can be omitted to use default value 
    void LogError(StatusCode error, const wchar_t * msg, const TraceRec* span = nullptr);

    void LogWarning(StatusCode error, const wchar_t * msg, const TraceRec* span = nullptr);

    void LogInfo(StatusCode error, const wchar_t * msg, const TraceRec* span = nullptr);

    void LogDebug(StatusCode error, const wchar_t * msg, const TraceRec* span = nullptr);


    // Logging functions, meant for perf counters
    // Calling of the following functions will be logged as events.
    // A listener of the events will processes these events and translate
    // them into either Autopilot perf counter or windows perf counters.
    //
    // A timer start/end will be matched by counter ID and span, once
    // matched, counter instance can be retrieved from the start event
    // Events and measurements using these functions are defined in Counters.xml

    void LogActionStart(EBCounter counter, const wchar_t* counterInstance, const TraceRec* span = nullptr);

    void LogActionEnd(EBCounter counter, StatusCode error, const TraceRec* span = nullptr);

    void LogCounterValue(EBCounter counter, const wchar_t* counterInstance, uint64_t value, const TraceRec* span = nullptr);

}


namespace std
{

    template<>
    struct hash<Tracer::TraceRec>
    {
        inline size_t operator()(const Tracer::TraceRec& value) const
        {
            static_assert(sizeof(value.m_parentSpanId) == 4, 
                "spand id is assumed to be 32 bit");
            size_t res = ((size_t)value.m_parentSpanId << 32) ^ (size_t)value.m_spanId;
            return res ^ value.m_traceId.m_id[0] ^ value.m_traceId.m_id[1];
        }
    };

    template<>
    struct equal_to<Tracer::TraceRec>
    {
        inline bool operator()(const Tracer::TraceRec& x, const Tracer::TraceRec& y) const 
        {
            return x == y;
        }
    };

}
