// NetBursts.cpp : Defines the entry point for the console application.
//

#pragma once
#include "stdafx.h"

#include <process.h>
#include <stack>
#include <time.h>
#include <unordered_set>

#include "StatusWin.hpp"
#include "SchedulerIOCP.hpp"
#include "UtilitiesWin.hpp"

using namespace std;

namespace    // private to this source file
{
    // Making our time stamps starting from Dec 01, 2015 to fit in
    // 32bit
    static const time_t START_TIME = 1448956800; 

    template<typename T, typename Container = std::vector<T> >
    class iterable_queue : public std::priority_queue<T, Container>
    {
    public:
        typedef typename Container::iterator iterator;
        typedef typename Container::const_iterator const_iterator;

        iterator begin() { return this->c.begin(); }
        iterator end() { return this->c.end(); }
        const_iterator begin() const { return this->c.begin(); }
        const_iterator end() const { return this->c.end(); }
    };


    struct IOCPrequest
    {
        int64_t         m_ticksDue;
        uint32_t        m_numberOfBytesTransferred;
        Schedulers::CompletionKey   m_completionKey;
        Schedulers::POverlapped     m_pOverlapped;

        IOCPrequest()
        {
            m_ticksDue = 0;
            m_numberOfBytesTransferred = 0;
            m_completionKey = 0;
            m_pOverlapped = 0;
        }

        IOCPrequest(
            int64_t            ticksDue,
            uint32_t           numberOfBytesTransferred,
            _In_ Schedulers::CompletionKey completionKey,
            _In_ Schedulers::POverlapped   pOverlapped
            )
        {
            m_ticksDue = ticksDue;
            m_numberOfBytesTransferred = numberOfBytesTransferred;
            m_completionKey = completionKey;
            m_pOverlapped = pOverlapped;
        }
    };

    // Queue entry in an activity. Structually the same with IOCP request
    // Define for a little compiler type checking
    // 
    struct ContinuationRequest
    {
        int64_t                         m_ticksDue;
        uint32_t                        m_dataSize;
        Schedulers::ContinuationBase*   m_pContinuation;
        void*                           m_pData;

        ContinuationRequest()
        {
            m_ticksDue = 0;
            m_dataSize = 0;
            m_pContinuation = 0;
            m_pData = 0;
        }

        ContinuationRequest(
            _In_ int64_t            ticksDue,
            _In_ uint32_t           size,
            _In_ Schedulers::ContinuationBase* pContinuation,
            _In_ void*   pData
            )
        {
            m_ticksDue = ticksDue;
            m_dataSize = size;
            m_pContinuation = pContinuation;
            m_pData = pData;
        }
    };

}

// provide specializations of std:: functors
namespace std
{
    // for std:: hash trait
    template<>
    struct less < IOCPrequest >
    {
        inline bool operator()(const IOCPrequest& left, const IOCPrequest& right) const
        {
            // We want <code>top</code> to return the earilest request, so it is reverse.
            return left.m_ticksDue > right.m_ticksDue;
        }
    };

    template<>
    struct less < ContinuationRequest >
    {
        inline bool operator()(const ContinuationRequest& left, const ContinuationRequest& right) const
        {
            // We want <code>top</code> to return the earilest request, so it is reverse.
            return left.m_ticksDue > right.m_ticksDue;
        }
    };
}

namespace Schedulers
{
    using namespace Utilities;

    MachineFrequency::MachineFrequency()
    {
        LARGE_INTEGER frequency;
        Audit::Assert(0 != QueryPerformanceFrequency(&frequency), "QueryPerformanceFrequency failed");
        m_ticksPerMicrosecond = frequency.QuadPart / 1.0e6;
        m_ticksPerMillisecond = frequency.QuadPart / 1.0e3;
        m_ticksPerSecond = (double) frequency.QuadPart;
    }

    class MachineFrequency g_frequency;

    class ContinuationPriorityQueueHandler : public ContinuationHandler
    {
    private:
        friend class SchedulerIOCP;

        // No assignment
        ContinuationPriorityQueueHandler& operator=(const ContinuationPriorityQueueHandler&) = delete;
        ContinuationPriorityQueueHandler(const ContinuationPriorityQueueHandler&) = delete;

    public:
        ContinuationPriorityQueueHandler(Scheduler& scheduler, ActionArena& arena, const Tracer::TraceRec& tracer)
            : m_pScheduler(&scheduler)
            , m_tracer(tracer)
        {
            m_pArena = &arena;
            InitPrivates();
        }

        ContinuationPriorityQueueHandler(Scheduler& scheduler, ActionArena& arena, const wchar_t* traceName, bool mustLog = false)
            : m_pScheduler(&scheduler)
            , m_tracer(traceName, mustLog)
        {
            m_pArena = &arena;
            InitPrivates();
        }

        ContinuationPriorityQueueHandler(Scheduler& scheduler, ActionArena& arena, const wchar_t* traceName, const Tracer::TraceRec& parent)
            : m_pScheduler(&scheduler)
            , m_tracer(traceName, parent)
        {
            m_pArena = &arena;
            InitPrivates();
        }

        const Tracer::TraceRec* GetTracer() const override
        {
            return &m_tracer;
        }

        Scheduler* GetScheduler() const override
        {
            return m_pScheduler;
        }

        // Schedule a continuation to run under this activity
        // Although rare, it is possible that multiple party may call this simultangously.
        //
        void Post(
            _In_ ContinuationBase& continuation,
            _In_ intptr_t valueHandle,
            _In_ uint32_t valueLength,
            _In_ uint32_t microsecondDelay = 0
            ) override
        {
            Audit::Assert(valueLength < 1 * SI::Gi, "data length too big in continuation.");
            Audit::Assert(m_continue,
                "Can not post to dying activity!");
            auto pScheduler = m_pScheduler;

            LARGE_INTEGER ticksNow;
            Audit::Assert(0 != QueryPerformanceCounter(&ticksNow), "QueryPerformanceCounter failed");
            int64_t whenDue = ticksNow.QuadPart + (int64_t)(microsecondDelay * g_frequency.TicksPerMicrosecond());
            bool isIdle = false;
            bool dueEarlier = false;
            bool enque = false;
            ContinuationRequest request{ whenDue, valueLength, &continuation, (void *)valueHandle };
            {
                Utilities::Exclude<SRWLOCK> guard{ m_srwQueueLock };
                if (m_continue){
                    bool queEmpty = m_queue.empty();
                    isIdle = queEmpty && m_status == ActivityStatus::Stopped;
                    if (!queEmpty)
                    {
                        auto oldhead = m_queue.top();
                        dueEarlier = (oldhead.m_ticksDue - request.m_ticksDue) > g_frequency.TicksPerMicrosecond();
                    }
                    m_queue.push(request);
                    enque = true;
                }
            }

            if (!enque)
            {
                Audit::OutOfLine::Fail(StatusCode::Unexpected,
                    "Activity dead while posting!");
                return;
            }

            if (isIdle)
            {
                // "What if somebody request shut down now?" 
                // It should not matter. CleanUp can only be called in a Run
                // we are NOT overlapping with Run here cause status is Stopped.
                // if shutdown requested during idle, we should wake up the activity
                // to clean up.

                // there is a possibility in Run, after it set the status, and release the lock
                // but before fliping m_inUtilFunctions, we got the lock and start working.
                // this should be benign, just to make sure my guess is right.
                Audit::Assert(m_waiting, "this should not overlap with run");

                if (m_status != ActivityStatus::Stopped)
                {
                    char buf[128];
                    sprintf_s(buf, "unexpected continuation status change, should be Stopped, found %d", m_status);
                    Audit::OutOfLine::Fail(StatusCode::Unexpected, buf);
                }
                Audit::Assert(!m_inScheduler, "An activity should not be double posted to a scheduler");
                m_inScheduler = true;
                m_status = ActivityStatus::Waiting;
                m_pScheduler->Post(this, 0, 0, NextDelayMicrosec());
            }
            else if (dueEarlier && m_continue)
            {
                // need to tell the scheduler we have a closer due time
                pScheduler->Reschedule(this, 0, 0);

                // would this overlap with Run? maybe. but in that case
                // current activity can not be in the scheduler queue
                // thus this call would be a no-op. 

                // If a Run finished before the Reschedule call and we are
                // shutting down. Then the Reschedule will promote the system
                // to run this activity earlier, which should be fine too.

                // If by this time we have completely shutdown, then this
                // activity can not be in the scheduler's queue. again an
                // no-op
            }
            // What if queue is empty but activity is still Running/Waiting -> isIdle == false?
            // this should only happen in Run function when the last continuation is runing. 
            // after the continuation finishes, the Run function will check the queue, which is not
            // empty since we just put one item in, so it will post the activity back, thus we don't
            // do it here.
        }

        // Should be called by one of the continuations to signal the end of process.
        //
        // This method should cause the activity to be removed from the scheduler,
        // all the continuations waiting will be abandoned. No effect on currently
        // running continuation though.
        //
        void RequestShutdown(
            _In_ StatusCode  reason,
            _In_ const wchar_t* msg = NULL
            ) override
        {
            if (!m_continue)
                return;

            if (msg != NULL)
            {
                Tracer::LogInfo(reason, msg, &m_tracer);
            }
            else {
                Tracer::LogInfo(reason, L"Request Shutdown.", &m_tracer);
            }

            m_continue = false;
        }

        bool IsShuttingDown() override{
            return !m_continue;
        }
        // Methods only called by the scheduler below:

        // Called by the scheduler when this activity's turn to run
        // The activity is posted back to thread pool after this method is finished.
        // Implementation of this method should include picking up a continuation to run.
        //
        // Should run sequentially, thus no lock required
        //
        virtual void Run(_In_ WorkerThread&  thread, _In_ intptr_t, _In_ uint32_t) override
        {
            Audit::Assert(m_inScheduler, "Run can only be invoked when we are in scheduler!");
            m_inScheduler = false; // we are no longer in scheduler queue any more.

            if (m_status == ActivityStatus::Shuttingdown){
                CleanUp();
                return;
            }

            Audit::Assert(m_waiting, "Overlapping call to activity functions!");
            if (!m_continue)
            {
                // shutdown requested, ignore pending jobs,
                // sleep for half ms and die
                m_status = ActivityStatus::Shuttingdown;
                m_inScheduler = true;     m_waiting = true;
                m_pScheduler->Post(this, 0, 0, 500);
                return;
            }

            // we need to remember to flip this before EVERY SINGLE exist
            // auto flip does not work well here
            m_waiting = false;
            Audit::Assert(m_status == ActivityStatus::Waiting, "Invalid Activity Status!");

            for (;;) // for all ready continuations
            {
                // Get a ready Continuation request 
                LARGE_INTEGER ticksNow;
                Audit::Assert(0 != QueryPerformanceCounter(&ticksNow), "QueryPerformanceCounter failed");

                bool hasReadyRequest = false;
                ContinuationRequest request;
                {
                    Utilities::Exclude<SRWLOCK> guard{ m_srwQueueLock };
                    if (m_queue.empty())
                    {
                        // Nobody's waiting, get out of the scheduler
                        // New incoming continuation will post this back in
                        m_status = ActivityStatus::Stopped;
                        m_waiting = true;
                        return;
                    }
                    request = m_queue.top();
                    if (request.m_ticksDue <= ticksNow.QuadPart)
                    {         
                        m_queue.pop();
                        hasReadyRequest = true;
                    }
                }
                if (!hasReadyRequest)
                    break; // no more ready continuations

                // Found a ready continuation, run it
                m_status = ActivityStatus::Running;
//                try {
                    request.m_pContinuation->OnReady(thread, (intptr_t)(request.m_pData), request.m_dataSize);
//                }
//                catch (errno_t err){
                    // Exception handling,
//                    request.m_pContinuation->Fail(err);
//                }
//                catch (const std::exception &ex){
                    // Exception handling,
//                    request.m_pContinuation->Fail(GetLastError(), ex.what());
//                }
                if (m_status != ActivityStatus::Running)
                {
                    char buf[128];
                    sprintf_s(buf, "Unexpected activity status change, should be Running, found %d", m_status);
                    Audit::OutOfLine::Fail(StatusCode::Unexpected, buf);
                }
                m_status = ActivityStatus::Waiting;

                if (!m_continue)
                {
                    // shutdown requested, linger for 1ms, comeback and die
                    m_status = ActivityStatus::Shuttingdown;
                    m_inScheduler = true;     m_waiting = true;
                    m_pScheduler->Post(this, 0, 0, 1000);
                    return;
                }
            }
            // m_queue can not be empty at this point, Run is the only
            // method that dequeue
            Audit::Assert(!m_queue.empty() && m_status==ActivityStatus::Waiting, "concurrent deque in activity?");
            Audit::Assert(!m_inScheduler, "An activity already posted, this should not happen!");

            m_inScheduler = true;     m_waiting = true;
            m_pScheduler->Post(this, 0, 0, NextDelayMicrosec());
        }

        // Register a recyclable resource, so that it can be automatically recycled
        // when shutdown
        //
        void RegisterDisposable(Utilities::Disposable* pRes) override
        {
            if (!m_continue){
                pRes->Dispose();
            }
            else{
                Utilities::Exclude<SRWLOCK> guard{ m_srwResourceLock };
                m_resources.push(pRes);
            }
        }

    private:

        // the lock is to allow "Schedule" to be called by others.
        priority_queue<ContinuationRequest>  m_queue;
        SRWLOCK                      m_srwQueueLock;

        Scheduler*                   m_pScheduler;
        const Tracer::TraceRec       m_tracer;

        volatile ActivityStatus m_status = ActivityStatus::Stopped;

        stack<Utilities::Disposable*> m_resources;
        SRWLOCK                      m_srwResourceLock;

        // one activity can only be in the scheduler que
        // once, use this flag to detect violation
        volatile bool                m_inScheduler = false;

        volatile bool                m_continue = true;

        // Two instances of Run must not run in parallel! use the flag to detect violation
        volatile bool                m_waiting = true;

        // Called by the constructors to init private members
        void InitPrivates()
        {
            InitializeSRWLock(&m_srwQueueLock);
            InitializeSRWLock(&m_srwResourceLock);
        }

        // Get next latest due time, should only be called by Run, to post this activity back
        // to scheduler.
        //
        // TODO!!! CODE CLONE, same code with SchedulerIOCP::NextTimeoutMillisec with slight change.
        // Need to find a semantic meaningful way to share code. Suggestions?
        //
        uint32_t NextDelayMicrosec()
        {
            int64_t firstPost = 0;
            {
                Utilities::Share<SRWLOCK> guard{ m_srwQueueLock };
                if (m_queue.empty())
                {
                    return 100000; // default 100 ms delay
                }
                firstPost = m_queue.top().m_ticksDue;
            }
            LARGE_INTEGER ticksNow;
            Audit::Assert(0 != QueryPerformanceCounter(&ticksNow), "QueryPerformanceCounter failed");
            double whenDue = ceil((firstPost - ticksNow.QuadPart) / g_frequency.TicksPerMicrosecond());
            return min((uint32_t)max(0.0, whenDue), (uint32_t)100000); // can only allow [0-100]ms
        }

        void CleanUp()
        {
            Audit::Assert(!m_continue && m_status == ActivityStatus::Shuttingdown,
                "Why are we shutting down without being requested?");

            {
                Utilities::Exclude<SRWLOCK> guard{ m_srwQueueLock };
                while (!m_queue.empty())
                {
                    auto request = m_queue.top();
                    m_queue.pop();
                    request.m_pContinuation->Cleanup();
                }
            }

            {
                Utilities::Exclude<SRWLOCK> guard{ m_srwResourceLock };
                while (!m_resources.empty())
                {
                    Utilities::Disposable* pRes = m_resources.top();
                    m_resources.pop();
                    pRes->Dispose();
                }
            }

            if ( !m_tracer.m_traceId.IsEmpty())
            {
                Tracer::EndSpan(&m_tracer);
            }
            auto arena = m_pArena;
            this->~ContinuationPriorityQueueHandler();

            arena->Retire(); // release all memory, including itself.
        }
    };

    Activity* ActivityFactory(Scheduler& scheduler, const Tracer::TraceRec& tracer)
    {
		ActionArena* pArena = scheduler.GetNewArena();
        ContinuationHandler* pHandler = pArena->allocate<ContinuationPriorityQueueHandler>(scheduler, *pArena, tracer);
        return pArena->allocate<Activity>(*pHandler);
    };

    Activity* ActivityFactory(Scheduler& scheduler, const wchar_t* traceName, bool mustLog)
    {
		ActionArena* pArena = scheduler.GetNewArena();
        ContinuationHandler* pHandler = pArena->allocate<ContinuationPriorityQueueHandler>(scheduler, *pArena, traceName, mustLog);
        return pArena->allocate<Activity>(*pHandler);
    };

    Activity* ActivityFactory(Scheduler& scheduler, const wchar_t* traceName, const Tracer::TraceRec& parent)
    {
		ActionArena* pArena = scheduler.GetNewArena();
        ContinuationHandler* pHandler = pArena->allocate<ContinuationPriorityQueueHandler>(scheduler, *pArena, traceName, parent);
        return pArena->allocate<Activity>(*pHandler);
    };

    // Keep track of current Activity, for use by tracer module
    __declspec(thread) const Schedulers::Work* threadActiveJob = nullptr;

    class ThreadIOCP : public WorkerThread
    {
    public:

        Scheduler* GetScheduler() { return reinterpret_cast<Scheduler*>(m_pScheduler); }

    private:
        friend class SchedulerIOCP;

        SchedulerIOCP*  m_pScheduler;       // the SchedulerIOCP which created this thread
        IOCPhandle      m_hIOCP;
        ThreadHandle    m_threadHandle;     // the OS handle for this thread
        const char*     m_faultReason;      // set only during a failure shutDown

        /* A thread dedicated to running work driven by Scheduler
        */
        ThreadIOCP(_In_ SchedulerIOCP& scheduler, IOCPhandle hIOCP)
        {
            m_pScheduler = &scheduler;
            m_faultReason = nullptr;
            m_hIOCP = hIOCP;
        }

    };

    class SchedulerIOCP : public Scheduler
    {
    public:
        // The worker threads run non-blocking tasks off the IOCP.
        // The current thread is not one of the worker threads.
        //
        SchedulerIOCP(
            /* One IO Completion Port will schedule all activity for these threads
            */
            IOCPhandle hIOCP,

            /* These threads will be dedicated to the IO Completions.
            */
            int numThreads,

            /* There are two dedicated completion key values on the IOCP:
            0: shutdown
            1: call the Primary WorkerAction
            Other completion keys may be added by registering WorkerActions.
            */
            _In_ Work* pPrimaryAction
            )
        {
            m_hIOCP = hIOCP;
            m_shutDown = false;
            m_pPrimaryAction = pPrimaryAction;
            m_pArenaSet = ArenaSet::ArenaSetFactory(ARENA_SIZE, 2048);
            InitializeSRWLock(&m_srwQueueLock);
            m_nextScheduled = 0;

            for (int i = 0; i < numThreads; ++i)
            {
                auto pWorker = new ThreadIOCP{ *this, m_hIOCP };

                // do not use CreateThread() directly, it does not initialize the CRT per thread.

                auto result = (ThreadHandle) ::_beginthread(StartWorker, 0, pWorker);
                Audit::Assert(result != -1L, "_beginthreadex");

                pWorker->m_threadHandle = result;
                m_threads.push_back(pWorker);
            }
        }

        ~SchedulerIOCP()
        {
            Cleanup();
        }

        /* Add an action the to completion keys recognized for this Scheduler.
        Returns the CompletionKey value to be used with the IOCP.
        Completions on this file will be routed to action.Run().
        */
        intptr_t RegisterFileWorker(FileHandle additionalFile, _In_ const Work* pAction)
        {
            return RegisterAdditionalIoHandle((HANDLE) additionalFile, pAction);
        }

        /* Add an action the to completion keys recognized for this Scheduler.
        Returns the CompletionKey value to be used with the IOCP.
        Completions on this file will be routed to action.Run().
        */
        intptr_t RegisterSocketWorker(SocketHandle additionalSocket, _In_ const Work* pAction)
        {
            return RegisterAdditionalIoHandle((HANDLE) additionalSocket, pAction);
        }

        IOCPhandle GetIOCP() { return m_hIOCP; }

        ActionArena* GetNewArena() override { return m_pArenaSet->Pop(); }

		ActionArena* GetArena(void* pData) override { return m_pArenaSet->GetActionArena(pData); }

        // Dispatch an item from the IOCP on the basis of the dwCompletionKey
        // The caller passes data already in a Registered Slice.
        // The action is always posted asynch, we never run it synchronously.
        // Non-zero delay is typically used in simulations or for pacing.
        //
        void Post(
            _In_ Work* pActivity,
            _In_ intptr_t valueHandle,
            _In_ uint32_t valueLength,
            _In_ uint32_t microsecondDelay = 0
            ) override
        {
            if (0 == microsecondDelay)
            {
                Audit::Assert(valueLength < 2 * SI::Gi);
                Audit::Assert(FALSE != PostQueuedCompletionStatus(
                    (HANDLE)m_hIOCP,
                    valueLength,
                    (intptr_t)pActivity,
                    (LPOVERLAPPED)valueHandle
                    ),
                    "Post to IOCP was refused");
            }
            else
            {
                LARGE_INTEGER ticksNow;
                Audit::Assert(0 != QueryPerformanceCounter(&ticksNow), "QueryPerformanceCounter failed");
                double whenDue = ticksNow.QuadPart + (microsecondDelay * g_frequency.TicksPerMicrosecond());
                IOCPrequest request{ (int64_t)whenDue, valueLength, (CompletionKey)pActivity, (POverlapped)valueHandle };
                {
                    Utilities::Exclude<SRWLOCK> guard{ m_srwQueueLock };
                    m_posted.push(request);
                }
            }
        }

        // Dispatch a task to the IOCP queue on the basis of the dwCompletionKey
        // The task will be run on or after QueryPerformanceCounter returns value
        // greater than "dueTick"
        //
        void PostWithDueTick(
            _In_ Work* pActivity,
            _In_ intptr_t valueHandle,
            _In_ uint32_t valueLength,
            _In_ int64_t  dueTick
            ) override
        {
            IOCPrequest request{ dueTick, valueLength, (CompletionKey)pActivity, (POverlapped)valueHandle };
            {
                Utilities::Exclude<SRWLOCK> guard{ m_srwQueueLock };
                m_posted.push(request);
            }
        }

        // An activity already scheduled has a much earlier due time.
        // The implementation may choose to ignore the request if the
        // acitivity is not found in the scheduler, or the old time is
        // close enough.
        //
        void Reschedule(
            _In_ Work* pActivity,
            _In_ intptr_t valueHandle,
            _In_ uint32_t valueLength,
            _In_ uint32_t microsecondDelay = 0
            ) override
        {
            bool updated = false;
            {
                Utilities::Exclude<SRWLOCK> guard{ m_srwQueueLock };
                for (IOCPrequest& i : m_posted)
                {
                    if (i.m_completionKey == (CompletionKey)pActivity
                        && i.m_numberOfBytesTransferred < 2 * SI::Gi)
                    {
                        i.m_numberOfBytesTransferred = UINT32_MAX;
                        updated = true;
                        break;
                    }
                }
            }

            if (updated)
            {
                Post(pActivity, valueHandle, valueLength, microsecondDelay);
            }
        }


        void RequestShutdown(
            _In_ StatusCode reason,
            _In_ const wchar_t*           msg = NULL
            ) override
        {
            if (!m_shutDown)
            {
                for (int i = 0; i < m_threads.size(); i++)
                {
                    Post(0, 0, 0);
                }
            }
            m_shutDown = true;
            Tracer::LogError(reason, msg);
        }

        // a very imprecise system time provider, in the 10s of seconds,
        // representing number of seconds from a certain past date.
        uint32_t GetCurTimeStamp() const override
        {
            return (uint32_t)(m_time - START_TIME);
        }

        DisposableBuffer* Allocate(
            uint32_t size,
            uint32_t alignment = 8,
            uint32_t offset = 0) override
        {
            if (size <= m_tPool.BufSizeLimit() && alignment <= m_tPool.BufSizeLimit()) {
                return m_tPool.Allocate(size, alignment, offset);
            }
            else if (size <= m_sPool.BufSizeLimit() && alignment <= m_sPool.BufSizeLimit()) {
                return m_sPool.Allocate(size, alignment, offset);
            }
            else if (size <= m_mPool.BufSizeLimit() && alignment <= m_mPool.BufSizeLimit()) {
                return m_mPool.Allocate(size, alignment, offset);
            }
            else if (size <= m_bPool.BufSizeLimit() && alignment <= m_bPool.BufSizeLimit()) {
                return m_bPool.Allocate(size, alignment, offset);
            }
            else {
                return HeapBuffer::Allocate(size, alignment, offset);
            }
        }

    private:
        friend WorkerThread;

        IOCPhandle                  m_hIOCP;
        std::vector<WorkerThread*>  m_threads;
        Work*                       m_pPrimaryAction;
        unique_ptr<ArenaSet>        m_pArenaSet;
        bool                        m_shutDown;

        time_t                      m_time = 0;
        int64_t                     m_lastTimedTick = 0;

        // guard when requests are added to or removed from the deferal queues.
        SRWLOCK m_srwQueueLock;

        // posted requests are waiting for a future time
        int64_t m_nextScheduled;

        // requests we want to happen at some future time
        iterable_queue<IOCPrequest> m_posted;

        // Small buffer pools to reduce memory churn.
        TinyBufferPool<1024> m_tPool;
        SmallBufferPool<2 * SI::Ki, 1024> m_sPool;
        SmallBufferPool<8 * SI::Ki, 1024> m_mPool;
        SmallBufferPool<32 * SI::Ki, 1024> m_bPool;

        // Add an action the to completion keys recognized for this Scheduler.
        // Returns the CompletionKey value to be used with the IOCP.
        // Completions on this file will be routed to action.Run().
        //
        intptr_t RegisterAdditionalIoHandle(HANDLE additionalIoHandle, _In_ const Work* pAction)
        {
            HANDLE result = ::CreateIoCompletionPort(additionalIoHandle, (HANDLE) m_hIOCP, (intptr_t) pAction, 0);
            if (result != (HANDLE) m_hIOCP)
            {
                Audit::OutOfLine::Fail((StatusCode)::GetLastError(), "Failed to add new file to IOCP! ");
            }
            return (intptr_t) pAction;
        }

        // peek at the scheduled queue and calculate a timeout for waiting
        // thead-safe
        //
        uint32_t NextTimeoutMillisec()
        {
            // move to a work-stealing model in future for efficiency

            // peek at the queue to see what is waiting, and whether we already arranged a timeout for it.

            int64_t firstPost = 0;
            bool reschedule = false;
            {
                Utilities::Share<SRWLOCK> guard{ m_srwQueueLock };
                if (!m_posted.empty())
                {
                    firstPost = m_posted.top().m_ticksDue;
                    reschedule = true;
                }
            }
            if (!reschedule)
            {
                return 100;     // default timeout is 100ms
            }
            LARGE_INTEGER ticksNow;
            Audit::Assert(0 != QueryPerformanceCounter(&ticksNow), "QueryPerformanceCounter failed");
            double whenDue = ceil((firstPost - ticksNow.QuadPart) / g_frequency.TicksPerMillisecond());

            // piggy back for getting system time, every 2G ticks
            if (ticksNow.QuadPart - m_lastTimedTick > (1 << 31))
            {
                m_lastTimedTick = ticksNow.QuadPart;
                time(&m_time);
            }
            return min((uint32_t)max(0.0, whenDue),(uint32_t)100);
        }

        // if something is waiting for schedule, and is now ripe, collect it.
        // thread safe.
        //
        bool CollectWaiting(
            __out uint32_t&      numberOfBytesTransferred,
            __out CompletionKey& completionKey,
            __out POverlapped&   pOverlapped
            )
        {
            // move to a work-stealing model in future for efficiency.  That would mean
            // first looking for work on a thread-local queue, and only if there is none would
            // we look at any other thread's queue.

            LARGE_INTEGER ticksNow;
            Audit::Assert(0 != QueryPerformanceCounter(&ticksNow), "QueryPerformanceCounter failed");

            // If the head of the queue is due to run, remove it.
            bool popped = false;
            IOCPrequest request;
            for(;;){
                Utilities::Exclude<SRWLOCK> guard{ m_srwQueueLock };
                if (!m_posted.empty() && m_posted.top().m_ticksDue <= ticksNow.QuadPart)
                {
                    request = m_posted.top();
                    m_posted.pop();
                    // Ignore badly formed requests.  Sender may time out, not our problem.
                    // This should have asserted in OUR code before being posted.  But, they should
                    // not be asserted on the listener, which should be immune to sender faults.

                    popped = request.m_numberOfBytesTransferred < 2 * SI::Gi;
                    if (popped)
                        break;
                }
                else break;
            }
            if (popped)
            {
                numberOfBytesTransferred = (uint32_t)request.m_numberOfBytesTransferred;
                completionKey = request.m_completionKey;
                pOverlapped = request.m_pOverlapped;
            }
            return popped;
        }

        /* We need a non-member function which Windows can call to vector us to Start().
        */
        static void StartWorker(void *workerInstance)
        {
            threadActiveJob = NULL;

            ThreadIOCP* pThread = reinterpret_cast<ThreadIOCP*>(workerInstance);
            pThread->m_pScheduler->RunThread(*pThread);
        }

        // run a thread in an infinite loop dispatching work from the IOCP, until m_shutDown
        //
        void RunThread(ThreadIOCP& thread)
        {
            // While the thread is running it is driven by a mix of IOCP events and scheduled events.
            // Timeouts on the IOCP are used to provide wakeups needed for scheduled events.

            // There are two kinds of Activities. the <i>Immediate</i> one are posted with zero delay
            // value, which are triggered via IOCP. The <i>Deferred</i> ones, on the other hand, are
            // stored in a queue and polled by this method

            while (!m_shutDown)
            {
                uint32_t msecDelay = NextTimeoutMillisec();      
                uint32_t numberOfBytes;
                CompletionKey completionKey;
                POverlapped pOverlapped;
                if (!GetQueuedCompletionStatus((HANDLE)m_hIOCP, (LPDWORD)&numberOfBytes, (PULONG_PTR)&completionKey, (LPOVERLAPPED*)&pOverlapped, msecDelay))
                {
                    errno_t reason = GetLastError();
                    if (reason != WAIT_TIMEOUT)
                    {
                        thread.m_faultReason = "GetQueuedCompletionStatus";
                        // this return terminates the thread
                        return;
                    }
                }
                else
                {
                    // Received Immediate Activity post via IOCP, now process it.
                    if (!Dispatch(thread, numberOfBytes, completionKey, pOverlapped))
                    {
                        if (thread.m_faultReason == NULL)
                        {
                            thread.m_faultReason = "RunThread Dispatch returned false";
                        }
                        // this return terminates the thread
                        return;
                    }
                }

                // look for due activities in deferred queue
                if (CollectWaiting(numberOfBytes, completionKey, pOverlapped))
                {
                    // We have at least one deferred job waiting. 
                    // put the subsequent activity to the IOCP queue
                    uint32_t number;
                    Schedulers::CompletionKey activity;
                    Schedulers::POverlapped overlapped;
                    while (CollectWaiting(number, activity, overlapped))
                    {
                        Audit::Assert(FALSE != PostQueuedCompletionStatus(
                            (HANDLE)m_hIOCP,
                            number,
                            (intptr_t)activity,
                            (LPOVERLAPPED)overlapped
                            ),
                            "Post to IOCP was refused");
                    }
                    // Now process the first one activity, 
                    if (!Dispatch(thread, numberOfBytes, completionKey, pOverlapped))
                    {
                        if (thread.m_faultReason == NULL)
                        {
                            thread.m_faultReason = "RunThread Dispatch returned false";
                        }
                        // this return terminates the thread
                        return;
                    }
                }

            };
        }

        /* Dispatch an item from the IOCP on the basis of the dwCompletionKey
        Return false when we need to shut down the thread.
        */
        bool Dispatch(
            _In_ WorkerThread& thread,
            uint32_t           numberOfBytesTransferred,
            _In_ CompletionKey completionKey,
            _In_ POverlapped   pOverlapped
            )
        {
            Work * pWork = nullptr;
            if (completionKey == 0)
            {
                m_shutDown = true;
            }
            else if (completionKey == 1)
            {
                pWork = m_pPrimaryAction;
            }
            else
            {
                pWork = reinterpret_cast<Work*>(completionKey);
            }
            if (pWork != nullptr && !m_shutDown)
            {
                // Set trace record to current thread
                threadActiveJob = pWork;
                pWork->Run(thread, pOverlapped, numberOfBytesTransferred);
                threadActiveJob = NULL;
            }
            return !m_shutDown;
        }

        void Cleanup()
        {
            if (!m_shutDown)
                RequestShutdown(StatusCode::OK, L"final clean up");
            m_threads.clear();
        }

        void ErrorExit(const char* reason)
        {
            Cleanup();
            throw std::runtime_error{ reason };
        }
    };

    // The worker threads run non-blocking tasks off the IOCP
    //
    unique_ptr<Scheduler> SchedulerIOCPFactory(
        /* One IO Completion Port will schedule all activity for these threads
        */
        IOCPhandle hIOCP,

        /* These threads will be dedicated to the IO Completions.
        */
        int numThreads,

        /* There are two dedicated completion key values on the IOCP:
        0: shutdown
        1: call the Primary WorkerAction
        Other completion keys may be added by registering WorkerActions.
        */
        _In_ Work* pPrimaryAction
        )
    {
        return make_unique<SchedulerIOCP>(hIOCP, numThreads, pPrimaryAction);
    }
}
