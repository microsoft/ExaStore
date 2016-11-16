// NetBursts.cpp : Defines the entry point for the console application.
//

#pragma once

#include "stdafx.h"

#include <memory>
#include <queue>

#include "StatusWin.hpp"
#include "Datagram.hpp"
#include "Utilities.hpp"
#include "Tracer.hpp"

namespace Schedulers
{
    using namespace Utilities;

    /* Avoid any inclusion of Windows headers in our *.hpp
    They cause complexity, difficulties in reconciling versions,
    and are generally archaic in style.  This project is aiming
    for a more modern C++ style and standard data types.

    We therefore wrap some unavoidable Windows types.
    */
    typedef intptr_t ThreadHandle;
    typedef intptr_t FileHandle;
    typedef intptr_t SocketHandle;
    typedef intptr_t POverlapped;
    typedef intptr_t CompletionKey;

    class Scheduler;

    class WorkerThread
    {
    public:
        virtual Scheduler* GetScheduler() = 0;
    };

    // Arenas are not thread-safe.  They may be passed between threads
    //  but only one thread may use an arena at any one time.
    // m_sizeLimit: the total size of the Arena
	// m_offset: the index of the first avaliable byte
	// m_disposableOffset: the index of the first disposable object registered; the end of index of avaliable byte;
	//   registered disposable objects index are located from m_disposableOffset to m_sizeLimit.
	class ActionArena
	{
		friend class ArenaSet;

		ArenaSet* const  m_pSet;
		void * const     m_arenaBytes;
		uint32_t         m_offset;
		uint32_t         m_disposableOffset;
		const uint32_t   m_sizeLimit;

		ActionArena& operator=(const ActionArena& rhs) = delete;
	public:
		ActionArena(ArenaSet& set, void* bytes, uint32_t sizeLimit);

		void* allocateBytes(size_t byteCount)
		{
			uint32_t size = (byteCount + 7) & ~7;
			Audit::Assert(size <= (m_disposableOffset - m_offset), "Arena over capacity!");
			auto placement = (void*)(m_offset + (char*)m_arenaBytes);
			m_offset += size;
			return placement;
		}

		// Judge whether the given pointer points to a position inside the Arena.
		//
		bool IsInArena(void* p)
		{
			return p >= m_arenaBytes && p < (char*)m_arenaBytes + m_sizeLimit;
		}

		bool IsEmpty() { return 0 == m_offset; }

		uint32_t RemainingSpace() { return m_disposableOffset - m_offset; }

		template <typename T, typename... _Types>
		T* allocate(_Types&&... _args)
		{
			auto placement = allocateBytes(sizeof(T));
			return new (placement)T(std::forward<_Types>(_args)...);
		}

		// convert to a handle compatible with an IOCP request
		template <typename T>
		CompletionKey Hibernate(T& request)
		{
			return (CompletionKey)(void*)*request;
		}

		void RegisterDisposable(Utilities::Disposable& pDisposable)
		{
			auto pDisSz = static_cast<uint32_t>(sizeof(Utilities::Disposable*));
			Audit::Assert(pDisSz <= (m_disposableOffset - m_offset), "Arena over capacity!");
			Audit::Assert(IsInArena((void*)&pDisposable), "only can register pDisposable object allocated on this arena.");

			m_disposableOffset = m_disposableOffset - pDisSz;

			auto pPDisp = (Utilities::Disposable**)(m_disposableOffset + (char*)m_arenaBytes);
			*pPDisp = &pDisposable;
		}

		// empty and recycle the whole ActionArena
		void Retire();
	};

    // The set of arenas may be shared by multiple threads, but each arena
    //  once activated must be used by one thread at a time.
    //
    class ArenaSet
    {
    public:
        virtual ActionArena* Pop() = 0;

        virtual void Push(ActionArena* pArena) = 0;

		virtual ActionArena* GetActionArena(void* pData) = 0;

        virtual ~ArenaSet() {}

        static std::unique_ptr<ArenaSet> ArenaSetFactory(size_t unitSize, uint32_t count);

    };


    // Defines an action scheduled through a scheduler
    //
    class Work
    {
    public:
        // pointer to a trace record 
        //
        virtual const Tracer::TraceRec* GetTracer() const
        {
            return &Tracer::g_emptySpan;
        }

        // This method is called by the scheduler when this work is ready to run.
        // Define actual work in this method;
        // Exception thrown from this method will crash the whole scheduler
        //
        virtual void Run(
            _In_ WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            ) = 0;

        virtual ~Work() = default;
    };

    // Classes related to Activities and Continuations
    //
    //    An activity is a sequence of interleaving continuations and non-CPU bound operations
    // (e.g. I/O operations, RPCs), with its local storage Arena. Most of the case the activity
    // object itself is allocated on the arena.
    //    A continuation is a non-blocking unit of work that must be executed within the context
    // of an activity.
    //    All continuations in an activity execute sequentially without overlapping.
    // An example activity can be process of an update request to the store: 
    //    (note this is for illustration purpose, in reality it is more complex)
    //    UpdateActivity -> 
    //        [Continuation: append request to micro-partition update queue]
    //        [Continuation: sent out replication requests to secondaries and candidates, asynchonously]
    //        [I/O: wait for responses]
    //        [Continuation: commit update to local store, trigger ack sent to client.]
    //
    //    An activity is allocated on an Arena, upon shutdown, the whole arena will be released.
    //
    //    An activity is NOT a sub class of Work, to prevent other's from call Scheduler->Post(Activity);

    enum class ActivityStatus
    {
        // Idle
        Stopped,

        // Defered continuations waiting to run
        Waiting,

        // A continuation is running
        Running, 

        Shuttingdown
    };

    // An activity may travel multiple components, each of which may
    // want to attach its own context to the activity. MultiContext
    // provide an "activity local" storage, trying to emulate "thread
    // local". Currently ugly type cast is used to achieve flexibility.
    //
    enum ContextIndex
    {
        BaseContext,
        ReplicationContext,
        CatalogContext,

        ContextSize
    };

    class ContinuationBase;

    // A helper interface for managing continuations for an activity.
    // Activity class delegate all scheduling related work
    // to this interface.
    //
    class ContinuationHandler : public Work
    {
    protected:
        ActionArena* m_pArena;
    public:
        ActionArena* GetArena() const
        {
            return m_pArena;
        }

        virtual Scheduler* GetScheduler() const = 0;

        // Schedule a continuation to run under this activity
        //
        virtual void Post(
            _In_ ContinuationBase& continuation,
            _In_ intptr_t valueHandle,
            _In_ uint32_t valueLength,
            _In_ uint32_t microsecondDelay = 0
            ) = 0;

        // This method should cause the activity to be removed from the scheduler,
        // all the continuations waiting will be abandoned. No effect on currently
        // running continuation though.
        //
        virtual void RequestShutdown(
            _In_ StatusCode  reason,
            _In_ const wchar_t* msg = NULL
            ) = 0;

        // Return whether shutdown has been requested.
        virtual bool IsShuttingDown() = 0;

        // Register a recyclable resource, so that it can be automatically recycled
        // when shutdown
        //
        virtual void RegisterDisposable(Utilities::Disposable* pRes) = 0;
    };

    // Shell class for Activity, complex logic of handling continuation queue
    // is hidden in ContinuationHandler
    // Nothing to reclaim clean up here, no need for DTOR 
    //
    class Activity
    {
    private:
        Activity& operator=(const Activity& rhs) = delete;
        Activity(const Activity&) = delete;

        ContinuationHandler&  m_handler;
        void*                 m_multiContext[ContextIndex::ContextSize];
    public:

        Activity(ContinuationHandler& handler)
            : m_handler(handler)
        {
            for (int i = 0; i < ContextIndex::ContextSize; i++)
            {
                m_multiContext[i] = nullptr;
            }
        }

        const Tracer::TraceRec* GetTracer() const
        {
            return m_handler.GetTracer();
        }

        ActionArena* GetArena() const
        {
            return m_handler.GetArena();
        }

        Scheduler* GetScheduler() const
        {
            return m_handler.GetScheduler();
        }

        void* GetContext(ContextIndex index) const
        {
            Audit::Assert(0 <= index && index < ContextIndex::ContextSize, "context index must be within range");
            Audit::Assert(m_multiContext[index] != nullptr, "Context must be set before retrieving");
            return m_multiContext[index];
        }

        // TODO!! destory all context automatically when activity clean up.
        // now user have to manually clean up by setting the context to nullptr
        void SetContext(ContextIndex index, void* pContext)
        {
            Audit::Assert(0 <= index && index < ContextIndex::ContextSize, "context index must be within range");
            if (pContext != nullptr)
                Audit::Assert(m_multiContext[index] == nullptr, "Context occupied.");
            m_multiContext[index] = (pContext);
        }

        // Schedule a continuation to run under this activity
        //
        void Post(
            _In_ ContinuationBase& continuation,
            _In_ intptr_t valueHandle,
            _In_ uint32_t valueLength,
            _In_ uint32_t microsecondDelay = 0
            )
        {
            // TODO!!! the assert create a circular dependency between ContinuationBase and Activity,
            // and compiler complained about "use of undefined type"
            //Audit::Assert(&continuation.m_activity == this, "Continuation is bind to another activity!");
            m_handler.Post(continuation, valueHandle, valueLength, microsecondDelay);
        }

        // This method should cause the activity to be removed from the scheduler,
        // all the continuations waiting will be abandoned. No effect on currently
        // running continuation though. After current running continuation is finished
        // the arena is reclaimed.
        //
        // TODO!! we should hunt for inactive activities when the system is under pressure and force shutdown.
        // forgetting to shutdown means resources leak. 
        //
        void RequestShutdown(
            _In_ StatusCode reason,
            _In_ const wchar_t* msg = nullptr
            )
        {
            m_handler.RequestShutdown(reason, msg);
        }

        // Returns whether shutdown has been requested.
        bool IsShuttingDown() {
            return m_handler.IsShuttingDown();
        }

        // Register a recyclable resource, so that it can be automatically recycled
        // when shutdown
        //
        void RegisterDisposable(Utilities::Disposable* pRes)
        {
            m_handler.RegisterDisposable(pRes);
        }

    };

    // A continuation is a sequence of continuously executing instructions without long
    // waiting(such as I / O operations).
    //
    // Each continuation must execute in the context of an Activity. Due to the sequential
    // nature of the Acitivity, no lock needed when accessing Activity local Arena.
    //
    class ContinuationBase : Utilities::Disposable
    {
    private:
        ContinuationBase& operator=(const ContinuationBase& rhs) = delete;
    public:
        Activity& m_activity;

        ContinuationBase(Activity& activity)
            : m_activity(activity)
        {
			GetArena()->RegisterDisposable(*this);
        }

        ActionArena* GetArena() const;

        // Clean up method is called in two cases:
        // 1. the continuation did not get a chance to run because of termination of the activity
        // 2. the continuation throws (see implementation of Fail below, overriding Fail function
        //    may change this behavior.
        // Note that Cleanup is not automatically called when an continuation finishes normally.
        // It is important to make Cleanup idempotent, as it may be called multiple times
        // in the case that it is posted to the same activity multiple times.
        virtual void Cleanup() = 0;

        // This method is called by the scheduler when it's this Activity's turn.
        // Define actual work in this method;
        //
        virtual void OnReady(
            _In_ WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            ) = 0;

        // This method is called when OnRead throws an exception.
        // Override this to define exception handling
        //
        virtual void Fail(
            _In_ StatusCode reason,
            _In_ const char*           msg = NULL
            )
        {
            Cleanup();
            Audit::OutOfLine::Fail(reason, msg);
        }

        // Schedule this continuation to run under its context activity
        //
        virtual void Post(
            _In_ intptr_t valueHandle,
            _In_ uint32_t valueLength,
            _In_ uint32_t microsecondDelay = 0
            ) 
        {
            m_activity.Post(*this, valueHandle, valueLength, microsecondDelay);
        }

        void Dispose() override
        {
            this->~ContinuationBase();
        }

        virtual ~ContinuationBase() {}

    };

    class EmptyEnvelope
    {
    public:
        void Recycle() {}
    };

    // Allow triggering of a continuation by a certain data item: continuation->Post(postable).
    // this is to allow certain resource to be passed around among continuations. 
    //
    // PostableT object must be recyclable. so that in the case of abanded continuation (due to activity
    // shutdown) or exception thrown, PostableT object is recycled not leaked.
    //
    // Author of the continuation must make sure that in normal execusion, PostableT object is either
    // hand over to someone else, or recycled. We can not automatically recycle it at the end of OnReady
    // because we don't know whether the same continuation will execute again.
    //
    // Recycle should be idempotent, as it maybe called multiple times if a single continuation
    // is posted multiple times
    //
    // PostableT should NOT be copiable, and it should only be movable, as once a resource is passed
    // from A to B, A should not have that resource anymore.
    // TODO!! find a way to enforce non-copiable
    // 
    template <class PostableT>
    class Continuation : public ContinuationBase
    {
        // TODO!! make the following work to reduce confusing compliation errors when 
        // the type parameter does not implement Recycle() function:
        // static_assert(std::is_function<decltype(PostableT::Recycle)>::value,
        //    "Type parameter must implement Recycle!");

    protected:
        PostableT m_postable;

        void ScheduleToRun(uint32_t length, uint32_t microsecondDelay)
        {
            Post((intptr_t)&(m_postable), length, microsecondDelay);
        }

    public:
        Continuation(Activity& activity)
            : ContinuationBase(activity)
        {}

        virtual void Receive(
            _In_ PostableT&&  postable,
            uint32_t    length = sizeof(PostableT),
            uint32_t    microsecondDelay = 0
            )
        {
            m_postable = std::move(postable);
            ScheduleToRun(length, microsecondDelay);
        }

        virtual ~Continuation()
        {
            m_postable.Recycle();
        }
    };

    // A Continuation can attach some context to the activity
    // with MultiContext, not caring about other contexts. Context objects should
    // be allocated on activity arena, for it is reclaimed after activity shutdown.
    //
    // Context and PostDataType are two orthogonal features. Ideally
    // the user should be able to pick and mix. Could not figure out
    // how to do it. This is the closest I can get.
    //
    template <typename Context, ContextIndex index, typename PostDataType = EmptyEnvelope>
    class ContinuationWithContext : public Continuation < PostDataType >
    {
    public:

        ContinuationWithContext(Activity& activity)
            : Continuation<PostDataType>(activity)
        {
            // This essentially associate an index with a type. currently we don't allow
            // double setting of the same context.
            // Ideally we should make the compiler complain about association of the same
            // index to another type.
            // 
            Audit::Assert(ContextIndex::BaseContext <= index && index < ContextIndex::ContextSize,
                "Invalid Context Index");
        }

        Context* GetContext()
        {
            return reinterpret_cast<Context*>(m_activity.GetContext(index));
        }

        void SetContext(Context* pContext)
        {
            m_activity.SetContext(index, reinterpret_cast<void*>(pContext));
        }
    };

    // Do nothing but terminate the activity;
    //
    class EndContinuation : public Continuation < BufferEnvelope >
    {
    public:
        EndContinuation(Activity& act)
            : Continuation(act)
        {}

        void OnReady(
            _In_ Schedulers::WorkerThread&,
            _In_ intptr_t,
            _In_ uint32_t
            ) override
        {
            Cleanup();
        }

        void Cleanup() override
        {
            m_postable.Recycle();
            m_activity.RequestShutdown(StatusCode::OK);
        }
    };

    class Scheduler
    {
    public:
        static const size_t ARENA_SIZE = 64 * SI::Ki;

        // Add an action the to completion keys recognized for this Scheduler.
        // Returns the CompletionKey value to be used with the IOCP.
        // Completions on this file will be routed to action.Run().
        //
        virtual intptr_t RegisterFileWorker(FileHandle additionalFile, _In_ const Work* pAction) = 0;

        // Add an action the to completion keys recognized for this Scheduler.
        // Returns the CompletionKey value to be used with the IOCP.
        // Completions on this file will be routed to action.Run().
        //
        virtual intptr_t RegisterSocketWorker(SocketHandle additionalSocket, _In_ const Work* pAction) = 0;

        // Allocate a new arena
        //
		virtual ActionArena* GetNewArena() = 0;

		// Get arena where pData is located
		//
		virtual ActionArena* GetArena(void* pData) = 0;

        virtual void Post(
            _In_ Work* pActivity,
            _In_ intptr_t valueHandle,
            _In_ uint32_t valueLength,
            _In_ uint32_t microsecondDelay = 0
            ) = 0;

        // Schedule a task to run, on or after QueryPerformanceCounter returns value
        // greater than "dueTick"
        //
        virtual void PostWithDueTick(
            _In_ Work* pActivity,
            _In_ intptr_t valueHandle,
            _In_ uint32_t valueLength,
            _In_ int64_t  dueTick
            ) = 0;

        // An activity already scheduled has a much earlier due time.
        // The implementation may choose to ignore the request if the
        // acitivity is not found in the scheduler, or the old time is
        // close enough.
        //
        virtual void Reschedule(
            _In_ Work* pActivity,
            _In_ intptr_t valueHandle,
            _In_ uint32_t valueLength,
            _In_ uint32_t microsecondDelay = 0
            ) = 0;


        virtual void RequestShutdown(StatusCode error, const wchar_t* msg) = 0;

        // a very imprecise system time provider, in the 10s of seconds,
        // representing number of seconds from a certain past date.
        virtual uint32_t GetCurTimeStamp() const = 0;

        virtual Utilities::DisposableBuffer* Allocate(
            uint32_t size,
            uint32_t alignment = 8,
            uint32_t offset = 0) = 0;

        virtual ~Scheduler() {};
    };

    // Creating an Activity
    // An activity must be bind to a scheduler, is allocated on an arena.
    // when shut down, the arena will be reclaimed and hence the memory used
    // for Activity object.
    //
    Activity* ActivityFactory(Scheduler& scheduler, const Tracer::TraceRec& tracer);

    // Create an Activity and starting a new trace with traceName,
    // We are using sample based tracing, where only some activities are traced,
    // Set parameter mustLog to true to ensure this activity is traced.
    // 
    Activity* ActivityFactory(Scheduler& scheduler, const wchar_t* traceName, bool mustLog = false);

    // Create an Acitivity and starting a sub trace of parameter "parent" with traceName
    Activity* ActivityFactory(Scheduler& scheduler, const wchar_t* traceName, const Tracer::TraceRec& parent);

    // We use windows API QueryPerformanceCounter and QueryPerformanceFrequency for high resolution time
    // stamps. These stamps are used to figure out when continuations are due.
    // documents can be found 
    // http://msdn.microsoft.com/en-us/library/windows/desktop/ee417693(v=vs.85).aspx
    //
    // There are concerns due to cpu dynamic frequency scaling, QueryPerformanceCounter may get different
    // values when running on different cores. However, as recent Intel CPUs all have invariant TSC support
    // this should not be a problem, see FAQ section of the following
    // http://msdn.microsoft.com/en-us/library/windows/desktop/dn553408(v=vs.85).aspx
    // Even with this, we must be very careful in using these ticks. System should not crash due to 
    // tick value discrepency from different cores. The worst case some continuation got delayed.
    //
    class MachineFrequency
    {
        double m_ticksPerMicrosecond;
        double m_ticksPerMillisecond;
        double m_ticksPerSecond;

    public:
        MachineFrequency();

        double TicksPerMicrosecond()
        {
            return m_ticksPerMicrosecond;
        }

        double TicksPerMillisecond()
        {
            return m_ticksPerMillisecond;
        }

        double TicksPerSecond()
        {
            return m_ticksPerSecond;
        }
    };
    extern class MachineFrequency g_frequency;

}
