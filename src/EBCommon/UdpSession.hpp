
// Session map, there's aw only used by ServiceBrokerRIO

#pragma once

#include <unordered_map>

#include "ServiceBroker.hpp"
#include "Cuckoo.hpp"

namespace ServiceBrokers
{
    // Max number of sessions in a session map
    const static uint32_t MAXSESSIONS = 716;

    // Max Time-to-live for sessions. if remain inactive
    // for longer than POLLINTERVAL * (SESSIONTICKS - 1)
    // the session would be reclaimed.
    // set very large number for manual debugging
    const static uint16_t SESSIONTICKS = 7;

    // poll each session every 750 microsecond
    // Current intra DC 99 percentile rtt is 300 - 500 microseconds when
    // network is considered "normal" (midean around 250 microseconds).
    // by 900 microseconds normal packets should have been back. 
    //
    // set to large number for manual debugging
    const static uint64_t POLLINTERVAL = 900;


    enum class SessionStatus : uint16_t
    {
        Dead = 0,

        // session just created, some one is filling
        // up the fields.
        Initialing,

        // working condition
        Sending,

        Receiving,

        Finished
    };

    template<typename Lock, typename LockInitializer>
    struct UdpSession
    {
        // points to the first and last slice
        uint16_t m_firstSlice = static_cast<uint16_t>(~0);
        uint16_t m_lastSlice = static_cast<uint16_t>(~0);

        // in receiving end, the expected sequence number of
        // the packet to be received next
        //
        // in sending end, the sequence number of 
        // the packet that is being sent but has
        // not get the ack yet
        int16_t m_seq = 0;

        // total number of packets to be transmitted
        const int16_t m_totalPck;

        // A ticking bomb. Each time there is an activity, it
        // is set to SESSION_TICK. It is periodically decremented.
        // the session can be destroyed when it is 0.
        uint16_t m_tickDown = SESSIONTICKS;
        
        SessionStatus m_status = SessionStatus::Dead;

        Lock m_lock;

        UdpSession(int16_t totalPackets)
            : m_totalPck(totalPackets)
        {
            Audit::Assert(totalPackets > 1 && totalPackets < 1024,
                "Invalide number of packets");
            LockInitializer()(&m_lock);
        }

    private:
        // Do not allow assignment
        UdpSession& operator=(const UdpSession&) = delete;
    };


    // We reuse a TraceRec to identify a UdpSession. The benifit
    // is that we can quickly identify a resend, and if the session
    // is still alive, drop the resend request.
    // The downside is that we may have a send session and a receive
    // session with the same TraceRec (e.g. in a remote read, a 
    // resend req comes while we are trying to deliver the result).
    //
    // One solution is to use <TraceRec, bool> as key, the bool
    // value is true if send. Another solution is to use seperate
    // maps for receive and send. We choose the latter because
    // of increased concurrency, and natural seperation that prevents
    // too many receive to starve the send sessions.
    //
    template<typename SessionVisitor, typename Lock, typename LockInitializer>
    class CuckooSessionMap : public Work
    {
    public:
        // using Cuckoo maps at 70% load ratio. experiments show
        // 90% of insertion at this load ratio can finish with
        // search depth of 2
        //
        typedef Utilities::CuckooTable<Tracer::TraceRec, 
            UdpSession<Lock, LockInitializer>, 
            70, true
        > MapType;

        typedef typename Utilities::CuckooTable<Tracer::TraceRec,
            UdpSession<typename Lock, typename LockInitializer>,
            70, true
        >::iterator MapIterType;

        typedef std::pair<Tracer::TraceRec, 
            UdpSession<Lock, LockInitializer>
        > EntryType;

    private:
        MapType m_sendTable;
        Lock m_sendLock;

        MapType m_rcvTable;
        Lock m_rcvLock;

        SessionVisitor* m_pVisitor;
        Schedulers::Scheduler* m_pScheduler;
        bool m_shuttingdown;

        bool CreateSession(
            MapType& map,
            Lock& lock,
            const Tracer::TraceRec& rec,
            int16_t totalPck,
            _Out_ EntryType*& pSession)
        {
            // Cuckoo session insert will fail if too many sessions.
            {
                Utilities::Exclude<Lock> guard{ lock };
                auto res = map.Insert(rec, totalPck);

                if (res.second == CuckooStatus::Ok || res.second == CuckooStatus::KeyDuplicated){
                    pSession = &(*(res.first));
                    if (CuckooStatus::Ok == res.second)
                    {
                        // TODO!! paranoid check, remove!
                        Audit::Assert(SessionStatus::Dead == pSession->second.m_status,
                            "New Session state corruptted");
                        pSession->second.m_status = SessionStatus::Initialing;
                    }
                }
                else {
                    pSession = nullptr;
                }
                return res.second == CuckooStatus::Ok;
            }
        }

        EntryType* Find(
            MapType& map,
            Lock& lock,
            const Tracer::TraceRec& rec)
        {
            Utilities::Share<Lock> guard{ lock };
            auto iter = map.Find(rec);
            if (iter == map.end())
            {
                return nullptr;
            }
            else
            {
                return &(*iter);
            }
        }

        void PollMap(MapType& map, Lock& lock)
        {
            // One of the major benifit of a stable Cuckoo hash where
            // an iterator won't be invalidated by insertion of some
            // other value
            //
            // might have two race conditions:
            // 
            // 1. We may miss newly inserted sessions, not a big deal.
            // we cover it next time
            //
            // 2. We may also get into a newly allocated slot for a 
            // session before initialization start

            if (m_shuttingdown)
                return;
            for (auto& entry : map){
                if (SessionStatus::Dead == entry.second.m_status)
                {
                    // uninitialized spot, skip
                    continue;
                }

                uint16_t ttl = entry.second.m_tickDown;
                Audit::Assert(ttl <= SESSIONTICKS, 
                    "UDP session ttl too big!");

                if (ttl > 0) {
                    ttl = InterlockedCompareExchange16
                        ((short*)&entry.second.m_tickDown, (short)(ttl - 1), (short)ttl);
                    if (ttl < SESSIONTICKS - 1){
                        m_pVisitor->PollSession(entry.second);
                    }
                }
                else{
                    // Remove expired session
                    if (m_pVisitor->CleanupSession(entry.second)){
                        Audit::Assert(SessionStatus::Dead == entry.second.m_status,
                            "New Session state corruptted.");
                        Utilities::Exclude<Lock> guard{ lock };
                        map.Erase(entry.first);
                    }
                }

                if (m_shuttingdown){
                    return;
                }
            }
        }

        void Poll(){
            PollMap(m_rcvTable, m_rcvLock);
            PollMap(m_sendTable, m_sendLock);
        }

    public:
        CuckooSessionMap()
            : m_pVisitor(nullptr)
            , m_shuttingdown(false)
            , m_pScheduler(nullptr)
            , m_rcvTable(MAXSESSIONS)
            , m_sendTable(MAXSESSIONS)
        {
            LockInitializer()(&m_rcvLock);
            LockInitializer()(&m_sendLock);
        }

        void StartPolling(Schedulers::Scheduler& scheduler)
        {
            Audit::Assert(m_pScheduler == nullptr,
                "Do not support replacing of scheduler in session map!");
            m_pScheduler = &scheduler;

            Audit::Assert(m_pVisitor != nullptr,
                "Session visitor expected!");
            m_pScheduler->Post(this, (intptr_t)this, 0);
        }

        void SetVisitor(SessionVisitor& visitor)
        {
            m_pVisitor = &visitor;
        }

        // Create a new session, returns true if there is no existing session
        // of the same trace id. pSession will be pointing to the session
        // object inside the map
        //
        bool CreateRcvSession(
            _In_ const Tracer::TraceRec& rec,
            uint16_t totalPck,
            _Out_ EntryType*& pSession)
        {
            return CreateSession(m_rcvTable, m_rcvLock, rec, totalPck, pSession);
        }

        bool CreateSendSession(
            _In_ const Tracer::TraceRec& rec,
            uint16_t totalPck,
            _Out_ EntryType*& pSession)
        {
            return CreateSession(m_sendTable, m_sendLock, rec, totalPck, pSession);
        }

        EntryType* FindRcvSession(_In_ const Tracer::TraceRec& rec)
        {
            return Find(m_rcvTable, m_rcvLock, rec);
        }

        EntryType* FindSendSession(_In_ const Tracer::TraceRec& rec)
        {
            return Find(m_sendTable, m_sendLock, rec);
        }


        // Periodical job for polling all sessions
        //
        void Run(
            _In_ Schedulers::WorkerThread&,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            ) override
        {
            Audit::Assert(continuationHandle == (intptr_t)this
                && length == 0,
                "expected parameter is self pointer");

            if (m_shuttingdown)
                return;

            LARGE_INTEGER ticksNow;
            Audit::Assert(0 != QueryPerformanceCounter(&ticksNow),
                "QueryPerformanceCounter failed");
            double nextDue = ticksNow.QuadPart +
                (POLLINTERVAL * g_frequency.TicksPerMicrosecond());

            Poll();

            if (m_shuttingdown)
                return;

            m_pScheduler->PostWithDueTick
                (this, (intptr_t)this, 0, (int64_t)nextDue);
        }

        virtual void RequestShutdown()
        {
            m_shuttingdown = true;
        }
    };

}