
#pragma once

#include "stdafx.h"

#include <array>

#include "Utilities.hpp"
#include "Datagram.hpp"
#include "Scheduler.hpp"
#include "Catalog.hpp"
#include "ServiceBroker.hpp"
#include "Exabytes.hpp"
#include "ReducedKeyMap.hpp"


namespace Exabytes
{

    //     An interface for the module for replication and fail-over.
    //     The module should be attached to a local partition, which
    // is actually a replica of a partition.
    //     We have only one actual implementation planed: Vertical Paxos II.
    // We use virtual functions to support a single node non-replication mode.
    //
    class Replicator {
    public:
        // Does current replica accept write req?
        // Writable implies readable with no staled data.
        //
        virtual bool Writable() = 0;

        // Does current replica accept read with staled data?
        virtual bool Readable() = 0;

        // Process command from manager: We received a new ballot
        // begin running for primary, details see Vertical Paxos paper
        //
        virtual void ProcessNewBallot(Datagram::ServerAddress src, Datagram::PartitionConfig) = 0;

        // Process command from manager: we are officially primary.
        // details see Vertical Paxos paper
        //
        virtual void ProcessActivateBallot(Datagram::ServerAddress src, uint64_t ballot) = 0;

        // On Primary, Process write and delete req from client
        // The actor that process incoming network request should create a
        // write activity, and put the update request into base context.
        // Parameter pWriter points to the last continuation of the write
        // activity.
        //
        virtual StatusCode ProcessClientUpdate(
            _In_ Schedulers::ContinuationBase* pWriter,
            size_t blobSize
            ) = 0;

        virtual StatusCode CommitUpdate(_In_ Schedulers::ContinuationBase* pWriter) = 0;

        // On Secondary, Process prepare req from primary
        virtual StatusCode ProcessPrepareReq(
            // source of the msg should be the primary
            Datagram::ServerAddress src, 

            // handle to an update activity. update req should be in base context
            Schedulers::ContinuationBase* pUpdate, 

            // length of the blob, verification purpose
            uint32_t bodyLength) = 0;
        
        // On Primary, Process ack of prepare req
        virtual void ProcessAck(
            Datagram::ServerAddress src,

            // Largest consequtive decree the secondary has
            uint64_t decree,

            // tick of the most recent prepare or beacon received from primary
            uint64_t tick
            ) = 0;

        virtual ~Replicator() {}
    };

    //   A dummy implementation that does not perform any replication at all
    // used by single node storage (not replicated)
    class NoneReplicator : public Replicator {
        // Does current replica accept write req?
        bool Writable() override;

        // Does current replica accept read with staled data?
        bool Readable() override;

        // Process command from manager
        void ProcessNewBallot(Datagram::ServerAddress src, 
            Datagram::PartitionConfig) override;

        void ProcessActivateBallot(Datagram::ServerAddress src, 
            uint64_t ballot) override;

        // On Primary, Process write and delete req from client
        StatusCode ProcessClientUpdate(
            _In_ Schedulers::ContinuationBase* pWriter,
            size_t blobSize
            ) override;

        // On Secondary, Process prepare req from primary
        StatusCode ProcessPrepareReq(
            Datagram::ServerAddress src, 
            Schedulers::ContinuationBase* pUpdate,
            uint32_t bodyLength) override;

        // On Primary, Process ack of prepare req
        virtual void ProcessAck(Datagram::ServerAddress src,
            uint64_t decree, uint64_t tick ) override;
    };


    const static size_t PREPARE_QUE_SIZE = 16;
    class PrepareQueue {
    private:
        std::array<Schedulers::ContinuationBase*, PREPARE_QUE_SIZE> m_array;

        // When the queue is not empty, it should be the smallest decree.
        // When the queue is empty, it should be the next decree it is expecting
        // It should always be (committed + 1) except in boot strap
        uint64_t m_startDecree;

        uint8_t m_count;
        uint8_t m_start;

        static ReplicationReqHeader& GetHeader(Schedulers::ContinuationBase* pUpdate){
            Schedulers::Activity& writeActivity = pUpdate->m_activity;

            ReplicationReqHeader* pReq = reinterpret_cast<ReplicationReqHeader*>
                (writeActivity.GetContext(Schedulers::ContextIndex::BaseContext));
            Audit::Assert(pReq != nullptr &&
                pReq->Description.KeyHash.IsInvalid()
                && pReq->Decree > 0 && pReq->Commit == 0,
                "Invalid item in prepare queue!");

            return *pReq;
        }


    public:
        PrepareQueue()
            : m_startDecree(0)
            , m_count(0)
            , m_start(0)

        {
            m_array.fill(nullptr);

        }

        StatusCode Put(Schedulers::ContinuationBase* pUpdate){
            ReplicationReqHeader& header = GetHeader(pUpdate);

            // (m_startDecree + m_count - 1) is the end decree
            // (m_start + m_count - 1) % COUNT is the end slot

            unsigned delta = 0;
            unsigned idx = static_cast<unsigned>(~0);

            if (m_count == 0)
            {
                if (m_startDecree == 0){
                    m_startDecree = header.Decree;
                }
                delta = 1;
                idx = m_start;
            }
            else if (header.Decree > (m_startDecree + m_count - 1))
            {
                delta = static_cast<int>(header.Decree - (m_startDecree + m_count - 1));
                idx = (header.Decree - m_startDecree + m_start) % PREPARE_QUE_SIZE;
            }
            else if (header.Decree < m_startDecree)
            {
                // prepare of already committed decree should not happen, except
                // in the case of inactive replica which lagged behind.
                // in that case it should go to a catch up queue.
                delta = static_cast<int>(m_startDecree - header.Decree);
                idx = (m_start + PREPARE_QUE_SIZE - (m_startDecree - header.Decree)) % PREPARE_QUE_SIZE;
            } 
            else {
                idx = (header.Decree - m_startDecree + m_start) % PREPARE_QUE_SIZE;
            }

            if (delta + m_count > PREPARE_QUE_SIZE)
            {
                return StatusCode::OutOfResources;
            }

            Schedulers::ContinuationBase* old = m_array[idx];
            if (old != nullptr)
            {
                ReplicationReqHeader& oldHeader = GetHeader(old);
                Audit::Assert(oldHeader.Ballot <= header.Ballot
                    || oldHeader.Description.KeyHash == header.Description.KeyHash,
                    "Prepare of a conflicting decree!");
            }

            m_array[idx] = pUpdate;

            // update tracking data
            m_count += static_cast<uint8_t>(delta);
            if (header.Decree < m_startDecree){
                m_startDecree = header.Decree;
                m_start = static_cast<uint8_t>(idx);
            }

            if (old != nullptr)
            {
                // usually this continuation does nothing but terminate the process
                old->Post((intptr_t)StatusCode::Abort, 0);
            }

            return StatusCode::OK;
        }

        Schedulers::ContinuationBase* PopMin(){
            Audit::NotImplemented();
            if (m_count > 0)
            {
                auto mu = m_array[m_start];
                m_array[m_start] = nullptr;

                m_count--;
                m_start = (m_start + 1) % PREPARE_QUE_SIZE;
                m_startDecree++;
                return mu;
            }
            else
            {
                return nullptr;
            }

        }

        Schedulers::ContinuationBase* GetByDecree(uint64_t decree){
            Audit::NotImplemented();
            if (decree < m_startDecree || decree >(m_startDecree + m_count - 1))
                return nullptr;
            else
                return m_array[(m_start + (decree - m_startDecree)) % PREPARE_QUE_SIZE];
        }

        uint64_t MinDecree(){
            return m_startDecree;
        }

        uint64_t MaxDecree(){
            return (m_startDecree + m_count - 1);
        }

        // remove all decrees greater than maxDecree
        //
        bool Truncate(uint64_t maxDecree){
            if (maxDecree < m_startDecree - 1)
                return false;
            if (maxDecree >= MaxDecree())
                return true;

            auto newCount = maxDecree + 1 - m_startDecree;
            Audit::Assert(newCount <= m_count,
                "Truncate can not increase the size!");
            for (auto i = newCount; i < m_count; i++){
                if (m_array[m_start + i] != nullptr){
                    m_array[m_start + i]->Post((intptr_t)StatusCode::Abort, 0);
                    m_array[m_start + i] = nullptr;
                }
            }
            m_count = (uint8_t)newCount;
            return true;
        }
    };


    //   A replication and fail-over protocol based on Vertical Paxos II
    // with lease based fault-detection, where the primary periodically
    // send beacon to secondaries. Note that replication requests are
    // treated as beacons.
    //
    class VPaxosReplica : public Replicator {
    private:
        // Data structures for VPaxos replication, actually only primary need
        // all of them, as a secondary, it only need to know who is the primary,
        // when was the last time we hear from the primary,

        Datagram::PartitionConfig m_membership;

        // last time we hear from each replica
        uint64_t m_rcvTime[4];

        // estimated decree of each replica
        uint64_t m_decree[4];


        // a decree number, where if we can bring the committed decree past this
        // number, we can ask the manager to activate our primary status
        uint64_t m_consolidate;

        // Prepare queue, used by all replicas
        PrepareQueue m_prepare;

        bool m_isPrimary;
        // if true, we are elected primary but in the middle of consolidation
        // can not process commit yet
        bool m_primaryPending;

        bool m_isActiveSecondary;

        bool m_isCandidate;

    public:
        VPaxosReplica()
        {
            Audit::NotImplemented();
        }

        // Does current replica accept write req?
        bool Writable() override;

        // Does current replica accept read with staled data?
        bool Readable() override;


        // All the processing function must be async and send the work on a single
        // activity to avoid race condition.

        // Process command from manager
        void ProcessNewBallot(Datagram::ServerAddress src, Datagram::PartitionConfig) override;
        void ProcessActivateBallot(Datagram::ServerAddress src, uint64_t ballot) override;

        // On Primary, Process write and delete req from client
        // The actor that process incoming network request should create a
        // write activity, and put the update request into base context.
        // Parameter pWriter points to the last continuation of the write
        // activity.
        //
        StatusCode ProcessClientUpdate(
            _In_ Schedulers::ContinuationBase* pWriter,
            size_t blobSize
            ) override;

        // On Secondary, Process prepare req from primary
        StatusCode ProcessPrepareReq(Datagram::ServerAddress src, Schedulers::ContinuationBase* pUpdate, uint32_t bodyLength) override;

        // On Primary, Process ack of prepare req
        void ProcessAck(Datagram::ServerAddress src,
            uint64_t decree, uint64_t tick) override;

    };



    //////////////////////////////////////////////////////
    // Actors to react on replication messages
    class PrepareActor : public ServiceBrokers::VerbWorker
    {
    private:
        ExabytesServer* const m_pServer;

        // assignment not allowed
        PrepareActor& operator=(const PrepareActor&) = delete;
    public:
        PrepareActor(Exabytes::ExabytesServer& server)
            : m_pServer(&server)
        {}

        BufferEnvelope AcquireBuffer(size_t msgSize) override
        {
            Audit::Assert(msgSize < 2 * SI::Gi, "packet size too big");

            auto pb = m_pServer->GetScheduler().Allocate((uint32_t)msgSize);
            return BufferEnvelope(pb);
        }

        ///////////////////////////////////////////////////////////////////////////
        // Actors for replication and fail-over messages
        // Called when we receive a "prepare" msg. This function creates an
        // update activity with the data in the base context, and hand over to the
        // partition. Note that we don't send success ack to client, even if
        // we later take over as primary, for simpliciy. The client may have timed
        // out anyway.
        //
        // Expected buffer content: Request, ReplicationHeader, Description, blob
        void DoWork(
            _In_ BufferEnvelope msg
            ) override;

    };

}