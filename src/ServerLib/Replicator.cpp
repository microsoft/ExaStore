#include "stdafx.h"

#include "Replicator.hpp"

namespace Exabytes{

    using namespace Utilities;
    using namespace Datagram;
    using namespace Schedulers;


    ////////////////////////////////////////////////////////////////////////
    // Implementation of NoneReplicator

    bool NoneReplicator::Writable() {
            return true;
    }

    bool NoneReplicator::Readable() {
            return true;
    }

    // Process command from manager
    void NoneReplicator::ProcessNewBallot(ServerAddress, PartitionConfig) {
            Audit::OutOfLine::Fail(StatusCode::Unexpected, "Isolated server does not process manager command");
    }
    void NoneReplicator::ProcessActivateBallot(ServerAddress, uint64_t) {
        Audit::OutOfLine::Fail(StatusCode::Unexpected, "Isolated server does not process manager command");
    }

    // On Secondary, Process prepare req from primary
    StatusCode NoneReplicator::ProcessPrepareReq(ServerAddress, Schedulers::ContinuationBase*, uint32_t) {
        Audit::OutOfLine::Fail(StatusCode::Unexpected, "Isolated server does not process replication");
        return StatusCode::Unexpected;
    }

    // On Primary, Process ack of prepare req
    void NoneReplicator::ProcessAck(ServerAddress, uint64_t, uint64_t)  {
        Audit::OutOfLine::Fail(StatusCode::Unexpected, "Isolated server does not process replication");
    }


    StatusCode NoneReplicator::ProcessClientUpdate(
        _In_ Schedulers::ContinuationBase* pContinuation,
        size_t blobSize ) 
    {
        Activity& writeActivity = pContinuation->m_activity;

        ReplicationReqHeader* pReq = reinterpret_cast<ReplicationReqHeader*>
            (writeActivity.GetContext(ContextIndex::BaseContext));
        Audit::Assert(pReq != nullptr 
            && !pReq->Description.KeyHash.IsInvalid()
            && pReq->Description.ValueLength == blobSize
            , "invalid write request buffer");

        return CommitUpdate(pContinuation);
    }


    ///////////////////////////////////////////////////////////////////////////
    // Implementation of Vertical Paxos II
    bool VPaxosReplica::Writable() {
        return m_isPrimary && !m_primaryPending;
    }

    bool VPaxosReplica::Readable() {
        return m_isPrimary || m_isActiveSecondary;
    }

    // Process command from manager
    void VPaxosReplica::ProcessNewBallot(ServerAddress, PartitionConfig) {
        Audit::NotImplemented();
    }
    void VPaxosReplica::ProcessActivateBallot(ServerAddress, uint64_t) {
        Audit::NotImplemented();
    }

    // On Secondary, Process prepare req from primary
    StatusCode VPaxosReplica::ProcessPrepareReq(ServerAddress, Schedulers::ContinuationBase*, uint32_t) {
        Audit::NotImplemented();
        return StatusCode::Unexpected;
    }

    // On Primary, Process ack of prepare req
    void VPaxosReplica::ProcessAck(ServerAddress, uint64_t, uint64_t)  {
        Audit::NotImplemented();
    }


    StatusCode VPaxosReplica::ProcessClientUpdate(
        _In_ Schedulers::ContinuationBase* pContinuation,
        size_t blobSize)
    {
        Activity& writeActivity = pContinuation->m_activity;

        ReplicationReqHeader* pReq = reinterpret_cast<ReplicationReqHeader*>
            (writeActivity.GetContext(ContextIndex::BaseContext));
        Audit::Assert(pReq != nullptr
            && !pReq->Description.KeyHash.IsInvalid()
            && pReq->Description.ValueLength == blobSize
            , "invalid write request buffer");

        Audit::NotImplemented();

        return StatusCode::Unexpected;
    }




    ///////////////////////////////////////////////////////////////////////////
    // Actors for replication and fail-over messages
    // Called when we receive a "prepare" msg. This function creates an
    // update activity with the data in the base context, and hand over to the
    // partition. Note that we don't send success ack to client, even if
    // we later take over as primary, for simpliciy. The client may have timed
    // out anyway.
    //
    void PrepareActor::DoWork(
        _In_ BufferEnvelope msg
        ) 
    {
        auto status = StatusCode::OK;
        auto pRequest = reinterpret_cast<Request*>
            ((char*)msg.Contents()->PData() + msg.Contents()->Offset());

        // TODO!! Need to verify message size to be reasonable.
        // TODO!! what if it is not?! crash? or respond with some kind of msg?
        //

        // We don't need a context to identify retry, the prepare queue should do it.
        ServerAddress src{ pRequest->RemoteIPv4, pRequest->RemotePort };

        // Start an activity for commiting to the store
        Activity* cmtJob = ActivityFactory(m_pServer->GetScheduler(), pRequest->Tracer);
        ActionArena* pArena = cmtJob->GetArena();

        // copy replication data into a buffer and set base context
        auto pReq = reinterpret_cast<Exabytes::ReplicationReqHeader*>(pRequest + 1);
        DisposableBuffer* buf = msg.ReleaseContents();
        // make sure the buffer is disposed when activity shutdown.
        cmtJob->RegisterDisposable(buf);

        cmtJob->SetContext(ContextIndex::BaseContext, pReq);
        Audit::Assert(pReq->Description.Timestamp != 0,
            "Replication req with no timestamp");

        auto pEnder = pArena->allocate<Schedulers::EndContinuation>(*cmtJob);

        // Hand over the commit activity to the right partition.

        auto pPartition = m_pServer->FindPartitionForKey(pReq->Description.KeyHash);

        if (pPartition == nullptr)
        {
            status = StatusCode::NotFound; // key not found
            wchar_t errmsg[256];
            auto pid = m_pServer->FindPartitionIdForKey(pReq->Description.KeyHash);

            swprintf_s(errmsg, L"Partition %016I32X for key %016I64X %016I64X not on local server",
                pid.Value(), 
                pReq->Description.KeyHash.ValueHigh(),
                pReq->Description.KeyHash.ValueLow());
            Tracer::LogError(StatusCode::NotFound, errmsg);
        }

        if (status == StatusCode::OK)
        {
            status = pPartition->GetReplicator().
                ProcessPrepareReq(src, pEnder, pReq->Description.ValueLength);
        }

        if (status != StatusCode::OK)
        {
            // So we got invalid prepare request, ignore it.
            // the primary would eventually timeout ???
            pEnder->Post((intptr_t)status, 0);
        }


    }
}