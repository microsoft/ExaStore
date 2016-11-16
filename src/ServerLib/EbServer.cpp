#pragma once

#include "stdafx.h"

#include <time.h>
#include <unordered_map>
#include <queue>
#include <stack>
#include <unordered_set>

#include "Exabytes.hpp"
#include "Replicator.hpp"
#include "EbPartition.hpp"
#include "ServiceBroker.hpp"
#include "UtilitiesWin.hpp"

namespace
{
    using namespace Datagram;
    using namespace Schedulers;
    using namespace Utilities;

    // Max number of coalescing buffers in the pool
    const int32_t NUMBERBUFFERS = 6;

    // After we processed an req and send back ack, the ack may
    // be lost during transit. We need to linger around, and resend
    // the ack if we see retry req. 
    // we stay for 10,000 micro_seconds. longer if we did see retry
    //
    const int32_t RECHECK_INTERVAL = 10000;

    void ErrPartitionNotFound(PartitionId pid, Key128 key)
    {
        wchar_t buf[256];

        swprintf_s(buf, L"Partition %016X for key %016IX %016IX not on local server", 
            pid.Value(), key.ValueHigh(), key.ValueLow());
        Tracer::LogError(StatusCode::NotFound, buf);
    }

    // Data structures to keep track of ongoing requests to avoid
    // unnecessary processing for client retries.
    //
    struct ReqContext
    {
        static const uint32_t RETRY_RCVD = 1;

        // Keep track of request so that we can send response
        Request m_req;

        // The continuation to send ack back, Setting this to non-null
        // means a response is available. 
        // m_pAck->Post(0, RETRY_RCVD) will be called when we receive
        // client retry. The continuation should send back response.
        //
        ContinuationBase* m_pAck = nullptr;

        std::atomic<uint32_t> m_refCnt = 0;

        // this is true when we did not see retry
        bool m_finishedWaiting = false;

        ReqContext(const Request& req)
            : m_req(req)
        {}

        ReqContext(const ReqContext& other)
            : m_req(other.m_req)
            , m_pAck(other.m_pAck)
            , m_refCnt(0)
            , m_finishedWaiting(other.m_finishedWaiting)
        {}

        void SendResponse()
        {
            if (!m_finishedWaiting && m_pAck != nullptr)
            {
                m_pAck->Post(0, RETRY_RCVD);
            }
        }
    };

    class ReqContextMap
    {
    private:
        std::unordered_map<Tracer::TraceRec, ReqContext> m_map;
        SRWLOCK m_lock;
    public:
        ReqContextMap()
        {
            InitializeSRWLock(&m_lock);
        }

        // This is called everytime we receive a client req over network.
        // Try to create a new context using parameter rec, it's 
        // m_finishedWaiting value will be cleared
        //
        // If there is an existing context,  we automatically send response
        // and returning nullptr, as nothing left to be done.
        // 
        // If there is no existing context, a new one will be
        // created and returned.
        //
        ReqContext* Insert(_In_ const Request& req)
        {
            ReqContext context{ req };
            ReqContext* ret = nullptr;
            {
                Utilities::Exclude<SRWLOCK> guard{ m_lock };
                auto res = m_map.insert(std::make_pair(context.m_req.Tracer, context));
                // a retry just came, let's wait longer
                ret = &(res.first->second);
                ret->m_finishedWaiting = false;

                if (res.second){
                    // new session
                    return ret;
                }
                else {
                    ret->m_refCnt++;
                }
            }
            ret->SendResponse();
            ret->m_refCnt--;

            return nullptr;
        }

        // try to remove a context if m_finishedWaiting is true
        // return false if the waiting is still not over
        bool Remove(_In_ const Tracer::TraceRec& rec)
        {
            Utilities::Exclude<SRWLOCK> guard{ m_lock };
            auto iter = m_map.find(rec);
            Audit::Assert(iter != m_map.end(), "Removing of non-existent request context!");
            auto pReq = &(iter->second);

            if (!pReq->m_finishedWaiting || pReq->m_refCnt.load() > 0)
            {
                // waiting is not over, or there are live references, we will try again later
                // if there is no retry comes in between now and next time remove is called
                // then we delete the item.
                pReq->m_finishedWaiting = true;
                return false;
            }

            pReq->m_pAck = nullptr;
            m_map.erase(iter);
            return true;
        }
    };

    // (re)Send Ack back to client
    //
    class AckContinuation : public Continuation<BufferEnvelope>
    {
    private:
        Exabytes::ExabytesServer* const m_pServer;
        ReqContextMap* const m_pContextMap;
        ReqContext* m_pContext;
        uint32_t m_blobSize;
        StatusCode m_status;
        
        bool m_running = false;

        void SendResponse(ServiceBrokers::ServiceBroker& broker)
        {
            auto sendSuccess = StatusCode::UnspecifiedError;
            auto& req = m_pContext->m_req;
            auto pBuf = m_postable.Contents();

            if (pBuf == nullptr)
            {
                if (req.Verb != ServiceVerb::Status)
                {
                    wchar_t buf[128];

                    swprintf_s(buf, L"Wrong response Verb, expected Status, actual: %d",
                        req.Verb);
                    Tracer::LogError(StatusCode::Unexpected, buf);
                    Audit::OutOfLine::Fail(StatusCode::Unexpected, "read should not return empty!");
                }
                auto holder = (uint64_t)m_status;
                sendSuccess = broker.Send(req, &holder, sizeof(holder));
            }
            else
            {
                if (req.Verb != ServiceVerb::Value)
                {
                    wchar_t buf[128];

                    swprintf_s(buf, L"Only Read request expects data blob! Found verb: %d",
                        m_pContext->m_req.Verb);
                    Tracer::LogError(StatusCode::Unexpected, buf);
                    Audit::OutOfLine::Fail(StatusCode::Unexpected, "Only Read request expects data blob!!");
                }
                void* pBlob = (char*)pBuf->PData() + pBuf->Offset();
                sendSuccess = broker.Send(req, pBlob, m_blobSize);
            }
            Audit::Assert(sendSuccess == StatusCode::OK, "network send error not handled");
            Tracer::RPCServerSend(&req.Tracer);
        }

        // Called when this continuation is first triggered, by
        // process completion.
        void OnProcessCompletion(
            _In_ intptr_t                   continuationHandle,
            _In_ uint32_t                   length
            )
        {
            if (length == 0)
            {
                Audit::Assert((void*)continuationHandle != (void*)&m_postable
                    && m_postable.Contents() == nullptr,
                    "error code expected.");

                m_status = (StatusCode)continuationHandle;
                Audit::Assert(m_pContext->m_req.Verb != ServiceVerb::RemoteRead
                    || m_status != StatusCode::OK,
                    "read should not return empty!");

                m_pContext->m_req.Verb = ServiceVerb::Status;
            }
            else
            {
                Audit::Assert((void*)continuationHandle == (void*)&m_postable,
                    "buffer containing data expected.");
                Audit::Assert(m_pContext->m_req.Verb == ServiceVerb::RemoteRead,
                    "only read should return data blob");

                auto pBuf = m_postable.Contents();
                Audit::Assert(length <= pBuf->Size() - pBuf->Offset(),
                    "data length exceeds buffer boundary!");
                Audit::Assert(length >= Exabytes::AddressAndSize::ADDRESSALIGN,
                    "Read record data too small.");

                m_pContext->m_req.Verb = ServiceVerb::Value;
                m_status = StatusCode::OK;
                m_blobSize = length;
            }
            SendResponse(m_pServer->GetBroker());

            // now we can reply directly to client retries, and start polling
            m_pContext->m_pAck = this;
            this->Post(0, 0, RECHECK_INTERVAL);
        }

        void Poll()
        {
            if (!m_pContextMap->Remove(*m_activity.GetTracer()))
            {
                // waiting is not over, need to linger around
                this->Post(0, 0, RECHECK_INTERVAL);
            }
            else
            {
                // we are done, clean up
                m_pContext = nullptr;
                m_postable.Recycle();
                m_activity.RequestShutdown(StatusCode::OK);
                return;
            }
        }

    public:
        AckContinuation(
            _In_ Activity& activity,
            Exabytes::ExabytesServer& server,
            ReqContextMap& contextMap,
            ReqContext& context
            )
            : Continuation(activity)
            , m_pServer(&server)
            , m_pContextMap(&contextMap)
            , m_pContext(&context)
            , m_blobSize(0)
            , m_status(StatusCode::UnspecifiedError)
        {
            Audit::Assert(m_pContext->m_pAck == nullptr,
                "ill formed processing context found near start of the process.");
        }

        void OnReady(
            _In_ Schedulers::WorkerThread&,
            _In_ intptr_t                   continuationHandle,
            _In_ uint32_t                   length
            ) override
        {
            m_running = true;
            Utilities::FlipWhenExist flipper(m_running);

            Audit::Assert(m_pContext != nullptr &&
                m_pContext->m_req.Tracer == *m_activity.GetTracer(),
                "remote request context lost");

            if (m_pContext->m_pAck == nullptr)
            {
                // Process finished, we have not sent the first response yet
                if (m_status != StatusCode::UnspecifiedError)
                {
                    Audit::OutOfLine::Fail(m_status,
                        "Unexpected status change before process completion!");
                }

                OnProcessCompletion(continuationHandle, length);

            }
            else {
                Audit::Assert(m_status != StatusCode::UnspecifiedError,
                    "Forget to set status when finish processing request?!");
                Audit::Assert(continuationHandle == 0,
                    "Invalid parameter to subsequent ack call!");

                switch (length)
                {
                case 0: // drying polling

                    Poll();
                    
                    break;

                case ReqContext::RETRY_RCVD: // client retry received

                    SendResponse(m_pServer->GetBroker());
                    // we don't need to start another poll loop;

                    break;
                default:
                    Audit::OutOfLine::Fail(StatusCode::Unexpected,
                        "Wrong op code for subsequent ack call.");
                    break;
                }
            }
        }

        void Cleanup() override
        {
            Audit::Assert(!m_running,
                "Cleanup overlap with ack process.");
            if (m_pContext != nullptr){
                Audit::OutOfLine::Fail(StatusCode::Unexpected,
                    "Remote processing canceled! Cleanup process should be refined for resource leak.");
                while (!m_pContextMap->Remove(*m_activity.GetTracer())){
                }
                m_pContext = nullptr;
            }

            m_postable.Recycle();
            m_activity.RequestShutdown(StatusCode::OK);
        }


    };

    // Work to start a new partition
    class PartitionStartup : public ContinuationBase
    {
    private:
        Request  m_req;
        PartitionId m_id;
        Exabytes::ExabytesServer* m_pServer;

    public:
        PartitionStartup(
            _In_ Activity& activity,
            Exabytes::ExabytesServer& server,
            const Request& req,
            PartitionId id
            )
            : ContinuationBase(activity)
            , m_pServer(&server)
            , m_req(req)
            , m_id(id)
        {}

        void Cleanup()
        {}

        void OnReady(_In_ Schedulers::WorkerThread&, _In_ intptr_t, _In_ uint32_t) override
        {
            auto status = StatusCode::OK;
            if (!m_pServer->PartitionExists(m_id))
            {
                status = m_pServer->StartPartition(m_id, true);
            }
            // send back ack
            Tracer::LogActionEnd(Tracer::EBCounter::AddPartition, status, &m_req.Tracer);

            m_req.Verb = ServiceVerb::Status;
            auto holder = (uint64_t)status;
            m_pServer->GetBroker().Send(m_req, &holder, sizeof(holder));
            Tracer::RPCServerSend(&m_req.Tracer);
        }
    };

    // Scheduler Dispatch will call a WriteActor to begin and execute a remote Write request.
    //
    class RemoteWriteActor : public ServiceBrokers::VerbWorker
    {
    private:
        Exabytes::ExabytesServer* const m_pServer;
        ReqContextMap* const m_pContextMap;

        // assignment not allowed
        RemoteWriteActor& operator=(const RemoteWriteActor&) = delete;

    public:
        RemoteWriteActor(
            Exabytes::ExabytesServer& server,
            ReqContextMap& contextMap
            )
            : m_pServer(&server)
            , m_pContextMap(&contextMap)
        { }

        BufferEnvelope AcquireBuffer(size_t msgSize) override
        {
            Audit::Assert(msgSize < 2 * SI::Gi, "Message size too big");
            // we need to leave space for replication header, by setting the offset.
            auto offset = (uint32_t)offsetof(Exabytes::ReplicationReqHeader, Description);
            auto pb = m_pServer->GetScheduler().Allocate(uint32_t(msgSize + offset), 16, offset);
            return BufferEnvelope(pb);
        }

        void DoWork(
            _In_ BufferEnvelope msg
            ) override
        {
            auto status = StatusCode::OK;
            auto pRequest = reinterpret_cast<Request*>
                ((char*)msg.Contents()->PData() + msg.Contents()->Offset());

            Tracer::RPCServerReceive(&pRequest->Tracer);

            auto pContext = m_pContextMap->Insert(*pRequest);
            if (pContext == nullptr)
            {
                // this is a retry, response sent by the context map
                return;
            }

            // Start an activity for write
            Activity* writeActor = ActivityFactory(m_pServer->GetScheduler(), pRequest->Tracer);
            ActionArena* pArena = writeActor->GetArena();

            
            DisposableBuffer* buf = msg.ReleaseContents();
            // make sure the buffer is disposed when activity shutdown.
            writeActor->RegisterDisposable(buf);

            // we have a buffer that is Space_for_replication_header, (offset) Request, Description, blob
            // What we want is Space_for_Request, replication_header, Description, blob
            buf->SetOffset(0);
            auto pReq = reinterpret_cast<Exabytes::ReplicationReqHeader*>
                    ((char*)buf->PData() + sizeof(Request));
            Audit::Assert((void*)&pReq->Description == (void*)(pRequest + 1),
                "Internal computation error for write request buffer layout.");

            writeActor->SetContext(ContextIndex::BaseContext, pReq);
            pReq->Description.Timestamp = m_pServer->GetScheduler().GetCurTimeStamp();
            pReq->Description.TaggedForExpiration = 0;

            auto pWriteCompletion = pArena->allocate<AckContinuation>
                (*writeActor, *m_pServer, *m_pContextMap, *pContext);

            auto pPartition = m_pServer->FindPartitionForKey(pReq->Description.KeyHash);
            auto bodyLength = buf->Size() - sizeof(Description) - sizeof(Request)
                - offsetof(Exabytes::ReplicationReqHeader, Description);
            if (bodyLength > Exabytes::AddressAndSize::VALUE_SIZE_LIMIT
                || bodyLength < Exabytes::AddressAndSize::ADDRESSALIGN
                || bodyLength != pReq->Description.ValueLength)
            {
                status = StatusCode::InvalidArgument;
            }
            else if (pPartition == nullptr)
            {
                status = StatusCode::NotFound; // key not found
                ErrPartitionNotFound(
                    m_pServer->FindPartitionIdForKey(pReq->Description.KeyHash),
                    pReq->Description.KeyHash);
            }

            if (status == StatusCode::OK)
            {
                pReq->Description.Serial = pPartition->AllocSerial();
                status = pPartition->GetReplicator().
                    ProcessClientUpdate(pWriteCompletion, bodyLength);
            }

            if (status != StatusCode::OK)
            {
                // error detected before dispaching completion
                pWriteCompletion->Post((intptr_t)status, 0);
            }

        }
    };

    // UDP socket verb, triggered by a udp packet requesting delete
    // Need to keep consistent with write actor
    //
    class RemoteDeleteActor : public ServiceBrokers::VerbWorker
    {
    private:
        Exabytes::ExabytesServer* m_pServer;
        ReqContextMap* const m_pContextMap;

        // assignment not allowed
        RemoteDeleteActor& operator=(const RemoteDeleteActor&) = delete;
    public:
        RemoteDeleteActor(
            Exabytes::ExabytesServer& server,
            ReqContextMap& contextMap
            )
            : m_pServer(&server)
            , m_pContextMap(&contextMap)
        {}

        BufferEnvelope AcquireBuffer(size_t msgSize) override
        {
            auto pb = m_pServer->GetScheduler().Allocate((uint32_t)msgSize);
            return BufferEnvelope(pb);
        }

        void DoWork(
            _In_ BufferEnvelope msg
            ) override
        {
            auto status = StatusCode::OK;
            auto pRequest = reinterpret_cast<Request*>
                ((char*)msg.Contents()->PData() + msg.Contents()->Offset());
            auto msgSize = msg.Contents()->Size() - msg.Contents()->Offset() - sizeof(Request);
            Tracer::RPCServerReceive(&pRequest->Tracer);

            auto pContext = m_pContextMap->Insert(*pRequest);
            if (pContext == nullptr)
            {
                // this is a retry, context map send the response right away if available.
                return;
            }

            // this is a new req and we created a context for it. now start process
            auto pKeyCaller = reinterpret_cast<const PartialDescription*>(pRequest + 1);
            // should call "collect" to retrive this object, 
            // this still works because it fits into one packet

            Audit::Verify(msgSize == sizeof(PartialDescription),
                "unexpected delete request size");

            // Start an activity for delete
            Activity* pJob = ActivityFactory(m_pServer->GetScheduler(), pRequest->Tracer);
            ActionArena* pArena = pJob->GetArena();

            // copy request into a buffer and que job
            Exabytes::ReplicationReqHeader* pReq = 
                reinterpret_cast<Exabytes::ReplicationReqHeader*>
                    (pArena->allocateBytes(sizeof(Exabytes::ReplicationReqHeader)));
            pReq->Description.KeyHash = pKeyCaller->KeyHash;
            pReq->Description.Owner = pKeyCaller->Caller;
            pReq->Description.ValueLength = 0;
            pReq->Description.TaggedForExpiration = 0;
            pReq->Description.Timestamp = m_pServer->GetScheduler().GetCurTimeStamp();

            pJob->SetContext(ContextIndex::BaseContext, pReq);

            auto pWriteCompletion = pArena->allocate<AckContinuation>
                (*pJob, *m_pServer, *m_pContextMap, *pContext);

            auto pPartition = m_pServer->FindPartitionForKey(pReq->Description.KeyHash);

            if (pPartition == nullptr)
            {
                status = StatusCode::NotFound; // key not found
                ErrPartitionNotFound(
                    m_pServer->FindPartitionIdForKey(pReq->Description.KeyHash),
                    pReq->Description.KeyHash);
            }

            if (status == StatusCode::OK)
            {
                pReq->Description.Serial = pPartition->AllocSerial();
                status = pPartition->GetReplicator().
                    ProcessClientUpdate(pWriteCompletion, 0);
            }

            if (status != StatusCode::OK)
            {
                // error detected before dispaching completion
                pWriteCompletion->Post((intptr_t)status, 0);
            }

        }
    };

    // Scheduler Dispatch will call a ReadAcceptor to begin and execute a remote Read request.
    //
    class RemoteReadActor : public ServiceBrokers::VerbWorker
    {
    private:
        Exabytes::ExabytesServer* const m_pServer;
        ReqContextMap* const m_pContextMap;

        // Assignment not allowed
        RemoteReadActor& operator=(const RemoteReadActor&) = delete;
    public:
        RemoteReadActor(
            Exabytes::ExabytesServer& server,
            ReqContextMap& contextMap
            )
            : m_pServer(&server)
            , m_pContextMap(&contextMap)
        {}

        BufferEnvelope AcquireBuffer(size_t msgSize) override
        {
            auto pb = m_pServer->GetScheduler().Allocate((uint32_t)msgSize);
            return BufferEnvelope(pb);
        }

        void DoWork(
            _In_ BufferEnvelope msg
            ) override
        {
            auto status = StatusCode::OK;
            auto pRequest = reinterpret_cast<Request*>
                ((char*)msg.Contents()->PData() + msg.Contents()->Offset());
            auto msgSize = msg.Contents()->Size() - msg.Contents()->Offset() - sizeof(Request);
            Tracer::RPCServerReceive(&pRequest->Tracer);

            auto pContext = m_pContextMap->Insert(*pRequest);
            if (pContext == nullptr)
            {
                // this is a retry, context map send the response right away if available.
                return;
            }

            // this is a new req and we created a context for it. now start process
            // should call "collect" to retrive this object, 
            // this still works because it fits into one packet
            auto pKeyCaller = reinterpret_cast<const PartialDescription*>(pRequest + 1);
            Audit::Verify(msgSize == sizeof(PartialDescription),
                "unexpected a read request size");

            Activity* readActor = ActivityFactory(m_pServer->GetScheduler(), pRequest->Tracer);
            auto pArena = readActor->GetArena();
            auto continuation = pArena->allocate<AckContinuation>
                (*readActor, *m_pServer, *m_pContextMap, *pContext);

            auto pPartition = m_pServer->FindPartitionForKey(pKeyCaller->KeyHash);

            if (pPartition == nullptr)
            {
                status = StatusCode::NotFound;
                ErrPartitionNotFound(
                    m_pServer->FindPartitionIdForKey(pKeyCaller->KeyHash),
                    pKeyCaller->KeyHash);
            }

            if (status == StatusCode::OK)
                status = pPartition->Read(100, *pKeyCaller, continuation);

            if (status != StatusCode::OK)
            {
                continuation->Post((intptr_t)status, 0);
            }
        }
    };

    class StartPartitionActor : public ServiceBrokers::VerbWorker
    {
    private:
        static const unsigned PATH_LEN_LIMIT = 256;
        Exabytes::ExabytesServer* m_pServer;

    public:
        StartPartitionActor(
            Exabytes::ExabytesServer& server
            )
            : m_pServer(&server)
        { }

        BufferEnvelope AcquireBuffer(size_t msgSize) override
        {
            auto pb = m_pServer->GetScheduler().Allocate((uint32_t)msgSize);
            return BufferEnvelope(pb);
        }

        void DoWork(
            _In_ BufferEnvelope msg
            ) override
        {
            auto pRequest = reinterpret_cast<Request*>
                ((char*)msg.Contents()->PData() + msg.Contents()->Offset());
            auto msgSize = msg.Contents()->Size() - msg.Contents()->Offset() - sizeof(Request);

            Tracer::RPCServerReceive(&pRequest->Tracer);
            Tracer::LogActionStart(Tracer::EBCounter::AddPartition,
                L"", &pRequest->Tracer);
            Audit::Assert(msgSize == sizeof(PartitionId),
                "only partition id expected in message");

            //TODO!! verify the message came from manager
            //

            const PartitionId* pId = reinterpret_cast <const PartitionId* >
                (pRequest + 1);

            Activity* job = ActivityFactory(m_pServer->GetScheduler(), pRequest->Tracer);
            auto pArena = job->GetArena();
            auto continuation = pArena->allocate<PartitionStartup>(*job, *m_pServer, *pRequest, *pId);
            continuation->Post(0, 0);
        }
    };

    // Memory buffer used for checkpointing, 
    class ChkPntBuffer 
    {
        // no copy
        ChkPntBuffer(const ChkPntBuffer&) = delete;
        ChkPntBuffer& operator()(const ChkPntBuffer&) = delete;

        const static size_t alignment = sizeof(size_t);
        static_assert((alignment & (alignment - 1)) == 0,
            "Expect alignment to be power of 2 for round up operation");

        enum struct Mode {Fresh, Storing, Loading };

        // need to round up to multiple of SECTORSIZE
        size_t m_capacity = 0;

        // buffer needs to align to SECTORSIZE for unbuffered file I/O
        char* m_buffer = nullptr;

        // Position for next write
        char* m_ptr = nullptr; 

        Mode m_mode = Mode::Fresh;

    public:
        ChkPntBuffer() = default;

        void Reserve(size_t capacity)
        {
            if (m_capacity >= capacity)
                return;

            Audit::Assert(capacity > 0, "Empty buffer not expected for checkpointing.");
            capacity = AlignPower2(Exabytes::SECTORSIZE, capacity);

            size_t curSize = 0;
            if (m_buffer != nullptr){
                curSize = m_ptr - m_buffer;
                Audit::Assert(curSize <= m_capacity, "check Point Buffer Internal error.");
                m_buffer = (char*)_aligned_realloc(m_buffer, capacity, Exabytes::SECTORSIZE);
}
            else
            {
                m_buffer = (char*)_aligned_malloc(capacity, Exabytes::SECTORSIZE);
            }
            Audit::Assert(m_buffer != nullptr, 
                "Failed to allocate memory for checkpoint buffer!");
            m_ptr = m_buffer + curSize;
            m_capacity = capacity;
            ZeroMemory(m_ptr, m_capacity - curSize);
        }

        void Reset()
        {
            ZeroMemory(m_buffer, m_capacity);
            m_ptr = m_buffer;
            m_mode = Mode::Fresh;
        }

        // Allocate a checkpoint record in the buffer and return a pointer
        // to which checkpoint data can be saved.
        // Checkpoint record layout:
        //     size_t partitionid
        //     size_t filePathStrLen
        //     wchar_t[] filePath  // padded for alignment
        //     size_t chkPntSize
        //     char[] chkPnt       // padded for alignment
        //     size_t chkSum      // (filePathStrLen << 32) | chkPntSize
        //
        // Returns: pointer to a memory segment where checkpoint data should
        // be saved to.
        //
        void* NewChkPnt(Datagram::PartitionId pid, const std::wstring& fileName, size_t size)
        {
            Audit::Assert(m_buffer != nullptr, "Check point buffer not intialized.");
            Audit::Assert(m_mode == Mode::Fresh || m_mode == Mode::Storing,
                "Buffer can not switch between saving and loading mode without reset!");
            m_mode = Mode::Storing;

            size_t filePathStrLen = fileName.length();
            size_t paddedStrSize = (((filePathStrLen + 1) * sizeof(wchar_t)) + (alignment - 1)) & (0 - alignment);
            size_t paddedChkPntSize = (size + (alignment - 1)) & (0 - alignment);

            size_t totalSize = 4 * sizeof(size_t) + paddedStrSize + paddedChkPntSize;
 
            // do a compare&swap and retry we can make this thing thread safe

            if (m_ptr + totalSize > (m_buffer + m_capacity))
            {
                // out of space, double the space.
                Reserve(m_capacity * 2);
            }

            char* position = m_ptr;
            m_ptr += totalSize;

            size_t* pPid = (size_t*)position;
            *pPid = pid.Value();
            position += sizeof(size_t);

            size_t* pStrLen = (size_t*)position;
            *pStrLen = filePathStrLen;
            position += sizeof(size_t);

            void* pStr = position;
            memcpy_s(pStr, paddedStrSize, fileName.c_str(), fileName.length()*sizeof(wchar_t));
            position += paddedStrSize;

            size_t* pChkPntSize = (size_t*)position;
            *pChkPntSize = size;
            position += sizeof(size_t);

            void* pChkPnt = position;
            // memcpy_s(pChkPnt, paddedChkPntSize, chkPtr, size);
            position += paddedChkPntSize;

            size_t* pChkSum = (size_t*)position;
            *pChkSum = (filePathStrLen << 32) | size;

            return pChkPnt;
        }

        size_t GetSize() const { return m_ptr - m_buffer; }

        void StoreToFile(AsyncFileIo::FileManager& file, ContinuationBase* pCompletion) const
        {
            Audit::Assert(m_mode == Mode::Storing,
                "Only Buffer in storing mode can be written to file!");
            Audit::Assert(m_ptr > m_buffer, "Can not store an empty checkpoint!");

            size_t size = m_ptr - m_buffer;
            size = AlignPower2(Exabytes::SECTORSIZE, size);
            Audit::Assert(size <= m_capacity, "Buffer overflow when storing checkpoint to file");
            file.Write(0, (uint32_t)size, m_buffer, pCompletion);
        }

        void LoadFromFile(AsyncFileIo::FileManager& file, ContinuationBase* pCompletion)
        {
            Audit::Assert(m_mode == Mode::Fresh && m_ptr == m_buffer,
                "Can not load to an non empty checkpoint!");
            Audit::Assert(m_capacity >= file.GetFileSize(),
                "Not enough buffer space for loading checkpoint");

            m_mode = Mode::Loading;
            auto fileSize = file.GetFileSize();
            Audit::Assert(fileSize <= SI::Gi, "Checkpoint file too big!");
            file.Read(0, (uint32_t)fileSize, m_buffer, pCompletion);
            m_ptr += file.GetFileSize();
        }

        // Extract the content of the checkpoints to fill the parameter
        // recTable
        //
        void Decode(_Out_ std::vector<Exabytes::RecoverRec>& recTable) const
        {
            Audit::Assert(m_mode == Mode::Loading,
                "Unexpected mode while decoding checkpoint!");
            char* position = m_buffer;

            bool corrupt = false;

            for (;;)
            {
                Exabytes::RecoverRec rec;

                size_t* pPid = (size_t*)position;
                position += sizeof(size_t);
                if (position > m_ptr){
                    corrupt = true;
                    Tracer::LogWarning(StatusCode::Unexpected, L"End of data in checkpoint.");
                    break;
                }
                rec.Pid = PartitionId((uint32_t)(*pPid));

                size_t* pStrLen = (size_t*)position;
                position += sizeof(size_t);
                if (position > m_ptr){
                    corrupt = true;
                    Tracer::LogWarning(StatusCode::Unexpected, L"End of data in checkpoint.");
                    break;
                }
                size_t strLen = *pStrLen;
                size_t paddedStrSize = (((strLen + 1) * sizeof(wchar_t)) + (alignment - 1)) & (0 - alignment);

                wchar_t* pStr = (wchar_t*)position;
                position += paddedStrSize;
                if (position > m_ptr){
                    corrupt = true;
                    Tracer::LogWarning(StatusCode::Unexpected, L"End of data in checkpoint.");
                    break;
                }
                if (pStr[strLen] != 0)
                {
                    corrupt = true;
                    Tracer::LogWarning(StatusCode::Unexpected, L"End of data in checkpoint.");
                    break;
                }

                size_t* pChkPntSize = (size_t*)position;
                position += sizeof(size_t);
                if (position > m_ptr){
                    corrupt = true;
                    Tracer::LogWarning(StatusCode::Unexpected, L"End of data in checkpoint.");
                    break;
                }
                rec.ChkPntSize = *pChkPntSize;
                size_t paddedChkPntSize = (rec.ChkPntSize + (alignment - 1)) & (0 - alignment);

                rec.pChkPnt = position;
                position += paddedChkPntSize;
                if (position > m_ptr){
                    corrupt = true;
                    Tracer::LogWarning(StatusCode::Unexpected, L"End of data in checkpoint.");
                    break;
                }

                size_t* pChkSum = (size_t*)position;
                position += sizeof(size_t);
                if (position > m_ptr){
                    corrupt = true;
                    Tracer::LogWarning(StatusCode::Unexpected, L"End of data in checkpoint.");
                    break;
                }
                if (strLen == 0 || (*pChkSum != ((strLen << 32) | rec.ChkPntSize)))
                {
                    corrupt = true;
                    Tracer::LogWarning(StatusCode::Unexpected, L"Data corruption found in checkpoint.");
                    break;
                }

                // reading and verification finished
                StatusCode status = OpenExistingFile(pStr, rec.FileHandle);
                if (status != StatusCode::OK)
                {
                    Tracer::LogError(status, L"Failed to open file from checkpoint record.");
                }
                else
                {
                    recTable.push_back(std::move(rec));
                }

                if (position == m_ptr)
                {
                    break;
                }
            }
        }

        ~ChkPntBuffer()
        {
            _aligned_free(m_buffer);
            m_buffer = nullptr;
        }

    };

    // Code to be executed after we finish reading the check point file
    class ChkPntLoader : public ContinuationBase
    {
        ChkPntBuffer* m_pBuffer;
        std::vector<Exabytes::RecoverRec>& m_recTable;
        ContinuationBase* m_pNext;

    public:
        ChkPntLoader(Activity& act, ChkPntBuffer* pBuffer,
            _Out_ std::vector<Exabytes::RecoverRec>& recoverTable, 
            ContinuationBase* pNext)
            : ContinuationBase(act)
            , m_pBuffer(pBuffer)
            , m_recTable(recoverTable)
            , m_pNext(pNext)
        {}

        void Cleanup() override
        {
            Audit::OutOfLine::Fail(StatusCode::Unexpected,
                "We can not do anything without finishing disk recovery.");
        }

        void OnReady(
            _In_ WorkerThread&,
            _In_ intptr_t,
            _In_ uint32_t
            )
        {
            m_pBuffer->Decode(m_recTable);
            if (m_pNext != nullptr)
            {
                m_pNext->Post(0, 0);
                m_pNext = nullptr;
            }
        }

    };
}

namespace Exabytes
{

    // A simple policy manager mapping from owner id to policies.
    // Retrieving policies from managers has not been implemented.
    // One way to do that is to retrieve the policy whenever a
    // new owner id enters the server. By the time we need to check
    // the policy (garbage collector) we should have the policy.
    //
    class StlMapPolicyTable : public PolicyManager
    {
        // Policy table, maps from owner key to data policy
        std::unordered_map<Key64, Policy> m_policyTable;
    public:
        const Policy GetPolicy(Key64 ownerId) const override
        {
            auto iter = m_policyTable.find(ownerId);
            if (iter == m_policyTable.end())
            {

                Audit::OutOfLine::Fail("No policy for owner id ");
            }
            return iter->second;
        }

        // Set policy for certain owner
        // Return true when the owner has existing policy registered
        // and replaced by this call.
        void SetPolicy(Key64 ownerId, const Policy& policy) override
        {
            m_policyTable[ownerId] = policy;
        }

    };

    std::unique_ptr<PolicyManager> PolicyTableFactory()
    {
        return std::make_unique<StlMapPolicyTable>();
    }

    class IsolatedServer : public ExabytesServer
    {
    private:

        // keeping track of incoming req, so that we can deal with
        // client retry more efficiently
        ReqContextMap m_contextMap;

        // a deligate for creating catalog, file store and piece
        // together a partition
        //
        std::unique_ptr<PartitionCreater> m_pPartitionCreater;

        // Delegate for recoverying data from files
        std::unique_ptr<FileRecover> m_pRecover;

        // Partition table
        std::unordered_map < PartitionId,
            std::unique_ptr < Partition >
        > m_partTable;
        mutable SRWLOCK m_tableLock;

        // Check point buffer
        ChkPntBuffer m_chkPnt;

        // Spare partition file handles
        std::stack<FileHandle> m_ssdHandles;
        std::stack<FileHandle> m_hddHandles;
        SRWLOCK m_handlesLock;

        // function that map item key to partition ID
        // do we want to support dynamic re-partitioning?
        PartitionId(*const m_partHash)(const Key128);

        RemoteReadActor m_readActor;
        RemoteWriteActor m_writeActor;
        RemoteDeleteActor m_deleteActor;
        StartPartitionActor m_partitionStarter;
        
        // a GC queue that direct a garbage collection actor to go around all file
        // stores. It also maintains an emergency collection queue. if a file store
        // is getting out of space, it is thrown in there to be collected next.
        //
        class RoundRobinGcQueue : public PartitionRotor
        {
        private:
            const RoundRobinGcQueue& operator=(const RoundRobinGcQueue&) = delete;
            RoundRobinGcQueue(const RoundRobinGcQueue&) = delete;

            IsolatedServer& m_server;

            std::queue<PartitionId> m_que;

            // emergency collection queue
            std::queue<PartitionId> m_emergencyQue;
            std::unordered_set<PartitionId> m_emergencySet;
            SRWLOCK m_qLock;

        public:
            RoundRobinGcQueue(IsolatedServer& server)
                : m_server(server)
            {
                InitializeSRWLock(&m_qLock);
            }

            // Called when the server is shutting down.
            // 
            void Shutdown()
            {
                Utilities::Exclude<SRWLOCK> guard{ m_qLock };
                std::queue<PartitionId>().swap(m_que);
                std::queue<PartitionId>().swap(m_emergencyQue);
                std::unordered_set<PartitionId>().swap(m_emergencySet);
            }

            void EmergencyCollect(PartitionId pid)
            {
                Utilities::Exclude<SRWLOCK> guard{ m_qLock };
                auto insertion = m_emergencySet.insert(pid);
                if (insertion.second)
                {
                    m_emergencyQue.push(pid);
                }
                }

            void AddPartition(PartitionId pid)
            {
                Utilities::Exclude<SRWLOCK> guard{ m_qLock };
                m_que.push(pid);
                // TODO!! a problem is if a partition is deleted
                // and then quickly add back again, there will be
                // duplicated item in the m_que, should we guard
                // against it with a hash set?
            }

            Partition* GetNext() override
            {
                PartitionId pid; // default is invalid                
                Partition* pPart = nullptr;

                {
                    Utilities::Exclude<SRWLOCK> guard{ m_qLock };
                    if (!m_emergencyQue.empty())
                    {
                        pid = m_emergencyQue.front();
                        m_emergencyQue.pop();
                        m_emergencySet.erase(pid);
                    }
                    else if (!m_que.empty())
                    {
                        pid = m_que.front();
                        m_que.pop();

                        // add back to the end of the queue
                        m_que.push(pid);
                    }
                }

                if (!pid.IsInvalid()){                    
                    // found a partition id, check it still exists
                    Utilities::Share<SRWLOCK> shareLock{ m_server.m_tableLock };

                    auto citer = m_server.m_partTable.find(pid);
                    if (citer != m_server.m_partTable.end())
                    {
                        pPart = citer->second.get();
                    }
                }
                return pPart;
            }               
        };

        RoundRobinGcQueue m_gcQ;
        GcActor* m_pGcActor;

        // Our catalog keeps most hash blocks on file to save memory. hash
        // blocks are read in when needed. We need to go around and discard
        // those blocks that are read in but no longer needed.
        // First we need another partition visitor to visit partitions
        //
        RoundRobinGcQueue m_partVisitor;

        // A simple continuation to clean uncessary hash blocks from
        // memory.
        class CatalogCleaner : public ContinuationBase
        {
            IsolatedServer& m_server;
            bool m_shutdownRequested = false;
        public:
            CatalogCleaner(Activity& act, IsolatedServer& server)
                :ContinuationBase(act)
                , m_server(server)
            {}

            void Cleanup() override
            {
                Tracer::LogError(StatusCode::Unexpected,
                    L"Catalog Cleaner Cancelled.");
            }

            void OnReady(
                _In_ WorkerThread&,
                _In_ intptr_t,
                _In_ uint32_t
                )
            {

                if (m_server.m_hashPoolManager->IsRunningLow()
                    && !m_shutdownRequested)
                {
                    auto pPart = m_server.m_partVisitor.GetNext();
                    if (pPart != nullptr)
                    {
                        auto pCatalog = pPart->GetCatalog();
                        pCatalog->DiscardHashBlocks();
                        Tracer::LogDebug(StatusCode::OK,
                            L"Finished discarding hash blocks from catalog.");
                        // process next catalog
                        this->Post(0, 0);
                        return;
                    }
                }

                if (m_shutdownRequested)
                {
                    Cleanup();
                    m_activity.RequestShutdown(StatusCode::OK);
                    return;
                }

                // we have enough space available so check back in 5 ms
                this->Post(0, 0, 5000);
                Tracer::LogDebug(StatusCode::OK,
                    L"No need for block flush at this time");
            }

            void Start() 
            {
                this->Post(0, 0);
            }

            void RequestShutdown() 
            {
                m_shutdownRequested = true;
            }

        };
        CatalogCleaner* m_pCleaner = nullptr;

        // Delay clean up of the shutdown partitions
        //
        class PartCollector : public Work
        {
            // When we signal a partition to shutdown, we stop new read/write
            // requests, and stop GC. But we need to wait for pending file I/O
            // to finish. Currently we don't have a efficient way to track all
            // pending I/O. So we wait for 200ms, hopefully no file I/O would
            // last that long, so we can safely discard this object.
            //
            // TODO!! make sure we safely discard RPC call back when replication
            // protocol is implemented.
            static const uint32_t DELAY_USEC = 200000;

            // dead partitions waiting to be cleaned
            std::queue<std::unique_ptr<Partition>> m_deadparts;
            SRWLOCK m_deadpartsLock;

            IsolatedServer& m_server;

            std::unique_ptr<Partition> GetOnePartition()
            {
                std::unique_ptr<Partition> ret;
                Utilities::Exclude<SRWLOCK> guard{ m_deadpartsLock };
                if (!m_deadparts.empty())
                {
                    ret = std::move(m_deadparts.front());
                    m_deadparts.pop();
                }

                return ret;
            }

            // We need to keep the file handle open even when we discard
            // the partition. We need these handles for future partitions
            //
            void RecycleHandle(Partition& part)
            {
                auto handle = part.Persister()->GetFileHandle();
                auto media = part.Persister()->GetMediaType();

                Audit::Assert(media == AddressSpace::SSD || media == AddressSpace::HDD,
                    "Unknown media type found in partition file!");
                auto pool = media == AddressSpace::SSD ? 
                    m_server.m_ssdHandles : m_server.m_hddHandles;

                {
                    Utilities::Exclude<SRWLOCK> guard{ m_server.m_handlesLock };
                    pool.push(handle);
                }
            }

        public:
            PartCollector(const PartCollector&) = delete;
            const PartCollector& operator=(const PartCollector&) = delete;

            PartCollector(IsolatedServer& server)
                : m_server(server)
            {
                InitializeSRWLock(&m_deadpartsLock);
            }

            // Delay 200ms and then release the partition object
            //
            void AddPartition(std::unique_ptr<Partition> ptr)
            {
                {
                    Utilities::Exclude<SRWLOCK> guard{ m_deadpartsLock };
                    m_deadparts.push(std::move(ptr));
                }
                m_server.GetScheduler().Post(this, 0, 0, DELAY_USEC);
            }

            void Run(
                _In_ WorkerThread&,
                _In_ intptr_t,
                _In_ uint32_t
                ) override
            {
                std::unique_ptr<Partition> ptr = GetOnePartition();
                if (!ptr)
                {
                    return;
                }
                wchar_t buf[256];
                swprintf_s(buf, L"Cleanup partition %016X",
                    ptr->ID.Value());
                Tracer::LogDebug(StatusCode::OK, buf);

                RecycleHandle(*ptr);
                // partition distroyed as unique pointer get out of scope
            }

            ~PartCollector()
            {
                // TODO!! do we need to stop new partitions added?
                for (;;)
                {
                    std::unique_ptr<Partition> ptr = GetOnePartition();
                    if (!ptr)
                    {
                        return;
                    }
                    RecycleHandle(*ptr);
                    // partition distroyed as unique pointer get out of scope
                }
            }
        };

        PartCollector m_partCollector;

    public:

        // Central class (single object intended) to represent a single machine
        // that has multiple partitions.
        //
        // we have many partitions, each partition has a file store, a catalog.
        // a garbage collector to circulate all file stores and we need buffers.
        // among which, buffers, file stores and garbage collectors are close
        // coupled. 
        // 
        IsolatedServer(
            std::unique_ptr<ServiceBrokers::ServiceBroker>& pBroker,
            std::unique_ptr<CoalescingBufferPool>& pBufferPool,
            std::unique_ptr<PolicyManager>& pPolicyManager,
            std::unique_ptr<CatalogBufferPool<>>& pHashPoolManager,
            std::unique_ptr<CatalogBufferPool<>>& pBloomKeyPoolManager,
            std::unique_ptr<PartitionCreater>& pPartitionCreater,

            std::unique_ptr<FileRecover>& pRecover,

            // partition hash function that maps key to partition
            PartitionId(*hashFunc)(const Key128),

            // factory for GcActor
            GcActor*(*gcFactory)(
                ExabytesServer& server,
                Activity& gcDriver,
                PartitionRotor& pStores
                )

            )
            : ExabytesServer(pBroker, pBufferPool, pPolicyManager, pHashPoolManager, pBloomKeyPoolManager)
            , m_pPartitionCreater(std::move(pPartitionCreater))
            , m_pRecover(std::move(pRecover))
            , m_partHash(hashFunc)
            , m_gcQ(*this)
            , m_partVisitor(*this)
            , m_readActor(*this, m_contextMap)
            , m_writeActor(*this, m_contextMap)
            , m_deleteActor(*this, m_contextMap)
            , m_partitionStarter(*this)
            , m_partCollector(*this)
        {
            // I am taking a function pointer (factory) as parameters in this constructor!
            // this nicely addressed multiple circular dependency problems related to
            // server/GcActor/GcQueue. Plus this does gives us more flexibility to have
            // multiple GC actors in the future.

            Activity* pActivity = ActivityFactory(*(m_pBroker->GetScheduler()), L"FileStoreGC", true);
            m_pGcActor = gcFactory(*this, *pActivity, m_gcQ);
            
            Activity* pCleanJob = ActivityFactory(*(m_pBroker->GetScheduler()), L"HashPoolClaimer", true);
            m_pCleaner = pCleanJob->GetArena()->allocate<CatalogCleaner>(*pCleanJob, *this);
            
            InitializeSRWLock(&m_tableLock);
            InitializeSRWLock(&m_handlesLock);

            m_pBroker->BindVerb(ServiceVerb::AddPartition, m_partitionStarter);
            m_pBroker->BindVerb(ServiceVerb::RemoteRead, m_readActor);
            m_pBroker->BindVerb(ServiceVerb::RemoteWrite, m_writeActor);
            m_pBroker->BindVerb(ServiceVerb::RemoteDelete, m_deleteActor);
        }

        void StartCleanup() override
        {
            ExabytesServer::StartCleanup();
         
            m_partVisitor.Shutdown();
            m_pCleaner->RequestShutdown();

            m_gcQ.Shutdown();
            m_pGcActor->RequestShutdown();
			m_pGcActor = nullptr;
            {
                Utilities::Share<SRWLOCK> shareLock{ m_tableLock };
                for (auto it = m_partTable.begin(); it != m_partTable.end(); it++)
                {
                    it->second.reset();
                }
            }
            m_partTable.clear();
        }

        void SaveCheckPoint(
            // Check point file to write to
            AsyncFileIo::FileManager& file, 

            // Can be nullptr if fire and forget
            ContinuationBase* pNotify) override
        {
            if (m_chkPnt.GetSize() == 0)
            {
                // we don't really know how much memory we need, if we used this buffer
                // to load checkpoint when we start, then most likely we already have
                // enough allocated.
                // Or else this is a guess.
                m_chkPnt.Reserve(SI::Mi);
            }

            {
                Utilities::Share<SRWLOCK> shareLock{ m_tableLock };
                for (auto& p : m_partTable)
                {
                    auto pid = p.first;
                    auto pPart = p.second.get();
                    auto& path = pPart->Persister()->GetFilePath();
                    auto size = pPart->GetCheckpointSize();

                    void* chkPtr = m_chkPnt.NewChkPnt(pid, path, size);
                    auto status = pPart->RetriveCheckpointData(chkPtr);
                    Audit::Assert(status == StatusCode::OK,
                        "Failure to retrieve check point from partition.");

                }
            }

            m_chkPnt.StoreToFile(file, pNotify);
        }

        void LoadCheckPoint(
            // Check point file to read from
            _In_ AsyncFileIo::FileManager& file,

            // Decoded checkpoint is saved into recoverTable
            _Out_ std::vector<RecoverRec>& recoverTable,

            // Notification that the async operation finished
            ContinuationBase& completion
            ) override
        {
            m_chkPnt.Reserve(file.GetFileSize());

            auto pChkPntLoader = completion.GetArena()->allocate<ChkPntLoader>
                (completion.m_activity, &m_chkPnt, recoverTable, &completion);
            m_chkPnt.LoadFromFile(file, pChkPntLoader);
        }

        void Recover(
            _Inout_ std::vector<RecoverRec>& recTable,
            _In_ FileRecoveryCompletion& notifier
            ) override
        {
            m_pRecover->RecoverFromFiles(*this, m_partHash, recTable, notifier);
        }

        PartitionId FindPartitionIdForKey(Datagram::Key128 key) const override
        {
            return m_partHash(key);
        }

        Partition* FindPartitionForKey(Datagram::Key128 key) const override
        {
            Utilities::Share<SRWLOCK> shareLock{ m_tableLock };

            PartitionId index = m_partHash(key);

            auto citer = m_partTable.find(index);
            if (citer == m_partTable.end())
            {
                return nullptr;
            }
            return citer->second.get();
        }

        bool PartitionExists(PartitionId id) const override
        {
            Utilities::Share<SRWLOCK> shareLock{ m_tableLock };
            return (m_partTable.find(id) != m_partTable.end());
        }

        void AddFileHandle(FileHandle handle, bool isSSD) override
        {
            std::stack<FileHandle>& pool = isSSD ? m_ssdHandles : m_hddHandles;
            {
                Utilities::Exclude<SRWLOCK> guard{ m_handlesLock };
                pool.push(handle);
            }
        }

        StatusCode CreatePartFile(const wchar_t* filePath, size_t size, bool isSSD) override
        {
            intptr_t handle = (intptr_t)INVALID_HANDLE_VALUE;
            StatusCode ret = OpenFile(filePath, size, handle);

            if (ret == StatusCode::OK)
                {
                AddFileHandle(handle, isSSD);
                }
                return ret;
            }

        std::pair<size_t, size_t> SpareCapacity(){
            Utilities::Share<SRWLOCK> guard{ m_handlesLock };
            return std::make_pair<size_t, size_t>(m_ssdHandles.size(), m_hddHandles.size());
        }

        StatusCode StartPartition(PartitionId id, bool isPrimary) override
        {
            FileHandle handle = (FileHandle)INVALID_HANDLE_VALUE;
            std::stack<FileHandle>& pool = isPrimary ? m_ssdHandles : m_hddHandles;

            StatusCode res = StatusCode::Unexpected;
            {
                Utilities::Exclude<SRWLOCK> guard{ m_handlesLock };
                if (pool.empty()){
                    Tracer::LogError(StatusCode::OutOfResources, 
                        L"No file handle available for new partition.");
                    return StatusCode::OutOfResources;
                }
                handle = pool.top();
                pool.pop();
            }
            {
                Utilities::Exclude<SRWLOCK> guard{ m_tableLock };
                if (m_partTable.find(id) != m_partTable.end())
                {
                    res = StatusCode::DuplicatedId;
                    Tracer::LogError(res, 
                        L"no duplicated partition id allowed on one server");
                }
                else {
                    m_partTable[id] = m_pPartitionCreater->CreatePartition(*this, id, handle, isPrimary);
                    m_gcQ.AddPartition(id);
                    m_partVisitor.AddPartition(id);
                    res = StatusCode::OK;
                }
            }
            if (res != StatusCode::OK && handle != (FileHandle)INVALID_HANDLE_VALUE){
                Utilities::Exclude<SRWLOCK> guard{ m_handlesLock };
                pool.push(handle);
            }
            return res;
        }

        StatusCode SetPartition(PartitionId pid, _In_ std::unique_ptr<Partition>& pPartition) override
        {
            StatusCode res = StatusCode::UnspecifiedError;

            Utilities::Exclude<SRWLOCK> guard{ m_tableLock };
            if (m_partTable.find(pid) != m_partTable.end())
            {
                res = StatusCode::DuplicatedId;
                Tracer::LogError(res,
                    L"no duplicated partition id allowed on one server");
            }
            else {
                m_partTable[pid] = std::move(pPartition);
                m_gcQ.AddPartition(pid);
                m_partVisitor.AddPartition(pid);
                res = StatusCode::OK;
            }

            return res;
        }

        void StopPartition(PartitionId pid) override
        {

            std::unique_ptr<Partition> pPart;
            {
                Utilities::Exclude<SRWLOCK> guard{ m_tableLock };

                auto citer = m_partTable.find(pid);
                if (citer == m_partTable.end())
                {
                    return;
                }

                pPart = std::move(citer->second);
                m_partTable.erase(citer);
            }
            Audit::Assert((bool)pPart, "Trying to stop an invalid partition!");

            wchar_t buf[256];
            auto numChars = swprintf_s(buf, L"Shutting down partition %016X",
                pid.Value());
            Audit::Assert(numChars < 256, "Buffer overflow in logging");
            Tracer::LogInfo(StatusCode::OK, buf);

            // this will stop new Read/Write and stop new
            // I/O issued by GC. Since we removed the partition
            // from the table, memory store spill should stop
            // too
            // Need to wait for pending I/O to finish
            // TODO!! SPILL FROM MEMORY STORE WILL CRASH THE PROGRAM?!!
            pPart->RequestShutdown();
            m_partCollector.AddPartition(std::move(pPart));
        }

        // Start to move value items from memory store to partitioned file stores
        void StartSweeper() override
        {
            GetMemoryStore()->StartSweeping();

            //TODO: this is temp. Eventually add another method for this
            m_pCleaner->Start();
        }

        // Start the GC of partitions file stores
        void StartGC() override
        {
            m_pGcActor->Start();
        }

        // When a file store gets full, call this
        // to have it garbage collected soon.
        void SendStoreToGc(PartitionId pid) override
        {
            m_gcQ.EmergencyCollect(pid);
        }

        ~IsolatedServer()
        {
            Utilities::Exclude<SRWLOCK> guard{ m_handlesLock };
            while (!m_ssdHandles.empty())
            {
                auto handle = m_ssdHandles.top();
                m_ssdHandles.pop();
                CloseHandle((HANDLE)handle);
            }
            while (!m_hddHandles.empty())
            {
                auto handle = m_hddHandles.top();
                m_hddHandles.pop();
                CloseHandle((HANDLE)handle);
            }
        }
    };

    std::unique_ptr<ExabytesServer> IsolatedServerFactory(
        std::unique_ptr<ServiceBrokers::ServiceBroker>& pBroker,
        std::unique_ptr<CoalescingBufferPool>& pBufferPool,
        std::unique_ptr<PolicyManager>& pPolicyManager,
        std::unique_ptr<CatalogBufferPool<>>& pHashPoolManager,
        std::unique_ptr<CatalogBufferPool<>>& pBloomKeyPoolManager,
        std::unique_ptr<PartitionCreater>& pPartitionCreater,
        std::unique_ptr<FileRecover>& pRecover,

        // partition hash function that maps key to partition
        PartitionId(*hashFunc)(const Key128),

        // factory for GcActor
		GcActor*(*gcFactory)(
            ExabytesServer& server,
            Activity& gcDriver,
            PartitionRotor& pStores
            )
        )
    {
        return std::make_unique<IsolatedServer>
            (pBroker, pBufferPool, pPolicyManager, pHashPoolManager, 
            pBloomKeyPoolManager, pPartitionCreater, pRecover, hashFunc, gcFactory);
    }
    
}
