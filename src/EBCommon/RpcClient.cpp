// RpcClient.cpp
// Default implementation of RpcClient interface.
//

#include "stdafx.h"

#include "Rpc.hpp"
#include "ServiceBrokerRIO.hpp"
#include "Cuckoo.hpp"
#include "UtilitiesWin.hpp"

// Local to file
namespace
{
    using namespace Datagram;
    using namespace Utilities;
    using namespace ServiceBrokers;
    using namespace Schedulers;

    // packet retry every 4096, 8192... microseconds, 
    static const unsigned MAX_RETRY = 8;
    static Utilities::DummyOp<ContinuationBase*> s_noop;

    // use trace id to map messages to their context
    //
    class RpcClientContextMap
    {
    private:
        // number of entries for the map should be alway power of 2,
        // here 10 means 1024 entries.
        static const size_t LOG_ENTRIES = 10;

        struct KeyOfEntry
        {
            const Tracer::TraceRec& operator()(ContinuationBase* h) const
            {
                return *(h->m_activity.GetTracer());
            }
        } m_keyDelegate;

        CuckooIndex <Tracer::TraceRec,
                     ContinuationBase*, 
                     nullptr,
                     KeyOfEntry, 
                     true> m_cuckoo;


        SRWLOCK m_contextMapLock;

    public:
        RpcClientContextMap()
            : m_cuckoo(LOG_ENTRIES, m_keyDelegate, s_noop) // have 1024 entries
        {
            InitializeSRWLock(&m_contextMapLock);
        }

        ContinuationBase* FindContextFor(const Tracer::TraceRec& id)
        {
            Share<SRWLOCK> guard{ m_contextMapLock };
            return m_cuckoo.Find(id);
        }

        ContinuationBase* DeleteContextFor(const Tracer::TraceRec& id)
        {
            Exclude<SRWLOCK> guard{ m_contextMapLock };
            auto ret = m_cuckoo.Find(id);
            if (ret != nullptr)
            {
                auto status = m_cuckoo.Erase(id);
                Audit::Assert(status, "Failed to delete found item from cuckoo hash!");
            }
            //TODO!! REMOVE THIS VALIDATION AFTER EXTENSIVE TESTING!
            Audit::Assert(!m_cuckoo.Erase(id), "Double Erase Should Not Be Successful!");
            return ret;
        }

        CuckooStatus CreateContextFor(
            _In_ const Tracer::TraceRec& id,
            _In_ ContinuationBase* pCompletion)
        {
            Exclude<SRWLOCK> guard{ m_contextMapLock };
            return m_cuckoo.Insert(id, pCompletion);
        }

    };

    // Dispatch all the response messages. Currently the only way to
    // distinguish a req message from a response one is by ServiceVerb
    //
    class ResponseActor : public VerbWorker
    {
    private:
        RpcClientContextMap& m_context;
        ServiceBroker& m_broker;

        // Copy and Assignment not allowed
        ResponseActor& operator=(const ResponseActor&) = delete;
        ResponseActor(ResponseActor&) = delete;

    public:
        ResponseActor(
            _In_ RpcClientContextMap& contextMap,
            _In_ ServiceBroker& broker
            )
            : m_context(contextMap)
            , m_broker(broker)
        {}

        BufferEnvelope AcquireBuffer(size_t msgSize) override
        {
            Audit::Assert(msgSize < 2 * SI::Gi, "packet size too big");
            auto pb = m_broker.GetScheduler()->Allocate((uint32_t)msgSize);
            return BufferEnvelope(pb);
        }

        void DoWork(
            _In_ BufferEnvelope msg
            ) override
        {
            auto pRequest = reinterpret_cast<Request*>
                ((char*)msg.Contents()->PData() + msg.Contents()->Offset());
            auto msgSize = msg.Contents()->Size() - msg.Contents()->Offset() - sizeof(Request);

            Audit::Assert(msgSize < 2 * SI::Gi, "packet size too big");
            Tracer::TraceRec tracer = pRequest->Tracer;

            // check tracer against states map to find promise
            ContinuationBase* pNext = m_context.DeleteContextFor(tracer);
            if (pNext == nullptr)
            {
                // we failed to get hold of context
                return;
            }

            // we are the one who grab and deleted the context, so we carry over the task
            Audit::Assert(*pNext->m_activity.GetTracer() == pRequest->Tracer,
                "TraceId mismatch with reply handler.");
            Audit::Assert(!pNext->m_activity.IsShuttingDown(),
                "Rpc unexpectedly interruptted when response is received.");
            // we successfully get hold of the notification
            // now dispatch it and successfully return

            if (pRequest->Verb == ServiceVerb::Status)
            {
                const StatusCode* pCode =
                    reinterpret_cast<const StatusCode*>(pRequest + 1);
                pNext->Post((intptr_t)*pCode, 0);
            }
            else {
                msg.Contents()->SetOffset(msg.Contents()->Offset() + sizeof(Request));
                Continuation<BufferEnvelope>* pCarrier =
                    dynamic_cast<Continuation<BufferEnvelope>*>(pNext);
                Audit::Assert(pCarrier != nullptr,
                    "continuation can not carry response message!");

                pCarrier->Receive(std::move(msg), (uint32_t)msgSize);
            }
            Tracer::RPCClientReceive(&tracer);
            return;
        }
    };


    // A continuation to send out request messages, and retry if needed.
    // retry delay increased exponentially
    //
    class RetrySend : public ContinuationBase
    {
    private:
        RpcClientContextMap& m_context;
        ServiceBroker& m_broker;

        // buffer holding the outgoing msg
        DisposableBuffer* m_pBuffer;

        unsigned m_retry = MAX_RETRY;
    public:

        RetrySend(
            _In_ Schedulers::Activity& activity,
            _In_ RpcClientContextMap& contextMap,
            _In_ ServiceBroker& broker,

            // Buffer containing outgoing message
            DisposableBuffer* pBuffer
            )
            : ContinuationBase(activity)
            , m_context(contextMap)
            , m_broker(broker)
            , m_pBuffer(pBuffer)
        {
            Audit::Assert(m_pBuffer != nullptr, "no message to send?");
            Audit::Assert(m_pBuffer->PData() != nullptr,
                "Invalid buffer found when sending RPC req!");
        }

        void Cleanup() override
        {
            Audit::Assert(m_activity.IsShuttingDown(),
                "Rpc req send canceled with no reason.");
            Audit::Assert(m_retry < MAX_RETRY, "Rpc canceled without first try!");
            if (m_pBuffer != nullptr)
            {
                Request* pReq = reinterpret_cast<Request*>(m_pBuffer->PData());
                auto tracer = pReq->Tracer;
                m_pBuffer->Dispose();
                m_pBuffer = nullptr;

                ContinuationBase* pNext = m_context.FindContextFor(tracer);
                Audit::Assert(pNext == nullptr, 
                    "Rpc Aborted before timeout or receiving response!");
            }
        }

        // This method is called by the scheduler when it's this Activity's turn.
        // Define actual work in this method;
        //
        void OnReady(_In_ WorkerThread&, _In_ intptr_t, _In_ uint32_t) override
        {   
            Request* pReq = reinterpret_cast<Request*>(m_pBuffer->PData());
            ContinuationBase* pNext = nullptr;

            if (m_retry == 0)
            {
                // timed out
                pNext = m_context.DeleteContextFor(pReq->Tracer);
                if (pNext != nullptr){
                    Tracer::RPCClientTimeout(&pReq->Tracer);
                    pNext->Post((intptr_t)StatusCode::TimedOut, 0);
                    pNext = nullptr;
                } // otherwise someone else get the token and its their job then
                Audit::Assert(*m_activity.GetTracer() == pReq->Tracer,
                    "TraceId changed while sending rpc req.");
                m_pBuffer->Dispose();
                m_pBuffer = nullptr;
                return;
            }

            // Not timed out yet, try to resend if we did not receive
            // response
            pNext = m_context.FindContextFor(pReq->Tracer);
            Audit::Assert(*m_activity.GetTracer() == pReq->Tracer,
                "TraceId changed while sending rpc req.");

            if (pNext == nullptr)
            {
                // someone finished the job
                Audit::Assert(m_retry < MAX_RETRY, "we have not send the msg yet!");
                m_pBuffer->Dispose();
                m_pBuffer = nullptr;
                return;
            }

            // resend message and come back later
            Tracer::RPCClientSend(&pReq->Tracer);
            m_broker.Send(*pReq, pReq + 1, m_pBuffer->Size() - sizeof(Request));
            this->Post(0, 0, 4096 << (MAX_RETRY - m_retry));
            m_retry--;
        }
    };

}

namespace Utilities {



    // Default implementation of RpcClient, which does exponential backup
    // retry, and bind to verb Status and Value for response to read
    // and write request.
    //
    class ExpBackupRetryClient : public RpcClient {
        RpcClientContextMap m_contextMap;

        ServiceBroker& m_broker;

        ResponseActor m_responseActor;

    public:
        ExpBackupRetryClient(
            _In_ ServiceBroker& broker,
            _In_ const ServiceVerb responseVerbs[],
            _In_ const size_t numVerbs
            )
            : m_broker(broker)
            , m_responseActor(m_contextMap, broker)
        {
            Audit::Assert(responseVerbs != nullptr,
                "Response messages can not be processed without proper verbs.");
            for (int i = 0; i < numVerbs; i++){
                m_broker.BindVerb(responseVerbs[i], m_responseActor);
            }
        }

        // Send a RPC request to a remote server,
        //
        StatusCode SendReq(
            // Triggered when finished
            //
            Schedulers::ContinuationBase* pCompletion,

            // Outgoing message. Buffer is released when call is finished.
            // The buff should begin with a Request structure, where the 
            // caller specifies remote address and action using RemoteIPv4
            // RemotePort and Verb fields
            //
            DisposableBuffer* pMsgBuf
            ) override
        {
            Audit::Assert(pCompletion != nullptr,
                "not supported yet, will disable message send");

            Activity& job = pCompletion->m_activity;

            Request* pReq = reinterpret_cast<Request*>(pMsgBuf->PData());

            pReq->Tracer = *job.GetTracer();

            // create rpc context pContext->m_pNotify will be triggered
            // when response is received.
            auto s = m_contextMap.CreateContextFor(pReq->Tracer, pCompletion);
            Audit::Assert(s == CuckooStatus::Ok,
                "Failed to create context for sending message");

            auto pSender = job.GetArena()->allocate<RetrySend>(
                job, m_contextMap, m_broker, pMsgBuf);
            pSender->Post(0, 0);

            return StatusCode::OK;
        }

    };

    // Construct a RpcClient with exponential backup retry, and Cuckoo map
    // as context match.
    //
    std::unique_ptr<RpcClient> ExpBackupRetryRpcClientFactory(
        // network module
        _In_ ServiceBroker& broker,

        // Service Verbs that are considered responses
        // for the rpc req sent.
        _In_ const ServiceVerb responseVerbs[],
        _In_ const size_t numVerbs
        )
    {
        return std::make_unique<ExpBackupRetryClient>(broker,
            responseVerbs, numVerbs);
    }


}