// FixedServerBroker.cpp
// Define a client broker for a fixed server. This is class
// is for test purpose. Most of the code should be migrate
// to a more mature client broker that query manager to
// get partition map.
//

#include "stdafx.h"

#include <future>

#include "Rpc.hpp"
#include "ServiceBrokerRIO.hpp"
#include "ClientBroker.hpp"
#include "UtilitiesWin.hpp"

// Local to file
namespace
{
    using namespace Datagram;
    using namespace Utilities;
    using namespace ServiceBrokers;
    using namespace Schedulers;

    class WriteAsyncNotifier : public ContinuationBase
    {
    private:
        std::promise<StatusCode>& m_promise;
    public:
        WriteAsyncNotifier(Activity& activity, std::promise<StatusCode>& promise)
            : ContinuationBase(activity)
            , m_promise(promise)
        {}

        void Cleanup() override
        {}

        void OnReady(
            _In_ WorkerThread&,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t
            ) override
        {
            m_promise.set_value((StatusCode)continuationHandle);
            Tracer::LogActionEnd(Tracer::EBCounter::ClientWrite, (StatusCode)continuationHandle);
            m_activity.RequestShutdown(StatusCode::OK);
        }

    };

    class ReadAsyncNotifier : public Continuation<BufferEnvelope>
    {
    private:
        std::promise<std::pair<StatusCode, DisposableBuffer*>>& m_promise;
    public:
        ReadAsyncNotifier(Activity& activity,
            std::promise<std::pair<StatusCode, DisposableBuffer*>>& promise
            )
            : Continuation(activity)
            , m_promise(promise)
        {}

        void OnReady(
            _In_ WorkerThread&,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            ) override
        {
            std::pair<StatusCode, DisposableBuffer*> payload;
            auto status = ((void*)continuationHandle == (void*)&m_postable) ?
                StatusCode::OK : (StatusCode)continuationHandle;

            if ((void*)continuationHandle == (void*)&m_postable)
            {
                // we have data
                Audit::Assert(length == m_postable.Contents()->Size() - m_postable.Contents()->Offset(),
                    "data buffer size mismatch");
                payload.first = StatusCode::OK;
                payload.second = m_postable.ReleaseContents();
            }
            else
            {
                Audit::Assert(length == 0, "no data expected");
                Audit::Assert(status != StatusCode::OK,
                    "expect error code when there is no data");
                payload.first = status;
                payload.second = nullptr;
            }
            m_promise.set_value(std::move(payload));
            Tracer::LogActionEnd(Tracer::EBCounter::ClientRead, status);
            m_postable.Recycle();
            m_activity.RequestShutdown(StatusCode::OK);
        }

        void Cleanup() override
        {}

    };

}

namespace ExaBroker
{

    // A dumb implementation that can only contact one
    // server. this is used as a placeholder for the
    // real one
    //
    class FixedServerBroker : public ClientBroker
    {
    private:

        const uint32_t m_serverIp;
        const uint16_t m_serverPort;

        std::unique_ptr<ServiceBroker> m_pBroker;

        std::unique_ptr<RpcClient> m_pRpc;

        StatusCode InternalWrite(
            // header of the value item, including key and owner info
            //
            Datagram::Description& header,

            // value blob
            //
            _In_ void* blob,

            // size of the blob, must be same with the length
            // in the header
            //
            size_t blobSize,

            // continuation to carry back result code
            // in the place of continuation handle
            ContinuationBase* pCompletion
            ) 
        {
			Tracer::LogActionStart(Tracer::EBCounter::ClientWrite, L"", pCompletion->m_activity.GetTracer());

            Audit::Assert(header.ValueLength < (1UL << 20),
                "values may not exceed 1MB size");
            Audit::Assert(header.ValueLength == blobSize, 
                "buffer size should be same with value length");

            DisposableBuffer* pMsgBuffer = m_pBroker->GetScheduler()->Allocate
                (uint32_t(sizeof(Request) + sizeof(Description) + blobSize));

            Request* pReq = reinterpret_cast<Request*>(pMsgBuffer->PData());

            pReq->RemoteIPv4 = m_serverIp;
            pReq->RemotePort = m_serverPort;
            pReq->Verb = ServiceVerb::RemoteWrite;

            Description* pDesc = reinterpret_cast<Description*>
                ((char*)pMsgBuffer->PData() + sizeof(Request));
            *pDesc = header;
            void* pValue = pDesc + 1;
            memcpy_s(pValue, 
                pMsgBuffer->Size() - sizeof(Request) - sizeof(Description), 
                blob, blobSize);

            return m_pRpc->SendReq(pCompletion, pMsgBuffer);
        }

        StatusCode InternalRead(
            Datagram::PartialDescription keyAndCaller,

            // Completion of read, override ProcessReadResult
            // to get result
            //
            ContinuationBase* pCompletion
            )
        {
			Tracer::LogActionStart(Tracer::EBCounter::ClientRead, L"", pCompletion->m_activity.GetTracer());

            DisposableBuffer* pMsgBuf = m_pBroker->GetScheduler()->Allocate( 
                sizeof(Request) + sizeof(PartialDescription));

            Request* pReq = reinterpret_cast<Request*>(pMsgBuf->PData());

            pReq->RemoteIPv4 = m_serverIp;
            pReq->RemotePort = m_serverPort;
            pReq->Verb = ServiceVerb::RemoteRead;

            PartialDescription* pDesc = reinterpret_cast<PartialDescription*>
                ((char*)pMsgBuf->PData() + sizeof(Request));
            *pDesc = keyAndCaller;

            return m_pRpc->SendReq(pCompletion, pMsgBuf);
        }

        StatusCode InternalDelete(
            Datagram::PartialDescription keyAndCaller,

            // Completion of delete
            ContinuationBase* pCompletion
            )
        {
			Tracer::LogActionStart(Tracer::EBCounter::ClientWrite, L"", pCompletion->m_activity.GetTracer());

            DisposableBuffer* pMsgBuf = m_pBroker->GetScheduler()->Allocate
                (sizeof(Request) + sizeof(PartialDescription));

            Request* pReq = reinterpret_cast<Request*>(pMsgBuf->PData());

            pReq->RemoteIPv4 = m_serverIp;
            pReq->RemotePort = m_serverPort;
            pReq->Verb = ServiceVerb::RemoteDelete;

            PartialDescription* pDesc = reinterpret_cast<PartialDescription*>
                ((char*)pMsgBuf->PData() + sizeof(Request));
            *pDesc = keyAndCaller;

            return m_pRpc->SendReq(pCompletion, pMsgBuf);
        }

    public:
        FixedServerBroker(uint32_t serverIp, uint16_t serverPort)
            : m_serverIp(serverIp)
            , m_serverPort(serverPort)
        {
            Tracer::InitializeLogging(L"D:\\data\\ExaBroker\\ExaBroker.etl");
            std::wstring name{ L"ExabyteClientBroker" };
            EnableLargePages();

            m_pBroker = ServiceBrokerRIOFactory(name);
            ServiceVerb responses[] = { ServiceVerb::Status, ServiceVerb::Value };
            m_pRpc = ExpBackupRetryRpcClientFactory(*m_pBroker, responses, sizeof(responses));
        }

        // Sync interface for writing
        //
        StatusCode Write(
            // header of the value item, including key and owner info
            //
            Description& header,

            // value blob
            //
            _In_ void* blob,

            // size of the blob, must be same with the length
            // in the header
            //
            size_t blobSize
            ) override
        {
            auto pActivity = Schedulers::ActivityFactory
                (*m_pBroker->GetScheduler(), L"ExaSyncWrite");

            std::promise<StatusCode> promise;
            std::future<StatusCode> resultFuture = promise.get_future();

            auto pNotify = pActivity->GetArena()->allocate<WriteAsyncNotifier>
                (*pActivity, promise);

            InternalWrite(header, blob, blobSize, pNotify);

            StatusCode result = resultFuture.get();
            return result;
        }

        // Async interface for writing
        //
        StatusCode Write(
            // header of the value item, including key and owner info
            //
            Datagram::Description& header,

            // value blob
            //
            _In_ void* blob,

            // size of the blob, must be same with the length
            // in the header
            //
            size_t blobSize,

            // continuation to carry back result code
            // in the place of continuation handle
            WriteCompletion* pCompletion
            ) override
        {
            return InternalWrite(header, blob, blobSize, pCompletion);
        }

        // Async interface for reading
        StatusCode Read(
            Datagram::PartialDescription keyAndCaller,

            // Completion of read, override ProcessReadResult
            // to get result
            //
            ReadCompletion* pCompletion
            ) override
        {
            return InternalRead(keyAndCaller, pCompletion);
        }

        StatusCode Read(
            Datagram::PartialDescription keyAndCaller,

            _Out_ std::string& buffer
            ) override
        {
            auto pActivity = Schedulers::ActivityFactory
                (*m_pBroker->GetScheduler(), L"SyncRead");

            std::promise<std::pair<StatusCode, DisposableBuffer*>> promise;
            std::future<std::pair<StatusCode, DisposableBuffer*>>
                resultFuture = promise.get_future();

            auto pNotify = pActivity->GetArena()->allocate<ReadAsyncNotifier>
                (*pActivity, promise);

            InternalRead(keyAndCaller, pNotify);

            auto result = resultFuture.get();
            if (result.first == StatusCode::OK)
                buffer.append((char*)result.second->PData() + result.second->Offset(), 
                result.second->Size() - result.second->Offset());

            if (result.second != nullptr)
            {
                result.second->Dispose();
            }

            return result.first;
        }

        StatusCode Read(
            Datagram::PartialDescription keyAndCaller,

            // buffer containing the value. caller should call
            // (*buffer)->Dispose() when done
            //
            _Out_ Utilities::DisposableBuffer** buffer
            ) override
        {
            std::promise<std::pair<StatusCode, DisposableBuffer*>> promise;
            std::future<std::pair<StatusCode, DisposableBuffer*>>
                resultFuture = promise.get_future();

            auto pActivity = Schedulers::ActivityFactory
                (*m_pBroker->GetScheduler(), L"SyncRead");
            auto pNotify = pActivity->GetArena()->allocate<ReadAsyncNotifier>
                (*pActivity, promise);

            InternalRead(keyAndCaller, pNotify);

            auto result = resultFuture.get();
            if (result.first == StatusCode::OK)
            {
                (*buffer) = result.second;
            }

            return result.first;
        }

        // Sync interface for deleting
        //
        StatusCode Delete(
            Datagram::PartialDescription keyAndCaller
            ) override
        {
            auto pActivity = Schedulers::ActivityFactory
                (*m_pBroker->GetScheduler(), L"ExaSyncDelete");

            std::promise<StatusCode> promise;
            std::future<StatusCode> resultFuture = promise.get_future();

            auto pNotify = pActivity->GetArena()->allocate<WriteAsyncNotifier>
                (*pActivity, promise);

            InternalDelete(keyAndCaller, pNotify);

            StatusCode result = resultFuture.get();
            return result;
        }

        // Async interface for reading
        StatusCode Delete(
            Datagram::PartialDescription keyAndCaller,

            // Completion of read, override ProcessReadResult
            // to get result
            //
            DeleteCompletion* pCompletion
            ) override
        {
            return InternalDelete(keyAndCaller, pCompletion);
        }


        ~FixedServerBroker()
        {
            Tracer::DisposeLogging();
        }
    };

    std::unique_ptr<ClientBroker> TestBrokerFactory(uint32_t serverIp, uint16_t serverPort)
    {
        return std::make_unique<FixedServerBroker>(serverIp, serverPort);
    }
}

