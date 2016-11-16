#pragma once
#include "stdafx.h"

#include <atomic>
#include <WS2tcpip.h>

#include "CppUnitTest.h"
#include "ServiceBrokerRIO.hpp"
#include "SchedulerIOCP.hpp"
#include "UtilitiesWin.hpp"
#include "TestUtils.hpp"
#include "Rpc.hpp"


using namespace Microsoft::VisualStudio::CppUnitTestFramework;
using namespace Utilities;
using namespace Datagram;
using namespace Schedulers;
using namespace ServiceBrokers;

namespace EBTest
{

    // Runing on the client, triggered when Rpc is finished.
    class ResponseProc : public Continuation<BufferEnvelope>{
        std::atomic<int>& m_count;
    public:
        ResponseProc(_In_ Activity& act, _Out_ std::atomic<int>& count)
            : Continuation(act)
            , m_count(count)
        {}

        void Cleanup() override {}

        void OnReady(
            _In_ WorkerThread&,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            ) override
        {
            Audit::Assert((void*)continuationHandle == (void*)&m_postable,
                "Buffer expected!");
                // we have data
            Audit::Assert(length == m_postable.Contents()->Size() - m_postable.Contents()->Offset(),
                    "data buffer size mismatch");
            Trace("Response received on client side.!");
            m_count--;
            m_postable.Recycle();
            m_activity.RequestShutdown(StatusCode::OK);
        }

    };

    // Running on the server side to reply to req message
    class Sender : public Continuation<BufferEnvelope>{
        ServiceBroker& m_network;
    public:
        Sender(Activity& act, ServiceBroker& network)
            : Continuation(act)
            , m_network(network)
        {}

        void Cleanup() override
        {}

        void OnReady(
            _In_ WorkerThread&,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            ) override
        {
            Audit::Assert((void*)continuationHandle == (void*)&m_postable,
                "Unexpected parameter posted to test sender");
            Request* pHeader = reinterpret_cast<Request*>
                ((char*)m_postable.Contents()->PData() + m_postable.Contents()->Offset());
            uint32_t blobSize = m_postable.Contents()->Size() - m_postable.Contents()->Offset() - sizeof(Request);

            pHeader->Verb = ServiceVerb::Value;
            Trace("Sending response to the client.");
            m_network.Send(*pHeader, pHeader + 1, blobSize);

            m_postable.Recycle();
            m_activity.RequestShutdown(StatusCode::OK);
        }

    };

    // on the server side, reacting to incoming packets.
    class EchoActor : public ServiceBrokers::VerbWorker
    {
    private:
        ServiceBroker& m_network;
    public:
        EchoActor(ServiceBrokers::ServiceBroker& broker)
            : m_network(broker)
        {}

        BufferEnvelope AcquireBuffer(size_t msgSize) override
        {
            auto pb = m_network.GetScheduler()->Allocate((uint32_t)msgSize);
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

            auto pActivity = Schedulers::ActivityFactory
                (*m_network.GetScheduler(), L"RpcClient Test Responder");
            auto pSender = pActivity->GetArena()->allocate<Sender>
                (*pActivity, m_network);

            // delay respond for 2ms to test wait and retry.
            pSender->Receive(std::move(msg), msg.Contents()->Size(), 2000);
        }
    };

    TEST_CLASS(RpcClientTestSuite)
    {
    public:

        // Test of RpcClient implementation, the test setup a server and a client
        // RpcClient is attached to the client, from which a bunch of message is
        // sent via the RpcClient object. We verify all response are received
        //
        TEST_METHOD(RpcClientTest)
        {
            Tracer::InitializeLogging(L"D:\\data\\testlog.etl");
            Utilities::EnableLargePages();

            // Setup server
            auto pServer = ServiceBrokerRIOFactory(L"EchoServer");
            Trace(pServer->HomeAddressIP().c_str());
            Trace(pServer->HomeAddressPort().c_str());

            EchoActor readActor{ *pServer };
            pServer->BindVerb(Datagram::ServiceVerb::RemoteRead, readActor);

            // Setup Client
            auto pClient = ServiceBrokerRIOFactory(L"RpcClient");
            ServiceVerb responses[] = { ServiceVerb::Value };
            auto pRpc = ExpBackupRetryRpcClientFactory(*pClient,
                responses, sizeof(responses));


            uint32_t  serverip;
            inet_pton(AF_INET, pServer->HomeAddressIP().c_str(), &(serverip));
            serverip = ntohl(serverip);

            uint16_t serverport = static_cast<uint16_t>(atoi(pServer->HomeAddressPort().c_str()));


            // Send rpcs
            const size_t blobSize = 128 * SI::Ki;
            std::atomic<int> totalReqs = 0;

            for (int i = 0; i < 20; i++){
                DisposableBuffer* pMsgBuffer = pClient->GetScheduler()->Allocate
                    (sizeof(Request) + sizeof(Description) + blobSize);

                Request* pReq = reinterpret_cast<Request*>(pMsgBuffer->PData());
                pReq->Verb = ServiceVerb::RemoteRead;

                pReq->RemoteIPv4 = serverip;
                pReq->RemotePort = serverport;

                Description* pDesc = reinterpret_cast<Description*>
                    ((char*)pMsgBuffer->PData() + sizeof(Request));

                // Get a random blob to write to  server
                RandomWrites writes(1024, 64 * 1024);
                void* pBlob;
                writes.GenerateRandomRecord(*pDesc, pBlob);
                pDesc->ValueLength = min(blobSize, pDesc->ValueLength);
                memcpy_s(pDesc + 1, blobSize, pBlob, pDesc->ValueLength);

                auto pActivity = Schedulers::ActivityFactory
                    (*pServer->GetScheduler(), L"RpcSend");
                auto pNotify = pActivity->GetArena()->allocate<ResponseProc>
                    (*pActivity, totalReqs);

                totalReqs++;
                pRpc->SendReq(pNotify, pMsgBuffer);
            }

            for (int i = 0; i < 10 && totalReqs > 0; i++)
            {
                Sleep(1000);
            }

            Audit::Assert(totalReqs == 0, "Unable to finish all requests.");

            pRpc.release();
            pClient.release();
            pServer.release();
            Sleep(1000);
            Tracer::DisposeLogging();
        }
    };


}