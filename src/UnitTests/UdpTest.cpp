#pragma once
#include "stdafx.h"

#include <utility>
#include <algorithm>
#include <random>
#include <unordered_map>
#include <memory>
#include <iostream>

#include "Utilities.hpp"
#include "ServiceBrokerRIO.hpp"
#include "SchedulerIOCP.hpp"

#include "UtilitiesWin.hpp"

#include <winsock2.h>
#include <ws2tcpip.h>
#include <Ws2def.h>
#include <Mswsock.h>
#include <iphlpapi.h>

#include "TestUtils.hpp"
#include "TestHooks.hpp"

#include "CppUnitTest.h"

using namespace Microsoft::VisualStudio::CppUnitTestFramework;

namespace EBTest
{
    using namespace std;
    using namespace Utilities;
    using namespace Datagram;
    using namespace Exabytes;
    using namespace Schedulers;
    using namespace ServiceBrokers;

    const int PACKETSIZE = ServiceBroker::DATAGRAMSIZE;

    class EchoProcessor : public VerbWorker
    {
    private:
        ServiceBroker* m_pBroker;
    public:
        EchoProcessor(ServiceBroker& broker)
            : m_pBroker(&broker)
        {}

        BufferEnvelope AcquireBuffer(size_t msgSize) override
        {
            auto pb = m_pBroker->GetScheduler()->Allocate((uint32_t)msgSize);
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

            m_pBroker->Send(*pRequest, pRequest+1, (uint32_t)(msgSize));

        }
    };

    // Runing on the client, triggered when Rpc is finished.
    class ResponseProcessor : public VerbWorker
    {
        std::atomic<int>& m_count;
        ServiceBroker* m_pBroker;
    public:
        ResponseProcessor(ServiceBroker& broker, _Out_ std::atomic<int>& count)
            : m_pBroker(&broker)
            , m_count(count)
        {}

        BufferEnvelope AcquireBuffer(size_t msgSize) override
        {
            auto pb = m_pBroker->GetScheduler()->Allocate((uint32_t)msgSize);
            return BufferEnvelope(pb);
        }

        void DoWork(
            _In_ BufferEnvelope msg
            ) override
        {
            // we have data
            Trace("Response received on client side.!");
            m_count--;
        }

    };


    TEST_CLASS(UdpTest)
    {
    public:

        TEST_METHOD(EchoTest)
        {
            Tracer::InitializeLogging(L"D:\\data\\testlog.etl");
            std::wstring name{ L"home service" };
            Utilities::EnableLargePages();
            std::unique_ptr<ServiceBroker> pSB = ServiceBrokerRIOFactory(name);
            Trace(pSB->HomeAddressIP().c_str());
            Trace(pSB->HomeAddressPort().c_str());

            // TODO!! write a echo processor and register with pSB
            EchoProcessor readActor{ *pSB };
            pSB->BindVerb(Datagram::ServiceVerb::RemoteRead, readActor);

            char buf[PACKETSIZE];
            Datagram::Request* pReq = reinterpret_cast<Datagram::Request*>(&buf);
            pReq->Verb = ServiceVerb::RemoteRead;
            pReq->Sequence = 0;
            Description* pDesc = reinterpret_cast<Description*>(buf + sizeof(Request));

            // Get a random blob to write to udp server
            RandomWrites writes(1024, 64 * 1024);
            void* pBlob;
            writes.GenerateRandomRecord(*pDesc, pBlob);
            uint32_t sizeLimit = PACKETSIZE - sizeof(Request) - sizeof(Description) - 8;
            pDesc->ValueLength = min(sizeLimit, pDesc->ValueLength);
            pDesc->ValueLength = pDesc->ValueLength & (0 - 16);
            memcpy_s(buf + sizeof(Request) + sizeof(Description), sizeLimit, pBlob, pDesc->ValueLength);

            uint32_t leftcrc = 0;
            uint32_t rightcrc = 0;
            ComputeCRC(buf, pDesc->ValueLength + sizeof(Request) + sizeof(Description),
                buf, pDesc->ValueLength + sizeof(Request) + sizeof(Description),
                leftcrc, rightcrc);
            auto crcptr = reinterpret_cast<uint32_t*>((char*)buf + pDesc->ValueLength + sizeof(Request) + sizeof(Description));
            *crcptr = leftcrc;
            crcptr++;
            *crcptr = rightcrc;


            SOCKET s = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
            if (s == INVALID_SOCKET)
            {
                auto error = ::WSAGetLastError();
                Audit::OutOfLine::Fail((StatusCode)error, "Failed to create client udp socket.");
            }

            sockaddr_in client;

            client.sin_family = AF_INET;
            inet_pton(AF_INET, pSB->HomeAddressIP().c_str(), &(client.sin_addr.s_addr));
            client.sin_port = 0;
            if (client.sin_addr.S_un.S_addr == htonl(INADDR_ANY)){
                client.sin_addr.S_un.S_addr = htonl(INADDR_LOOPBACK);
            }

            sockaddr_in server;
            server.sin_family = AF_INET;
            server.sin_addr.s_addr = client.sin_addr.s_addr;
            server.sin_port = htons(static_cast<uint16_t>(atoi(pSB->HomeAddressPort().c_str())));

            if (::bind(s, (sockaddr*)&client, sizeof(client)) != 0)
            {
                auto error = ::WSAGetLastError();
                Audit::OutOfLine::Fail((StatusCode)error, "Failed to bind client upd socket.");
            }

            int ret = ::sendto(s, buf, pDesc->ValueLength + sizeof(Request) + sizeof(Description) + 8, 0, (sockaddr*)&server, sizeof(server));
            if (ret == SOCKET_ERROR){
                auto error = ::WSAGetLastError();
                Audit::OutOfLine::Fail((StatusCode)error, "Udp Send failure.");
            }
            else if (ret < pDesc->ValueLength + sizeof(Request)+sizeof(Description)) {
                Audit::OutOfLine::Fail("Partial Send");
            }
            
            Sleep(3000);
            char recvBuf[PACKETSIZE];
            ret = ::recv(s, recvBuf, PACKETSIZE, 0);
            if (ret == SOCKET_ERROR){
                auto error = ::WSAGetLastError();
                Audit::OutOfLine::Fail((StatusCode)error, "Udp Receive failure.");
            }
            else if (ret < pDesc->ValueLength + sizeof(Request)+sizeof(Description)) {
                Audit::OutOfLine::Fail("Partial Receive");
            }

            Audit::Assert(0 == memcmp(buf+sizeof(Request), recvBuf+sizeof(Request), pDesc->ValueLength + sizeof(Description)), "echo failure");

            pSB.release();
            Sleep(1000);
            Tracer::DisposeLogging();
        }


        TEST_METHOD(PingPongTest)
        {
            Tracer::InitializeLogging(L"D:\\data\\testlog.etl");
            Utilities::EnableLargePages();

            // Setup server
            auto pServer = ServiceBrokerRIOFactory(L"EchoServer");
            Trace(pServer->HomeAddressIP().c_str());
            Trace(pServer->HomeAddressPort().c_str());

            EchoProcessor readActor{ *pServer };
            pServer->BindVerb(Datagram::ServiceVerb::RemoteRead, readActor);

            // Setup Client
            auto pClient = ServiceBrokerRIOFactory(L"RpcClient");
            std::atomic<int> totalReqs = 0;
            ResponseProcessor responseActor{ *pClient, totalReqs };
            pClient->BindVerb(ServiceVerb::RemoteRead, responseActor);

            uint32_t  serverip;
            inet_pton(AF_INET, pServer->HomeAddressIP().c_str(), &(serverip));
            serverip = ntohl(serverip);

            uint16_t serverport = static_cast<uint16_t>(atoi(pServer->HomeAddressPort().c_str()));

            const uint32_t blobSize = 128 * SI::Ki;

            for (int i = 0; i < 20; i++){
                DisposableBuffer* pMsgBuffer = pClient->GetScheduler()->Allocate
                    (sizeof(Request) + sizeof(Description) + blobSize);

                Request* pReq = reinterpret_cast<Request*>(pMsgBuffer->PData());
                pReq->Tracer = Tracer::TraceRec(L"");
                pReq->Verb = ServiceVerb::RemoteRead;

                pReq->RemoteIPv4 = serverip;
                pReq->RemotePort = serverport;

                Description* pDesc = reinterpret_cast<Description*>
                    ((char*)pMsgBuffer->PData() + sizeof(Request));

                // Get a random blob to write to  server
                RandomWrites writes(1024, 100 * 1024);
                void* pBlob;
                writes.GenerateRandomRecord(*pDesc, pBlob);
                pDesc->ValueLength = min(blobSize, pDesc->ValueLength);
                memcpy_s(pDesc + 1, blobSize, pBlob, pDesc->ValueLength);

                totalReqs++;
                pClient->Send(*pReq, pDesc, blobSize + sizeof(Description));
                pMsgBuffer->Dispose();
            }

            for (int i = 0; i < 1000 && totalReqs > 0; i++)
            {
                Sleep(100);
            }

            Audit::Assert(totalReqs == 0, "Unable to finish all requests.");

            pClient.release();
            pServer.release();
            Sleep(1000);
            Tracer::DisposeLogging();
        }

    };

}
