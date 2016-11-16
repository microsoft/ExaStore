#pragma once
#include "stdafx.h"

#include <utility>
#include <vector>
#include <memory>
#include <iostream>
#include <random>
#include <unordered_map>

#include "Datagram.hpp"
#include "Cuckoo.hpp"
#include "ServiceBrokerRIO.hpp"

#include "UtilitiesWin.hpp"

#include <ws2tcpip.h>

using namespace Datagram;
using namespace Utilities;
using namespace Schedulers;
using namespace ServiceBrokers;

// packet retry every 256 512 1024.. microseconds, retry 14 times
// so we have timeout around 4 seconds
static const unsigned MAX_RETRY = 14;

static DummyOp<Continuation<BufferEnvelope>*> s_noop;

// use trace id to map messages to their context
//
class RpcContextMap
{
private:
    struct KeyOfEntry
    {
        const Tracer::TraceRec& operator()(Continuation<BufferEnvelope>* h) const
        {
            return *(h->m_activity.GetTracer());
        }
    } m_keyDelegate;

    CuckooIndex <Tracer::TraceRec, Continuation<BufferEnvelope>*, nullptr, KeyOfEntry, true>
        m_cuckoo;

    SRWLOCK m_contextMapLock;

public:
    RpcContextMap()
        : m_cuckoo(8, m_keyDelegate, s_noop) // have 128 entries
    {
        InitializeSRWLock(&m_contextMapLock);
    }

    Continuation<BufferEnvelope>* FindContextFor(const Tracer::TraceRec& id)
    {
        Share<SRWLOCK> guard{ m_contextMapLock };
        return m_cuckoo.Find(id);
    }

    bool DeleteContextFor(const Tracer::TraceRec& id)
    {
        Exclude<SRWLOCK> guard{ m_contextMapLock };
        return m_cuckoo.Erase(id);
    }

    CuckooStatus CreateContextFor(
        _In_ const Tracer::TraceRec& id,
        _In_ Continuation<BufferEnvelope>* pCompletion)
    {
        Exclude<SRWLOCK> guard{ m_contextMapLock };
        return m_cuckoo.Insert(id, pCompletion);
    }

};

// an actor to send out request messages, and retry if needed.
//
class RetrySend : public Schedulers::ContinuationBase
{
private:
    RpcContextMap& m_map;

    ServiceBroker& m_net;

    // buffer holding the outgoing msg
    Request* m_pBuffer;

    uint32_t m_bufSize;

    unsigned retry = MAX_RETRY;

public:

    RetrySend(Schedulers::Activity& activity,
        RpcContextMap& context,
        ServiceBroker& broker,

        // Buffer containing outgoing message
        Request* pBuffer,
        uint32_t bufSize
        )
        : ContinuationBase(activity)
        , m_map(context)
        , m_net(broker)
        , m_pBuffer(pBuffer)
        , m_bufSize(bufSize)
    {
        Audit::Assert(m_pBuffer != nullptr, "no message to send?");
    }

    void Cleanup() override
    {
        if (m_pBuffer != nullptr)
        {
            m_map.DeleteContextFor(m_pBuffer->Tracer);
            m_pBuffer = nullptr;
        }
    }

    void OnReady(
        _In_ Schedulers::WorkerThread&,
        _In_ intptr_t,
        _In_ uint32_t
        ) override
    {
        Continuation<BufferEnvelope>* pNext = m_map.FindContextFor(m_pBuffer->Tracer);

        if (pNext == nullptr)
        {
            // someone finished the job
            Audit::Assert(retry < MAX_RETRY, "we have not send the msg yet!");
            m_pBuffer = nullptr;
            return;
        }

        // response not received yet
        if (retry == 0)
        {
            // failed

            if (m_map.DeleteContextFor(m_pBuffer->Tracer))
            {
                // we successfully get hold of the notification
                Tracer::RPCClientTimeout(&m_pBuffer->Tracer);
                pNext->Post((intptr_t)StatusCode::TimedOut, 0);
                pNext = nullptr;
            } // other wise someone else get the token and its their job then
            m_pBuffer = nullptr;
            return;
        }

        // resend message and come back later
        Tracer::RPCClientSend(&m_pBuffer->Tracer);
        m_net.Send(*m_pBuffer, m_pBuffer + 1, m_bufSize - sizeof(Request));
        this->Post(0, 0, 256 << (MAX_RETRY - retry));
        retry--;
    }
};


class ResponseWorker : public ServiceBrokers::VerbWorker
{
private:
    RpcContextMap& m_map;
    ServiceBroker& m_net;

    // no assignment
    ResponseWorker& operator=(const ResponseWorker&) = delete;
public:
    ResponseWorker(RpcContextMap& map,
        ServiceBroker&  net
        )
        : m_map(map)
        , m_net(net)
    {}

    BufferEnvelope AcquireBuffer(size_t msgSize) override
    {
        Audit::Assert(msgSize < 2 * SI::Gi, "packet size too big");

        auto pb = m_net.GetScheduler()->Allocate((uint32_t)msgSize);
        return BufferEnvelope(pb);
    }

    void DoWork(
        _In_ BufferEnvelope msg
        ) override
    {
        auto pRequest = reinterpret_cast<Request*>
            ((char*)msg.Contents()->PData() + msg.Contents()->Offset());
        Tracer::TraceRec tracer = pRequest->Tracer;

        Continuation<BufferEnvelope>* pNext = m_map.FindContextFor(tracer);

        if (pNext != nullptr && m_map.DeleteContextFor(tracer))
        {
            // we successfully get hold of the notification
            // now dispatch it and successfully return

            pNext->Receive(std::move(msg), msg.Contents()->Size());

            Tracer::RPCClientReceive(&tracer);

            return;
        }
        // we failed 
        Tracer::LogWarning(StatusCode::InvalidHandle, L"Response processing lost in race", &tracer);
        return;

    }
};


// Reaction for receiving response.
// m_sent points to the buffer of sent message, in this tests
// those buffers do not get reclaimed.

class RcvActor : public Continuation<BufferEnvelope>
{
private:
    Request* m_sent;
    uint32_t m_size;

public:
    RcvActor(Activity& act, Request* sent, uint32_t size)
        : Continuation(act), m_sent(sent), m_size(size)
    { }

    void Cleanup() override {}

    void OnReady(
        _In_ Schedulers::WorkerThread&,
        _In_ intptr_t       continuationHandle,
        _In_ uint32_t       length
        ) override
    {
        if ((void*)continuationHandle == (void*)&m_postable)
        {
            auto wrap = m_postable.Contents();
            auto blob = (char*)wrap->PData() + wrap->Offset();
            std::cout << "Message received! expected: " << m_size << " received: " << wrap->Size() - wrap->Offset() << std::endl;

            if (wrap->Size() - wrap->Offset() != m_size ){
                Audit::OutOfLine::Fail("Size Mismatch");
            }
            Audit::Assert(0 == memcmp((char*)m_sent + sizeof(Request), blob+sizeof(Request), m_size - sizeof(Request)),
                "data corrupted!");
        }
        else
        {
            auto status = (StatusCode)continuationHandle;

            Audit::Assert(length == 0, "Error code does not come with data");
            Audit::Assert(status != StatusCode::OK,
                "expect error code when there is no data");

            if (status == StatusCode::TimedOut){
                std::cout << "Message timedout! expected: " << m_size - sizeof(Request) << std::endl;
            }
            else {
                Audit::OutOfLine::Fail(status, "Unexpected error code");
            }
        }
        m_activity.RequestShutdown(StatusCode::OK);
    }
};


int _tmain(int argc, _TCHAR* argv[])
{
    if (argc != 4)
    {
        std::cout << "Usage: UdpTestClient <remote ip> <port> <packet_size>" << std::endl;
        return -1;
    }

    sockaddr_in server;
    server.sin_family = AF_INET;
    InetPton(AF_INET, argv[1], &server.sin_addr);
    server.sin_port = htons(static_cast<uint16_t>(_wtoi(argv[2])));

    uint32_t PacketSize = _wtoi(argv[3]);
    if (PacketSize > 65536 * 100)
    {
        std::cout << "Packet size too big!";
        PacketSize = 65536 * 100;
    }

    std::random_device m_seed;
    std::uniform_int_distribution<uint64_t> m_r;

    std::vector<std::pair<Request*, uint32_t>> tosend;
    while (PacketSize > 32767)
    {
        void* buf = malloc(PacketSize & (0 - sizeof(uint64_t)));
        uint64_t* pU64 = (uint64_t*)buf;
        uint64_t* pBeyond = (uint64_t*)buf + (PacketSize / sizeof(uint64_t) - 1);
        while (pU64 < pBeyond)
        {
            *pU64++ = m_r(m_seed);
        }

        Request* pReq = reinterpret_cast<Request*>(buf);
        pReq->Verb = ServiceVerb::RemoteRead;
        pReq->RemoteIPv4 = ntohl(server.sin_addr.S_un.S_addr);
        pReq->RemotePort = ntohs(server.sin_port);

        tosend.push_back(std::make_pair(pReq, PacketSize & (0 - 8)));

        PacketSize = PacketSize / 2;
    }

    WSADATA wsaData;
    int iResult = ::WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (0 != iResult) {
        auto error = ::WSAGetLastError();
        Audit::OutOfLine::Fail((StatusCode)error, "Failed to set up WSA.");
    }


    Tracer::InitializeLogging(L"D:\\data\\testlog.etl");
    RpcContextMap map;

    std::wstring name{ L"home service" };
    Utilities::EnableLargePages();
    std::unique_ptr<ServiceBrokers::ServiceBroker> pSB = ServiceBrokers::ServiceBrokerRIOFactory(name);
    std::cout << "Address: " << pSB->HomeAddressIP() << "  Port: " << pSB->HomeAddressPort() << std::endl;

    ResponseWorker readActor{ map, *pSB };
    pSB->BindVerb(Datagram::ServiceVerb::RemoteRead, readActor);

    for (int i = 0; i<1000000; i++){
        for (auto snd : tosend)
        {
            Request* buf = snd.first;
            uint32_t size = snd.second;
            //Description* pDesc = reinterpret_cast<Description*>((char*)buf + sizeof(Request));

            auto pActivity = Schedulers::ActivityFactory
                (*pSB->GetScheduler(), L"UDP Test");
            buf->Tracer = *pActivity->GetTracer();
            auto pNotify = pActivity->GetArena()->allocate < RcvActor >
                (*pActivity, buf, size);

            auto s = map.CreateContextFor(*pActivity->GetTracer(), pNotify);
            Audit::Assert(s == CuckooStatus::Ok,
                "Failed to create context for sending message");

            auto pSender = pActivity->GetArena()->allocate<RetrySend>(
                *pActivity, map, *pSB, buf, size);
            pSender->Post(0, 0);
            std::cout << "Message sent! size: " << size - sizeof(Request) << std::endl;

        }
        Sleep(8000);
    }

    std::cout << "Finished!" << std::endl;
    // TODO!! I realy want to make sure at this point the server or client's available
    // RIO buffer is full, to check for resources leak. I have been using debugger to 
    // do that. But how to achieve that automatically?
    pSB.reset();
    Sleep(1000);
    Tracer::DisposeLogging();
    return 0;
}

