#pragma once
#include "stdafx.h"

#include <iostream>
#include <utility>
#include <algorithm>
#include <random>
#include <vector>
#include <map>
#include <memory>
#include <unordered_map>

#include "Exabytes.hpp"
#include "ServiceBrokerRIO.hpp"
#include "Utilities.hpp"

#define WIN32_LEAN_AND_MEAN 1
#include <Windows.h>
#include <WinSock2.h>

namespace Utilities
{
    void EnableLargePages();
}

using namespace std;
using namespace Datagram;
using namespace Schedulers;
using namespace Exabytes;

class Sender : public Schedulers::Work
{
private:
    ServiceBrokers::ServiceBroker* m_pBroker;
    Utilities::DisposableBuffer* m_buf;
    ActionArena* m_pArena;
public:
    Sender(ServiceBrokers::ServiceBroker& broker, Utilities::DisposableBuffer* buffer, ActionArena* pArena)
        : m_pBroker(&broker)
        , m_buf(buffer)
        , m_pArena(pArena)
    {}

    void Run(
        _In_ WorkerThread&,
        _In_ intptr_t,
        _In_ uint32_t
        ) override
    {
        auto pRequest = reinterpret_cast<Request*>
            ((char*)m_buf->PData() + m_buf->Offset());

        m_pBroker->Send(*pRequest, pRequest + 1, m_buf->Size() - m_buf->Offset() - sizeof(Request));
        
        std::cout << "Echo package from " << std::hex << pRequest->RemoteIPv4 << std::dec <<
            ":" << pRequest->RemotePort << ", size " << m_buf->Size() - m_buf->Offset() << std::endl;
        m_buf->Dispose();
        m_pArena->Retire();
    }
};

class EchoActor : public ServiceBrokers::VerbWorker
{
private:
    ServiceBrokers::ServiceBroker* m_pBroker;
public:
    EchoActor(ServiceBrokers::ServiceBroker& broker)
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
        auto pRequest = reinterpret_cast<Request*>
            ((char*)msg.Contents()->PData() + msg.Contents()->Offset());
        Request header = *pRequest;

		ActionArena* pArena = m_pBroker->GetScheduler()->GetNewArena();
        auto pSender = pArena->allocate<Sender>(*m_pBroker, msg.ReleaseContents(), pArena);
        m_pBroker->GetScheduler()->Post(pSender, 0, 0);
    }
};


int _tmain(int, _TCHAR**)
{
    Tracer::InitializeLogging(L"D:\\data\\testlog.etl");
    std::wstring name{ L"UDP Test Server" };
    Utilities::EnableLargePages();
    std::unique_ptr<ServiceBrokers::ServiceBroker> pSB = ServiceBrokers::ServiceBrokerRIOFactory(name, 52491);
    std::cout << "Address: " << pSB->HomeAddressIP() << "  Port: " << pSB->HomeAddressPort() << std::endl;

    EchoActor readActor{ *pSB };
    pSB->BindVerb(Datagram::ServiceVerb::RemoteRead, readActor);

    std::cout << "Press Any Key to Stop" << std::endl;
    getchar();

    pSB.reset();
    Sleep(2000);
    Tracer::DisposeLogging();
    return 0;
}

