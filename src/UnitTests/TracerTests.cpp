#pragma once
#include "stdafx.h"

#include "Exabytes.hpp"

#include "UtilitiesWin.hpp"
#include "ServiceBrokerRIO.hpp"

#include "CppUnitTest.h"

#include<iostream>

using namespace Microsoft::VisualStudio::CppUnitTestFramework;
using namespace Utilities;

namespace Tracer
{
    const TraceRec* GetCurrentTraceHeader();
}

namespace EBServerTest
{
    ///////////////////////////
    // This is the test for Tracer functionality.
    // Also serves an example on how to instrument network request/response
    // library to enable distributed tracing. The goal is to instrument a
    // small portion of the program (network and thread pooling library),
    // so that the rest of the program can use it for free. Reducing programmer
    // work at the same time improving consistency.
    //
    bool tracingFinished = false;

    class NameContext{
        Schedulers::ActionArena* m_pArena;
        wchar_t* m_pName;
    public:
 
        NameContext(Schedulers::Activity& activity)
            :m_pArena(activity.GetArena())
        {}

        const wchar_t* GetName() const
        {
            return m_pName;
        }

        void SetName(const wchar_t* name)
        {
            size_t len = wcslen(name);
            m_pName = (wchar_t *)m_pArena->allocateBytes((len+1)*sizeof(wchar_t));
            memcpy_s(m_pName, len + 1, name, len + 1);
        }

    };

    class TraceEnder : public Schedulers::ContinuationWithContext<NameContext, Schedulers::ContextIndex::BaseContext>
    {
    public:
        TraceEnder(Schedulers::Activity& activity)
            : ContinuationWithContext<NameContext, Schedulers::ContextIndex::BaseContext>(activity)
        {}

        void Cleanup() override
        {}

        void OnAvailable(
            _In_ Schedulers::WorkerThread&  thread,
            _In_ size_t*  pData
            )
        {
            Audit::NotImplemented();
        }

        void OnReady(
            _In_ Schedulers::WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       messageLength
            )
        {
            // Test Oracle, make sure we are in root span now
            Audit::Assert(Tracer::GetCurrentTraceHeader()->m_parentSpanId == 0, "Test should finish in root span!");
            Audit::Assert(!Tracer::GetCurrentTraceHeader()->m_traceId.IsEmpty(), "Test should not finish in empty span!");

            Tracer::LogDebug(StatusCode::OK, GetContext()->GetName());
            Tracer::EndSpan();
            m_activity.RequestShutdown(StatusCode::OK);
            tracingFinished = true;
        }

    };

    struct RPCContext
    {
        std::wstring m_name;
        Schedulers::ContinuationBase* m_callback;
    };


    // This actor simulate an async thread handling call back from an RPC return.
    class RPCHandler : public Schedulers::ContinuationWithContext<RPCContext, Schedulers::ContextIndex::BaseContext>
    {
    public:
        RPCHandler(Schedulers::Activity& activity, const wchar_t* name, ContinuationBase* pNext)
            : Schedulers::ContinuationWithContext<RPCContext, Schedulers::ContextIndex::BaseContext>(activity)
        {
            RPCContext* pContext = m_activity.GetArena()->allocate<RPCContext>();
            pContext->m_callback = pNext;
            pContext->m_name.assign(name, name + wcslen(name));
            SetContext(pContext);
        }

        void OnAvailable(
            _In_ Schedulers::WorkerThread&  thread,
            _In_ size_t*  pData
            )
        {
            Audit::NotImplemented();
        }

        void Cleanup() override {}

        void OnReady(
            _In_ Schedulers::WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t         messageLength
            )
        {
            // Test Oracle, make sure we are in sub span now
            Audit::Assert(Tracer::GetCurrentTraceHeader()->m_parentSpanId != 0, "RPC should be in sub span!");

            Tracer::LogDebug(StatusCode::OK, GetContext()->m_name.c_str());
            Tracer::RPCClientReceive();
            Tracer::EndSpan();

            // this is the end of the RPC span, the continuation should be in parent span
            GetContext()->m_callback->Post(0, 0);

            m_activity.RequestShutdown(StatusCode::OK);
        }
    };

    class TraceSpliter : public Schedulers::ContinuationWithContext<NameContext, Schedulers::ContextIndex::BaseContext>
    {
    public:
        TraceSpliter(Schedulers::Activity& activity)
            : ContinuationWithContext<NameContext, Schedulers::ContextIndex::BaseContext>(activity)
        {
        }

        void OnAvailable(
            _In_ Schedulers::WorkerThread&  thread,
            _In_ size_t*  pData
            )
        {
            Audit::NotImplemented();
        }

        void Cleanup() override
        {}

        // This actor start a new span, and fork into two 
        // basically simulating a starting point of an RPC, where we prepare and send a request (pAnotherName, rpcSpan)
        void OnReady(
            _In_ Schedulers::WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t         messageLength
            )
        {

            // Test Oracle, make sure we are in root span now
            Audit::Assert(Tracer::GetCurrentTraceHeader()->m_parentSpanId == 0, "RPC caller should be in root span!");

            // prepare simulated "RPC" data, starting in a new Activity
            wchar_t buffer[1024];
            size_t stringLen = wcslen(GetContext()->GetName());
            wcscpy_s(buffer, stringLen + 1, GetContext()->GetName());
            wchar_t * pPart = buffer + stringLen;
            wcscpy_s(pPart, 20, L" Sub Span");

            auto pRpcCallback = GetArena()->allocate<TraceEnder>(m_activity);

            Tracer::TraceRec rpcSpan(buffer, *Tracer::GetCurrentTraceHeader());
            Schedulers::Activity* rpcActivity = Schedulers::ActivityFactory(*thread.GetScheduler(), rpcSpan);

            // Simulate an async RPC call
            auto pRpcCall = rpcActivity->GetArena()->allocate<RPCHandler>(*rpcActivity, buffer, pRpcCallback);

            Tracer::RPCClientSend(&rpcSpan);

            pRpcCall->Post(0, 0, 100000);

        }
    };

    class TraceStarter : public Schedulers::ContinuationWithContext<NameContext, Schedulers::ContextIndex::BaseContext>
    {
    public:

        TraceStarter(Schedulers::Activity& activity, const wchar_t* name)
            : ContinuationWithContext<NameContext, Schedulers::ContextIndex::BaseContext>(activity)
        {
            auto pContext = m_activity.GetArena()->allocate<NameContext>(m_activity);
            SetContext(pContext);
            pContext->SetName(name);
        }

        void OnAvailable(
            _In_ Schedulers::WorkerThread&  thread,
            _In_ size_t*  pData
            )
        {
            Audit::NotImplemented();
        }

        void Cleanup() override {}

        // This actor start a new trace
        void OnReady(
        _In_ Schedulers::WorkerThread&  thread,
        _In_ intptr_t       continuationHandle,
        _In_ uint32_t         messageLength
        )
        {
            Tracer::LogDebug(StatusCode::OK, L"Test Start");

            auto pSpliter = GetArena()->allocate<TraceSpliter>(m_activity);

            m_activity.Post(*pSpliter, 0, 0);
        }

    };


    TEST_CLASS(TracerTest)
    {
    public:

        TEST_METHOD(MultiTraceTest)
        {
            // TODO redirect to a temp file for portability
            Tracer::InitializeLogging(L"D:\\data\\testlog.etl");
            std::wstring name{ L"home service" };
            Utilities::EnableLargePages();
            std::unique_ptr<ServiceBrokers::ServiceBroker> pSB = ServiceBrokers::ServiceBrokerRIOFactory(name);
            std::cout << "address " << pSB->HomeAddressIP() << " port " << pSB->HomeAddressPort() << '\n';

            Schedulers::Scheduler* pScheduler = pSB->GetScheduler();

            Tracer::TraceRec tracer(L"Root Span --");

            Schedulers::Activity* rootActivity = Schedulers::ActivityFactory(*pScheduler, tracer);
            TraceStarter * starter = rootActivity->GetArena()->allocate<TraceStarter>(*rootActivity, L"Test Root Span  ");

            rootActivity->Post(*starter, 0, 0);

            while (!tracingFinished)
            {
                Sleep(3000);
            }

            Tracer::DisposeLogging();
        }

        TEST_METHOD(SingleContinuationTest)
        {
            std::wstring name{ L"home service" };
            Utilities::EnableLargePages();
            std::unique_ptr<ServiceBrokers::ServiceBroker> pSB = ServiceBrokers::ServiceBrokerRIOFactory(name);
            std::cout << "address " << pSB->HomeAddressIP() << " port " << pSB->HomeAddressPort() << '\n';

            Schedulers::Scheduler* pScheduler = pSB->GetScheduler();

            Tracer::TraceRec tracer(L"Root Span --");

            Schedulers::Activity* rootActivity = Schedulers::ActivityFactory(*pScheduler, tracer);
            TraceEnder * starter = rootActivity->GetArena()->allocate<TraceEnder>(*rootActivity);
            auto pContext = rootActivity->GetArena()->allocate<NameContext>(*rootActivity);
            starter->SetContext(pContext);

            rootActivity->Post(*starter, 0, 0);

            while (!tracingFinished)
            {
                Sleep(3000);
            }

            Tracer::DisposeLogging();

        }
    };
}
