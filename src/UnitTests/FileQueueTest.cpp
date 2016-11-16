#pragma once
#include "stdafx.h"

#include "UtilitiesWin.hpp"
#include "ServiceBrokerRIO.hpp"
#include "FileManager.hpp"

#include "CppUnitTest.h"

#include<iostream>

using namespace Microsoft::VisualStudio::CppUnitTestFramework;
using namespace Utilities;

namespace FileQueueTest
{
    struct ValueContext
    {
        AsyncFileIo::FileManager* m_pFile = nullptr;
        uint64_t m_expectedValue[512];
        uint64_t m_readValue[512];
        uint32_t readLeft;
        Schedulers::ContinuationBase* m_pRead = nullptr;
        Schedulers::ContinuationBase* m_pWrite = nullptr;
    };

    // A ReadContinuation is called when the async file read finishes.
    //
    class ReadContinuation : public Schedulers::ContinuationWithContext<ValueContext, Schedulers::ContextIndex::BaseContext>
    {
    public:
        ReadContinuation(
            _In_ Schedulers::Activity& activity
            )
            : Schedulers::ContinuationWithContext<ValueContext, Schedulers::ContextIndex::BaseContext>(activity)
        {}

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
            _In_ uint32_t       length
            )
        {
            Tracer::LogInfo(StatusCode::OK, L"Read Finished");

            // oracle: verify read successful
            Audit::Assert((StatusCode)continuationHandle == StatusCode::OK, "Failed async reading!");
            Audit::Assert(length == 4096, "read length error!");
            Audit::Assert(GetContext()->m_expectedValue[0] == GetContext()->m_readValue[0], "Mismatched value read from file!");

            auto pCon = GetContext();
            pCon->readLeft--;

            if (pCon->readLeft > 0){
                pCon->m_expectedValue[0] -= 16252;
                pCon->m_pFile->Write(4096, 4096, (pCon->m_expectedValue), pCon->m_pWrite);
            }

        }
    };

    class WriteContinuation : public Schedulers::ContinuationWithContext < ValueContext, Schedulers::ContextIndex::BaseContext >
    {
    public:
        WriteContinuation(
            _In_ Schedulers::Activity& activity
            )
            : Schedulers::ContinuationWithContext<ValueContext, Schedulers::ContextIndex::BaseContext>(activity)
        {}

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
            _In_ uint32_t       length
            )
        {
            // oracle: verify write successful
            Tracer::LogInfo(StatusCode::OK, L"Write Finished");
            Audit::Assert((StatusCode)continuationHandle == StatusCode::OK, "Failed async writing!");
            Audit::Assert(length == 4096, "write length error!");

            GetContext()->m_pFile->Read(4096, 4096, (GetContext()->m_readValue), GetContext()->m_pRead);
        }

    };

    TEST_CLASS(FileQueueRWTest)
    {
    public:

        TEST_METHOD(FileReadWriteTest)
        {
            Tracer::InitializeLogging(L"D:\\data\\testlog.etl");
            std::wstring name{ L"home service" };
            Utilities::EnableLargePages();
            std::unique_ptr<ServiceBrokers::ServiceBroker> pSB = ServiceBrokers::ServiceBrokerRIOFactory(name);
            std::cout << "address " << pSB->HomeAddressIP() << " port " << pSB->HomeAddressPort() << '\n';

            Schedulers::Scheduler* pScheduler = pSB->GetScheduler();

            auto fileQueue = AsyncFileIo::FileQueueFactory(L"D:\\data\\testfile.bin", *pScheduler, 4 * SI::Gi);

            Schedulers::Activity* activity1 = Schedulers::ActivityFactory(*pScheduler, L"File IO Span 1 ");
            ValueContext* pContext = activity1->GetArena()->allocate<ValueContext>();
            pContext->readLeft = 10;
            pContext->m_pFile = fileQueue.get();
            activity1->SetContext(Schedulers::ContextIndex::BaseContext, pContext);

            WriteContinuation * firstwrite = activity1->GetArena()->allocate<WriteContinuation>(*activity1);
            ReadContinuation * read = activity1->GetArena()->allocate<ReadContinuation>(*activity1);
            pContext->m_pRead = read;
            pContext->m_pWrite = firstwrite;

            {
                firstwrite->GetContext()->m_expectedValue[0] = 98765432101;
                fileQueue->Write(4096, 4096, (firstwrite->GetContext()->m_expectedValue), firstwrite);
            }

            while (firstwrite->GetContext()->readLeft>0)
            {
                Sleep(3000);
            }
            activity1->RequestShutdown(StatusCode::OK);

            fileQueue.reset();
            pSB.reset();
            Tracer::DisposeLogging();
        }
    };
}
