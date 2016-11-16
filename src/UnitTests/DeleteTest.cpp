#pragma once
#include "stdafx.h"


#include "ServiceBrokerRIO.hpp"
#include "Exabytes.hpp"
#include "ReducedKeyMap.hpp"
#include "MemoryStore.hpp"
#include "EbPartition.hpp"
#include "EbServer.hpp"
#include "Utilities.hpp"
#include "ChainedFileStore.hpp"

#include "UtilitiesWin.hpp"

#include "CppUnitTest.h"

#include "TestHooks.hpp"
#include "TestUtils.hpp"

using namespace std;

using namespace Microsoft::VisualStudio::CppUnitTestFramework;

namespace EBServerTest
{
    using namespace EBTest;

    using namespace Datagram;
    using namespace Exabytes;
    using namespace ServiceBrokers;

    TEST_CLASS(RandomWriteReadTest)
    {
    private:
        std::unique_ptr<Exabytes::ExabytesServer> m_pServer;
        Schedulers::Scheduler* m_pScheduler;
        std::unique_ptr<ServiceBrokers::ServiceBroker> m_pSB;

    public:

        TEST_METHOD(DeleteTest)
        {
            auto numOfPartitions = 2;
            Tracer::InitializeLogging(L"D:\\data\\testlog.etl");
            std::wstring name{ L"home service" };
            Utilities::EnableLargePages();
            m_pSB = ServiceBrokers::ServiceBrokerRIOFactory(name);
            m_pScheduler = m_pSB->GetScheduler();

            std::unique_ptr<Exabytes::PolicyManager> pPolicies = std::make_unique<DummyPolicyManager>(60);
            std::unique_ptr<Exabytes::PartitionCreater> pParts = std::make_unique<TestPartitionCreator>();
            std::unique_ptr<Exabytes::FileRecover> pRecover = CircularLogRecoverFactory();
            auto pBufferPool = Exabytes::BufferPoolFactory(2 * SI::Mi, 6, Exabytes::MakeFileCoalescingBuffer);
			auto pHashPool = Exabytes::SharedCatalogBufferPoolFactory(INMEMHASHBLKCOUNT, TOTALHASHBLKS, numOfPartitions, HASHBLOCKSIZE);
			auto pBloomKeyPool = Exabytes::SharedCatalogBufferPoolFactory(INMEMHASHBLKCOUNT, TOTALHASHBLKS, numOfPartitions, BLOOMKEYBLOCKSIZE);

            m_pServer = Exabytes::IsolatedServerFactory(
                m_pSB,
                pBufferPool,
                pPolicies,
                pHashPool,
				pBloomKeyPool,
                pParts,
                pRecover,
                BinHash,
                Exabytes::FileStoreGcFactory);

            unique_ptr<Exabytes::KeyValueInMemory> pMemoryStore =
                Exabytes::MemoryStoreFactory(*m_pServer, 512 * SI::Mi);
            m_pServer->SetMemoryStore(pMemoryStore);

            for (int i = 0; i < numOfPartitions; i++)
            {
                wchar_t logMsg[64];
                swprintf_s(logMsg, 63, L"D:\\data\\ExabytesData.part%d", i);
                auto res = m_pServer->CreatePartFile(logMsg, 1024 * SI::Mi, true);
            if (res != Utilities::StatusCode::OK){
                Audit::OutOfLine::Fail(res);
            }

                res = m_pServer->StartPartition(i, true);
            if (res != Utilities::StatusCode::OK){
                Audit::OutOfLine::Fail(res);
            }
            }

            m_pServer->StartSweeper();
            m_pServer->StartGC();


            // setup finished, start issue requests
            vector<pair<Datagram::PartialDescription, uint32_t>> inventory;
            unique_ptr<RepeatedAction> writes = RepeatedWriteFactory(
                *m_pServer, *m_pScheduler, inventory);
            unique_ptr<RepeatedAction> reads = RepeatedReadFactory(
                *m_pServer, *m_pScheduler, inventory);
            unique_ptr<RepeatedAction> deletes = RepeatedDeleteFactory(
                *m_pServer, *m_pScheduler, inventory);

            writes->Run(8);
            deletes->Run(4);
            reads->Run(10);

            writes->Run(8);

            Sleep(30000); // wait until multi-parts can be sweeped
            vector<pair<Datagram::PartialDescription, uint32_t>> paddingInventory;
            unique_ptr<RepeatedAction> paddingWrites = RepeatedWriteFactory(
                *m_pServer, *(m_pScheduler), paddingInventory);
            paddingWrites->Run(8); // make sure all multi-parts are sweeped to disk
            Sleep(10000);

            deletes->Run(3);
            reads->Run(20);

            m_pServer->StartCleanup();
            Sleep(2000);
            m_pServer.reset();
            m_pSB.reset();
            Sleep(1000);
            for (int i = 0; i < numOfPartitions; i++)
            {
                wchar_t fileName[64];
                swprintf_s(fileName, 63, L"D:\\data\\ExabytesData.part%d", i);
                ::DeleteFile(fileName);
            }
            Tracer::DisposeLogging();
        }


    };
}