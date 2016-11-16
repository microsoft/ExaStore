#pragma once
#include "stdafx.h"

#include <memory>
#include <utility>
#include <vector>
#include <map>
#include <algorithm>
#include <cstdlib>
#include <string>
#include <cstdio>
#include <iostream>
#include <sstream>
#include <fstream>

#include "ServiceBrokerRIO.hpp"
#include "Exabytes.hpp"
#include "ReducedKeyMap.hpp"
#include "MemoryStore.hpp"
#include "EbPartition.hpp"
#include "EbServer.hpp"
#include "Utilities.hpp"
#include "ChainedFileStore.hpp"

#include "UtilitiesWin.hpp"
#include <winsock2.h>
#include <ws2tcpip.h>

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
        
        TEST_METHOD(ThousandWriteRead)
        {
            auto numOfPartitions = 2;
            Tracer::InitializeLogging(L"D:\\data\\testlog.etl");
            std::wstring name{ L"home service" };
            Utilities::EnableLargePages();
            m_pSB = ServiceBrokers::ServiceBrokerRIOFactory(name);
            std::cout << "address " << m_pSB->HomeAddressIP() << " port " << m_pSB->HomeAddressPort() << '\n';
            m_pScheduler = m_pSB->GetScheduler();

            std::unique_ptr<Exabytes::PolicyManager> pPolicies = std::make_unique<DummyPolicyManager>(60);
            std::unique_ptr<Exabytes::PartitionCreater> pParts = std::make_unique<TestPartitionCreator>();
            auto pBufferPool = Exabytes::BufferPoolFactory(2 * SI::Mi, 6, Exabytes::MakeFileCoalescingBuffer);
			auto pHashPool = Exabytes::SharedCatalogBufferPoolFactory(INMEMHASHBLKCOUNT, TOTALHASHBLKS, numOfPartitions, HASHBLOCKSIZE);
			auto pBloomKeyPool = Exabytes::SharedCatalogBufferPoolFactory(INMEMHASHBLKCOUNT, TOTALHASHBLKS, numOfPartitions, BLOOMKEYBLOCKSIZE);

            auto pRecover = Exabytes::CircularLogRecoverFactory();

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
                wchar_t fileName[64];
                swprintf_s(fileName, 63, L"D:\\data\\ExabytesData%d.part", i);
                auto res = m_pServer->CreatePartFile(fileName, 1024 * SI::Mi, true);
            if (res != Utilities::StatusCode::OK){
                Audit::OutOfLine::Fail(res);
            }

                res = m_pServer->StartPartition(i, true);
            if (res != Utilities::StatusCode::OK){
                Audit::OutOfLine::Fail(res);
            }
            }

            TestHooks::LocalStoreTestHooks testhook;

            Datagram::Key128 dummyKey{ 0ULL, 0ULL };
            auto pPart = m_pServer->FindPartitionForKey(dummyKey);
            auto pFileStore = pPart->Persister();
            // set the writing edge near the end of the circular buffer to test the ability for wrapping around buffer end.
            size_t fileSize = testhook.SizeofChainedFileStore(pFileStore);
            size_t allocEdge = fileSize - SI::Mi; // 1 M from the end
            testhook.SetEdgesofChainedFileStore(pFileStore, allocEdge, allocEdge + fileSize);
            testhook.EnableQuickGC(pFileStore, 0.1, 64 * SI::Mi);

            m_pServer->StartSweeper();
            m_pServer->StartGC();

            // setup finished, start issue requests
            vector<pair<Datagram::PartialDescription, uint32_t>> inventory;
            unique_ptr<RepeatedAction> writes = RepeatedWriteFactory(
                *m_pServer, *m_pScheduler, inventory, false);
            unique_ptr<RepeatedAction> deletes = RepeatedDeleteFactory(
                *m_pServer, *m_pScheduler, inventory);
            unique_ptr<RepeatedAction> reads = RepeatedReadFactory(
                *m_pServer, *m_pScheduler, inventory);

            writes->Run(1000);

            deletes->Run(900);

            writes->Run(2000);

            Sleep(10000); // wait until multi-parts can be sweeped
            vector<pair<Datagram::PartialDescription, uint32_t>> paddingInventory;
            unique_ptr<RepeatedAction> paddingWrites = RepeatedWriteFactory(
                *m_pServer, *(m_pScheduler), paddingInventory, true);
            paddingWrites->Run(8); // make sure all multi-parts are sweeped to disk
            Sleep(10000);

            reads->Run(2000);

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