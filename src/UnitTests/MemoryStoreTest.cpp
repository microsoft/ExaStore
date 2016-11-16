#pragma once
#include "stdafx.h"

#include <utility>
#include <algorithm>
#include <random>
#include <unordered_map>
#include <memory>
#include <iostream>

#include "Exabytes.hpp"
#include "UtilitiesWin.hpp"
#include "ServiceBrokerRIO.hpp"
#include "EbServer.hpp"
#include "EbPartition.hpp"
#include "MemoryStore.hpp"

#include "TestHooks.hpp"
#include "TestUtils.hpp"

#include "CppUnitTest.h"

using namespace Microsoft::VisualStudio::CppUnitTestFramework;

namespace EBTest
{
    class MockPartitionCreator : public Exabytes::PartitionCreater
    {
    public:
        std::unique_ptr<Exabytes::Partition> CreatePartition(
            Exabytes::ExabytesServer& server,
            Datagram::PartitionId id,
            Schedulers::FileHandle handle,
            bool isPrimary
            ) override
        {
            if (!isPrimary)
                Audit::NotImplemented("Secondary partition not supported yet.");

            Schedulers::Scheduler& scheduler = server.GetScheduler();

            std::unique_ptr<Exabytes::KeyValueOnFile> pDiskStore =
                MockDiskStoreFactory(scheduler);
            std::unique_ptr<Exabytes::Catalog> pCatalog = Exabytes::BlobCatalogFactory(scheduler, L"", 0, 0);

            return Exabytes::IsolatedPartitionFactory(server, pCatalog, pDiskStore, id);
        }
    };

    TEST_CLASS(MemoryStoreTest)
    {
        std::unique_ptr<Exabytes::ExabytesServer> m_pServer;
        Exabytes::KeyValueInMemory* m_pMemStore;
        TestHooks::LocalStoreTestHooks m_testhook;
        Schedulers::Scheduler* m_pScheduler;
        std::unique_ptr<ServiceBrokers::ServiceBroker> m_pSB;

        void Setup()
        {
            Tracer::InitializeLogging(L"D:\\data\\testlog.etl");
            std::wstring name{ L"home service" };
            Utilities::EnableLargePages();
            m_pSB = ServiceBrokers::ServiceBrokerRIOFactory(name);
            std::cout << "address " << m_pSB->HomeAddressIP() << " port " << m_pSB->HomeAddressPort() << '\n';
            m_pScheduler = m_pSB->GetScheduler();

            random_device m_seed;
            uniform_int_distribution<uint64_t> m_r;

            auto pBufferPool = Exabytes::BufferPoolFactory(2 * SI::Mi, 6, MockBufferFactory);
            std::unique_ptr<Exabytes::PartitionCreater> pParts =
                std::make_unique<MockPartitionCreator>();

            // adding min buffer space. Cannot make this null coz IsolatedServer will start GC on this buffer pool. This hack should not
            // affect this test in any way since we ar testing Memory Store
            std::unique_ptr<Exabytes::CatalogBufferPool<>> pDummyHashPool = Exabytes::SharedCatalogBufferPoolFactory(4, 8, 1, 4096);
            std::unique_ptr<Exabytes::CatalogBufferPool<>> pDummyBloomKeyPool = nullptr;
            std::unique_ptr<Exabytes::FileRecover> pRecover = std::make_unique<DummyRecover>();

            m_pServer = Exabytes::IsolatedServerFactory(
                m_pSB, 
                pBufferPool,
                Exabytes::PolicyTableFactory(),
                pDummyHashPool,
				pDummyBloomKeyPool,
                pParts,
                pRecover,
                BinHash, 
                DummyGcFactory);

            unique_ptr<Exabytes::KeyValueInMemory> pMemoryStore =
                Exabytes::MemoryStoreFactory(*m_pServer, 64 * SI::Mi);
            m_pMemStore = pMemoryStore.get();

            m_pServer->SetMemoryStore(pMemoryStore);

            m_pServer->AddFileHandle((Schedulers::FileHandle)INVALID_HANDLE_VALUE, true);
            m_pServer->AddFileHandle((Schedulers::FileHandle)INVALID_HANDLE_VALUE, true);
            auto res = m_pServer->StartPartition(0, true);
            if (res != Utilities::StatusCode::OK){
                Audit::OutOfLine::Fail(res);
            }
            res = m_pServer->StartPartition(1, true);
            if (res != Utilities::StatusCode::OK){
                Audit::OutOfLine::Fail(res);
            }

            // set the writing edge near the end of the circular buffer to test the ability for wrapping around buffer end.
            size_t allocEdge = m_testhook.SizeOfMemoryStore(m_pMemStore)
                               - 512 * 1024; 
            m_testhook.SetEdgesOfMemoryStore(m_pMemStore, allocEdge, 
                allocEdge + m_testhook.SizeOfMemoryStore(m_pMemStore));
        }

        void Teardown()
        {
            m_pServer->StartCleanup();
            Sleep(1000);
            m_pServer.reset();
            m_pSB.reset();
            Sleep(1000);
            Tracer::DisposeLogging();
        }

    public:

        // Test for MemoryStore, using a mock disk store as persistor
        // create a memory store without starting sweeper, with edges close to the end of the buffer.
        // write a bunch of items in, read it out and verify they are correct;
        // start the sweeper and waiting until all items wrote to mock persister;
        // read from the mock persister to verify they are correct
        TEST_METHOD(MemoryStoreWriteRead)
        {
            Setup();

            shared_ptr<InventoryVector> pInventory = make_shared<InventoryVector>();
            unique_ptr<RepeatedAction> writes = RepeatedMemStoreWriteFactory(
                *m_pMemStore,*(m_pScheduler), pInventory);
            
            writes->Run(10);

            vector<pair<Datagram::PartialDescription, uint32_t>> inventory;
            pInventory->SetAndReleaseSelf(inventory);
            unique_ptr<RepeatedAction> reads = RepeatedReadFactory(
                *m_pServer, *m_pScheduler, inventory);
            reads->Run(10);

            m_pServer->StartSweeper();

            shared_ptr<InventoryVector> pPaddingInventory = make_shared<InventoryVector>();
            // fill partial buffer, ensure previous writes flushed to disk store.
            unique_ptr<RepeatedAction> paddingwrites =
                RepeatedMemStoreWriteFactory(*m_pMemStore, *m_pScheduler, pPaddingInventory);
            paddingwrites->Run(4); // 2MB buffer, 4 is more than enough to fill a buffer.

            Sleep(3000); // wait until all record sweeped to mock store
            unique_ptr<RepeatedAction> secondReads = 
                RepeatedReadFactory(*m_pServer, *m_pScheduler, inventory);
            secondReads->Run(10);

            Teardown();
        }

        // Same as above, except multi-part write
        TEST_METHOD(MemoryStoreMutiWriteRead)
        {
            Setup();

            vector<pair<Datagram::PartialDescription, uint32_t>> inventory;
            unique_ptr<RepeatedAction> writes = 
                RepeatedMultiWriteFactory(*m_pServer, *m_pScheduler, inventory);
            writes->Run(25);

            m_pServer->StartSweeper();

            Sleep(30000);
            shared_ptr<InventoryVector> pPaddingInventory = make_shared<InventoryVector>();
            // fill partial buffer, ensure previous writes flushed to disk store.
            unique_ptr<RepeatedAction> paddingwrites = 
                RepeatedMemStoreWriteFactory(*m_pMemStore, *m_pScheduler, pPaddingInventory);
            paddingwrites->Run(8); // 2MB buffer, 4 is enough to fill a buffer.

            Sleep(30000); // wait until all record sweeped to mock store
            unique_ptr<RepeatedAction> secondReads = 
                RepeatedReadFactory(*m_pServer, *m_pScheduler, inventory);
            secondReads->Run(6);

            Teardown();
        }

    };

}