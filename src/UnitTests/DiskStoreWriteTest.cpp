#pragma once
#include "stdafx.h"

#include <utility>
#include <algorithm>
#include <random>
#include <unordered_map>
#include <memory>
#include <iostream>
#include <future>

#include "UtilitiesWin.hpp"
#include "ServiceBrokerRIO.hpp"
#include "ChainedFileStore.hpp"
#include "EbServer.hpp"
#include "EbPartition.hpp"
#include "MemoryStore.hpp"

#include "TestUtils.hpp"
#include "TestHooks.hpp"

#include "CppUnitTest.h"

using namespace Microsoft::VisualStudio::CppUnitTestFramework;

namespace EBTest
{
    using namespace std;
    using namespace Datagram;
    using namespace Utilities;
    using namespace Schedulers;
    using namespace Exabytes;
    
    // Very simple Mock Implementation of MemStore to aid for DiskStore testing
    class MockMemStore : public KeyValueInMemory
    {
    public:
        MockMemStore(ExabytesServer& server)
            : KeyValueInMemory(server)
        {}

        size_t Size() const override
        {
            return 0;
        }

        StatusCode Read(
            int timeoutMillisecs,
            const Datagram::PartialDescription& description,
            _Out_ Utilities::BufferEnvelope& blobEnvelop) override

        {
            Audit::NotImplemented("Not supported!");
            return StatusCode::Unexpected;
        }

        StatusCode GetDescription(
            const Datagram::Key128& keyHash,
            _Out_ Datagram::Description& description
        ) override
        {
            Audit::NotImplemented("Not supported!");
            return StatusCode::Unexpected;
        }

        StatusCode Write(
            _In_ const Datagram::Description& description,
            _In_reads_(dataLength) const void* pData,
            size_t dataLength
        ) override
        {
            Audit::NotImplemented("Not supported!");
            return StatusCode::Unexpected;
        }

        void StartSweeping() override
        {
            Audit::NotImplemented("Not supported!");
        }


        void StartCleanup() override 
        { }
        
        std::pair<uint64_t, uint64_t> StartDump(
            AsyncFileIo::FileManager& file,
            Schedulers::ContinuationBase* pNotify ) override
        {
            Audit::NotImplemented("Not supported!");
            return std::make_pair(0, 0);
        }

        size_t GetCheckpointSize() const override
        {
            Audit::NotImplemented("Not supported!");
            return 0;
        }

        StatusCode RetriveCheckpointData(_Out_ void* buffer) const override
        {
            Audit::NotImplemented("Not supported!");
            return StatusCode::OK;
        }

        ~MockMemStore()
        { }
    };

    std::unique_ptr<KeyValueInMemory> MockMemStoreFactory(
        _In_ ExabytesServer& server
    )
    {
        return std::make_unique<MockMemStore>(server);
    }

    class DiskStoreTestContext 
    {
    public:
        vector<pair<Datagram::Key128, uint32_t>> m_inventory;

        // just need it to get buffer from
        ExabytesServer*                          m_pServer;
        KeyValueOnFile*                          m_pStore;
        Catalog*                                 m_pCatalog;
        size_t                                   m_pendingReads;
        bool&                                    m_testFinished;

        DiskStoreTestContext(ExabytesServer& server, KeyValueOnFile& store, Catalog& catalog, _Out_ bool& finished)
            : m_pServer(&server)
            , m_pStore(&store)
            , m_pCatalog(&catalog)
            , m_pendingReads(0)
            , m_testFinished(finished)
        {}
    };

    // A ReadContinuation is called when the Read has obtained the data ready to be
    // copied to the waiting reader.
    //
    class ReadContinuation : public ContinuationWithContext<DiskStoreTestContext, ContextIndex::BaseContext, BufferEnvelope>
    {
        PartialDescription  m_partialDescription;
        uint32_t            m_crc;
    public:
        ReadContinuation(
            _In_ Activity& activity,
            _In_ PartialDescription& partialDescription,
            _In_ uint32_t crc
            )
            : ContinuationWithContext<DiskStoreTestContext, ContextIndex::BaseContext, BufferEnvelope>(activity)
            , m_partialDescription(partialDescription)
            , m_crc(crc)
        {}

        void Cleanup() override {}

        void OnReady(
            _In_ WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            )
        {
            Audit::Assert(length > 0, "no data returned");

            Utilities::DisposableBuffer* pIOBuffer = m_postable.Contents();
            void* pBlob = (void*)((char*)(pIOBuffer->PData()) + pIOBuffer->Offset());

            auto crc = Utilities::PsuedoCRC(pBlob, length);
            if (m_crc != crc)
            {
                Audit::Assert(false, "CRC did not match after collection");
            }
            m_postable.Recycle();
            // send a confirmation back to the reader.
            ::InterlockedDecrement(&GetContext()->m_pendingReads);
            EBTest::Trace("Finished read, remaining: %d", GetContext()->m_pendingReads);

            // this should exit and leave the running room for others.
            if (GetContext()->m_pendingReads <= 0)
            {
                GetContext()->m_testFinished = true;
            }
        }
    };

    // got called by the catalog continuation,
    // so it has the address to conduct reading
    class ReadStore : public ContinuationWithContext<DiskStoreTestContext, ContextIndex::BaseContext>
    {
    private:
        PartialDescription m_desc;
        uint32_t m_crc;
    public:
        ReadStore(Activity& activity, PartialDescription& desc, uint32_t crc)
            : ContinuationWithContext(activity)
            , m_desc(desc)
            , m_crc(crc)
        {}

        void OnReady(
            _In_ WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            )
        {
            const AddressAndSize& location = AddressAndSize::Reinterpret(continuationHandle);
            Audit::Assert(!location.IsVoid(), "item key not find in catalog");
            DiskStoreTestContext* pContext = GetContext();
            auto pContinuation = GetArena()->allocate<ReadContinuation>(m_activity, m_desc, m_crc);
            // issue read
            Audit::Assert(StatusCode::OK == 
                pContext->m_pStore->Read(100, m_desc, location, pContinuation), 
                "KeyValueStore->Read");
        }

        void Cleanup()
        {}

    };

    class ReadStart : public ContinuationWithContext<DiskStoreTestContext, 
                                                     ContextIndex::BaseContext, 
                                                     HomingEnvelope<CoalescingBuffer>>
    {
        vector<pair<Datagram::Key128, uint32_t>>::const_iterator m_iter;

    public:
        ReadStart(
            _In_ Activity& activity
            )
            : ContinuationWithContext(activity)
        {
            GetContext()->m_pendingReads = GetContext()->m_inventory.size();

            m_iter = GetContext()->m_inventory.cbegin();
        }

        void Cleanup() override {}

        void OnReady(
            _In_ WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            )
        {
            DiskStoreTestContext* pContext = GetContext();
            if (m_iter != pContext->m_inventory.cend())
            {
                pair<Datagram::Key128, uint32_t> pair = *m_iter;

            Datagram::PartialDescription descriptor;
            ::ZeroMemory(&descriptor, sizeof(descriptor));
            descriptor.KeyHash = pair.first;
            auto crc = pair.second;
            auto pContinuation = GetArena()->allocate<ReadStore>(m_activity, descriptor, crc);
            AddressAndSize prior{ AddressAndSize::INVALIDADDRESS, 0 };
                pContext->m_pCatalog->Locate(descriptor.KeyHash, prior, pContinuation);

                m_iter++;
                if (m_iter != pContext->m_inventory.cend())
            {
                    this->Post(0, 0, 1000);
            }
        }
        }
    };


    class DiskStoreBufferFiller : public ContinuationWithContext<DiskStoreTestContext, ContextIndex::BaseContext, HomingEnvelope<CoalescingBuffer>>
    {
    public:

        DiskStoreBufferFiller(Activity& activity)
            : ContinuationWithContext(activity)
        { }

        void OnReady(
            _In_ WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            )
        {
            // call something to keep write to it, then flush, then read
            RandomWrites writes;
            m_postable.m_pResource->m_origin = CoalescingBuffer::Origin::Memory;
            while (GetContext()->m_inventory.size() < 150){
                Description header;
                void* dataParts[1];
                uint32_t dataSizes[1];
                writes.GenerateRandomRecord(header, dataParts[0]);
                dataSizes[0] = header.ValueLength;
                header.Timestamp = GetContext()->m_pServer->GetScheduler().GetCurTimeStamp();
                AddressAndSize invalid;
                auto error = m_postable.m_pResource->Append(header, dataParts, dataSizes, 1, invalid);
                if (error == StatusCode::OutOfMemory)
                {
                    GetContext()->m_pStore->Flush(m_postable.m_pResource, nullptr); 
                    GetContext()->m_pServer->ObtainEmptyBuffer(*this);
                    EBTest::Trace("Flushing buffer, writes %d", GetContext()->m_inventory.size());
                    return;
                }
                Audit::Assert(error == StatusCode::OK, "unknown error");
                auto crc = Utilities::PsuedoCRC(dataParts[0], dataSizes[0]);
                GetContext()->m_inventory.push_back(pair < Datagram::Key128, uint32_t > {header.KeyHash, crc});
            }

            auto pRead = GetArena()->allocate<ReadStart>(m_activity);
            GetContext()->m_pStore->Flush(m_postable.m_pResource, pRead); // read will start as soon as flush finishes.
            EBTest::Trace("Flushing buffer, writes %d", GetContext()->m_inventory.size());
        }

        void Cleanup(){

        }
    };

    // only support 1 partition
    class SinglePartitionCreator : public Exabytes::PartitionCreater
    {
    public:
        std::unique_ptr<Exabytes::Partition> CreatePartition(
            Exabytes::ExabytesServer& server,
            Datagram::PartitionId id,
            Schedulers::FileHandle handle,
            bool isPrimary
            ) override
        {
            Audit::Assert(id.Value() == 0, "only support 1 partition");

            TestHooks::LocalStoreTestHooks testhook;
            Schedulers::Scheduler& scheduler = server.GetScheduler();

            // get path for the catalog file
            std::wstring path;
            size_t fileSize;
            auto status = GetFileInfoFromHandle(handle, path, fileSize);
            path.append(L"-catlogFile");

            std::unique_ptr<Exabytes::Catalog> pCatalog =
                Exabytes::ReducedMapCatalogFactory(scheduler, path.c_str(), server.GetCatalogBufferPool(), server.GetCatalogBufferPoolForBloomKey() ,0xBAADF00DADD5C731ull);

            unique_ptr<Exabytes::KeyValueOnFile> pDiskStore = Exabytes::ChainedFileStoreFactory(
                scheduler,
                Exabytes::AddressSpace::SSD,
                handle
                );
            KeyValueOnFile* ptr = dynamic_cast<KeyValueOnFile*>(pDiskStore.get());
            // set the writing edge near the end of the circular buffer to test the ability for wrapping around buffer end.
            size_t allocEdge = testhook.SizeofChainedFileStore(ptr) - 1536 * 1024; // 1.5 M from the end
            testhook.SetEdgesofChainedFileStore(ptr, allocEdge, allocEdge + testhook.SizeofChainedFileStore(ptr));

            return Exabytes::IsolatedPartitionFactory(
                server, pCatalog, pDiskStore, 0);
        }
    };

    class LdChkPntCompletion : public ContinuationBase, public Exabytes::FileRecoveryCompletion
    {
    public:
        ExabytesServer& m_server;
        std::vector<RecoverRec>& m_table;
        std::promise<bool>& m_loadFinished;

        LdChkPntCompletion(Activity& act, ExabytesServer& server, std::vector<RecoverRec>& table, std::promise<bool>& loadFinished)
            : ContinuationBase(act)
            , m_server(server)
            , m_table(table)
            , m_loadFinished(loadFinished)
        {}

        void RecoverFinished(
            // For verification purpose
            const std::vector<RecoverRec>& recTable
            ) override
        {
            Audit::Assert(&recTable == &m_table,
                "Mismatched recovery context!");
            Audit::Assert(m_table[0].Status == StatusCode::OK,
                "Data recovery failed!");
            m_server.SetPartition(m_table[0].pPartition->ID, m_table[0].pPartition);

            m_loadFinished.set_value(true);

            // TODO this request may not have any effect, as this activity
            // may never be triggered again. so we have a resource leak here.
            // luckily this is a unit test we don't care that much about the
            // leak.
            m_activity.RequestShutdown(StatusCode::OK);
        }


        void Cleanup() override
        {
            Audit::NotImplemented("test clean up not supported yet.");
        }


        // This method is called by the scheduler when it's this Activity's turn.
        // Define actual work in this method;
        //
        void OnReady(
            _In_ WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            ) override
        {
            m_server.Recover(m_table, *this);
        }
    };

    TEST_CLASS(DiskStoreTest)
    {
    public:

        TEST_METHOD(DiskStoreWriteTest)
        {
            const size_t partitionSize = 128 * SI::Mi;

            Tracer::InitializeLogging(L"D:\\data\\testlog.etl");
            std::wstring name{ L"home service" };
            Utilities::EnableLargePages();
            std::unique_ptr<ServiceBrokers::ServiceBroker> pSB = ServiceBrokers::ServiceBrokerRIOFactory(name);
            std::cout << "address " << pSB->HomeAddressIP() << " port " << pSB->HomeAddressPort() << '\n';

            Schedulers::Scheduler* pScheduler = pSB->GetScheduler();

            std::unique_ptr<PolicyManager> pPolicies = std::make_unique<DummyPolicyManager>(60);
            std::unique_ptr<Exabytes::PartitionCreater> pParts =
                std::make_unique<SinglePartitionCreator>();

			auto pHashPool = Exabytes::SharedCatalogBufferPoolFactory(INMEMHASHBLKCOUNT, TOTALHASHBLKS, 1, HASHBLOCKSIZE);
			auto pBloomKeyPool = Exabytes::SharedCatalogBufferPoolFactory(INMEMHASHBLKCOUNT, TOTALHASHBLKS, 1, BLOOMKEYBLOCKSIZE);

            std::unique_ptr<Exabytes::FileRecover> pRecover = Exabytes::CircularLogRecoverFactory();

            std::unique_ptr<Exabytes::ExabytesServer> pServer = Exabytes::IsolatedServerFactory(
                pSB,
                Exabytes::BufferPoolFactory(2 * SI::Mi, 6, Exabytes::MakeFileCoalescingBuffer),
                pPolicies,
                pHashPool,
				pBloomKeyPool,
                pParts,
                pRecover,
                DummyHash,
                Exabytes::FileStoreGcFactory);

            std::unique_ptr<Exabytes::KeyValueInMemory> pMemStore =
                MockMemStoreFactory(*pServer);
            pServer->SetMemoryStore(pMemStore);            

            auto res = pServer->CreatePartFile(L"D:\\data\\Exabytes.dat", partitionSize, true);
            if (res != Utilities::StatusCode::OK){
                Audit::OutOfLine::Fail(res);
            }
            res = pServer->StartPartition(0, true);
            if (res != Utilities::StatusCode::OK){
                Audit::OutOfLine::Fail(res);
            }

            pServer->StartGC();

            Activity* testActivity = ActivityFactory(*pScheduler, L"new test trace");
            bool testFinished = false;

            auto pPartition = pServer->FindPartitionForKey(Key128(1234, 1234));
            auto pCatalog = pPartition->GetCatalog();
            auto pStore = pPartition->Persister();

            DiskStoreTestContext context
                (*pServer, *pStore, *pCatalog, testFinished);

            testActivity->SetContext(ContextIndex::BaseContext, &context);

            DiskStoreBufferFiller* bufferHandler = testActivity->GetArena()->allocate<DiskStoreBufferFiller>(*testActivity);

            pServer->ObtainEmptyBuffer(*bufferHandler);
            while (!testFinished)
                Sleep(1000);
            auto pChkPntFile = AsyncFileIo::FileQueueFactory(L"D:\\data\\Exabytes.chk", *pScheduler, 0);
            pServer->SaveCheckPoint(*pChkPntFile, nullptr);

            // Tear down
            pServer->StopPartition(pPartition->ID);
            Sleep(1000);
            pServer->StartCleanup();
            Sleep(1000);
            pChkPntFile->Close();
            pChkPntFile.reset();
            pServer.reset();
            Sleep(1000);


            // Rebuild everything, simulating a restart
            auto pNet = ServiceBrokers::ServiceBrokerRIOFactory(name);
            std::cout << "address " << pNet->HomeAddressIP() << " port " << pNet->HomeAddressPort() << '\n';

            pScheduler = pNet->GetScheduler();

            std::unique_ptr<PolicyManager> pP = std::make_unique<DummyPolicyManager>(60);
            auto pNewHashPool = Exabytes::SharedCatalogBufferPoolFactory(INMEMHASHBLKCOUNT, TOTALHASHBLKS, 1, HASHBLOCKSIZE);
            auto pNewBloomKeyPool = Exabytes::SharedCatalogBufferPoolFactory(INMEMHASHBLKCOUNT, TOTALHASHBLKS, 1, BLOOMKEYBLOCKSIZE);
            std::unique_ptr<PartitionCreater> pC = std::make_unique<SinglePartitionCreator>();
            auto pNewServer = Exabytes::IsolatedServerFactory(
                pNet,
                Exabytes::BufferPoolFactory(2 * SI::Mi, 6, Exabytes::MakeFileCoalescingBuffer),
                pP,
                pNewHashPool,
                pNewBloomKeyPool,
                pC,
                Exabytes::CircularLogRecoverFactory(),
                DummyHash,
                Exabytes::FileStoreGcFactory);

            std::unique_ptr<Exabytes::KeyValueInMemory> pNewMemStore =
                MockMemStoreFactory(*pNewServer);
            pNewServer->SetMemoryStore(pNewMemStore);

            std::vector<RecoverRec> recoverTable;

            std::promise<bool> finishLoading;
            auto f = finishLoading.get_future();

            auto pRecoverJob = ActivityFactory(*pScheduler, L"RecoveryTest");
            auto pRecCompletion = pRecoverJob->GetArena()->allocate<LdChkPntCompletion>(*pRecoverJob, *pNewServer, recoverTable, finishLoading);

            intptr_t handle;
            StatusCode openSuccess = OpenExistingFile(L"D:\\data\\Exabytes.chk", handle);
            Audit::Assert(openSuccess == StatusCode::OK,
                "Failed to open checkpoint file");
            auto pLdChkPntFile = AsyncFileIo::FileQueueFactory(handle, *pScheduler);
            pNewServer->LoadCheckPoint(*pLdChkPntFile, recoverTable, *pRecCompletion);

            f.get();

            // Read once more
            EBTest::Trace("Read Once Again");
            testActivity = ActivityFactory(*pScheduler, L"new test trace");
            testFinished = false;

            pPartition = pNewServer->FindPartitionForKey(Key128(1234, 1234));
            context.m_pServer = pNewServer.get();
            context.m_pStore = pPartition->Persister();
            context.m_pCatalog = pPartition->GetCatalog();

            testActivity->SetContext(ContextIndex::BaseContext, &context);

            auto pRead = testActivity->GetArena()->allocate<ReadStart>(*testActivity);
            pRead->Post(0, 0);
            while (!testFinished)
                Sleep(1000);

            pNewServer->StartCleanup();
            Sleep(5000);
            pNewServer.reset();
            Sleep(1000);
            ::DeleteFile(L"D:\\data\\Exabytes.dat");
            Tracer::DisposeLogging();
        }
    };

}
