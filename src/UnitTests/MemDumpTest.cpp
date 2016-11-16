#pragma once
#include "stdafx.h"

#include <utility>
#include <algorithm>
#include <random>
#include <unordered_map>
#include <memory>
#include <future>
#include <iostream>

#include "Exabytes.hpp"
#include "UtilitiesWin.hpp"
#include "ServiceBrokerRIO.hpp"
#include "EbServer.hpp"
#include "EbPartition.hpp"
#include "MemoryStore.hpp"
#include "FileManager.hpp"

#include "TestHooks.hpp"
#include "TestUtils.hpp"

#include "CppUnitTest.h"


using namespace Microsoft::VisualStudio::CppUnitTestFramework;
using namespace Datagram;
using namespace Exabytes;
using namespace Schedulers;

namespace EBTest
{

    class DummyServer : public ExabytesServer
    {
    public:
        DummyServer(
            std::unique_ptr<ServiceBrokers::ServiceBroker>& pBroker,
            std::unique_ptr<CoalescingBufferPool>& pBufferPool,
            std::unique_ptr<PolicyManager>& pPolicyManager,
            std::unique_ptr<CatalogBufferPool<>>& pHashPoolManager,
            std::unique_ptr<CatalogBufferPool<>>& pBloomKeyPoolManager
            )
            : ExabytesServer(pBroker, pBufferPool, pPolicyManager, pHashPoolManager, pBloomKeyPoolManager)
        {}

        void SaveCheckPoint(
            // Check point file to write to
            AsyncFileIo::FileManager& file,

            // Can be nullptr if fire and forget
            ContinuationBase* pNotify)
        {
            Audit::NotImplemented("Dummy has no real functionality!");
        }

        void LoadCheckPoint(
            // Check point file to read from
            _In_ AsyncFileIo::FileManager& file,

            // Decoded checkpoint is saved into recoverTable
            _Out_ std::vector<RecoverRec>& recoverTable,

            // Notification that the async operation finished
            _In_ ContinuationBase& completion
            )
        {
            Audit::NotImplemented("Dummy has no real functionality!");
        }

        void StartSweeper() {}

        void StartGC() {}

        void SendStoreToGc(Datagram::PartitionId) {}

        bool PartitionExists(Datagram::PartitionId) const 
        {
            return false;
        }

        StatusCode CreatePartFile(const wchar_t* filePath, size_t size, bool isSSD)
        {
            return StatusCode::Unexpected;
        }

        void AddFileHandle(Schedulers::FileHandle handle, bool isSSD) {}

        std::pair<size_t, size_t> SpareCapacity() 
        {
            return std::make_pair(0, 0);
        }

        StatusCode StartPartition(Datagram::PartitionId id, bool isPrimary) 
        {
            return StatusCode::Unexpected;
        }

        void StopPartition(Datagram::PartitionId id) {}

        Partition* FindPartitionForKey(Datagram::Key128 key) const 
        {
            Audit::NotImplemented("Dummy has no real functionality!");
            return nullptr;
        }

        Datagram::PartitionId FindPartitionIdForKey(Datagram::Key128 key) const 
        {
            Audit::NotImplemented("Dummy has no real functionality!");
            return 0;
        }

        void Recover(
            _Inout_ std::vector<RecoverRec>& recTable,
            _In_ FileRecoveryCompletion& notifier
            )
        {
            Audit::NotImplemented("Dummy has no real functionality!");
        }

        StatusCode SetPartition(
            Datagram::PartitionId pid,
            _In_ std::unique_ptr<Partition>& pPartition) override
        {
            Audit::NotImplemented("Dummy has no real functionality!");
            return StatusCode::Abort;
        }

    };

    class FileLoadCompletion : public ContinuationBase
    {
        std::promise<bool>& m_prom;

    public:
        FileLoadCompletion(Activity& act, std::promise<bool>& prom)
            : ContinuationBase(act)
            , m_prom(prom)
        {}

        void Cleanup() override
        {
            // ignore clean up in testing
        }

        void OnReady(
            _In_ WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            ) override
        {
            Audit::Assert(continuationHandle == (intptr_t)StatusCode::OK,
                "Failure in dump memory store to file");
            // signal the main thread to continue
            m_prom.set_value(true);
        }
    };

    class FileDumpCompletion : public ContinuationBase
    {
        unique_ptr<Exabytes::KeyValueInMemory>& m_pMem;
        Exabytes::ExabytesServer& m_svr;
        AsyncFileIo::FileManager& m_file;
        std::promise<bool>& m_prom;

    public:
        std::pair<uint64_t, uint64_t>* m_pEdges = nullptr;

        FileDumpCompletion(Activity& act,
            unique_ptr<Exabytes::KeyValueInMemory>& pMem,
            Exabytes::ExabytesServer& svr,
            AsyncFileIo::FileManager& file,
            std::promise<bool>& prom
            )
            : ContinuationBase(act)
            , m_pMem(pMem)
            , m_svr(svr)
            , m_file(file)
            , m_prom(prom)
        {}

        void Cleanup() override
        {
            // ignore clean up in testing
        }

        void OnReady(
            _In_ WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            ) override
        {
            Audit::Assert(continuationHandle == (intptr_t)StatusCode::OK,
                "Failure in dump memory store to file");

            auto pArena = GetArena();
            auto pNotify = pArena->allocate<FileLoadCompletion>(m_activity, m_prom);

            // create another memory store
            Audit::Assert(m_pEdges != nullptr, "Failed to obtain memory store edges during file write!");
            m_pMem = Exabytes::MemoryStoreFactory(m_svr, (size_t)64 * SI::Mi, m_file, *m_pEdges, pNotify);
        }

    };

    TEST_CLASS(MemoryStoreDumpTest)
    {
        TEST_METHOD(MemoryStoreDump)
        {
            Tracer::InitializeLogging(L"D:\\data\\testlog.etl");
            std::wstring name{ L"home service" };
            Utilities::EnableLargePages();

            std::unique_ptr<ServiceBrokers::ServiceBroker> pSB = 
                ServiceBrokers::ServiceBrokerRIOFactory(name);
            Scheduler& scheduler = *(pSB->GetScheduler());

            // We don't really need a server for testing, we just want the memory store.
            // only if I can just stick in a dummy server there for creating memory store.
            auto pBufferPool = Exabytes::BufferPoolFactory(2 * SI::Mi, 6, MockBufferFactory);
            
            // if we dont need a server then no need to assign memory for these buffers
            std::unique_ptr<Exabytes::CatalogBufferPool<>> pDummyHashPool = nullptr;
            std::unique_ptr<Exabytes::CatalogBufferPool<>> pDummyBloomKeyPool = nullptr;

            std::unique_ptr<Exabytes::ExabytesServer> pServer = 
                std::make_unique<DummyServer>(
                pSB,
                pBufferPool,
                Exabytes::PolicyTableFactory(),
                pDummyHashPool,
                pDummyBloomKeyPool);

            unique_ptr<KeyValueInMemory> pMemoryStore = MemoryStoreFactory(*pServer, 64 * SI::Mi);

            TestHooks::LocalStoreTestHooks testhook;
            // set the writing edge near the end of the circular buffer to test the ability for wrapping around buffer end.
            size_t allocEdge = testhook.SizeOfMemoryStore(pMemoryStore.get())
                - 576 * 1024;
            testhook.SetEdgesOfMemoryStore(pMemoryStore.get(), allocEdge,
                allocEdge + testhook.SizeOfMemoryStore(pMemoryStore.get()));

            // finish setup, begin write

            shared_ptr<InventoryVector> pInventory = make_shared<InventoryVector>();
            KeyValueInMemory& mStore = *pMemoryStore;

            std::unique_ptr<RepeatedAction> writes = RepeatedMemStoreWriteFactory(
                mStore, scheduler, pInventory);

            writes->Run(20);

            auto fileQueue = AsyncFileIo::FileQueueFactory(L"MemStoreDump.bin", scheduler, 64 * SI::Mi);

            // dump to file and read back in again.
            std::promise<bool> finished;
            std::future<bool> future = finished.get_future();

            // create activity for creating another memory store and load the file content
            // into it.
            auto pJob = ActivityFactory(scheduler, L"Memory Dump");
            auto pNotify = pJob->GetArena()->allocate<FileDumpCompletion>(*pJob, pMemoryStore, *pServer, *fileQueue, finished);

            auto edges = pMemoryStore->StartDump(*fileQueue, pNotify);
            pNotify->m_pEdges = &edges;

            // wait until we dump this memory store to disk, read it back in
            // and create a new one.
            future.get();

            // read

            std::vector<pair<Datagram::PartialDescription, uint32_t>> inventory;
            pInventory->SetAndReleaseSelf(inventory);
            for (auto& pair : inventory)
            {
                BufferEnvelope resBuf;

                PartialDescription descriptor;
                ::ZeroMemory(&descriptor, sizeof(descriptor));
                descriptor = pair.first;
                auto expCrc = pair.second;

                auto status = pMemoryStore->Read(100, descriptor, resBuf);
                Audit::Assert(status == StatusCode::OK, "Read failure");

                auto length = resBuf.Contents()->Size() - resBuf.Contents()->Offset();

                auto crc = Utilities::PsuedoCRC(resBuf.Contents()->PData(), length);
                resBuf.Recycle();

                if (expCrc != crc)
                {
                    Audit::Assert(false, "CRC did not match after read");
                }

            }

            pServer.reset();
            Sleep(1000);
            Tracer::DisposeLogging();

        }

    };
}