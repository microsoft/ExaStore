#pragma once
#include "stdafx.h"

#include <algorithm>
#include <iostream>
#include <memory>
#include <random>
#include <utility>
#include <vector>
#include <unordered_set>

#include "ServiceBrokerRIO.hpp"
#include "MemoryStore.hpp"
#include "UtilitiesWin.hpp"

#include "TestHooks.hpp"
#include "TestUtils.hpp"

#include "CppUnitTest.h"

namespace EBTest
{
    using namespace std;
    using namespace Datagram;
    using namespace Schedulers;
    using namespace Exabytes;
    using namespace Microsoft::VisualStudio::CppUnitTestFramework;
    using namespace Utilities;

   
	class CompactJob : public Schedulers::ContinuationBase
    {
    private:
        bool m_stopping;
        bool m_inprocess;
        ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>* m_pCatalog;

    public:
        CompactJob(Activity& activity, ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>& catalog)
            : ContinuationBase(activity)
            , m_stopping(false)
            , m_inprocess(false)
            , m_pCatalog(&catalog)
        {}

        void Stop()
        {
            m_stopping = true;
        }

        bool InProcess()
        {
            return m_inprocess;
        }

        void OnReady(
            _In_ WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       messageLength
            ) override
        {            
            if (!m_stopping)
            {
                m_inprocess = true;
                m_pCatalog->Compact(this);
            }
            else {
                m_inprocess = false;
            }
        }

        void Cleanup() override
        { }
    };

    class TaskCompleted : public Schedulers::ContinuationBase
    {
    private:
        int& m_complete;

    public:
        TaskCompleted(Activity& activity, int& complete)
            : ContinuationBase(activity)
            , m_complete(complete)
        {}

        void OnReady(
            _In_ WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       messageLength
            ) override
        {
            Audit::Assert((StatusCode)continuationHandle == StatusCode::OK, "Catalog GC failed");
            m_complete++;
        }

        void Cleanup() override
        { }
    };

    // Garbage collector for hash block pool.
    class BufferPoolGarbageCollector : public ContinuationBase
    {
    private:
        bool m_shutdownRequested = false;
        ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>* m_pCatalog;
        CatalogBufferPool<uint32_t>* m_pHashPool;
        bool m_pause = false;

    public:

        BufferPoolGarbageCollector(
            Activity& activity,
            ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>* pCatalog,
            CatalogBufferPool<uint32_t>& hashPool
            )
            : ContinuationBase(activity)            
            , m_pCatalog(pCatalog)
            , m_pHashPool(&hashPool)
        { }

        void Start()
        {
            m_pause = false;
            this->Post(0, 0);
        }

        void Pause()
        {
            m_pause = true;
        }

        void RequestShutdown()
        {
            m_shutdownRequested = true;
        }
        
        void OnReady(
            _In_ WorkerThread&,
            intptr_t       continuationHandle,
            uint32_t       messageLength
            ) override
        {
            if (m_shutdownRequested)
            {
                Cleanup();
                m_activity.RequestShutdown(StatusCode::OK);
                return;
            }

            if (m_pause)
            {
                this->Post(0, 0, 5000);
                return;
            }

            if (m_pHashPool->IsRunningLow())
            {
                m_pCatalog->DiscardHashBlocks();                                
                this->Post(0, 0);  
                return;
            }

            // we have enough space available so check back in 5 ms
            this->Post(0, 0, 5000);
        }

        void Cleanup() override
        {}
    };
    
    TEST_CLASS(CatalogTest)
    {
        vector<ReducedMapItem<mt19937_64>> m_items;
        unique_ptr<Exabytes::CatalogBufferPool<>> m_pHashPool = nullptr;
        unique_ptr<Exabytes::CatalogBufferPool<>> m_pBloomKeyBlockPool = nullptr;        
        unique_ptr<ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>> m_pCatalog;
        BufferPoolGarbageCollector* m_pHashPoolGc = nullptr;
        std::unique_ptr<ServiceBrokers::ServiceBroker> m_pSB;
        Schedulers::Scheduler* m_pScheduler = nullptr;

        std::random_device m_rDev;
        std::uniform_int_distribution<uint64_t> m_r;
        mt19937_64 m_gen;

        void Setup(unsigned itemCount)
        {
            Tracer::InitializeLogging(L"D:\\data\\testlog.etl");
            std::wstring name{ L"Reduced Catalog Test" };
            Utilities::EnableLargePages();

            m_pSB = ServiceBrokers::ServiceBrokerRIOFactory(name);
            std::cout << "address " << m_pSB->HomeAddressIP() << " port " << m_pSB->HomeAddressPort() << '\n';

            m_gen.seed(m_r(m_rDev));

            uniform_int_distribution<uint64_t> uniform;
            lognormal_distribution<double> logNormal{ 1, 1 };

            m_pScheduler = m_pSB->GetScheduler();

            m_items.clear();
            for (unsigned i = 0; i < itemCount; ++i)
            {
                ReducedMapItem<mt19937_64> x{ m_gen, uniform, logNormal };
                m_items.push_back(x);
            }

            m_pBloomKeyBlockPool = Exabytes::SharedCatalogBufferPoolFactory(INMEMHASHBLKCOUNT, TOTALHASHBLKS, 1, BLOOMKEYBLOCKSIZE);
            m_pHashPool = Exabytes::SharedCatalogBufferPoolFactory(INMEMHASHBLKCOUNT, TOTALHASHBLKS, 1, HASHBLOCKSIZE);
            // we use a repeatable randomizer in test
            m_pCatalog = Exabytes::ReducedKeyMapFactory(*m_pScheduler, L"D:\\data\\file-catlogFile", *m_pHashPool.get(), *m_pBloomKeyBlockPool.get(), 0xBAADF00DADD5C731ull);
            
            Activity* pHashPoolGCActivity = Schedulers::ActivityFactory(*m_pScheduler, L"Hash Pool GC");
            m_pHashPoolGc = pHashPoolGCActivity->GetArena()->allocate<BufferPoolGarbageCollector>(*pHashPoolGCActivity, m_pCatalog.get(), *m_pHashPool.get());            
        }

        void Shutdown()
        {
            m_pHashPoolGc->RequestShutdown();
            Tracer::DisposeLogging();
        }

    public:

        // Test for ReducedKeyMap
        // write a bunch of items in, read it out and verify they are correct;
        //
        TEST_METHOD(ReducedMapAdd)
        {
            
			static const unsigned ItemCount = 600000;
            Setup(ItemCount);

            m_pHashPoolGc->Start();

			// Test Add
            for (unsigned i = 0; i < ItemCount; ++i)
            {
                auto x = m_items[i];
                auto address = Reduction::CompressAddress(x.FirstLocation.FullAddress());
                auto size = Reduction::CompressSize(x.FirstLocation.Size());
                m_pCatalog->Add(x.Key, address, size);
            }

			unique_ptr<RepeatedAction> tryLocate =
				RepeatedSerialCatalogLocateFactory(*m_pCatalog, *m_pScheduler, m_items, m_gen);
			tryLocate->Run(ItemCount);

            // Discard should not overlap with relocate or expire
            m_pHashPoolGc->Pause();

            // Test relocate
			unique_ptr<RepeatedAction> pRelocate = 
				RepeatedCatalogRelocateFactory(*m_pCatalog, *m_pScheduler, m_items, m_gen);
			pRelocate->Run(ItemCount/2);

			tryLocate = RepeatedSerialCatalogLocateFactory(*m_pCatalog, *m_pScheduler, m_items, m_gen);
			tryLocate->Run(ItemCount);
            
            // Test expire
			unique_ptr<RepeatedAction> pExpire =
				RepeatedCatalogItemExpireFactory(*m_pCatalog, *m_pScheduler, m_items, m_gen);
			pExpire->Run(ItemCount/2);

            m_pHashPoolGc->Start();

			tryLocate = RepeatedSerialCatalogLocateFactory(*m_pCatalog, *m_pScheduler, m_items, m_gen);
			tryLocate->Run(ItemCount);            

            Shutdown();
        }

        TEST_METHOD(ReducedMapKeyOverlap)
        {

            static const unsigned ItemCount = 500000;
            Setup(ItemCount);

            m_pHashPoolGc->Start();

            // Test Add
            for (unsigned i = 0; i < ItemCount; ++i)
            {
                auto x = m_items[i];
                auto address = Reduction::CompressAddress(x.FirstLocation.FullAddress());
                auto size = Reduction::CompressSize(x.FirstLocation.Size());
                m_pCatalog->Add(x.Key, address, size);                
            }
            for (unsigned i = ItemCount-1; i > 0; --i)
            {
                auto x = m_items[i];
                auto address = Reduction::CompressAddress(x.SecondLocation.FullAddress());
                auto size = Reduction::CompressSize(x.SecondLocation.Size());
                m_pCatalog->Add(x.Key, address, size);
            }           

            unique_ptr<RepeatedAction> tryLocate =
                RepeatedSerialCatalogLocateFactory(*m_pCatalog, *m_pScheduler, m_items, m_gen);
            tryLocate->Run(ItemCount);            

            Shutdown();
        }

        TEST_METHOD(ReducedMapCompaction)
        {
			static const unsigned ItemCount = 1000000;
            Setup(ItemCount);

            m_pHashPoolGc->Start();

            // add all items into catalog
            for (unsigned i = 0; i < ItemCount; ++i)
            {                
                auto x = m_items[i];
                auto address = Reduction::CompressAddress(x.FirstLocation.FullAddress());
                auto size = Reduction::CompressSize(x.FirstLocation.Size());
                m_pCatalog->Add(x.Key, address, size);
            }

            m_pCatalog->SetGCStart();

            // remove about half of it from catalog
			unique_ptr<RepeatedAction> pExpire =
				RepeatedCatalogItemExpireFactory(*m_pCatalog, *m_pScheduler, m_items, m_gen);
			pExpire->Run(ItemCount/2);

			unique_ptr<RepeatedAction> tryLocate = RepeatedSerialCatalogLocateFactory(*m_pCatalog, *m_pScheduler, m_items, m_gen);
			tryLocate->Run(ItemCount);
            
			// start compaction
			auto pActivity = Schedulers::ActivityFactory(*m_pScheduler, L"Compaction");
			auto pArena = pActivity->GetArena();
			auto pContinuation = pArena->allocate<CompactJob>(*pActivity, *m_pCatalog);
			pContinuation->Post(0, 0);
            
			// wait for it to start processing and then set stop
			while (!pContinuation->InProcess())
				        Sleep(500);
            
			pContinuation->Stop();

			// wait for compaction to finish
			while (pContinuation->InProcess())
				Sleep(500);

            int complete = 0;
            auto pNotify = pArena->allocate<TaskCompleted>(*pActivity, complete);
            m_pCatalog->StartCatalogFileGC(pNotify);

            while (complete == 0)
                Sleep(5000);

            m_pCatalog->SetGCEnd();                       

			// Test compaction 
			tryLocate = RepeatedSerialCatalogLocateFactory(*m_pCatalog, *m_pScheduler, m_items, m_gen);
			tryLocate->Run(ItemCount/2);

            m_pCatalog->SweepToFileStore();

            tryLocate = RepeatedSerialCatalogLocateFactory(*m_pCatalog, *m_pScheduler, m_items, m_gen);
            tryLocate->Run(ItemCount);

            pActivity->RequestShutdown(StatusCode::OK);

            Shutdown();
        }

        TEST_METHOD(ReducedMapConcurrentCompaction)
        {
			static const unsigned ItemCount = 600000;
            Setup(ItemCount);

            m_pHashPoolGc->Start();

            unordered_set<unsigned> elimatedSet;
            uniform_int_distribution<uint16_t> shorts;

            // add all items into catalog
            for (unsigned i = 0; i < ItemCount; ++i)
            {
                auto x = m_items[i];
                auto address = Reduction::CompressAddress(x.FirstLocation.FullAddress());
                auto size = Reduction::CompressSize(x.FirstLocation.Size());
                m_pCatalog->Add(x.Key, address, size);
            }

            m_pCatalog->SetGCStart();
            // remove almost all of it from catalog
			unique_ptr<RepeatedAction> pExpire =
				RepeatedCatalogItemExpireFactory(*m_pCatalog, *m_pScheduler, m_items, m_gen);
			pExpire->Run(ItemCount * 4/5);

			unique_ptr<RepeatedAction> tryLocate = RepeatedSerialCatalogLocateFactory(*m_pCatalog, *m_pScheduler, m_items, m_gen);
			tryLocate->Run(ItemCount);
            
            auto pActivity = Schedulers::ActivityFactory(*m_pScheduler, L"Compaction");
            auto pArena = pActivity->GetArena();
            auto pContinuation = pArena->allocate<CompactJob>(*pActivity, *m_pCatalog);
            pContinuation->Post(0, 0);

            // query overlapping with compaction
			tryLocate = RepeatedSerialCatalogLocateFactory(*m_pCatalog, *m_pScheduler, m_items, m_gen);
			tryLocate->Run(ItemCount);
            
            // insert more (overlapping with compaction)
			for (unsigned i = 0; i < ItemCount; ++i)
            {
				// we expired this item so add it back
				if (m_items[i].FirstLocation.IsVoid())
				{
					auto x = m_items[i];
					auto address = Reduction::CompressAddress(x.SecondLocation.FullAddress());
					auto size = Reduction::CompressSize(x.SecondLocation.Size());
					m_pCatalog->Add(x.Key, address, size);					
					m_items[i].FirstLocation = x.SecondLocation;
				}                
            }

            // query again
			tryLocate = RepeatedSerialCatalogLocateFactory(*m_pCatalog, *m_pScheduler, m_items, m_gen);
			tryLocate->Run(ItemCount);            
            
			// stop compaction
            pContinuation->Stop();
            while (pContinuation->InProcess())
                Sleep(500);

            pActivity->RequestShutdown(StatusCode::OK);

            m_pCatalog->SetGCEnd();

            Shutdown();
        }        
    };

}
