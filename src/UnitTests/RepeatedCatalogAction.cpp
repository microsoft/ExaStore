#pragma once
#include "stdafx.h"
#include "CppUnitTest.h"

#include "TestUtils.hpp"

#include "UtilitiesWin.hpp"


namespace EBTest
{
	using namespace Datagram;
	using namespace Exabytes;
	using namespace Schedulers;
	using namespace Utilities;

	//using namespace Microsoft::VisualStudio::CppUnitTestFramework;
	
	// A RepeatedLocateContinuation is called when the TryLocate for a key in catalog finishes
	//
	class RepeatedLocateContinuation : public ContinuationBase
	{
		ReducedMapItem<mt19937_64>*		m_pItem;
		long*                           m_pActionCount;
        RepeatedAction*					m_pTestGenerator;
        RunForSameKeyAgain*             m_pRunForSameKey;
        int m_retry = 0;
	public:
		RepeatedLocateContinuation(
			_In_ Activity& activity,
			_In_ ReducedMapItem<mt19937_64>& item,
			_Inout_ long* pActionCount,
            _In_ RepeatedAction& generator,
            _In_ RunForSameKeyAgain pRunForSameKey
            )
            : ContinuationBase(activity)
            , m_pItem(&item)
            , m_pActionCount(pActionCount)
            , m_pTestGenerator(&generator)
            , m_pRunForSameKey(pRunForSameKey)        
		{}

		void OnReady(
			_In_ WorkerThread&  thread,
			_In_ intptr_t       continuationHandle,
			_In_ uint32_t       length
			) override
		{
            StatusCode status = StatusCode::Unexpected;
            auto retrievedAddr = AddressAndSize::Reinterpret(continuationHandle);

            if (m_pItem->FirstLocation.IsVoid() && retrievedAddr.IsVoid())
            {
                status = StatusCode::OK;
            }
			else if (retrievedAddr.IsVoid())
			{
				Audit::OutOfLine::Fail(status, "Failed to locate item from Catalog!");
			}
            else if (m_pItem->FirstLocation.FullAddress() == retrievedAddr.FullAddress())
            {
                auto expectedSize = Reduction::RoundUpSize(m_pItem->FirstLocation.Size());
                Audit::Assert(expectedSize == retrievedAddr.Size(), "Size should match. Possible memory corruption");
                status = StatusCode::OK;
            }

            if (status == StatusCode::Unexpected)
            {
                Audit::Assert(++m_retry < 6, "Too many retries while trying to locate the key");

                // generate a retry
                m_pRunForSameKey(*m_pTestGenerator, *m_pItem, retrievedAddr.FullAddress(), this);
                return;
            }

            // send a confirmation back to the reader.
            int newValue = ::InterlockedDecrement(m_pActionCount);
            
			m_activity.RequestShutdown(StatusCode::OK);
		}

		void Cleanup() override
		{}
	};

    // Creates a new activity for each locate requests and sends out the requests at a fixed interval
    // this is not a serial request generator
	class RepeatedLocate : public RepeatedAction
	{
		ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>* m_pCatalog;
		Scheduler*           m_pScheduler;
		Activity*            m_pLocateActivity;
		long                 m_requestsRemaining;
		long                 m_locateIncomplete;
		mt19937_64				 m_gen;
		vector<ReducedMapItem<mt19937_64>>* m_pInventory;
		uniform_int_distribution<uint16_t> shorts;
        int     m_numOfCollisions = 0;

	public:
		RepeatedLocate(ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>& catalog, Scheduler& scheduler,
			vector<ReducedMapItem<mt19937_64>>& inventory, mt19937_64 gen)
			: m_pScheduler(&scheduler)
			, m_pInventory(&inventory)
			, m_pCatalog(&catalog)
			, m_gen(gen)
		{
			m_pLocateActivity = Schedulers::ActivityFactory(*m_pScheduler, L"Locate Random");
		}

		// Generate locate on a separate Turn
		//
		bool LocateOneMore()
		{
			unsigned i = shorts(m_gen) % m_pInventory->size();

			// Construct locate call back with its own context
			auto pActivity = Schedulers::ActivityFactory(*m_pScheduler, L"Locate Random");
			auto pArena = pActivity->GetArena();
            auto pContinuation = pArena->allocate<RepeatedLocateContinuation>(*pActivity, (*m_pInventory)[i], &m_locateIncomplete, *this, &LocateSameKeyAgainFunc);

			// issue locate
			m_pCatalog->TryLocate((*m_pInventory)[i].Key, INVALID_FILEOFFSET, pContinuation);

			--m_requestsRemaining;
			return 0 < m_requestsRemaining;
		}

		static bool LocateAgainFunc(RepeatedAction& testGen)
		{
			return ((RepeatedLocate*)(&testGen))->LocateOneMore();
		}

        bool LocateAgain(ReducedMapItem<mt19937_64>& item, size_t addr, ContinuationBase& notify)
        {
            auto shortAddress = Reduction::CompressAddress(addr);
            m_numOfCollisions++;
            // issue locate
            m_pCatalog->TryLocate(item.Key, shortAddress, &notify);
            return 0;
        }        

        static bool LocateSameKeyAgainFunc(RepeatedAction& testGen, ReducedMapItem<mt19937_64>& item, size_t addr, ContinuationBase* pNotify = nullptr)
        {
            return ((RepeatedLocate*)(&testGen))->LocateAgain(item, addr, *pNotify);
        }

		void Run(int count) override
		{
			m_requestsRemaining = count;
			m_locateIncomplete = count;
            
            auto pArena = m_pLocateActivity->GetArena();
            auto locateActor = pArena->allocate<RunAgainActor>(*m_pLocateActivity, *this, &LocateAgainFunc); 
			locateActor->Post((intptr_t) this, 0, 0);

			while (0 < m_locateIncomplete)
			{
				EBTest::Trace("Locate count remaining %d", m_requestsRemaining);
				Sleep(1000);
			}

            EBTest::Trace("Number of collisions encountered %d", m_numOfCollisions);
		}
	};

	std::unique_ptr<RepeatedAction> RepeatedCatalogLocateFactory(ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>& catalog, Scheduler& scheduler,
		vector<ReducedMapItem<mt19937_64>>& inventory, mt19937_64 gen)
	{
		return make_unique<RepeatedLocate>(catalog, scheduler, inventory, gen);
	}


	// A RepeatedReLocateContinuation is called when the relocate operation in catalog
	// finishes.
	//
	class RepeatedRelocateContinuation : public ContinuationBase
	{
		long*							m_pActionCount;
		RepeatedAction*					m_pTestGenerator;
		RunAgain*						m_pRunAgainProc;		

	public:
		RepeatedRelocateContinuation(
			_In_ Activity& activity,
			_Inout_ long* pActionCount,
			_In_ RepeatedAction& generator,
			_In_ RunAgain pRunAgainProc
			)
			: ContinuationBase(activity)
			, m_pActionCount(pActionCount)
			, m_pTestGenerator(&generator)
			, m_pRunAgainProc(pRunAgainProc)
		{}
		
		void OnReady(
			_In_ WorkerThread&  thread,
			_In_ intptr_t       continuationHandle,
			_In_ uint32_t       length
			) override
		{
			auto result = (StatusCode)continuationHandle;
			Audit::Assert(result == StatusCode::OK, "relocation failed");
						
			// send a confirmation back to the reader.
			int newValue = ::InterlockedDecrement(m_pActionCount);			
			
			// initiate another operation
			m_pRunAgainProc(*m_pTestGenerator);
			
		}

		void Cleanup() override
		{}
	};
		
	// Serial Relocate request generator
	// NOTE: 2 relocates should never overlap
	//
	class RepeatedRelocate : public RepeatedAction
	{
		ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>* m_pCatalog;
		Scheduler*           m_pScheduler;		
		long                 m_requestsRemaining;
		long                 m_requestsIncomplete;
		mt19937_64				 m_gen;
		vector<ReducedMapItem<mt19937_64>>*	m_pInventory;
		uniform_int_distribution<uint32_t>	m_shorts;
		ContinuationBase*           		m_pCompletionContinuation;

	public:
		RepeatedRelocate(ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>& catalog, Scheduler& scheduler,
			vector<ReducedMapItem<mt19937_64>>& inventory, mt19937_64 gen)
			: m_pScheduler(&scheduler)
			, m_pInventory(&inventory)
			, m_pCatalog(&catalog)
			, m_gen(gen)
		{
			auto pRelocateActivity = Schedulers::ActivityFactory(*m_pScheduler, L"ReLocate Random");
			m_pCompletionContinuation = pRelocateActivity->GetArena()->allocate<RepeatedRelocateContinuation>(
				*pRelocateActivity, &m_requestsIncomplete, *this, &RelocateAgainFunc);
		}

		// Generate relocate
		//
		bool RelocateOneMore()
		{
			if (m_requestsRemaining <= 0)
				return 0;

			unsigned i = m_shorts(m_gen) % m_pInventory->size();
			auto item = (*m_pInventory)[i];
			
			Audit::Assert(!item.FirstLocation.IsVoid(), "Cannot relocate already expired item");						 

			// issue relocate
			uint32_t oldAddr = Reduction::CompressAddress(item.FirstLocation.FullAddress());
			uint32_t newAddr = Reduction::CompressAddress(item.SecondLocation.FullAddress());
			m_pCatalog->Relocate(item.Key, oldAddr, newAddr, m_pCompletionContinuation);

			item.FirstLocation = item.SecondLocation;
			(*m_pInventory)[i] = item;

			--m_requestsRemaining;
			return 0;
		}

		static bool RelocateAgainFunc(RepeatedAction& testGen)
		{
			return ((RepeatedRelocate*)(&testGen))->RelocateOneMore();
		}

		void Run(int count) override
		{
			m_requestsRemaining = count;
			m_requestsIncomplete = count;			

			// Initiate Relocates
			RelocateOneMore();

			while (0 < m_requestsIncomplete)
			{
				EBTest::Trace("Relocate count remaining %d", m_requestsRemaining);
				Sleep(1000);
			}
		}
	};

	std::unique_ptr<RepeatedAction> RepeatedCatalogRelocateFactory(ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>& catalog, Scheduler& scheduler,
		vector<ReducedMapItem<mt19937_64>>& inventory, mt19937_64 gen)
	{		
		return make_unique<RepeatedRelocate>(catalog, scheduler, inventory, gen);
	}

	// Serial expire request generator
	// NOTE: 2 expires should never overlap
	//
	class RepeatedExpire : public RepeatedAction
	{
		ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>* m_pCatalog;
		Scheduler*           m_pScheduler;
		Activity*            m_pExpireActivity;
		long                 m_requestsRemaining;
		mt19937_64				 m_gen;
		vector<ReducedMapItem<mt19937_64>>* m_pInventory;
		uniform_int_distribution<uint32_t> m_shorts;
		ContinuationBase*            m_pCompletionContinuation;
	public:
		RepeatedExpire(ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>& catalog, Scheduler& scheduler,
			vector<ReducedMapItem<mt19937_64>>& inventory, mt19937_64 gen)
			: m_pScheduler(&scheduler)
			, m_pInventory(&inventory)
			, m_pCatalog(&catalog)
			, m_gen(gen)
		{
			auto pExpireActivity = Schedulers::ActivityFactory(*m_pScheduler, L"Expire Random");			
			m_pCompletionContinuation = pExpireActivity->GetArena()->allocate<RepeatedRelocateContinuation>(
				*pExpireActivity, &m_requestsRemaining, *this, &ExpireAgainFunc);
		}

		// Generate expire request on a separate Turn
		//
		bool ExpireOneMore()
		{
			if (m_requestsRemaining < 1)
				return 0;

			unsigned i = m_shorts(m_gen) % m_pInventory->size();
			auto item = (*m_pInventory)[i];
			
			// we already expired this item so find a new one
			while (item.FirstLocation.IsVoid())
			{
				i = m_shorts(m_gen) % m_pInventory->size();
				item = (*m_pInventory)[i];
                //::InterlockedDecrement(&m_requestsRemaining);
			}
			
			// issue relocate
			uint32_t oldAddr = Reduction::CompressAddress(item.FirstLocation.FullAddress());
			uint32_t newAddr = Reduction::UNUSED_ADDRESS;
			m_pCatalog->Relocate(item.Key, oldAddr, newAddr, m_pCompletionContinuation);

			// change the location to void so we know this is expired entry
			item.FirstLocation = AddressAndSize{};
			(*m_pInventory)[i] = item;
			
			return 0;
		}

		static bool ExpireAgainFunc(RepeatedAction& testGen)
		{
			return ((RepeatedExpire*)(&testGen))->ExpireOneMore();
		}

		void Run(int count) override
		{
			m_requestsRemaining = count;
			ExpireOneMore();

			uint32_t seconds = 0;
			while (0 < m_requestsRemaining)
			{
				EBTest::Trace("Expire count remaining %d", m_requestsRemaining);
				Sleep(1000);
				++seconds;
			}
		}
	};

	std::unique_ptr<RepeatedAction> RepeatedCatalogItemExpireFactory(ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>& catalog, Scheduler& scheduler,
		vector<ReducedMapItem<mt19937_64>>& inventory, mt19937_64 gen)
	{
		return make_unique<RepeatedExpire>(catalog, scheduler, inventory, gen);
	}	

    // A RepeatedLocateContinuation is called when the TryLocate for a key in catalog finishes
    //
    class RepeatedSerialLocateContinuation : public ContinuationBase
    {
        ReducedMapItem<mt19937_64>*		m_pItem;
        long*							m_pActionCount;
        RepeatedAction*					m_pTestGenerator;
        RunAgain*						m_pRunAgainProc;
        RunForSameKeyAgain*             m_pRunForSameKey;
        unsigned                        m_retry = 0;
        
    public:
        RepeatedSerialLocateContinuation(
            _In_ Activity& activity,
            _Inout_ long* pActionCount,
            _In_ RepeatedAction& generator,
            _In_ RunAgain pRunAgainProc,
            _In_ RunForSameKeyAgain pRunForSameKey
            )
            : ContinuationBase(activity)
            , m_pActionCount(pActionCount)
            , m_pTestGenerator(&generator)
            , m_pRunAgainProc(pRunAgainProc)
            , m_pRunForSameKey(pRunForSameKey)
        {}

        void ExpectedItem(_In_ ReducedMapItem<mt19937_64>& item)
        {
            m_pItem = &item;
        }

        void OnReady(
            _In_ WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            ) override
        {
            StatusCode status = StatusCode::Unexpected;
            auto retrievedAddr = AddressAndSize::Reinterpret(continuationHandle);
            
            if (m_pItem->FirstLocation.IsVoid() && retrievedAddr.IsVoid())
            {
                status = StatusCode::OK;
            }
            else if (m_pItem->FirstLocation.FullAddress() == retrievedAddr.FullAddress())
            {
                auto expectedSize = Reduction::RoundUpSize(m_pItem->FirstLocation.Size());
                Audit::Assert(expectedSize == retrievedAddr.Size(), "Size should match. Possible memory corruption");                
                status = StatusCode::OK;
            }

            if (status == StatusCode::Unexpected)
            {                
                Audit::Assert(++m_retry < 6, "Too many retries while trying to locate the key");
                m_pRunForSameKey(*m_pTestGenerator, *m_pItem, retrievedAddr.FullAddress(), nullptr);
                return;
            }

            m_retry = 0;
            // send a confirmation back to the reader.
            int newValue = ::InterlockedDecrement(m_pActionCount);

            // initiate another operation
            m_pRunAgainProc(*m_pTestGenerator);                                   
        }

        void Cleanup() override
        {}
    };

    // Generated repeated locate requests serially
    // Good for level 1 basic unit testing
    class RepeatedSerialLocate : public RepeatedAction
    {
        ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>* m_pCatalog;
        Scheduler*           m_pScheduler;
        Activity*            m_pLocateActivity;
        long                 m_requestsRemaining;
        long                 m_locateIncomplete;
        mt19937_64				 m_gen;
        vector<ReducedMapItem<mt19937_64>>* m_pInventory;
        uniform_int_distribution<uint16_t> shorts;
        RepeatedSerialLocateContinuation*	m_pCompletionContinuation;
        int            m_numOfCollisions = 0;        

    public:
        RepeatedSerialLocate(ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>& catalog, Scheduler& scheduler,
            vector<ReducedMapItem<mt19937_64>>& inventory, mt19937_64 gen)
            : m_pScheduler(&scheduler)
            , m_pInventory(&inventory)
            , m_pCatalog(&catalog)
            , m_gen(gen)
        {
            auto pLocateActivity = Schedulers::ActivityFactory(*m_pScheduler, L"Locate Random");
            m_pCompletionContinuation = pLocateActivity->GetArena()->allocate<RepeatedSerialLocateContinuation>(
                *pLocateActivity, &m_requestsRemaining, *this, &LocateAgainFunc, &LocateSameKeyAgainFunc);
        }

        // Generate locate on a separate Turn
        //
        bool LocateOneMore()
        {
            if (m_requestsRemaining < 0)
                return 0;            
            
            m_pCompletionContinuation->ExpectedItem((*m_pInventory)[m_requestsRemaining]);

            // issue locate
            m_pCatalog->TryLocate((*m_pInventory)[m_requestsRemaining].Key, INVALID_FILEOFFSET, m_pCompletionContinuation);

            return 0;
        }
        
        bool LocateAgain(ReducedMapItem<mt19937_64>& item, size_t addr)
        {
            auto shortAddress = Reduction::CompressAddress(addr);
            m_numOfCollisions++;
            // issue locate
            m_pCatalog->TryLocate(item.Key, shortAddress, m_pCompletionContinuation);
            return 0;
        }

        static bool LocateAgainFunc(RepeatedAction& testGen)
        {
            return ((RepeatedSerialLocate*)(&testGen))->LocateOneMore();
        }

        static bool LocateSameKeyAgainFunc(RepeatedAction& testGen, ReducedMapItem<mt19937_64>& item, size_t addr, ContinuationBase* pNext = nullptr)
        {
            return ((RepeatedSerialLocate*)(&testGen))->LocateAgain(item, addr);
        }

        void Run(int count) override
        {
            Audit::Assert(count <= m_pInventory->size(), "For RepeatedSerialLocate count has to be less than inventory size. Check RepeatedLocated.");
            m_requestsRemaining = count-1;
            LocateOneMore();

            while (0 < m_requestsRemaining)
            {
                EBTest::Trace("Locate count remaining %d", m_requestsRemaining);
                Sleep(10000);
            }

            EBTest::Trace("Number of collisions encountered %d", m_numOfCollisions);
        }
    };    
    
    std::unique_ptr<RepeatedAction> RepeatedSerialCatalogLocateFactory(ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>& catalog, Scheduler& scheduler,
        vector<ReducedMapItem<mt19937_64>>& inventory, mt19937_64 gen)
    {
        return make_unique<RepeatedSerialLocate>(catalog, scheduler, inventory, gen);
    }	
}
