
#pragma once

#include "stdafx.h"

#include "Datagram.hpp"
#include "Utilities.hpp"
#include "Exabytes.hpp"
#include "ReducedKeyMap.hpp"
#include <Windows.h>

#include <algorithm>
#include <random>
#include <unordered_map>
#include <memory>

namespace EBTest
{
    using namespace std;
	
    static const uint32_t g_catalogCapacity = 2 * SI::Mi;
    
    // every key maps to a single partition: 0
    extern Datagram::PartitionId DummyHash(const Datagram::Key128 key);

    // every key maps to partition 0 or 1
    extern Datagram::PartitionId BinHash(const Datagram::Key128 key);

    class DummyPolicyManager : public Exabytes::PolicyManager
    {
        Datagram::Policy m_policy;
    public:
        DummyPolicyManager(uint16_t expireMinutes)
        {
            // get around constructor to give it a small expiration
            // for testing.
            uint16_t* p = reinterpret_cast<uint16_t*>(&m_policy);
            *p = expireMinutes;
        }
        // Each data owner can have it's own policy, such as
        // expiration time for its data.
        //
        const Datagram::Policy GetPolicy(Datagram::Key64) const override
        {
            return m_policy;
        }

        // Set policy for certain owner
        void SetPolicy(Datagram::Key64, const Datagram::Policy&) override
        {
            // do nothing
        }
    };

    class DummyRecover : public Exabytes::FileRecover
    {
        void RecoverFromFiles(
            Exabytes::ExabytesServer& server,
            // partition hash, used to decide which partition
            // a key belongs to
            _In_ Datagram::PartitionId(*partHash)(const Datagram::Key128),

            // In: file handle and checkpoint, Out: status and partition
            _Inout_ std::vector<Exabytes::RecoverRec>& recTable,

            // Completion callback, parameter past to RecoverFinished
            // should be the same with the one given by the parameter
            // above
            // 
            _In_ Exabytes::FileRecoveryCompletion& notifier
            ) override
        {
            Audit::NotImplemented("Not supported!");
        }

    };

    class RandomWrites
    {
        unique_ptr<Utilities::LargePageBuffer> m_pNoise;
        unique_ptr<Utilities::LargePageBuffer> m_pBuf;
        size_t m_noiseSize;
        random_device m_seed;
        uniform_int_distribution<uint64_t> m_r;

        size_t m_average;
        size_t m_variation;

    public:
        RandomWrites(size_t variation = 31 * 1024, size_t average = 512 * SI::Ki)
        {
            m_variation = variation;
            m_average = average;

            const size_t randomReserveSize = 8 << 20;

            unsigned scrapCount;
            m_pNoise = make_unique<Utilities::LargePageBuffer>(randomReserveSize, 1, m_noiseSize, scrapCount);
            Audit::Assert(1 == scrapCount, "random pattern size not a multiple of large-page size");
            uint64_t* pU64 = (uint64_t*) m_pNoise->PBuffer();
            uint64_t* pBeyond = (uint64_t*) (m_pNoise->PData(m_noiseSize));
            while (pU64 < pBeyond)
            {
                *pU64++ = m_r(m_seed);
            }

            size_t actualSize;
            unsigned actualCount;
            m_pBuf = make_unique<Utilities::LargePageBuffer>((size_t) 2 * SI::Mi, 1, actualSize, actualCount);

            Audit::Assert(1 == actualCount, "m_buffers size not a multiple of large-page size");
        }


        // Fill a buffer with some random data
        //
        void Scribble(_In_ size_t length, _Out_writes_(length) void* pValue)
        {
            // In order to run fast enough I cheat and mutate random selections from the
            // pregenerated buffer against a single fresh random key.

            uint64_t* pSrc = (uint64_t*) (m_pNoise->PBuffer()) + (m_r(m_seed) % m_noiseSize) / sizeof(uint64_t);
            uint64_t* pTop = (uint64_t*) (m_pNoise->PData(m_noiseSize));
            uint64_t x = m_r(m_seed);
            uint64_t* pOut = (uint64_t*) pValue;
            uint64_t* pLast = (length / sizeof(uint64_t)) + pOut;
            while (pOut < pLast)
            {
                x = *pSrc++ ^ (x << 37) ^ (x >> 27);
                *pOut++ = x;
                if (pSrc == pTop)
                    pSrc = (uint64_t*) (m_pNoise->PBuffer());
            }
        }

        // Generate a random data record.
        // Can not call another time before you are done with this item, since we have only one buffer
        //
        void GenerateRandomRecord(_Out_ Datagram::Description& descriptor, _Out_ void*& blob)
        {
            // we are single threaded because the test generator has only one buffer in which to synthesize test data.
            // spin up more test generators if you want parallel testing

            uint64_t length = m_r(m_seed);
            length = m_average + (length % (m_variation) -m_variation); // 512k + 0~31k
            Scribble(length, m_pBuf->PBuffer());

            ::memset(&descriptor, 0, sizeof(descriptor));
            Datagram::Key128 temp{ m_pBuf->PBuffer(), length };
            descriptor.KeyHash = temp;
            descriptor.ValueLength = (uint32_t) length;
            descriptor.Owner = 0xCafeF00d0000ULL;
            blob = m_pBuf->PBuffer();
        }

    };

    extern Exabytes::GcActor* DummyGcFactory(
        Exabytes::ExabytesServer& server,
        Schedulers::Activity& gcDriver,
        Exabytes::PartitionRotor& stores
        );

    extern std::unique_ptr<Exabytes::CoalescingBuffer> MockBufferFactory(
        size_t bufferSize,
        Exabytes::CoalescingBufferPool& pool
        );


    extern unique_ptr<Exabytes::KeyValueOnFile> MockDiskStoreFactory(
        _In_ Schedulers::Scheduler& scheduler
        );

    // Vector to maintain inventory and operations for tests
    class InventoryVector
    {
    private:
        vector<pair<Datagram::PartialDescription, uint32_t>> inventory;
        SRWLOCK m_vectorLock;
        
    public:
        InventoryVector::InventoryVector()
        {
            InitializeSRWLock(&m_vectorLock);
        }

        // push_back in the vector
        void push_back(pair<Datagram::PartialDescription, uint32_t> KVPair);

        // This moves the private: inventory vector to the passed in vector.
        // TODO: Change all the tests to use this InventoryVector and then we can remove this
        // method all toghether. Right now changing all the tests is not necessary as
        // they do not need a thread safe vector as read\delete tests are implemented
        // different then the write ones
        void SetAndReleaseSelf(vector<pair<Datagram::PartialDescription, uint32_t>>& vector);
    };

	// defines the test items for catalog tests
	template<class SEED>
	struct ReducedMapItem
	{
		Datagram::Key128  Key;
		Exabytes::AddressAndSize FirstLocation;
		Exabytes::AddressAndSize SecondLocation;

		ReducedMapItem(
			SEED& seed,
			uniform_int_distribution<uint64_t>& uniform,
			lognormal_distribution<double>& logNormal)
			:
			Key(uniform(seed), uniform(seed))
		{
			size_t size = (size_t)ceil(10000 * logNormal(seed));
			// temporary limit to 1MB for this test
			while ((1 << 20) <= size)
			{
				size = size / 2;
			}

			size_t firstAddr = (uniform(seed) >> 32) << 4;
			size_t secondAddr = (uniform(seed) >> 32) << 4;
			FirstLocation = AddressAndSize(firstAddr, size);
			SecondLocation = AddressAndSize(secondAddr, size);
		}
	};

    class RepeatedAction
    {
    public:
        virtual void Run(int count) = 0;
        virtual ~RepeatedAction() {};
    };

    extern std::unique_ptr<RepeatedAction>
        RepeatedMemStoreWriteFactory(
        _In_ Exabytes::KeyValueInMemory& store,
        _In_ Schedulers::Scheduler& scheduler,
        _Inout_ shared_ptr<InventoryVector> pInventory
        );

    extern std::unique_ptr<RepeatedAction>
        RepeatedReadFactory(
        _In_ Exabytes::ExabytesServer& server,
        _In_ Schedulers::Scheduler& scheduler,
        _Inout_ vector<pair<Datagram::PartialDescription, uint32_t>>& inventory
        );

    extern std::unique_ptr<RepeatedAction>
        RepeatedMultiWriteFactory(
        _In_ Exabytes::ExabytesServer& server,
        _In_ Schedulers::Scheduler& scheduler,
        _Inout_ vector<pair<Datagram::PartialDescription, uint32_t>>& inventory
        );

    extern std::unique_ptr<RepeatedAction> RepeatedWriteFactory(
        _In_ Exabytes::ExabytesServer& server,
        _In_ Schedulers::Scheduler& scheduler,
        _Inout_ vector<pair<Datagram::PartialDescription, uint32_t>>& inventory,
        bool allowMultiParts = false
        );

    extern std::unique_ptr<RepeatedAction> RepeatedDeleteFactory(
        _In_ Exabytes::ExabytesServer& server,
        _In_ Schedulers::Scheduler& scheduler,
        _Inout_ vector<pair<Datagram::PartialDescription, uint32_t>>& inventory
        );

	extern std::unique_ptr<RepeatedAction> RepeatedCatalogLocateFactory(
		_In_ Exabytes::ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>& catalog,
		_In_ Schedulers::Scheduler& scheduler,
		_In_ vector<ReducedMapItem<mt19937_64>>& inventory,
		_In_ mt19937_64 gen
		);

	extern std::unique_ptr<RepeatedAction> RepeatedCatalogRelocateFactory(
		Exabytes::ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>& catalog,
		Schedulers::Scheduler& scheduler,
		vector<ReducedMapItem<mt19937_64>>& inventory,
		mt19937_64 gen
		);

	extern std::unique_ptr<RepeatedAction> RepeatedCatalogItemExpireFactory(
		Exabytes::ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>& catalog,
		Schedulers::Scheduler& scheduler,
		vector<ReducedMapItem<mt19937_64>>& inventory,
		mt19937_64 gen
		);

	extern std::unique_ptr<RepeatedAction> RepeatedSerialCatalogLocateFactory(
		_In_ Exabytes::ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>& catalog,
		_In_ Schedulers::Scheduler& scheduler,
		_In_ vector<ReducedMapItem<mt19937_64>>& inventory,
		_In_ mt19937_64 gen
		);

    void Trace (
        _In_ const wchar_t* format ,
        _In_ ...
        );

    void Trace (
        _In_ const char* format ,
        _In_ ...
        );

    class TestPartitionCreator : public Exabytes::PartitionCreater
    {
        std::random_device m_seed;
        std::uniform_int_distribution<uint64_t> m_r;

    public:        
        std::unique_ptr<Exabytes::Partition> CreatePartition(
            Exabytes::ExabytesServer& server,
            Datagram::PartitionId id,
            intptr_t handle,
            bool isPrimary
            ) override;
    };

	typedef bool RunAgain(_In_ RepeatedAction& actions);

    typedef bool RunForSameKeyAgain(_In_ RepeatedAction& actions, ReducedMapItem<mt19937_64>& item, size_t addr, Schedulers::ContinuationBase* pNotify);

	// This Actor allows tests to be generated as distinct Turns, looped
	// until the final one registers as finished
	//
	class RunAgainActor : public Schedulers::ContinuationBase
	{
	private:
		RepeatedAction* m_pTestGenerator;
		RunAgain*       m_pRunAgainProc;

	public:
		RunAgainActor(Schedulers::Activity& activity, RepeatedAction& generator, RunAgain pRunAgainProc)
			: Schedulers::ContinuationBase(activity)
		{
			m_pTestGenerator = &generator;
			m_pRunAgainProc = pRunAgainProc;
		}

		// This Actor controls the standard pattern of chained turns, used here for Sweep
		//
		void OnReady(
			_In_ Schedulers::WorkerThread&  thread,
			_In_ intptr_t       continuationHandle,
			_In_ uint32_t       messageLength
			)
		{
			// we really don't need parameters.  So, just using them for a little self-checking.
			RepeatedAction* pTestGen = (RepeatedAction*)continuationHandle;
			Audit::Verify(m_pTestGenerator == pTestGen);
			Audit::Verify(messageLength == 0);

			// The retiring loop will be a sequence of turns which chain to each other.
			// Each turn may copy blobs to the SSD staging area, or may flush to SSD,
			// or may do nothing if there is no pending memory.
			// When a turn is finished it posts the next turn, selecting anything from
			// zero to 100ms of delay depending upon estimated congestion.

			auto keepGoing = m_pRunAgainProc(*m_pTestGenerator);

			if (keepGoing)
			{
				Post((intptr_t)m_pTestGenerator, 0, 100);
			}
		}

		void Cleanup() override
		{}
	};
}
