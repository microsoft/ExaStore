#pragma once

#include "stdafx.h"

#include "Catalog.hpp"
#include "ReducedKeyMap.hpp"


namespace Exabytes
{
    using namespace Schedulers;    
    
    // An adaptor class that links ReducedKeyMap to Catalog
    // This class utilize Reduction namespace functions to
    // translate address and size back and forth.
    //
    class ReducedMapCatalog : public Catalog
    {
    private:
        std::unique_ptr<ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>>
            m_pKeyMap;
        Partition* m_pPartition = nullptr;
        
    public:
        ReducedMapCatalog(
            std::unique_ptr<ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>>& pKeyMap
            )
            : m_pKeyMap(std::move(pKeyMap))
        {}

        // Add an address associated with a key.
        // Does not check if the key already exists, the addition becomes
        // the current value and the old address may remain visible as a
        // prior version.
        // Throws exception for out of memory.
        //
        bool Add(Datagram::Key128 key,
            AddressAndSize addressAndSize) override
        {
            auto shortAddress = 
                Reduction::CompressAddress(addressAndSize.FullAddress());
            auto shortSize = 
                Reduction::CompressSize(addressAndSize.Size());
            m_pKeyMap->Add(key, shortAddress, shortSize);
            return true;
        }

        // Async Interface for retrieving the address of the specified key.
        // result AddressAndSize is posted to pContinuation's continuation handle (64bit).
        // possible false conlision may happen. Caller need to verify validity of the 
        // result by checking the store.
        //
        void Locate(Datagram::Key128 key,
            AddressAndSize prior,
            Schedulers::ContinuationBase* pContinuation) override
        {
            uint32_t shortAddress = INVALID_FILEOFFSET;
            bool isRetry = !prior.IsVoid();

            if (isRetry)
            {
                shortAddress = Reduction::CompressAddress(prior.FullAddress());
            }

            m_pKeyMap->TryLocate(key, shortAddress, pContinuation);            
        }

        // Assign a new address to a key-address instance already in the catalog.
        // The new address may be in the same sub-space or a new sub-space.
        // This operation does not create a new version.
        //
        void Relocate(Datagram::Key128 key,
            AddressAndSize oldLocation,
            AddressAndSize newLocation,
            Schedulers::ContinuationBase* pContinuation) override
        {
            uint32_t oldAddress = Reduction::CompressAddress(oldLocation.FullAddress());
            uint32_t newAddress = Reduction::CompressAddress(newLocation.FullAddress());
            
            m_pKeyMap->Relocate(key, oldAddress, newAddress, pContinuation);            
        }

        // inform the catalog that this entry has been evicted from data store
        void Expire(Datagram::Key128 key,
            AddressAndSize oldLocation,
            Schedulers::ContinuationBase* pContinuation) override
        {        
            uint32_t oldAddress = Reduction::CompressAddress(oldLocation.FullAddress());                        
            m_pKeyMap->Relocate(key, oldAddress, Reduction::UNUSED_ADDRESS, pContinuation);         
        }

        // Initiates shutdown for catalog
        void RequestShutdown() override
        {
            m_pKeyMap->RequestShutdown();
        }

        // One catalog belongs to one partition.
        // this sets a "parent" pointer to its partition. 
        void SetPartition(Partition* pPart) override
        {
            Audit::Assert(m_pPartition == nullptr,
                "A catalog can only belong to one partition");
            m_pPartition = pPart;
        }

        void Compact(ContinuationBase* pNotify) override
        {
            m_pKeyMap->Compact(pNotify);
        }

        // Set GC state = ON for this store
        void SetGCStart() override
        {
            m_pKeyMap->SetGCStart();
        }

        // Sets GC state = OFF for this store
        void SetGCEnd() override
        {
            m_pKeyMap->SetGCEnd();
        }        

        // Adds flushable blocks to catalog file store
        // flush candidate queue
        void SweepToFileStore()
        {
            m_pKeyMap->SweepToFileStore();
        }        

        void StartCatalogFileGC(Schedulers::ContinuationBase* pNotify) override
        {
            m_pKeyMap->StartCatalogFileGC(pNotify);
        }

        // Discards all the eligible hash blocks from in mem
        // hash block pool
        void DiscardHashBlocks() override
        {
            m_pKeyMap->DiscardHashBlocks();
        }
        
        size_t GetAvailableSpace() override
        {
            return m_pKeyMap->GetAvailableSpace();
        }
    };

    std::unique_ptr<Catalog>  ReducedMapCatalogFactory(
        _In_ Schedulers::Scheduler& scheduler,  

        // the name of the file to be used 
        _In_ const wchar_t* pathName, 

        _In_ CatalogBufferPool<>&  hashPoolManager,

        _In_ CatalogBufferPool<>&  bloomKeyPoolManager,

        // a random value unique to this service
        uint64_t randomizer        
        )
    {
        
        return std::make_unique<ReducedMapCatalog>(
            ReducedKeyMapFactory(scheduler, pathName, hashPoolManager, bloomKeyPoolManager, randomizer)
            );
    }

}