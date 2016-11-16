// FlashStore.hpp : an iNode system for Exabytes
//

#pragma once

#include "stdafx.h"

#include <xstddef>

#include "Exabytes.hpp"
#include "Scheduler.hpp"

namespace Exabytes
{
    // A minimal map is a best effort map which stores Key-Address pairs without guarantee of uniqueness.
    // The caller is responsible for restarting a search if the address produced is not the one they want.
    // The design also supports versioning, where multiple addresses stored over time with the same key
    // may all remain in the map.  The addresses will be found latest-first order.
    //
    template<typename KeyType, typename AddressType, typename SizeType>
    class ReducedKeyMap
    {
    public:
        // Add an address associated with a key.
        // Does not check if the key already exists, the addition becomes the current value
        // and the old address may remain visible as a version.
        // Throws exception for out of memory.
        //
        virtual void Add(KeyType key,
            AddressType address,
            SizeType size) = 0;

        // Retrieve the address of the specified key.
        // Return false if the key is not in the catalog.
        //        
        virtual void TryLocate(KeyType key,
            AddressType prior,
            Schedulers::ContinuationBase* pContinuation) = 0;

        // Assign a new address to a key-address instance already in the catalog.
        // The new address must be in the same space.
        // This operation does not create a new version and does not change size.
        // Returns false if the key is not found.
        //
        virtual void Relocate(KeyType key,
            AddressType oldLocation,
            AddressType newLocation,
            Schedulers::ContinuationBase* pContinuation) = 0;        

        // Initiates catalog shutdown
        // This is a part of partition shutdown process.
        // Catalog shutdown should stop further hash block and bloom key 
        // block spills to SSD
        virtual void RequestShutdown() = 0;

        // Garbage collector of the map
        //
        virtual void Compact(Schedulers::ContinuationBase* pNotify) = 0;

        // Set GC state = ON for this store
        virtual void SetGCStart() = 0;

        // Sets GC state = OFF for this store
        virtual void SetGCEnd() = 0;        

        // Adds all the flushable blocks to catalog 
        // file store flush queue
        virtual void SweepToFileStore() = 0;        

        // Starts catalog file store GC
        virtual void StartCatalogFileGC(Schedulers::ContinuationBase* pNotify) = 0;

        // Discards all the eligible hash blocks from in mem
        // hash block pool
        virtual void DiscardHashBlocks() = 0;

        // Returns the available space in catalog in terms of number
        // of entries
        //
        virtual size_t GetAvailableSpace() = 0;
                
        virtual ~ReducedKeyMap() {};
    };

    extern
        std::unique_ptr<ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>>
        ReducedKeyMapFactory(
        // Asynchronous work should be posted using the scheduler.
        _In_ Schedulers::Scheduler& scheduler,          

        // the name of the file to be used
        _In_ const wchar_t* pathName,   

        // shared pool among all catalogs for storing in memory hash blocks
        _In_ CatalogBufferPool<>& hashPoolManager,

        // shared pool among all catalogs for storing in memory bloom key blocks
        _In_ CatalogBufferPool<>& bloomKeyPoolManager,

        // a random value unique to this service
        uint64_t randomizer          
        );

    extern std::unique_ptr<ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>> ReducedKeyCatalogFactory();
    
    extern const unsigned INMEMHASHBLKCOUNT;
    extern const unsigned TOTALHASHBLKS;
    extern const unsigned CATALOGCAPACITY;
    extern const uint16_t INVALID_PAGE;
    extern const unsigned CATALOGBUCKETCOUNT; 
    extern const uint32_t INVALID_FILEOFFSET;
    extern const unsigned HASHBLOCKSIZE;
    extern const unsigned BLOOMKEYBLOCKSIZE;
}

