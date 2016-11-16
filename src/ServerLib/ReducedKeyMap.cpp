// NetBursts.cpp : Defines the entry point for the console application.
//

#pragma once
#include "stdafx.h"

#include <algorithm>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include <stack>
#include <future>

#include "Exabytes.hpp"
#include "ReducedKeyMap.hpp"
#include "Bloom.hpp"
#include "UtilitiesWin.hpp"
#include "BlockFile.hpp"

namespace Exabytes
{
    using namespace Datagram;
    using namespace Schedulers;
    using namespace Utilities;    

    // hash entries per page.  8 bytes per entry.
    const unsigned ENTRYCOUNT = BloomBlock::CAPACITY; // 512

    // defining all the catalog size constants
    // there are 1024 buckets in the hash table
    static const unsigned BUCKETBITS = 10;
    const unsigned CATALOGBUCKETCOUNT = 1u << BUCKETBITS;
    
    // num of keys allowed per partition, an upper bound, 
    // we can not actually reach this number due to space needed for compaction
    // and bucket imbalancing 
    const unsigned CATALOGCAPACITY = 10 * SI::Mi;

    // num of hash blocks in a partition,
    const unsigned TOTALHASHBLKS = CATALOGCAPACITY / ENTRYCOUNT;
    
    // Each bucket is a linked list of hash blocks, this is the length of that linked list
	static const unsigned BUCKET_CHAIN_LEN = TOTALHASHBLKS / CATALOGBUCKETCOUNT;
	static_assert((CATALOGCAPACITY / CATALOGBUCKETCOUNT) % ENTRYCOUNT == 0,
		"Catalog capacity does not align with hash block entry count.");


    using BloomKeyBlock = uint32_t[ENTRYCOUNT];
    static_assert(sizeof(BloomKeyBlock) == 2048, "Bloom Key block size should be power of 2 and no larger than 4k.");

    const unsigned HASHBLOCKSIZE = 4096;
    const unsigned BLOOMKEYBLOCKSIZE = sizeof(BloomKeyBlock);

    // size of catalog file store
    // + 5 as buffer
    const unsigned CATALOGFILESTORECAPACITY = (TOTALHASHBLKS * HASHBLOCKSIZE) + 5 * SI::Mi;
    const unsigned BloomKeyFileStoreCapacity = (TOTALHASHBLKS * BLOOMKEYBLOCKSIZE) + 5 * SI::Mi;

	// We only keep a portion of hash blocks in memory, most are spilled to SSD
	// In each bucket, we keep the head block in memory, and starting this position we spill blocks to SSD
	const unsigned FIRSTBLOCKTOFLUSH = 2;
	const unsigned INMEMHASHBLKCOUNT = (CATALOGBUCKETCOUNT * (FIRSTBLOCKTOFLUSH - 1));

    const unsigned INMEMBLOOMKEYBLKCOUNT = INMEMHASHBLKCOUNT;    

    const uint16_t INVALID_PAGE = std::numeric_limits<uint16_t>::max();
    const uint32_t INVALID_IDX = std::numeric_limits<uint32_t>::max();
    const uint32_t INVALID_FILEOFFSET = std::numeric_limits<uint32_t>::max();
    
    // A data structure to hold all the bloom filters for one partition;
    // Currently we use 16 bit index as there's only 20k bloom filters needed
    // for a partition;
    class BloomFilters {
        static_assert(sizeof(BloomBlock) == 1024,
            "Bloom filter type should only include the bit set, no other field should be in there!");

        std::unique_ptr<LargePageBuffer> m_pMemory;
    public:
        BloomFilters(size_t capacity)
        {
            Audit::Assert(capacity < std::numeric_limits<uint16_t>::max(),
                "We need a wider index type!");
            size_t actualSize;
            uint32_t actualCount;
            m_pMemory = std::make_unique<LargePageBuffer>(
                sizeof(BloomBlock), (unsigned)capacity, actualSize, actualCount);
            Audit::Assert(actualCount == capacity,
                "Failed to allocate memory for the size of bloom filters.");

            std::memset(m_pMemory->PBuffer(), 0, actualSize);
        }

        BloomBlock* GetBloomFilter(uint16_t index)
        {
            // just make sure the tail of the bloom is not out of bound
            m_pMemory->PData((index + 1) * sizeof(BloomBlock));
            return reinterpret_cast<BloomBlock*>
                (m_pMemory->PData(index * sizeof(BloomBlock)));
        }
    };

    // This block takes 4 bytes and represents a hashKey = 28 bit; size = 4 bit 
    // size is only an estimate starting from 2K it goes up exponentially
    // with 4 bits we can represent 2K, 4K, 8K, 16K, 32K, 64K ... with 0 being reserved as Invalid. 
    struct KeyNSize
    {
        uint32_t m_hashKey : 28;
        uint32_t m_size : 4;

        KeyNSize() : KeyNSize(0, 0){}

        KeyNSize(uint32_t key, uint8_t size)
            : m_hashKey(key)
            , m_size(size)
        {}
    };

    // Hash blocks each hold up to ENTRYCOUNT items, pushed in order of arrival
    class HashBlock
    {
        // if the block is not full then the key part of the last entry will be
        //  in the reserved value range, and is a count of the keys allocated so far.

        // we may want to split the HashEntry into two arrays, so the lower 16 or 32 bits
        // can be scanned with vector instructions for a faster search.
    public:
        
        KeyNSize m_hashKeyAndSize[ENTRYCOUNT];
        uint32_t m_deltas[ENTRYCOUNT];

        bool IsFull() 
        {
            // if the last entry of hash key is < Reserved values then that means
            // it is the count value and therefore hashblock is NOT FULL
            if (m_hashKeyAndSize[ENTRYCOUNT - 1].m_hashKey < Reduction::RESERVEDVALUES)
            {
                Audit::Assert(m_hashKeyAndSize[ENTRYCOUNT - 1].m_hashKey < ENTRYCOUNT, "Unexpected count value");
                return false;
            }
            else
                return true;            
        }

        // If the block is not full then the key part of the last entry will be
        // in the reserved value range, and is a count of the keys allocated so far.        
        uint16_t GetCount()
        {
            if (m_hashKeyAndSize[ENTRYCOUNT - 1].m_hashKey < Reduction::RESERVEDVALUES)
            {
                Audit::Assert(m_hashKeyAndSize[ENTRYCOUNT - 1].m_hashKey < ENTRYCOUNT, "Unexpected count value");
                return m_hashKeyAndSize[ENTRYCOUNT - 1].m_hashKey;
            }
            else
            {
                return ENTRYCOUNT;
            }
        }

        void IncrementCount()
        {
            Audit::Assert(m_hashKeyAndSize[ENTRYCOUNT - 1].m_hashKey < Reduction::RESERVEDVALUES 
                && m_hashKeyAndSize[ENTRYCOUNT - 1].m_hashKey < ENTRYCOUNT - 1, "Unexpected count value");

            ++m_hashKeyAndSize[ENTRYCOUNT - 1].m_hashKey;
        }

        // Add a hash entry in to the block. Taking note of the bloom key
        // for bloom filter rebuilding during compaction
        //
        void Add(KeyNSize keyNSize, uint32_t address)
        {            
            Audit::Assert(!IsFull(), "should not add to a full block");
            Audit::Assert(Reduction::RESERVEDVALUES < keyNSize.m_hashKey, "a reserved value cannot be used as a reduced key");

            uint16_t count = m_hashKeyAndSize[ENTRYCOUNT - 1].m_hashKey;
            
            // sequence below is important
            // this may overlap with Find, we don't want the caller of find
            // to locate the right key but retrieve some garbage delta or size
            // values, just before the right values are put in place.
            m_deltas[count] = address;
            m_hashKeyAndSize[count] = keyNSize;
            if (count < ENTRYCOUNT - 1)
                IncrementCount();
        }

        bool Find(uint32_t reducedKey, _Inout_ unsigned& index, _Out_ uint32_t& delta, _Out_ uint8_t& size)
        {
            uint16_t count = GetCount();
                        
            // valid range [0 .. (m_count-1)]
            index = std::min(count - 1u, index);
            while (index < count &&
                (   m_hashKeyAndSize[index].m_hashKey != reducedKey
                 || m_deltas[index] == Reduction::UNUSED_ADDRESS)
                )
            {
                // we are searching the reverse direction than they
                // are inserted
                // -1 > m_count for unsigned int
                --index;
            }
            if (index < count)
            {
                delta = m_deltas[index];
                size = m_hashKeyAndSize[index].m_size;
                return true;
            }
            else
            {
                delta = ~0UL;
                size = 0;
                return false;
            }
        }

        // used by Relocation to indicate that a valid item is moved from old location
        // to a new one. when the new address is invalid, then the whole entry
        // is invalidated
        // 
        bool ResetAddress(uint32_t reducedKey,  uint32_t oldDelta, uint32_t newDelta)
        {
            unsigned index = 0;
            uint16_t count = GetCount();

            for (;;)
            {
                while (index < count
                    && !(m_hashKeyAndSize[index].m_hashKey == reducedKey
                    && m_deltas[index] == oldDelta
                    )
                    )
                {
                    ++index;
                }

                if (index < count)
                {
                    m_deltas[index] = newDelta;

                    if (m_hashKeyAndSize[index].m_size != Reduction::INVALID_SIZE)
                    {
                        if (newDelta == Reduction::UNUSED_ADDRESS)
                            m_hashKeyAndSize[index].m_size = Reduction::INVALID_SIZE;
                        return true;
                    }

                    // if the size is invalid, then this is 
                    // a delete entry, we need to keep
                    // looking to relocate the real entry
                }
                else
                {
                    return false;
                }
            }
        }

        void Clear()
        {
            std::memset(this, 0, sizeof(HashBlock));
        }
    };
    static_assert(sizeof(HashBlock) == HASHBLOCKSIZE,
        "Counstant value should match.");
    
    // Atomic queue with version control
    // at any given time items of a particular version will
    // be in the queue. On modifying the version the queue resets
    template <typename T>
    class AtomicQueue
    {
    private:
        std::queue<T> m_que;
        SRWLOCK   m_lock;

    public:
        AtomicQueue()
        {
            InitializeSRWLock(&m_lock);
        }

        void Enqueue(_In_ T item)
        {            
            Utilities::Exclude < SRWLOCK >  guard{ m_lock };           
            m_que.push(item);                        
        }

        void Dequeue(_Out_ T& item)
        {
            Utilities::Exclude<SRWLOCK> guard{ m_lock };
            if (m_que.empty())
            {
                return;
            }
            else
            {
                item = m_que.front();
                m_que.pop();
                return;
            }
        }        

        void Reset()
        {
            Utilities::Exclude<SRWLOCK> guard{ m_lock };
            while (!m_que.empty())
                m_que.pop();
        }

        size_t Size()
        {
            Utilities::Share < SRWLOCK >  guard{ m_lock };
            return m_que.size();
        }

    };    

    // Continuation to keep track of multiple operation completes
    // This continuation triggers the final continuation after it is
    // triggered m_counter number of times
    class CountDownActor : public ContinuationBase
    {
    private:
        uint32_t            m_counter;
        uint32_t            m_triggeredCount;
        StatusCode          m_result;
        ContinuationBase*   m_pNotify;
        bool                m_retireArenaAfterCompletion;

    public:
        CountDownActor(Activity& activity, uint32_t count, ContinuationBase* pNext, bool retireArenaAfterCompletion = false)
            : ContinuationBase(activity)
            , m_counter(count)
            , m_pNotify(pNext)
            , m_retireArenaAfterCompletion(retireArenaAfterCompletion)
            , m_result(StatusCode::OK)
            , m_triggeredCount(0)
        {}

        void OnReady(
            _In_ WorkerThread&,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t
            ) override
        {
            auto status = (StatusCode)continuationHandle;
            if (status != StatusCode::OK)
                m_result = status;

            m_triggeredCount++;
            if (m_triggeredCount == m_counter)
            {
                m_pNotify->Post((intptr_t)m_result, 0);
                m_triggeredCount = 0;

                if (m_retireArenaAfterCompletion)
                {
                    auto pArena = this->GetArena();
                    Audit::Assert(pArena != this->m_activity.GetArena(), "Arena retire called on live activity.");
                    pArena->Retire();
                }
            }
        }
        void Cleanup() override
        {
            if (m_retireArenaAfterCompletion)
            {
                auto pArena = this->GetArena();
                Audit::Assert(pArena != this->m_activity.GetArena(), "Arena retire called on live activity.");
                pArena->Retire();
            }
        }
    };    

    // A hash map for large cold collections.
    // The map may spill to disk, is densely packed, and resistant to misuse.
    //
    class ReducedKeyBlockHash : public ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>
    {
        // API is patterned like .NET Dictionary, but stripped to essentials.
        // The std:unordered_map pattern requires iterators, multiple calls for primitive operations,
        // and guarantees on iteration which are not appropriate here.        

    private:
        // We group 512 hash entries into one page, each page is consists of a
        // bloom filter (always in memory), a HashBlock and a BloomKeyBlock.
        // The later two maybe in memory or on SSD. So we need a map to keep track
        // of them
        struct HashPageMapEntry
        {
            uint32_t m_inMemHashBlkIndex;
            uint32_t m_fileOffset;
            uint32_t m_inMemBloomKeyBlkIndex;
            uint32_t m_bloomKeyFileOffset;

            HashPageMapEntry() : HashPageMapEntry(INVALID_IDX, INVALID_IDX, INVALID_FILEOFFSET, INVALID_FILEOFFSET) {}

            HashPageMapEntry(uint32_t inMemHashBlockIndex,
                uint32_t bloomKeyIndex,
                uint32_t fileOffset,
                uint32_t bloomKeyFileOffset
            )
                : m_inMemHashBlkIndex(inMemHashBlockIndex)
                , m_fileOffset(fileOffset)
                , m_inMemBloomKeyBlkIndex(bloomKeyIndex)
                , m_bloomKeyFileOffset(bloomKeyFileOffset)
            {}
        };

        HashPageMapEntry m_pageMap[TOTALHASHBLKS];
        uint16_t m_nextPage[TOTALHASHBLKS];

        // Bloom filter has 1 to 1 relation to hash pages, but Bloom is always in DRAM
        std::unique_ptr<BloomFilters> m_pBlooms;

        SimpleAtomicPool<uint16_t> m_freePages;

        // This each bucket is a linked list of hash pages
        uint16_t m_bucketHeads[CATALOGBUCKETCOUNT];

        // one lock per bucket, readers must read-lock for stability,
        // any mutation requires a write-lock.
        SRWLOCK m_srwBucketLock[CATALOGBUCKETCOUNT];

        // each bucket has a DOS defense seed;
        uint64_t m_hashSeeds[CATALOGBUCKETCOUNT];

        // Pointer to the pool of in memory bloom key blocks, shared among
        // all the partitions. ExabytesServer owns this pool    
        CatalogBufferPool<>* m_pBloomKeyPool;

        // Pointer to the pool of in memory hash blocks, shared among all
        // partitions. ExabytesServer owns this pool
        CatalogBufferPool<>* m_pHashPool;        

        std::random_device   m_randomSeed;
        std::uniform_int_distribution<uint64_t> m_random;

        Scheduler* m_pScheduler;        

        // version will increment every time we compact any part of the index.
        unsigned    m_version;
        
        bool m_fileStoreGCInProgress;        
        class BlockSweepContinuation;
        BlockSweepContinuation* m_pBlockSweeper;

        // Used for logging
        std::wstring m_partitionString;

        // file store for hash blocks
        std::unique_ptr<BlockFile<HashBlock>> m_pHashBlkStore;
        std::unique_ptr<BlockBuffer<HashBlock>> m_pHashBlockBuffer;
                
        // file store for bloom key blocks
        std::unique_ptr<BlockFile<BloomKeyBlock>> m_pBloomKeyBlkStore;
        std::unique_ptr<BlockBuffer<BloomKeyBlock>> m_pBloomKeyBuffer;

        // every time file store GC starts for this store we increment
        // this version. After a block flush returns if the gcVersion 
        // is same then catalog is updated otherwise it is discarded
        std::atomic<uint32_t> m_gcVersion;       
        
        // when set further hash and bloom key block flush will stop
        bool m_shuttingDown;

        AtomicQueue<BlockIdentifier> m_flushCandidates;        

        //TODO remove the following 2 counters after testing
        uint64_t m_acquiredInMemHashBlocks = 0;
        size_t m_keysInPartition = 0;

        // every instance of map will use the randomizer to ensure its buckets are not mapped the
        // same way as buckets in any other instance, part of defense against DOS
        const uint64_t    m_randomizer;

        inline uint64_t Combine(const uint64_t key, const uint64_t randomizer)
        {
            // XOR is weak because it can be learned by observing results and performance.
            // A CRC style of combination might be used in future.

            return key ^ randomizer;
        }

        // Reduce the key to just the bits which select a bucket.
        //
        inline unsigned MapToBucket(const uint64_t halfKey)
        {
            uint64_t k = Combine(halfKey, m_randomizer);
            return (CATALOGBUCKETCOUNT - 1) & (unsigned)
                ((k >> (BUCKETBITS * 6)) ^ (k >> (BUCKETBITS * 5)) ^ (k >> (BUCKETBITS * 4))
                ^ (k >> (BUCKETBITS * 3)) ^ (k >> (BUCKETBITS * 2)) ^ (k >> (BUCKETBITS * 1))
                ^ k);
        }            

        // walks through the entire bucket and pushes the block which require to be loaded in memory
        // to the designated stack provided
        void GenerateLoadList(uint16_t bucket, 

            // stack for collecting bloom key blocks not in memory
            std::stack<BlockIdentifier>& bloomBlockList, 

            // stack for collecting hash blocks not in memory
            std::stack<BlockIdentifier>& hashBlockList
            )
        {
            // points to the head of the bucket
            uint16_t headPage = m_bucketHeads[bucket];

            for (uint16_t i = headPage;  i != INVALID_PAGE;
                i = m_nextPage[i])
            {
                if (m_pageMap[i].m_inMemBloomKeyBlkIndex == INVALID_IDX)
                {
                    bloomBlockList.push(BlockIdentifier{ bucket, i });
                }
                if (m_pageMap[i].m_inMemHashBlkIndex == INVALID_IDX)
                {
                    hashBlockList.push(BlockIdentifier{ bucket, i });
                }
            }
        }
        
        // Compaction of a bucket:
        // Each bucket is a linked list of blocks. Here we traverse the list
        // backwards, from oldest to youngest, copy valid entries to a new
        // list, regenerating bloom filter along the way.
        //
        // Key map compaction should NOT over lap with itself, or TryLocate
        // 
        void CompactBucket(
            // bucket which should be compacted
            unsigned bucket)             
        {
            // we need a stack to walk the linked list backwards
            std::stack<uint16_t> linkedPages;

            // points to the current block in the new list
            uint16_t newPage = INVALID_PAGE;
            HashBlock* pDestBlock = nullptr;
            BloomBlock* pDestBloom = nullptr;
            BloomKeyBlock* pDestBloomKeyBlock = nullptr;

            // points to the current block in the old list
            uint16_t oldPage = INVALID_PAGE;

            // points to the head of the old list
            uint16_t headPage = INVALID_PAGE;            

            unsigned retry = 0;
            bool finished = false;
            while (!finished)
            {
                if (linkedPages.empty())
                {
                    Audit::Assert(++retry < 8,
                        "too many interleaving of Add and compaction");

                    // update our copy of the linked list with new blocks
                    headPage = m_bucketHeads[bucket];
                    for (uint16_t i = headPage; i != oldPage; i = m_nextPage[i])
                    {
                        Audit::Assert(i != INVALID_PAGE, "broken link in bucket");
                        Audit::Assert(m_pageMap[i].m_inMemHashBlkIndex != INVALID_IDX,
                            "All blocks have to be in memory by this time");                        
                        linkedPages.push(i);
                    }
                    Audit::Assert(!linkedPages.empty(), "orphan blocks?");
                    if (linkedPages.size() == 1 && retry == 1)
                    {
                        // skipping compaction for this bucket
                        Audit::Assert(headPage == linkedPages.top(),
                            "Unexpected State. List top block id should be equal to head block id.");                        
                        Tracer::LogDebug(StatusCode::OK, 
                            L"Skipping compaction for this bucket since we have only 1 hash block");
                        return;
                    }                        
                }

                // iterate all linked blocks backwards
                oldPage = linkedPages.top();
                linkedPages.pop();
                                
                HashBlock* pSrcBlock = (HashBlock*)m_pHashPool->GetBlock(m_pageMap[oldPage].m_inMemHashBlkIndex);
                uint16_t srcBlockCount = pSrcBlock->GetCount();
                auto pSrcBloomKey = (BloomKeyBlock*)m_pBloomKeyPool->GetBlock(m_pageMap[oldPage].m_inMemBloomKeyBlkIndex);

                // copy all valid entry from pSrcBlock to pDestBlock
                unsigned nEntriesCopied;
                bool blockChanged = false;
                for (nEntriesCopied = 0; nEntriesCopied < srcBlockCount;
                    nEntriesCopied++)
                {
                    if (pDestBlock == nullptr || pDestBlock->IsFull())
                    {
                        Audit::Assert(!blockChanged,
                            "changed destination block twice for a single block?");
                        blockChanged = true;

                        auto freshPage = AcquireHashBlkId();                       
                        m_nextPage[freshPage] = newPage;
                        newPage = freshPage;
                        
                        pDestBlock = (HashBlock*)m_pHashPool->GetBlock(m_pageMap[newPage].m_inMemHashBlkIndex);
                        Audit::Assert(pDestBlock->GetCount() == 0, 
                            "fresh block should be empty");
                        pDestBloom = m_pBlooms->GetBloomFilter(newPage);
                        pDestBloomKeyBlock = (BloomKeyBlock*)m_pBloomKeyPool->GetBlock(m_pageMap[newPage].m_inMemBloomKeyBlkIndex);
                    }
                    
                    if (pSrcBlock->m_deltas[nEntriesCopied] != Reduction::UNUSED_ADDRESS)
                    {
                        (*pDestBloomKeyBlock)[pDestBlock->GetCount()] = (*pSrcBloomKey)[nEntriesCopied];
                        pDestBlock->Add(pSrcBlock->m_hashKeyAndSize[nEntriesCopied],
                            pSrcBlock->m_deltas[nEntriesCopied]
                            );                            
                        pDestBloom->Add((*pSrcBloomKey)[nEntriesCopied]);

                        // no need to log from here... just logging from Add is enough for testing
                        m_keysInPartition++;
                    }
                }

                if (linkedPages.empty())
                {
                    Audit::Assert(oldPage == headPage,
                        "link head should be the last item");

                    auto sparePage = AcquireHashBlkId();
                    {
                        Utilities::Exclude < SRWLOCK >
                            bucketGuard{ m_srwBucketLock[bucket] };

                        // check for newly added entries
                        // more items added right before we got lock

                        // In the case of a new active block just added
                        // we could potentially have this loop outside of
                        // the critical section. However, this loop should
                        // be short (bulk of the work already done above).
                        // Doing that would require even more code clone.
                        // not worth it.
                        srcBlockCount = pSrcBlock->GetCount();

                        for (; nEntriesCopied < srcBlockCount; nEntriesCopied++)
                        {
                            if (pDestBlock->IsFull())
                            {
                                Audit::Assert(!blockChanged,
                                    "filled more than one destination block from a single block?");
                                blockChanged = true;

                                m_nextPage[sparePage] = newPage;
                                newPage = sparePage;
                                sparePage = INVALID_PAGE;

                                pDestBlock = (HashBlock*)m_pHashPool->GetBlock(m_pageMap[newPage].m_inMemHashBlkIndex);
                                Audit::Assert(pDestBlock->GetCount() == 0,
                                    "fresh block should be empty");
                                pDestBloom = m_pBlooms->GetBloomFilter(newPage);
                                pDestBloomKeyBlock = (BloomKeyBlock*)m_pBloomKeyPool->GetBlock(m_pageMap[newPage].m_inMemBloomKeyBlkIndex);
                            }
                            if (pSrcBlock->m_deltas[nEntriesCopied] != Reduction::UNUSED_ADDRESS)
                            {
                                (*pDestBloomKeyBlock)[pDestBlock->GetCount()] = (*pSrcBloomKey)[nEntriesCopied];
                                pDestBlock->Add(pSrcBlock->m_hashKeyAndSize[nEntriesCopied],
                                    pSrcBlock->m_deltas[nEntriesCopied]
                                    );                                    
                                pDestBloom->Add((*pSrcBloomKey)[nEntriesCopied]);

                                // no need to log from here... just logging from Add is enough for testing
                                m_keysInPartition++;
                            }
                        }
                        if (m_bucketHeads[bucket] == oldPage)
                        {
                            // so this is still the active block. swap the linked list
                            Audit::Assert(newPage != INVALID_PAGE, "Head block id of new link cannot be Invalid");
                            m_bucketHeads[bucket] = newPage;
                            m_version++;
                            finished = true;

                            // May be we can use Shared lock instead of exlusive
                            // it is safe to interleave with TryLocate, as we 
                            // clear out the old blocks AFTER we update version,
                        }
                        else
                        {
                            // a new block is added before we grab lock.
                            // now continue to the next iteration to get that block
                        }
                    } // release exclusive bucket lock

                    if (sparePage != INVALID_PAGE)
                    {
                        FreeAllIndices(sparePage);                        
                    }
                }

            } // next block

            // release old link
            for (uint16_t i = headPage; INVALID_PAGE != i;)
            {
                auto next = m_nextPage[i];
                FreeAllIndices(i);
                i = next;
            }            

            // finished compaction for this bucket
			Tracer::LogDebug(StatusCode::OK, L"Compaction completed for current bucket");
            return;
        }

    public:
        ReducedKeyBlockHash(
            _In_ Scheduler& scheduler,  //Asynchronous work should be posted using the scheduler.
            _In_ const wchar_t* pathName,   // the name of the file to be used
            _In_ CatalogBufferPool<>& hashPoolManager, // shared hash pool buffer
            _In_ CatalogBufferPool<>& bloomKeyPoolManager, // shared bloom key pool buffer
            uint64_t randomizer             // a random value unique to this service            
            )
            : m_pScheduler(&scheduler)
            , m_pHashPool(&hashPoolManager)
            , m_pBloomKeyPool(&bloomKeyPoolManager)
            , m_randomizer(randomizer)
            , m_partitionString(pathName)
            , m_shuttingDown(false)
        {            
            static_assert(4 == sizeof(KeyNSize),
                "size of Partial Hash Block unexpected");
            static_assert(1024 == sizeof(BloomBlock),
                "size of a Bloom filter block unexpected - design error?");
            static_assert(4096 == sizeof(HashBlock),
                "size of a Hash block unexpected - design error?");
            static_assert(TOTALHASHBLKS < std::numeric_limits<uint16_t>::max(),
                "Total capacity needs to fit in 16bit address space.");
            Audit::Assert(nullptr != pathName,
                "spill to SSD needs a path name");

            m_partitionString.erase(m_partitionString.find('-'));
            Audit::Assert(!m_partitionString.empty(), "partition string cant be empty. We need it for debugging");
            // the number of entries allowed in the in-memory catalog            
            // should be less than or equal to total number of allowed entries            
            Audit::Assert((ENTRYCOUNT * 3 * CATALOGBUCKETCOUNT) <= CATALOGCAPACITY && CATALOGCAPACITY <= (1u << 24));
            Audit::Assert((CATALOGCAPACITY & 0xFFFF) == 0);

            static_assert(INMEMHASHBLKCOUNT <= TOTALHASHBLKS,
                "In memory hash block capacity should be <= total hash block capacity.");

            m_fileStoreGCInProgress = false;
            m_gcVersion = 0;
            Tracer::LogCounterValue(Tracer::EBCounter::GCVersion, m_partitionString.c_str(), m_gcVersion);

            // create bloom filters
            m_pBlooms = std::make_unique<BloomFilters>(TOTALHASHBLKS);

            // assign hash block ids to buckets
            // hash block ids starts from 0 and go upto MAXHASHBLKCAPACITY
            // we assign 0 to CATALOGBUCKETCOUNT to buckets and enter all the rest in 
            // free ids list
            for (uint16_t i = 0; i < CATALOGBUCKETCOUNT; ++i)
            {
                m_bucketHeads[i] = i;
                InitializeSRWLock(&m_srwBucketLock[i]);
                do {
                    m_hashSeeds[i] = m_random(m_randomSeed);
                } while (!Reduction::SeedIsAdequate(m_hashSeeds[i]));

                // Initialize the hash blk header for assigned hash block id
                m_pageMap[i] = HashPageMapEntry{
                    m_pHashPool->AcquireBlkIdx(),
                    m_pBloomKeyPool->AcquireBlkIdx(),
                    INVALID_FILEOFFSET, 
                    INVALID_FILEOFFSET
                };
                m_nextPage[i] = INVALID_PAGE;
                m_acquiredInMemHashBlocks++;
            }

            Audit::Assert(m_acquiredInMemHashBlocks == CATALOGBUCKETCOUNT, "Number of acquired hash blocks should be equal to catalog bucket count");
            Tracer::LogCounterValue(Tracer::EBCounter::InMemHashBlksAcquired, m_partitionString.c_str(), m_acquiredInMemHashBlocks);

            // Add all the left over ids to the free pool
            for (uint16_t i = static_cast<uint16_t>(TOTALHASHBLKS-1); CATALOGBUCKETCOUNT <= i; --i)
            {
                m_freePages.Release(i);
                m_nextPage[i] = INVALID_PAGE;
            }

            // create filestore for hash blocks
            m_pHashBlkStore = BlockFileFactory<HashBlock>(
                scheduler,
                pathName,
                CATALOGFILESTORECAPACITY);
                         
            std::wstring bloomFile = std::wstring(pathName) + L"BloomKey";
            m_pBloomKeyBlkStore = BlockFileFactory<BloomKeyBlock>(
                scheduler,
                bloomFile.c_str(),
                BloomKeyFileStoreCapacity);
            
            // create buffer to write hash blocks to catalog store
            m_pHashBlockBuffer = std::make_unique<BlockBuffer<HashBlock>>();
            m_pBloomKeyBuffer = std::make_unique<BlockBuffer<BloomKeyBlock>>();

            // create block sweeper continuation
            auto pFlushActivity = Schedulers::ActivityFactory(
                *(const_cast<Scheduler*>(m_pScheduler)), L"HashBlockFlusher"
                );

            pFlushActivity->SetContext(Schedulers::ContextIndex::BaseContext, this);
            m_pBlockSweeper = pFlushActivity->GetArena()->allocate<BlockSweepContinuation>(*pFlushActivity); 
            
            Tracer::LogCounterValue(Tracer::EBCounter::CatalogFlushQueLen, m_partitionString.c_str(),
                m_flushCandidates.Size());
        }
           
        // Push a new key and value into the Map.
        //
        void Add(Key128 key,
            uint32_t address,
            uint8_t size) override
        {                
            // incrementing the counter here itself as this is really do or die... If we fail to insert
            // we assert. Plus remove this after testing phase
            // This is really an estimate just for testing phase...
            m_keysInPartition++;
            Tracer::LogCounterValue(Tracer::EBCounter::KeysInPartition, m_partitionString.c_str(), m_keysInPartition);

            auto halfKey = Reduction::HalveKey(key);
            unsigned bucket = MapToBucket(halfKey);

            unsigned version = m_version;
            auto hashKey = Reduction::To28BitKey(halfKey, m_hashSeeds[bucket]);
            uint16_t headPage = m_bucketHeads[bucket];                        

            for (unsigned retry = 0; INVALID_PAGE != headPage && retry < 8; retry++)
            {
                // we use different seeds for hash key and bloom key to reduce collision.
                auto bloomKey = Reduction::QuarterKey(halfKey,
                    m_hashSeeds[(CATALOGBUCKETCOUNT - 1) & (bucket+1)]);
                {
                    // Exclusive lock since we are writing a block
                    Utilities::Exclude < SRWLOCK >  guard{ m_srwBucketLock[bucket] };
                    auto pCurrentBlock = (HashBlock*)m_pHashPool->GetBlock(m_pageMap[headPage].m_inMemHashBlkIndex);
                    BloomBlock* pBloom = m_pBlooms->GetBloomFilter(headPage);

                    if (version == m_version)
                    {
                        if (!pCurrentBlock->IsFull())
                        {                            
                            // enter the bloom key to the bloom key block
                            auto bloomKeyBlock = (BloomKeyBlock*)m_pBloomKeyPool->GetBlock(m_pageMap[headPage].m_inMemBloomKeyBlkIndex);
                            (*bloomKeyBlock)[pCurrentBlock->GetCount()] = bloomKey;
                            pCurrentBlock->Add({ hashKey, size }, address);                            
                            pBloom->Add(bloomKey);                            
                            return;
                        }
                    }
                }
                if (version == m_version)
                {
                    // the block must have been full, need a new one                                        
                    auto newPage = AcquireHashBlkId();                    
                    {
                        // insert the empty block in front of the list
                        Utilities::Exclude<SRWLOCK> bucketGuard{ m_srwBucketLock[bucket] };
                        if (version == m_version)
                        {
                            m_nextPage[newPage] = headPage;                            
                            m_bucketHeads[bucket] = newPage;
                            newPage = INVALID_PAGE;
                        }
                    }
                    if (newPage != INVALID_PAGE)
                    {
                        FreeAllIndices(newPage);
                    }
                    else if (!m_fileStoreGCInProgress)
                    {
                        // So we added new head block, now we need to flush the older block

                        static_assert(FIRSTBLOCKTOFLUSH == 2,
                            "blkId assignment should also change if FIRSTBLOCKTOFLUSH field value changes");
                        //TODO: change to original for production
                        auto blkId = headPage;// m_pageMap[hashBlkId].m_nextBlockId;
                        if (blkId != INVALID_PAGE)
                        {
                            m_flushCandidates.Enqueue(BlockIdentifier{ (uint16_t)bucket, blkId });
                            if (m_flushCandidates.Size() >= BlockCapacity)
                            {
                                m_pBlockSweeper->Post(0, m_gcVersion);
                            }

                            Tracer::LogCounterValue(Tracer::EBCounter::CatalogFlushQueLen, m_partitionString.c_str(),
                                m_flushCandidates.Size());
                        }
                    }
                }
                // restart
                version = m_version;
                hashKey = Reduction::To28BitKey(halfKey, m_hashSeeds[bucket]);
                headPage = m_bucketHeads[bucket];
            }
            Audit::Assert("hash entry insertion failed!");
        }

        // Return a probable match (48 bits of the key do match).
        // Caller should retrieve the key and value at this address.
        // If the full key does not match, use the failed address as a prior and try again.
        //
        void TryLocate(Datagram::Key128 key,
            uint32_t priorAddress,
            Schedulers::ContinuationBase* pContinuation)        
        {
            bool isRetry = (priorAddress != INVALID_FILEOFFSET);
            
            uint32_t resultAddress = ~0UL;
            uint8_t resultSize = 0;
            
            auto halfKey = Reduction::HalveKey(key);
            unsigned bucket = MapToBucket(halfKey);
            StatusCode result = StatusCode::NotFound;

            unsigned version = m_version;
            auto bloomKey = Reduction::QuarterKey(halfKey,
                m_hashSeeds[(CATALOGBUCKETCOUNT - 1) & (bucket + 1)]);
            auto hashKey = Reduction::To28BitKey(halfKey, m_hashSeeds[bucket]);
            auto hashBlkId = m_bucketHeads[bucket];

            while (INVALID_PAGE != hashBlkId)
            {
                auto pBloom = m_pBlooms->GetBloomFilter(hashBlkId);
                if (pBloom->Test(bloomKey))                                
                {
                    Utilities::Share <SRWLOCK>bucketGuard{ m_srwBucketLock[bucket] };
                    auto memBlk = m_pageMap[hashBlkId].m_inMemHashBlkIndex;
                    if (memBlk == INVALID_IDX)
                    {
                        result = StatusCode::PendingIO;
                        break;
                    }
                    auto pHashBlock = (HashBlock*)m_pHashPool->GetBlock(memBlk);
                    Audit::Assert(pHashBlock != nullptr, "pHashBlock cannot be null here. Possible memory corruption");
                    if (version == m_version)
                    {
                        // search from the lastest (higher number index)
                        // to the oldest (lower number index)
                        unsigned index = std::numeric_limits<uint32_t>::max();
                        while (pHashBlock->Find(hashKey, index, resultAddress, resultSize) && version == m_version)
                        {
                            if (isRetry)
                            {
                                if (priorAddress == resultAddress)
                                {
                                    isRetry = false;                                                                        
                                }
                                
                                if (index == 0)
                                {
                                    // we found a match at index 0 therefore there is no other match possible in this hash block.
                                    // break will try to find next hash block in the linked list of this bucket which may have this key
                                    break;
                                }
                                else
                                {
                                    index--;
                                }
                            }
                            else
                            {
                                // we *probably* found it.  Match must be confirmed by the caller
                                //  retrieving the value which is stored with its header and full key.
                                // If that fails, the caller retries rejecting this as a prior.

                                // The caller should quickly retry if the full key does not match.
                                // In some special cases multiple retries may be needed in order to
                                // beat GC and to bypass collisions                                
                                size_t fullAddress = Reduction::ExpandAddress(resultAddress);
                                size_t fullSize = Reduction::ExpandSize(resultSize);
                                AddressAndSize resultAddrSz(fullAddress, fullSize);                                
                                pContinuation->Post((intptr_t)resultAddrSz.Raw(), 0);
                                return;
                            }
                        }
                    }
                }//      
                if (version == m_version)
                {
                    // what if version changed here
                    // next iteration there will be another version check
                    // so we are ok
                    hashBlkId = m_nextPage[hashBlkId];
                }
                else
                {
                    // restart if the version changed under us
					isRetry = false;
                    version = m_version;
                    bloomKey = Reduction::QuarterKey(halfKey,
                        m_hashSeeds[(CATALOGBUCKETCOUNT - 1) & (bucket + 1)]);
                    hashKey = Reduction::To28BitKey(halfKey, m_hashSeeds[bucket]);
                    hashBlkId = m_bucketHeads[bucket];
                }
            }
            if (result == StatusCode::PendingIO)
            {
                auto pArena = pContinuation->m_activity.GetScheduler()->GetNewArena();
                TryLocateCallbackContinuation* pRetryContinuation = pArena->allocate<TryLocateCallbackContinuation>(
                    pContinuation->m_activity,
                    pArena,
                    key,
                    priorAddress,
                    pContinuation,
                    this
                    );

                LoadBlock(BlockIdentifier{ (uint16_t)bucket, (uint16_t)hashBlkId }, pRetryContinuation, pArena);
                return;
            }
            else
            {
                AddressAndSize invalid; // default value is invalid
                pContinuation->Post((intptr_t)(invalid.Raw()), 0);
            }            
        }        

        // Assign a new address to a key-address instance already in the catalog.
        // The instance is invalidated if the new address is UNUSED_ADDRESS
        // Returns false if the key is not found.
        //
        void Relocate(Key128 key,
            uint32_t priorAddress,
            uint32_t newAddress,
            Schedulers::ContinuationBase* pNotify) override
        {
            auto halfKey = Reduction::HalveKey(key);
            unsigned bucket = MapToBucket(halfKey);

            unsigned version = m_version;
            auto bloomKey = Reduction::QuarterKey(halfKey, 
                m_hashSeeds[(CATALOGBUCKETCOUNT - 1) & (bucket + 1)]);
            auto hashKey = Reduction::To28BitKey(halfKey, m_hashSeeds[bucket]);
            auto hashBlkId = m_bucketHeads[bucket];
            StatusCode status = StatusCode::NotFound;

            while (INVALID_PAGE != hashBlkId)
            {
                auto pBloom = m_pBlooms->GetBloomFilter(hashBlkId);
                if (pBloom->Test(bloomKey))
                {                    
                    // we are changing the content of the block here.
                    // Using exclusive lock: we are not really worry about two
                    // Relocate overlap, or even Relocate overlap with Add.
                    // we are worried about Relocate overlap with blocks spilling
                    // to SSD
                    //
                    Utilities::Exclude <SRWLOCK>bucketGuard{ m_srwBucketLock[bucket] };
                    auto index = m_pageMap[hashBlkId].m_inMemHashBlkIndex;
                    if (index == INVALID_IDX)
                    {
                        status = StatusCode::PendingIO;
                        break;
                    }
                    auto pHashBlock = (HashBlock*)m_pHashPool->GetBlock(index);
                    if (pHashBlock != nullptr && version == m_version)
                    {
                        if (pHashBlock->ResetAddress(hashKey, priorAddress, newAddress))
                        {
                            // we changed the content of this block so invalidate the filestore entry
                            m_pageMap[hashBlkId].m_fileOffset = INVALID_FILEOFFSET;
                            status = StatusCode::OK;
                            break;
                        }
                    }                    
                }
                if (version == m_version)
                {
                    hashBlkId = m_nextPage[hashBlkId];
                }
                else
                {
                    // restart if the version changed under us
                    version = m_version;
                    bloomKey = Reduction::QuarterKey(halfKey,
                        m_hashSeeds[(CATALOGBUCKETCOUNT - 1) & (bucket + 1)]);
                    hashKey = Reduction::To28BitKey(halfKey, m_hashSeeds[bucket]);
                    hashBlkId = m_bucketHeads[bucket];
                }
            }
            if (status == StatusCode::PendingIO)
            {
                auto pArena = m_pScheduler->GetNewArena();
                ReLocateCallbackContinuation* pRetryContinuation = pArena->allocate<ReLocateCallbackContinuation>(
                    pNotify->m_activity,                    
                    pArena,
                    key,
                    priorAddress,
                    newAddress,
                    pNotify,
                    this
                    );

                LoadBlock(BlockIdentifier{ (uint16_t)bucket, (uint16_t)hashBlkId }, pRetryContinuation, pArena);
                return;
            }
            else if (status == StatusCode::OK)
            {
                pNotify->Post((intptr_t)StatusCode::OK, 0);
                return;
            }
            else
            {
                // at this point the catalog does not get rid of entries without
                // being told to. we have a problem if the old entry can not
                // be found
                Audit::OutOfLine::Fail(StatusCode::StateChanged, "hash entry disappeared? how come?");
            }
        }

        void Compact(ContinuationBase* pNotify) override
        {
			// compaction is always called by GC which is a long running activity
			// if we allocate this continuation on GC's arena then it will eventually run out of 
			// arena space. Hence we need an external arena
			auto pArena = m_pScheduler->GetNewArena();
            StartCompactionContinuation* pStartCompaction = 
                pArena->allocate<StartCompactionContinuation>(
                    pNotify->m_activity,                    
                    pNotify,
                    this,
					pArena);
            
            pStartCompaction->Post(0, 0);
        }                             
                
        void SetGCStart() override
        {
            m_fileStoreGCInProgress = true;
            m_flushCandidates.Reset();
            m_gcVersion++;
            Tracer::LogCounterValue(Tracer::EBCounter::GCVersion, m_partitionString.c_str(), m_gcVersion);

            // again this is just an estimate to aid for debugging...
            // Since we compact the whole catalog we can set this to 0 and increment it 
            // in catalog compaction mtd
            m_keysInPartition = 0;
        }

        void SetGCEnd() override
        {
            m_fileStoreGCInProgress = false;
			Tracer::LogDebug(StatusCode::OK, L"GC started reset for current partition catalog");
        }               

        void SweepToFileStore()
        {      
            Audit::Assert(!m_fileStoreGCInProgress, "GC cannot be on for this partition at this time.");

            int startingBlock = FIRSTBLOCKTOFLUSH;
            
            // go through each bucket and add all the flushable blocks
            // to flushcandidate queue
            for (int i = 0; i < CATALOGBUCKETCOUNT; i++)
            {
                int blockNum = 0;
                auto headBlkId = m_bucketHeads[i];
                for (uint16_t blkId = headBlkId;  blkId != INVALID_PAGE;
                    blkId = m_nextPage[blkId])
                {
                    blockNum++;
                    if (blockNum >= startingBlock && m_pageMap[blkId].m_fileOffset == INVALID_FILEOFFSET)
                    {
                        m_flushCandidates.Enqueue(BlockIdentifier{ (uint16_t)i, blkId });
                    }
                }                
            }
            
			Tracer::LogDebug(StatusCode::OK, L"All blocks added to flush queue");
			Tracer::LogCounterValue(Tracer::EBCounter::CatalogFlushQueLen,
                m_partitionString.c_str(), m_flushCandidates.Size());
			m_pBlockSweeper->Post(0, m_gcVersion);
        }                

        void StartCatalogFileGC(Schedulers::ContinuationBase* pNotify) override
        {
            auto pArena = m_pScheduler->GetNewArena();
            auto countDownActor = pArena->allocate<Exabytes::CountDownActor>(pNotify->m_activity, 2, pNotify, true);
            m_pHashBlkStore->StartCatalogStoreGC(countDownActor);
            m_pBloomKeyBlkStore->StartCatalogStoreGC(countDownActor);            
        }

        void RequestShutdown() override
        {
            m_shuttingDown = true;            
        }

        ~ReducedKeyBlockHash() 
        {
            if (m_pBlockSweeper != nullptr)
            {
                // shutdown activity for hash and bloom key block flushing
                // since we should have already turned off
                // sweeping and gc, this activity should
                // be idle by now.
                m_pBlockSweeper->m_activity.RequestShutdown(StatusCode::OK);
                m_pBlockSweeper = nullptr;
            }            
        }

        private:

            // Similar to FinishBloomKeyFlushing. Consider revising FinishBloomKeyFlushing when changing this method.
            StatusCode FinishFlushing(
                BlockBuffer<HashBlock>* pBlkBuffer,
                uint32_t expectedGCVersion
                )
            {
                Audit::Assert(pBlkBuffer != nullptr, "Buffer cannot be null");                
                auto bufferIndex = pBlkBuffer->GetBufferIndex();
                auto storeBlockOffset = pBlkBuffer->GetStoreBlockOffset();
                auto numBlocks = BlockCapacity;
                auto storeBlockCount = CATALOGFILESTORECAPACITY / sizeof(HashBlock);
                static_assert(CATALOGFILESTORECAPACITY / sizeof(HashBlock) < std::numeric_limits<uint32_t>::max(),
                    "Can not fit catalog file into 32");
                size_t blockCount = 0;
                StatusCode result = StatusCode::OK;

                // go through the buffer index and update header for each block id
                while (blockCount < numBlocks)
                {
                    uint32_t inMemIndex = INVALID_IDX;
                    {
                        // Exclusive lock since we are modifying a block
                        Utilities::Exclude < SRWLOCK >  guard{ m_srwBucketLock[bufferIndex->bucket] };
                        // if GC started - stop and return
                        if (m_gcVersion != expectedGCVersion)
                        {
                            result = StatusCode::Abort;
                            break;
                        }

                        // we dont need to check if fileoffset invalid because we dont care if we overwrite it. 
                        // The block in file will be reclaimed during catalog file GC
                        m_pageMap[bufferIndex->blkId].m_fileOffset =
                            (uint32_t)((storeBlockOffset + blockCount) % storeBlockCount);
                        inMemIndex = m_pageMap[bufferIndex->blkId].m_inMemHashBlkIndex;
                        m_pageMap[bufferIndex->blkId].m_inMemHashBlkIndex = INVALID_IDX;
                    }

                    // there is a possibility that this block is added to flush que twice (sweeptofilestore and Add overlap)
                    // we do try to eliminate duplicate flush in BlockSweepContinuation however if the blocks are flushed in same
                    // cycle then we cant avoid this at BlockSweep level.
                    if (INVALID_IDX != inMemIndex)
                    {
                        auto pHashBlock = (HashBlock*)m_pHashPool->GetBlock(inMemIndex);
                        pHashBlock->Clear();
                        m_pHashPool->ReleaseBlock(inMemIndex);
                        m_acquiredInMemHashBlocks--;
                    }
                    else
                    {
                        Tracer::LogError(StatusCode::DuplicatedId,
                            L"inMemIndx is invalid during finish flush operation. Poosible case of duplicate block flush");
                    }
                    blockCount++;
                    bufferIndex++;
                }

                pBlkBuffer->ResetBuffer();
                Tracer::LogActionEnd(Tracer::EBCounter::HashBlockFlush, result);

                if (m_gcVersion == expectedGCVersion)
                {
                    // since the gc version has not changed so the result should be success
                    Audit::Assert(result == StatusCode::OK, "Failed to update catalog after block flush");
                }
                return result;
            }

            // Similar to FinishFlushing. Consider revising FinishFlushing when changing this method.
            StatusCode FinishBloomKeyFlushing(
                BlockBuffer<BloomKeyBlock>* pBlkBuffer,
                uint32_t expectedGCVersion
                )
            {
                Audit::Assert(pBlkBuffer != nullptr, "Buffer cannot be null");
                auto bufferIndex = pBlkBuffer->GetBufferIndex();
                auto storeBlockOffset = pBlkBuffer->GetStoreBlockOffset();
                auto numBlocks = BlockCapacity;
                auto storeBlockCount = BloomKeyFileStoreCapacity / BLOOMKEYBLOCKSIZE;
                size_t blockCount = 0;
                StatusCode result = StatusCode::OK;

                // go through the buffer index and update header for each block id
                while (blockCount < numBlocks)
                {
                    if (m_gcVersion != expectedGCVersion)
                    {
                        result = StatusCode::Abort;
                        break;
                    }

                    // we dont need to check if fileoffset invalid because we dont care if we overwrite it. 
                    // The block in file will be reclaimed during file GC
                    m_pageMap[bufferIndex->blkId].m_bloomKeyFileOffset = (storeBlockOffset + blockCount) % storeBlockCount;
                    auto inMemIndex = m_pageMap[bufferIndex->blkId].m_inMemBloomKeyBlkIndex;
                    m_pageMap[bufferIndex->blkId].m_inMemBloomKeyBlkIndex = INVALID_IDX;

                    Audit::Assert(m_gcVersion == expectedGCVersion,
                        "Possible operation overlap with GC. Add a shared lock for catalog updation");

                    // there is a possibility that this block is added to flush que twice (sweeptofilestore and Add overlap)
                    // we do try to eliminate duplicate flush in BlockSweepContinuation however if the blocks are flushed in same
                    // cycle then we cant avoid this at BlockSweep level.
                    if (INVALID_IDX != inMemIndex)
                    {
                        auto pBloomKeyBlock = (BloomKeyBlock*)m_pBloomKeyPool->GetBlock(inMemIndex);
                        std::memset(pBloomKeyBlock, 0, BLOOMKEYBLOCKSIZE);
                        m_pBloomKeyPool->ReleaseBlock(inMemIndex);
                    }
                    else
                    {
                        Tracer::LogError(StatusCode::DuplicatedId,
                            L"inMemIndx is invalid during finish flush operation. Poosible case of duplicate block flush");
                    }
                    blockCount++;
                    bufferIndex++;
                }

                pBlkBuffer->ResetBuffer();                
                Tracer::LogActionEnd(Tracer::EBCounter::ShadowBlockFlush, result);

                if (m_gcVersion == expectedGCVersion)
                {
                    // since the gc version has not changed so the result should be success
                    Audit::Assert(result == StatusCode::OK, "Failed to update catalog after bloom key block flush");
                }
                return result;
            }

            // Updates the catalog after a block load completes
            void FinishBlockRead(
                // block identifier for the block which was loaded
                BlockIdentifier identifier, 

                // index in the shared pool where this block was loaded
                uint32_t index)
            {
                StatusCode result = StatusCode::OK;
                bool blockLoaded = false;

                // we finish loading this block in memory only if it is still a valid block .i.e if it
                // was not recycled between when load was issued and this point.
                {
                    Utilities::Exclude <SRWLOCK>bucketGuard{ m_srwBucketLock[identifier.bucket] };
                    if (identifier.fileOffset == m_pageMap[identifier.blkId].m_fileOffset && m_pageMap[identifier.blkId].m_inMemHashBlkIndex == INVALID_IDX)
                    {
                        // this block is still valid and is not in memory so update catalog and finish read operation
                        m_pageMap[identifier.blkId].m_inMemHashBlkIndex = index;
                        blockLoaded = true;
                    }
                }

                if (!blockLoaded)
                {                
                    // the block is already loaded by another read operation
                    // or was recycled by reloate\compact\flush sequence
                    // so discard this read
                    auto pHashBlock = (HashBlock*)m_pHashPool->GetBlock(index);
                    pHashBlock->Clear();
                    m_pHashPool->ReleaseBlock(index);
                    m_acquiredInMemHashBlocks--;
                    // may be create another status for this situation : BlockExists
                    result = StatusCode::DuplicatedId;
                }  

				Tracer::LogActionEnd(Tracer::EBCounter::HashBlockLoad, result);
            }

            // Updates the catalog after a block load completes
            void FinishBloomKeyLoad(
                // block identifier for the block which was loaded
                BlockIdentifier identifier,

                // index in the shared pool where this block was loaded
                uint32_t index)
            {
                Audit::Assert(m_pageMap[identifier.blkId].m_inMemBloomKeyBlkIndex == INVALID_IDX,
                    "Who loaded this? Only filestore GC can issue bloom key load");

                m_pageMap[identifier.blkId].m_inMemBloomKeyBlkIndex = index;                               
                Tracer::LogActionEnd(Tracer::EBCounter::ShadowBlockLoad, StatusCode::OK);
            }
            			
			// Continuation triggered when async file reads completes
			// in a Catalog FileStore. These reads are triggered by
			// a locate\relocate\compaction operation
			//
            class BlockReadNotifier : public ContinuationBase
            {
            private:
                uint32_t m_blkIndex;
                BlockIdentifier m_identifier;
                ReducedKeyBlockHash* m_pCatalogStore;
                ContinuationBase* m_pNotify;

            public:
                BlockReadNotifier(Activity& activity,                                        
                    BlockIdentifier identifier,
                    uint32_t index,
                    ReducedKeyBlockHash* pCatalogStore,
                    ContinuationBase* pNotify
                    )
                    : ContinuationBase(activity)
                    , m_blkIndex(index)
                    , m_identifier(identifier)
                    , m_pCatalogStore(pCatalogStore)
                    , m_pNotify(pNotify)
                {}

                void OnReady(
                    _In_ WorkerThread&,
                    _In_ intptr_t       continuationHandle,
                    _In_ uint32_t       
                    ) override
                {
                    // this is posted by a file I/O queue, continuationHandle holds a status code, 
                    StatusCode status = (StatusCode)continuationHandle;
					Tracer::LogActionEnd(Tracer::EBCounter::CatalogStoreRead, status);
                    Audit::Assert( status == StatusCode::OK, "Block load failed");

                    m_pCatalogStore->FinishBlockRead(m_identifier, m_blkIndex);					
                    m_pNotify->Post((intptr_t)StatusCode::OK, 0);
                    m_pNotify = nullptr;                    
                }

                void Cleanup() override
                {                
                    if (m_pNotify != nullptr)
                    {
                        m_pNotify->Cleanup();
                        m_pNotify = nullptr;
                    }                    
                }
            };                                                                                    

            // Continuation is triggered after a block flush to 
            // catalog filestore finishes
            class OnFinishedFlushingContinuation : public ContinuationBase
            {
            private:                
                ReducedKeyBlockHash* m_pCatalogStore;
                // continuation to trigger after task finishes
                ContinuationBase* m_pNotify;

                // hashblock buffer which was written to the file
                BlockBuffer<HashBlock>* m_pBuffer;

                // bloomkey block
                BlockBuffer<BloomKeyBlock>* m_pBloomKeyBuffer;

            public:
                OnFinishedFlushingContinuation(
                    Activity& activity,
                    ReducedKeyBlockHash* pCatalogStore,
                    ContinuationBase* pNotify,
                    BlockBuffer<HashBlock>* pBuffer,
                    BlockBuffer<BloomKeyBlock>* pBloomKeyBuffer
                    )
                    : ContinuationBase(activity)
                    , m_pCatalogStore(pCatalogStore)
                    , m_pNotify(pNotify)
                    , m_pBuffer(pBuffer)
                    , m_pBloomKeyBuffer(pBloomKeyBuffer)
                {}

                void OnReady(
                    _In_ WorkerThread&,
                    _In_ intptr_t       continuationHandle,
                    _In_ uint32_t       expectedVersion
                    ) override
                {
                    if (m_pCatalogStore->m_shuttingDown)
                    {
                        // catalog is shutting down. No need to complete flushing
                        // no need to notify next continuation either since that activity
                        // may have been shutdown
                        return;
                    }

                    auto bufferType = static_cast<BufferType>(continuationHandle);
                    StatusCode result = StatusCode::UnspecifiedError;

                    switch (bufferType)
                    {
                    case BufferType::HashBlock:
                        result = m_pCatalogStore->FinishFlushing(m_pBuffer, expectedVersion);
                        break;

                    case BufferType::BloomKeyBlock:
                        result = m_pCatalogStore->FinishBloomKeyFlushing(m_pBloomKeyBuffer, expectedVersion);
                        break;

                    default:
                        Audit::OutOfLine::Fail(StatusCode::Unexpected, "unrecognized state");
                        break;
                    }

                    // notify the next continuation
                    if (m_pNotify != nullptr)
                    {
                        m_pNotify->Post((intptr_t)result, expectedVersion);
                    }
                }

                void Cleanup() override
                {
                    if (m_pNotify != nullptr)
                    {
                        m_pNotify->Post((intptr_t)StatusCode::Abort, 0);
                        m_pNotify = nullptr;
                    }
                }
            };

            // Appends writeBuffer with blocks from flushCandidate queue
            // and starts block write. Post should include Status and expected gc version as parameters
            class BlockSweepContinuation : public ContinuationWithContext<ReducedKeyBlockHash, BaseContext>
            {
            private:
                OnFinishedFlushingContinuation* m_pFlushCompletion;

            public:
                BlockSweepContinuation(Schedulers::Activity& activity)
                    : ContinuationWithContext(activity)
                {
                    m_pFlushCompletion = m_activity.GetArena()->allocate<OnFinishedFlushingContinuation>(
                        this->m_activity,
                        GetContext(),
                        this,
                        GetContext()->m_pHashBlockBuffer.get(),
                        GetContext()->m_pBloomKeyBuffer.get());
                }

                void OnReady(
                    _In_ WorkerThread&,
                    _In_ intptr_t ,
                    _In_ uint32_t expectedVersion
                    ) override
                {
                    // this triggered by mem sweeper during Add operation or by OnFinishFlushed continuation
                    // after a flush finishes. In both cases it checks whether the condition is right
                    // to start a new flush cycle and returns if not 
                    Tracer::LogCounterValue(Tracer::EBCounter::CatalogFlushQueLen, GetContext()->m_partitionString.c_str(),
						GetContext()->m_flushCandidates.Size());
                    
                    auto pBuffer = GetContext()->m_pHashBlockBuffer.get();
                    auto pBloomKeyBuffer = GetContext()->m_pBloomKeyBuffer.get();

                    if (!(pBuffer->IsEmpty() && pBloomKeyBuffer->IsEmpty())
                        || GetContext()->m_flushCandidates.Size() < BlockCapacity
                        || expectedVersion != GetContext()->m_gcVersion
                        || GetContext()->m_fileStoreGCInProgress
                        || GetContext()->m_shuttingDown)
                    {
                        // Either a buffer flush is in progress or 
                        // we dont have enough blocks to fill the buffer
                        // or gc started
                        // or catalog shutdown started
                        // so return.
                        return;
                    }
                    
                    while (!pBuffer->IsFull())
                    {
                        Audit::Assert(!pBloomKeyBuffer->IsFull(), 
                            "Unexpexted State. Bloom key buffer cannot be full when hash block buffer still has space");

                        BlockIdentifier blkIdentifier;
                        GetContext()->m_flushCandidates.Dequeue(blkIdentifier);                        
                        if (blkIdentifier.blkId == INVALID_PAGE)
                        {
                            // we just checked we had enough blocks so may be gc started
                            Audit::Assert(GetContext()->m_gcVersion != expectedVersion, "Unexpected state. Queue cannot be empty");
                            pBuffer->ResetBuffer();
                            pBloomKeyBuffer->ResetBuffer();
                            return;
                        }
                                                
                        // There is a possibility of duplicate flush (when Add and SweepToFileStore overlap)
                        // in that case m_inMemHashBlkIndex will be invalid for duplicate flush. We check it and discard the
                        // duplicate block
                        if (GetContext()->m_pageMap[blkIdentifier.blkId].m_inMemHashBlkIndex == INVALID_IDX)
                        {
                            Tracer::LogWarning(StatusCode::DuplicatedId, L"Encountered duplicate block in BlockSweepContinuation");
                            continue;
                        }

                        // No need of acquiring lock here: untill now gc has not started but it may
                        // start now and the relocte may overlap with block copy. If it does then we
                        // may have corrupted data in the write buffer which is OK as we will discard
                        // it anyways as gc started. 
                        void* blkPointer = GetContext()->m_pHashPool->GetBlock
                            (GetContext()->m_pageMap[blkIdentifier.blkId].m_inMemHashBlkIndex);
                        auto result = pBuffer->Append(
                            blkPointer, blkIdentifier.bucket, blkIdentifier.blkId);
                        Audit::Assert(StatusCode::OK == result, "Unexpected error during hash buffer append"); 

                        // hash block append was successfull now append the bloomkey block 
                        blkPointer = GetContext()->m_pBloomKeyPool->GetBlock
                            (GetContext()->m_pageMap[blkIdentifier.blkId].m_inMemBloomKeyBlkIndex);

                        result = pBloomKeyBuffer->Append(
                            blkPointer, blkIdentifier.bucket, blkIdentifier.blkId);
                        Audit::Assert(StatusCode::OK == result, "Unexpected error during bloom key buffer append");
                    }

                    Audit::Assert(pBuffer->IsFull() && pBloomKeyBuffer->IsFull(), "Unexpected state. Both the buffers should be full");
                    
                    // start hash block flush to catalog file store
					// TODO: change this we need a partition id instead of 0
                    Tracer::LogActionStart(Tracer::EBCounter::HashBlockFlush, GetContext()->m_partitionString.c_str());
                    GetContext()->m_pHashBlkStore->StartBlockWrite(pBuffer, *m_pFlushCompletion, expectedVersion);

                    // start bloom key block flush to bloom key file store for catalog                    
                    Tracer::LogActionStart(Tracer::EBCounter::ShadowBlockFlush, GetContext()->m_partitionString.c_str());
                    GetContext()->m_pBloomKeyBlkStore->StartBlockWrite(pBloomKeyBuffer, *m_pFlushCompletion, expectedVersion);

                    return;                                        
                }
                                    
                void Cleanup() override
                {}
            };
			                        
            // continuation to serially load all the bloom key blocks for a single bucket
            class LoadBloomKeyBlocks : public ContinuationBase
            {
            private:
                std::stack<BlockIdentifier> m_linkedBlocks;
                ContinuationBase* m_pNotify;
                ReducedKeyBlockHash* m_pCatalog;
                uint32_t m_currentBlkIndex = INVALID_IDX;
                bool m_init;

            public:
                LoadBloomKeyBlocks(Activity& activity, ContinuationBase* pNotify, ReducedKeyBlockHash* pCatalog)
                    : ContinuationBase(activity)
                    , m_pNotify(pNotify)
                    , m_pCatalog(pCatalog)
                    , m_init(true)
                {}

                void SetNewRequest(std::stack<BlockIdentifier>& loadList)
                {
                    Audit::Assert(m_linkedBlocks.empty(), "Cannot start new load requets");
                    m_linkedBlocks = std::move(loadList);
                    m_init = true;
                }

                void OnReady(
                    _In_ WorkerThread&,
                    _In_ intptr_t       continuationHandle,
                    _In_ uint32_t
                    ) override

                {
                    if (!m_init)
                    {
                        // this is posted after File IO finishes. ContinuationHandle holds the status
                        // of block load
                        Audit::Assert(StatusCode::OK == (StatusCode)continuationHandle, "Block Load failed");
                        m_pCatalog->FinishBloomKeyLoad(m_linkedBlocks.top(), m_currentBlkIndex);
                        m_linkedBlocks.pop();
                    }

                    if (!m_linkedBlocks.empty())
                    {
                        // issue load
                        m_currentBlkIndex = m_pCatalog->LoadBloomKeys(m_linkedBlocks.top(), this);
                        m_init = false;
                        return;
                    }

                    // all blocks in the list loaded
                    m_pNotify->Post((intptr_t)StatusCode::OK, 0);
                }

                void Cleanup() override{}
            };

            // continuation to serially load all hash blocks for a bucket
            // Should only be posted during compaction
            class LoadHashBlocksForCompaction : public ContinuationBase
            {
                std::stack<BlockIdentifier> m_loadList;
                ActionArena* m_pLoadRequestArena;
                ReducedKeyBlockHash* m_pCatalog;
                ContinuationBase* m_pNext;
                bool m_init;

            public:
                LoadHashBlocksForCompaction(Activity& activity, ReducedKeyBlockHash* pCatalog, ContinuationBase* pNext)
                    : ContinuationBase(activity)
                    , m_pCatalog(pCatalog)                    
                    , m_pNext(pNext)
                    , m_init(true)
                    , m_pLoadRequestArena(nullptr)
                {                    
                    Audit::Assert(m_pNext != nullptr, "Next Continuation cannot be null");
                }

                void SetNewRequest(std::stack<BlockIdentifier>& loadList)
                {
                    Audit::Assert(m_loadList.empty(), "Cannot start new load requets");
                    m_loadList = std::move(loadList);
                    m_init = true;
                }

                void OnReady(
                    _In_ WorkerThread&,
                    _In_ intptr_t       continuationHandle,
                    _In_ uint32_t
                    ) override
                {
                    if (!m_init)
                    {
                        Audit::Assert(m_pCatalog->m_fileStoreGCInProgress,
                            "Unexpected state. Serail block load should only be posted during compaction.");

                        Audit::Assert(StatusCode::OK == (StatusCode)continuationHandle, "Hash Block Load failed");
                        m_loadList.pop();
                        
                        // retire the arena which was created for the Load Block Request
                        // note: this continuation is not running on m_pLoadRequestArena
                        Audit::Assert(m_pLoadRequestArena != nullptr, "load request arena cannot be null at this point");
                        if (m_pLoadRequestArena->IsEmpty())
                        {
                            // retiring of an empty arena cause exception
                            // an arena can be empty in cases when we create a new arena and start a load request
                            // in line 1848 however the block is loaded by some read request and hence we terminate the load
                            // so this new arena is never used (0 allocation) and therefore retire logic will hit an assertion.
                            m_pLoadRequestArena->allocateBytes(1);
                        }
                        m_pLoadRequestArena->Retire();
                        m_pLoadRequestArena = nullptr;                        
                    }

                    if (!m_loadList.empty())
                    {
						m_pLoadRequestArena = m_pCatalog->m_pScheduler->GetNewArena();
                        m_pCatalog->LoadBlock(m_loadList.top(), this, m_pLoadRequestArena);
                        m_init = false;
                        return;
                    }
                    
                    m_pNext->Post((intptr_t)StatusCode::OK, 0);
                }

                void Cleanup() override{}
            };

            // Async catalog compaction
            class StartCompactionContinuation : public ContinuationBase
            {
            private:
                uint16_t m_currentBucket;
                ContinuationBase* m_pNotify;
                ReducedKeyBlockHash* m_pCatalogStore;
                ActionArena* m_pArena;
                LoadBloomKeyBlocks* m_pBloomKeyLoader;
                LoadHashBlocksForCompaction* m_pHashBlockLoader;

                bool initState = true;

            public:
                StartCompactionContinuation(
                    Activity& activity,
                    ContinuationBase* pNotify,
                    ReducedKeyBlockHash* pCatalogStore,
                    ActionArena* pArena
                    )
                    : ContinuationBase(activity)
                    , m_pNotify(pNotify)
                    , m_pCatalogStore(pCatalogStore)
                    , m_pArena(pArena)
                    , m_currentBucket(0)
                {
                    // create continuations for serial block loads
                    auto countDownActor = m_pArena->allocate<CountDownActor>(this->m_activity, 2, this);
                    m_pBloomKeyLoader = m_pArena->allocate<LoadBloomKeyBlocks>(this->m_activity, countDownActor, m_pCatalogStore);
                    m_pHashBlockLoader = m_pArena->allocate<LoadHashBlocksForCompaction>(this->m_activity, m_pCatalogStore, countDownActor);
                }

                // starts serial block load of hash blocks and bloom key blocks
                void StartBlockLoad()
                {
                    // generate blocks to be loaded for current bucket
                    std::stack<BlockIdentifier> bloomLoadList;
                    std::stack<BlockIdentifier> hashLoadList;
                    m_pCatalogStore->GenerateLoadList(m_currentBucket, bloomLoadList, hashLoadList);

                    // start loading bloom key blocks serially
                    m_pBloomKeyLoader->SetNewRequest(bloomLoadList);
                    m_pBloomKeyLoader->Post(0, 0);

                    // start loading hash blocks serially
                    m_pHashBlockLoader->SetNewRequest(hashLoadList);
                    m_pHashBlockLoader->Post(0, 0);
                }

                void OnReady(
                    _In_ WorkerThread&,
                    _In_ intptr_t continuationHandle,
                    _In_ uint32_t
                    ) override
                {    
                    Audit::Assert(m_pCatalogStore->m_fileStoreGCInProgress,
                        "Unexpected state. Compaction cannot be called when gc is not on for this partition");

                    Audit::Assert((StatusCode)continuationHandle == StatusCode::OK, "Failure encountered during block load");

                    // First time when this continuation is triggered
                    // we need to start with block load first. Next time onwards this is triggered
                    // after block load finishes
                    if (initState)
                    {
                        StartBlockLoad();
                        initState = false;
                        return;
                    }
                    
                    // blocks are in memory so start compaction
                    m_pCatalogStore->CompactBucket(m_currentBucket);
                    m_currentBucket++;
                    
                    if (m_currentBucket < CATALOGBUCKETCOUNT)
                    {
                        StartBlockLoad();
                        return;
                    }
                    
                    // compaction finished trigger next continuation
                    if (m_pNotify != nullptr)
                    {
                        m_pNotify->Post(0, 0);
                        m_pNotify = nullptr;
                    }

                    // we are done with compaction for this catalog. So retire the arena
                    if (m_pArena != nullptr)
                    {
                        auto pArena = m_pArena;
                        m_pArena = nullptr;
                        pArena->Retire();
                    }
                }

                void Cleanup() override
                {
                    if (m_pNotify != nullptr)
                    {
                        m_pNotify->Cleanup();
                        m_pNotify = nullptr;
                    }
                    if (m_pArena != nullptr)
                    {
                        auto pArena = m_pArena;
                        m_pArena = nullptr;
                        pArena->Retire();
                    }
                }
            };			
            
			// Called after block load finishes. 
            // if it was successful then we retry the operation
            class TryLocateCallbackContinuation : public Schedulers::ContinuationBase
            {
            private:
                ActionArena* m_pArena;
                Key128 m_key;
                uint32_t m_prior;
                ContinuationBase* m_pNext;
                ReducedKeyBlockHash* m_pCatalog;
            public:
                TryLocateCallbackContinuation(Schedulers::Activity& activity,                    
                    ActionArena* pArena,
                    Key128 key,
                    uint32_t prior,
                    ContinuationBase* pNext,
                    ReducedKeyBlockHash* pCatalog
                    )
                    : Schedulers::ContinuationBase(activity)                    
                    , m_pArena(pArena)
                    , m_key(key)
                    , m_prior(prior)
                    , m_pNext(pNext)
                    , m_pCatalog(pCatalog)
                {}

                void OnReady(
                    _In_ WorkerThread&,
                    _In_ intptr_t       continuationHandle,
                    _In_ uint32_t
                    ) override
                {
                    // this continuation is called after block load finished
                    // status should be OK to retry the operation
                    Audit::Assert((StatusCode)continuationHandle == StatusCode::OK, "Block Load failed");
                    m_pCatalog->TryLocate(m_key, m_prior, m_pNext);

                    // done with this continuation so retire the arena
                    if (m_pArena != nullptr)
                    {
                        auto pArena = m_pArena;
                        m_pArena = nullptr;
                        pArena->Retire();
                    }
                }

                void Cleanup() override
                {
                    if (m_pArena != nullptr)
                    {
                        auto pArena = m_pArena;
                        m_pArena = nullptr;
                        pArena->Retire();
                    }
                }
            };

            // Called after block load finishes. 
            // if it was successful then we retry the operation
            class ReLocateCallbackContinuation : public Schedulers::ContinuationBase
            {
            private:                
                ActionArena* m_pArena;
                Key128 m_key;
                uint32_t m_oldAddr;
                uint32_t m_newAddr;
                ContinuationBase* m_pNext;
                ReducedKeyBlockHash* m_pCatalog;
            public:
                ReLocateCallbackContinuation(Schedulers::Activity& activity,                    
                    ActionArena* pArena, Key128 key,
                    uint32_t oldAddress, uint32_t newAddress,
                    ContinuationBase* pNext,  ReducedKeyBlockHash* pCatalog
                    )
                    : ContinuationBase(activity)
                    , m_pArena(pArena)
                    , m_key(key)
                    , m_oldAddr(oldAddress)
                    , m_newAddr(newAddress)
                    , m_pNext(pNext)
                    , m_pCatalog(pCatalog)
                {}

                void OnReady(
                    _In_ WorkerThread&,
                    _In_ intptr_t       continuationHandle,
                    _In_ uint32_t
                    ) override
                {
                    // this continuation is called after block load finished
                    // status should be OK to retry the operation
                    Audit::Assert((StatusCode)continuationHandle == StatusCode::OK, "Block Load failed");
                    m_pCatalog->Relocate(m_key, m_oldAddr, m_newAddr, m_pNext);

                    // done with this continuation so retire the arena
                    if (m_pArena != nullptr)
                    {
                        auto pArena = m_pArena;
                        m_pArena = nullptr;
                        pArena->Retire();
                    }
                }

                void Cleanup() override
                {
                    if (m_pArena != nullptr)
                    {
                        auto pArena = m_pArena;
                        m_pArena = nullptr;
                        pArena->Retire();
                    }
                }
            };

            // starts async block load from file store
            void LoadBlock(
                BlockIdentifier hashBlock,

                // Contiuation to be triggered after the block load finishes
                // Status Code OK provided in the continuationHandle suggests that the
                // block is now in memory
                ContinuationBase* pNotify,

                // arena which should be used for this request
                ActionArena* pArena
                )
            {    
                unsigned int blkIndex = INVALID_IDX;
                unsigned int fileOffset = INVALID_FILEOFFSET;
                {
                    Utilities::Share <SRWLOCK>bucketGuard{ m_srwBucketLock[hashBlock.bucket] };
                    blkIndex = m_pageMap[hashBlock.blkId].m_inMemHashBlkIndex;
                    fileOffset = m_pageMap[hashBlock.blkId].m_fileOffset;
                }
                
                // some other read operation loaded this block
                if (blkIndex != INVALID_IDX)
                {
                    pNotify->Post(0, 0);
                    return;
                }
                Audit::Assert(fileOffset != INVALID_FILEOFFSET, "Hash block map inconsistent");

                // after the block load we need to verify the current fileoffset for this block still matches
                // this fileoffset otherwise we will be loading a stale block in memory
                hashBlock.fileOffset = fileOffset;
				
				Tracer::LogActionStart(Tracer::EBCounter::HashBlockLoad, m_partitionString.c_str());

                // at this point we know this block is not in memory and its fileoffset is
                // valid. If this changes its ok. Continuation triggered on load finish
                // will take care of it
                auto index = m_pHashPool->AcquireBlkIdx();
                m_acquiredInMemHashBlocks++;
                                
                BlockReadNotifier* pContinuation = pArena->allocate<BlockReadNotifier>(
                    pNotify->m_activity, hashBlock, index, this, pNotify);

                // no need to lock the block as we have not yet updated the hashblk map so
                // no one can touch this block
                void* pBlkPointer = m_pHashPool->GetBlock(index);
                m_pHashBlkStore->ReadBlock(fileOffset, pBlkPointer, pContinuation);
            }                  

            // starts async bloom key block load from file store
            // return the index in shared bloom key in mem buffer which was
            // acquired to load currentBlock
            uint32_t LoadBloomKeys(
                // bloom key block to load
                BlockIdentifier currentBlock,

                // Continuation to trigger after block load finishes
                // this continuation should update the catalog
                ContinuationBase* pNotify                
                )
            {               
                // we dont need any locks here. At this point GC has started and only
                // GC thread will issue bloom key loads + no flushing will start
                Audit::Assert(m_pageMap[currentBlock.blkId].m_bloomKeyFileOffset != INVALID_FILEOFFSET, "Hash block map inconsistent");
                
                Tracer::LogActionStart(Tracer::EBCounter::ShadowBlockLoad, m_partitionString.c_str());

                auto index = m_pBloomKeyPool->AcquireBlkIdx();                               
                void* pBlkPointer = m_pBloomKeyPool->GetBlock(index);
                m_pBloomKeyBlkStore->ReadBlock(m_pageMap[currentBlock.blkId].m_bloomKeyFileOffset, pBlkPointer, pNotify);
                return index;
            }

            void DiscardHashBlocks() override
            {                                
                std::vector<uint32_t> toReclaim;
                toReclaim.reserve(BUCKET_CHAIN_LEN);

                // go through each bucket and discard all the eligible blocks from inMemory Pool
                // By eligible we mean: Block which is currently in memory but has a valid copy in fileStore 
                // and its position in the chain is higher than the min threshold position(FIRSTBLOCKTOFLUSH) set for in memory pool
                for (int i = 0; i < CATALOGBUCKETCOUNT; i++)
                {
                    if (m_fileStoreGCInProgress)
                    {
                        // no discarding when GC is in progress
                        return;
                    }

                    { // Critical Section
                        Utilities::Exclude < SRWLOCK > guard{ m_srwBucketLock[i] };
                        int blockNum = 0;
                        auto headBlkId = m_bucketHeads[i];
                        for (uint16_t blkId = headBlkId;  blkId != INVALID_PAGE;
                            blkId = m_nextPage[blkId])
                        {
                            blockNum++;
                            if (blockNum >= FIRSTBLOCKTOFLUSH
                                && m_pageMap[blkId].m_fileOffset != INVALID_FILEOFFSET
                                && m_pageMap[blkId].m_inMemHashBlkIndex != INVALID_IDX
                                )
                            {
                                toReclaim.push_back(m_pageMap[blkId].m_inMemHashBlkIndex);
                                m_pageMap[blkId].m_inMemHashBlkIndex = INVALID_IDX;
                            }
                        }
                    } // end of Critical Section

                    while (!toReclaim.empty())
                    {
                        auto inMemIndex = toReclaim.back();
                        toReclaim.pop_back();
                        auto pHashBlock = (HashBlock*)m_pHashPool->GetBlock(inMemIndex);
                        pHashBlock->Clear();
                        m_pHashPool->ReleaseBlock(inMemIndex);  
                        m_acquiredInMemHashBlocks--;
                    }
                }

                Tracer::LogDebug(StatusCode::OK, L"Discarding Blocks from HashBlock Pool finished for current catalog");
                Tracer::LogCounterValue(Tracer::EBCounter::InMemHashBlksAcquired,
                    m_partitionString.c_str(), m_acquiredInMemHashBlocks);                                
            }

            // acquires hash block ID and indexes into hash, bloom and bloom key pool
            uint16_t AcquireHashBlkId()
            {
                // aquire free hash block id from the pool
                uint16_t hashBlkId = m_freePages.Aquire();                
                Audit::Assert(0 <= hashBlkId && hashBlkId < TOTALHASHBLKS,
                    "invalid id from free pool");

                // Initialize the hash block header         
                Audit::Assert((m_pageMap[hashBlkId].m_inMemHashBlkIndex == INVALID_IDX) 
                    && (m_pageMap[hashBlkId].m_inMemBloomKeyBlkIndex == INVALID_IDX)
                    && (m_pageMap[hashBlkId].m_fileOffset == INVALID_FILEOFFSET)
                    && (m_pageMap[hashBlkId].m_bloomKeyFileOffset == INVALID_FILEOFFSET)
                    && (m_nextPage[hashBlkId] == INVALID_PAGE),
                    "Newly allocated hash page in unexpected state.");
                
                m_pageMap[hashBlkId].m_inMemHashBlkIndex = m_pHashPool->AcquireBlkIdx();
                m_pageMap[hashBlkId].m_inMemBloomKeyBlkIndex = m_pBloomKeyPool->AcquireBlkIdx();
                
                m_acquiredInMemHashBlocks++;
                Tracer::LogCounterValue(Tracer::EBCounter::InMemHashBlksAcquired, m_partitionString.c_str(), m_acquiredInMemHashBlocks); 

                return hashBlkId;
            }            

            void FreeAllIndices(uint16_t hashBlkId)
            {
                auto pBloomBlock = m_pBlooms->GetBloomFilter(hashBlkId);
                pBloomBlock->Clear();

                HashBlock* pBlock = (HashBlock*)m_pHashPool->GetBlock(m_pageMap[hashBlkId].m_inMemHashBlkIndex);
                pBlock->Clear();
                m_pHashPool->ReleaseBlock(m_pageMap[hashBlkId].m_inMemHashBlkIndex);

                m_acquiredInMemHashBlocks--;

                auto pBloomKeyBlock = (BloomKeyBlock*)m_pBloomKeyPool->GetBlock(m_pageMap[hashBlkId].m_inMemBloomKeyBlkIndex);
                std::memset(pBloomKeyBlock, 0, BLOOMKEYBLOCKSIZE);
                m_pBloomKeyPool->ReleaseBlock(m_pageMap[hashBlkId].m_inMemBloomKeyBlkIndex);

                // reset all fields
                m_nextPage[hashBlkId] = INVALID_PAGE;
                m_pageMap[hashBlkId].m_fileOffset = INVALID_FILEOFFSET;
                m_pageMap[hashBlkId].m_bloomKeyFileOffset = INVALID_FILEOFFSET;
                m_pageMap[hashBlkId].m_inMemHashBlkIndex = INVALID_IDX;
                m_pageMap[hashBlkId].m_inMemBloomKeyBlkIndex = INVALID_IDX;

                m_freePages.Release(hashBlkId);
            }
                        
            size_t GetAvailableSpace() override
            {
                return (m_freePages.GetPoolSize() * ENTRYCOUNT) - BUCKET_CHAIN_LEN;
            }
    };

    std::unique_ptr<ReducedKeyMap<Datagram::Key128, uint32_t, uint8_t>>
        ReducedKeyMapFactory(
        _In_ Scheduler& scheduler,                              // Asynchronous work should be posted using the scheduler.
        _In_ const wchar_t* pathName,                           // the name of the file to be used
        _In_ Exabytes::CatalogBufferPool<>& hashPoolManager,      // Shared buffer for hash pool
        _In_ Exabytes::CatalogBufferPool<>& bloomKeyPoolManager,      // Shared buffer for bloom key blocks pool
        uint64_t randomizer                                    // a random value unique to this service
        )
    {
        return std::make_unique<ReducedKeyBlockHash>(scheduler, pathName, hashPoolManager, bloomKeyPoolManager, randomizer);
    }              

    template<typename Block>
    class CatalogFileStore : public BlockFile < Block >
    {
    private:
        
        // Triggered when file write completes
        // After all the writes are complete(1 or 2) it triggers the
        // next continuation
        class BufferFlushCompletion : public Schedulers::ContinuationBase
        {
        private:
            uint8_t m_numOfWrites;
            ContinuationBase* m_pNotify;
            ActionArena* m_pArena;
            uint32_t m_expectedVersion;
            BufferType m_bufferType;

        public:
            BufferFlushCompletion(Schedulers::Activity& activity,
                ContinuationBase* pNotify,
                ActionArena* pArena,
                uint32_t expectedVersion,
                BufferType bufferType
                )
                : ContinuationBase(activity)
                , m_pNotify(pNotify)
                , m_pArena(pArena)
                , m_expectedVersion(expectedVersion)
                , m_bufferType(bufferType)
            {
                // we will atleast have 1 write otherwise we wont be here            
                m_numOfWrites = 1;
            };

            void IncrementWrites()
            {
                m_numOfWrites++;
                Audit::Assert(m_numOfWrites < 3, "num of writes can never exceed 2");
            }

            void OnReady(
                _In_ WorkerThread&,
                _In_ intptr_t       continuationHandle,
                _In_ uint32_t
                ) override
            {
                // this is posted by a file I/O queue, continuationHandle holds a status code,                                         
                Audit::Assert((StatusCode)continuationHandle == StatusCode::OK, "hashblock buffer flush encountered error.");
                m_numOfWrites--;

                if (m_numOfWrites == 0)
                {
					Tracer::LogActionEnd(Tracer::EBCounter::CatalogStoreWrite,
						StatusCode::OK);

                    m_pNotify->Post((intptr_t)m_bufferType, m_expectedVersion);
                    m_pNotify = nullptr;
                    
					// retire the arena
                    if (m_pArena != nullptr)
                    {
                        auto pArena = m_pArena;
                        m_pArena = nullptr;
                        pArena->Retire();
                    }                    
                }
            }

            void Cleanup() override
            {
                if (m_pNotify != nullptr)
                {
                    m_pNotify->Cleanup();
                }
                
                if (m_pArena != nullptr)
                {                            
                    auto pArena = m_pArena;
                    m_pArena = nullptr;
                    pArena->Retire();
                }
                
            }
        };

        // Triggerd to start block write to file store
        class BufferFlushTask : public ContinuationWithContext < CatalogFileStore, BaseContext >
        {
        private:
            ContinuationBase* m_pNext;
            ActionArena* m_pArena;
            BlockBuffer<Block>* m_pBuffer;
        public:
            BufferFlushTask(Activity& activity, ContinuationBase* pNext, BlockBuffer<Block>* pBuffer, ActionArena* pArena)
                : ContinuationWithContext(activity)
                , m_pNext(pNext)
                , m_pBuffer(pBuffer)
                , m_pArena(pArena)
            {}

            void OnReady(
                _In_ WorkerThread&,
                _In_ intptr_t       continuationHandle,
                _In_ uint32_t       expectedVersion
                ) override
            {
                // this is posted by the StartWriteBlock mtd
                Audit::Assert((StatusCode)continuationHandle == StatusCode::OK,
                    "Unexpected state. Should always be OK.");

                Tracer::LogActionStart(Tracer::EBCounter::CatalogStoreWrite, 
                    GetContext()->m_filePath.c_str());

                // schedule the write I/O completion on the same activity.
				BufferFlushCompletion* pFlushContinuation = m_pArena->allocate<BufferFlushCompletion>(
					m_activity,
					m_pNext,
					m_pArena,
					expectedVersion,
                    typeid(Block) == typeid(HashBlock) ? BufferType::HashBlock : BufferType::BloomKeyBlock);

                Audit::Assert(GetContext()->WriteBlocks(m_pBuffer, *pFlushContinuation) == StatusCode::OK);
                m_pNext = nullptr;
                m_pArena = nullptr;
            }

            void Cleanup() override
            {
                if (m_pNext != nullptr)
                {
                    m_pNext->Cleanup();
                    m_pNext = nullptr;
                }
                else
                {
                    // if we can call the cleanup for the m_pNext continuation
                    // then it will retire the arena otherwise try it here
                    if (m_pArena != nullptr)
                    {
                        auto pArena = m_pArena;
                        m_pArena = nullptr;
                        pArena->Retire();
                    }
                }
            }

        };

        Schedulers::Scheduler* m_pScheduler;

        // A thread to sequentialize all buffer flushing operations 
        //
        Schedulers::Activity* m_pAppendThread;

        // Next use of the file, perpetually increases and should be modulo the allocated size.
        size_t m_writingEdge;

        // GC boundary of file, perpetually increases and should be modulo the allocated size.
        size_t m_erasingEdge;

        // the file that back this store
        std::unique_ptr<AsyncFileIo::FileManager> m_pFile;
        const std::wstring m_filePath;
        const size_t  m_fileSize;        

        void ResetFileStore()
        {
            m_writingEdge = 0;
            m_erasingEdge = m_fileSize;

            Tracer::LogCounterValue(Tracer::EBCounter::FileStoreAvailableBytes,
                m_filePath.c_str(),
                m_erasingEdge - m_writingEdge);
        }

        // Should only be called from pAppendThread activity to make 
        // sure it is not being called concurrently
        Utilities::StatusCode WriteBlocks(
            BlockBuffer<Block>* pBuffer,
            BufferFlushCompletion& next
            ) 
        {
            size_t trailingEdge = m_erasingEdge;
            size_t allocationEdge = m_writingEdge;
            size_t availableCapacity = trailingEdge - allocationEdge;
            auto bufferCapacity = BlockCapacity * sizeof(Block);

            Audit::Assert(!pBuffer->IsEmpty(),
                "Can't write an empty buffer");
            if (availableCapacity < bufferCapacity)
            {
                Tracer::LogCounterValue(Tracer::EBCounter::FileStoreAvailableBytes,
                    m_filePath.c_str(),
                    m_erasingEdge - m_writingEdge);

                Audit::OutOfLine::Fail(StatusCode::OutOfMemory, "Catalog FileStore could not keep up with data. Out of memory");
            }

            size_t storeOffset = allocationEdge % m_fileSize;
            Audit::Assert(0 == (storeOffset % SSD_READ_ALIGNMENT), "Write location should be aligned. Bad offset value.");

            // this will be used to update the catalog entries after the hash blocks are
            // successfully written to fileStore            
            pBuffer->SetStoreOffset(storeOffset);

            // since this is a circular store we may end up with 2 parts to write if we reach file end
            auto dataSize = bufferCapacity - pBuffer->GetAvailableSize();
            Audit::Assert(0 == (dataSize % SSD_READ_ALIGNMENT), "buffer size must align for unbuffered I/O");

            void* pFirstPart = pBuffer->PData(0);
            size_t firstPartLength = std::min(dataSize, m_fileSize - storeOffset);            
            size_t scndPartLength = dataSize - firstPartLength;
            void* pScndPart = scndPartLength == 0 ? nullptr : pBuffer->PData(firstPartLength);
            
            if (scndPartLength > 0)
            {
                next.IncrementWrites();
            }

            // Start async write to file
            m_pFile->Write(storeOffset, (int)firstPartLength, pFirstPart, &next);

            // if second part present then initiate write for that too
            if (scndPartLength > 0)
            {
                m_pFile->Write(0, (int)scndPartLength, pScndPart, &next);
            }

            // increase the writing edge as we dont want to write to the same location again
            // if the async write fails then the memory will be reclaimed during garbage collection
            m_writingEdge += dataSize;
            Tracer::LogCounterValue(Tracer::EBCounter::FileStoreAvailableBytes,
                m_filePath.c_str(),
                m_erasingEdge - m_writingEdge);
            return StatusCode::OK;
        }

        public:
            // Should only be called from a factory method that ensure
            // the fileSize and filePath value are consistent with
            // the actual file.
            //
            CatalogFileStore(
                std::wstring&& path,
                size_t fileSize,
                Schedulers::Scheduler& scheduler
                )
                : m_filePath(std::move(path))
                , m_fileSize(fileSize)
                , m_pScheduler(&scheduler)
                , m_writingEdge(0)
                , m_erasingEdge(fileSize)
            {
                m_pAppendThread = ActivityFactory(scheduler, L"BlockFile", true);
                m_pAppendThread->SetContext(Schedulers::ContextIndex::BaseContext, this);
                Audit::Assert(m_fileSize % sizeof(Block) == 0
                    && m_fileSize % SSD_READ_ALIGNMENT == 0,
                    "File size needs to be aligned to block boundary and SSD read alignment!");

                // create a file to back this store
                m_pFile = (AsyncFileIo::FileQueueFactory(m_filePath.c_str(), scheduler, fileSize));
                Tracer::LogCounterValue(Tracer::EBCounter::FileStoreAvailableBytes,
                    m_filePath.c_str(),
                    m_erasingEdge - m_writingEdge);

                static_assert( 0 == (sizeof(Block) % (size_t)SSD_READ_ALIGNMENT), "read aligment broken");
            }

            Utilities::StatusCode ReadBlock(
                uint32_t blockOffset,
                void* pDestBlob,
                _In_ Schedulers::ContinuationBase* pContinuation
                ) override
            {
                Tracer::LogActionStart(Tracer::EBCounter::CatalogStoreRead, m_filePath.c_str());

                size_t readOffset = blockOffset * sizeof(Block);
                Audit::Assert(readOffset < m_fileSize, "attempt to read from an invalid location");               
                Audit::Assert(0 == (readOffset % SSD_READ_ALIGNMENT), "alignment broken. Bad offset value");

                uint32_t readSize = sizeof(Block);
                Audit::Assert(readSize < 16 * SI::Mi, "data item size should be smaller than 16M");
                Audit::Assert(readOffset + readSize <= m_fileSize, "block file read past file end");

                Audit::Assert(0 != m_pFile->Read(readOffset, readSize, pDestBlob, pContinuation),
                    "ReadFile failure not yet handled");

                return StatusCode::OK;
            }
            
            void StartBlockWrite(BlockBuffer<Block>* pBuffer, ContinuationBase& next, uint32_t expectedVersion)
            {
                // Post continuation for buffer flush
                // we need to make sure file writes are not called concurrently hence we post continuation
                // on appendThread activity. Since this is a long running activity we create external arena 
                // to allocate all the continuaitons. This arena should be retired after next is posted
				auto pArena = m_pScheduler->GetNewArena();
                BufferFlushTask* pBufferFlushTask = pArena->allocate<BufferFlushTask>(
                    *m_pAppendThread,
                    &next,
                    pBuffer,
                    pArena);

                pBufferFlushTask->Post(0, expectedVersion);
            }

            void StartCatalogStoreGC(ContinuationBase* pNotify)
            {
                // right now we dont have a garbae collector for catalog store
                // since we compact all the buckets we can literally recliam the 
                // whole file
                ResetFileStore();
                pNotify->Post(0, 0);
            }

            ~CatalogFileStore()
            {
                if (m_pAppendThread != nullptr)
                {
                    // shutdown activity for buffer flushing
                    // although file write may be in process and 
                    // it might try to post on this activity after we request shutdown
                    // in that case we will see an assertion "cannot post to dying activity"
                    m_pAppendThread->RequestShutdown(StatusCode::OK);
                    m_pAppendThread = nullptr;
                }
                m_pFile->Close(true);
                m_pFile.reset();
            }
        };
    
    template<typename Block>
    std::unique_ptr<BlockFile<Block>>
        BlockFileFactory(

        _In_ Schedulers::Scheduler& scheduler,
        _In_ const wchar_t* pathName,
        //_In_ std::wstring&& pathName,

        _In_ size_t fileSize

        )
    {        
        return std::make_unique<CatalogFileStore<Block>>(pathName, fileSize, scheduler);
    }        
}
