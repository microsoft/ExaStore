// Exabytes : an iNode system for Exabytes
//
#pragma once
#include "stdafx.h"

#include "Exabytes.hpp"
#include "ReducedKeyMap.hpp"

#include "ChainedFileStore.hpp"
#include "EbPartition.hpp"

#include "UtilitiesWin.hpp"

#include <process.h>
#include <unordered_map>
#include <utility>
#include <algorithm>
#include <queue>
#include <stack>

using namespace std;

namespace
{
    using namespace Datagram;
    using namespace Schedulers;
    using namespace Exabytes;
    using namespace Utilities;

}

namespace Exabytes
{
    namespace
    {
        /* map a hash-key to an actual location in the local storage space.
        The location 64 bits will typically be divided into a number of different
        sub-spaces (DRAM, SSD, HDD, for example), but such divisions within
        the address space are opaque to the catalog.
        Thread-safe.
        */
        class BlobCatalog : public Catalog
        {
        private:
            // Current implementation uses unordered_map.  This is likely inefficient, but a safe start.
            // In the long run we need to worry about storage efficiency.  The minimum entry is 24 bytes.
            // With overheads to support search, we might expect around 32 bytes.  An average value is expected
            // to be around 10kB, which means the catalog has to be about 0.3% of the SSD or HDD use.
            // That is around 3.2GB per TB, very large.  Too large for HDD.  We might look at switching to
            // some form of Priority Hash Index, especially if we can distinguish between hot and cold keys.
            // That should allow us to keep much less in DRAM, and spill efficiently to SSD.

            SRWLOCK m_srwLock;
            std::unordered_map<Key128, AddressAndSize, hash<Key128>, equal_to<Key128>> m_map; //  { 4096 };  VS13up3 C2797 bug
            Partition* m_pPartition = nullptr;

        public:
            BlobCatalog()
            {
                InitializeSRWLock(&m_srwLock);
            }

            // Add an address associated with a key.
            // Does not check if the key already exists, the addition becomes the current value
            // and the old address may remain visible as a version.
            // Throws exception for out of memory.
            //
            virtual bool Add(Key128 key,
                AddressAndSize addressAndSize) override
            {
                bool success = false;
                {
                    Utilities::Exclude<SRWLOCK> guard{ m_srwLock };

                    auto result = m_map.insert(make_pair(key, addressAndSize));
                    success = result.second;
                    Audit::Assert(success, 
                        "duplicate keys not allowed in this implementation");
                }
                return success;
            }

            // Retrieve the address of the specified key.
            // Return (via continuation handle of pContinuation)INVALIDADDRESS
            // if the key is not in the catalog.
            //
            void Locate(Key128 key, AddressAndSize, ContinuationBase* pContinuation) override
            {
                AddressAndSize result;
                {
                    Utilities::Share<SRWLOCK> region{ m_srwLock };
                    auto found = m_map.find(key);
                    if (found != m_map.end())
                    {
                        result = found->second;
                    }
                }
                pContinuation->Post((intptr_t)(result.Raw()), 0);
            }

            // Assign a new address to a key already in the catalog.  The new address
            // may be in the same sub-space or a new sub-space.
            // Returns false if the key is not found.
            //
            void Relocate(Key128 key, AddressAndSize oldLocation, AddressAndSize newLocation, ContinuationBase* pContinuation) override
            {
                auto success = StatusCode::StateChanged;
                {
                    Utilities::Exclude<SRWLOCK> guard{ m_srwLock };
                    auto result = m_map.find(key);
                    if (result != m_map.end() && result->second == oldLocation)
                    {
                        result->second = newLocation;
                        success = StatusCode::OK;
                    }
                    else
                    {
                        Audit::OutOfLine::Fail("invalid old address");
                    }
                }
                pContinuation->Post((intptr_t)success, 0);
            }

            void Expire(Key128 key, AddressAndSize location, ContinuationBase* pContinuation) override
            {
                auto success = StatusCode::StateChanged;
                if (Remove(key, location))
                {
                    success = StatusCode::OK;
                }
                pContinuation->Post((intptr_t)success, 0);
            }

            // Removes a key from the catalog and confirms the address at which it was located.
            // Returns INVALIDADDRESS if the key is not found.
            //
            bool Remove(Key128 key, AddressAndSize)
            {
                bool success = false;
                {
                    Exclude<SRWLOCK> guard{ m_srwLock };
                    auto result = m_map.erase(key);
                    success = result > 0;
                }
                return success;
            }

            void RequestShutdown() override
            {
                Audit::OutOfLine::Fail("Not implemented");
            }

            void SetPartition(Partition* pPart) override
            {
                Audit::Assert(m_pPartition == nullptr,
                    "A catalog can only belong to one partition");
                m_pPartition = pPart;
            }

            void Compact(Schedulers::ContinuationBase*) override
            {}

            // Set GC state = ON for this store
            void SetGCStart() override
            {
                Audit::OutOfLine::Fail("Not implemented");
            }

            // Sets GC state = OFF for this store
            void SetGCEnd() override
            {
                Audit::OutOfLine::Fail("Not implemented");
            }

            void SweepToFileStore() override
            {
                Audit::OutOfLine::Fail("Not implemented");
            }

            void StartCatalogFileGC(Schedulers::ContinuationBase*) override
            {
                Audit::OutOfLine::Fail("Not implemented");
            }

            void DiscardHashBlocks() override
            {
                Audit::OutOfLine::Fail("Not implemented");
            }

            size_t GetAvailableSpace() override
            {
                // return a very large value
                // Dont Care since used in MemStore Tests Only
                return UINT_MAX;
            }
        };
    }

    unique_ptr<Catalog> BlobCatalogFactory(
        // Asynchronous work should be posted using the scheduler.
        //
        _In_ Scheduler& , 

        // the name of the file to be used
        //
        _In_ const wchar_t* ,  

        // a random value unique to this service
        //
        uint64_t ,

        // how many values should we allow
        //
        uint32_t  
        )
    {
        return make_unique<BlobCatalog>();
    }

    const RecoverRec& RecoverRec::operator=(RecoverRec&& other)
    {
        FileHandle = other.FileHandle;
        pChkPnt = other.pChkPnt;
        ChkPntSize = other.ChkPntSize;
        Pid = other.Pid;
        Status = other.Status;
        pPartition = std::move(other.pPartition);

        other.FileHandle = (intptr_t)INVALID_HANDLE_VALUE;
        other.pChkPnt = nullptr;
        other.ChkPntSize = 0;
        other.Status = StatusCode::UnspecifiedError;
        other.Pid = PartitionId();

        return *this;
    }

    RecoverRec::RecoverRec(RecoverRec&& other)
        : FileHandle(other.FileHandle)
        , pChkPnt(other.pChkPnt)
        , ChkPntSize(other.ChkPntSize)
        , Pid(other.Pid)
        , Status(other.Status)
        , pPartition(std::move(other.pPartition))
    {
        other.FileHandle = (intptr_t)INVALID_HANDLE_VALUE;
        other.pChkPnt = nullptr;
        other.ChkPntSize = 0;
        other.Pid = PartitionId();
        other.Status = StatusCode::UnspecifiedError;
    }

    RecoverRec::RecoverRec()
    {
        FileHandle = (intptr_t)INVALID_HANDLE_VALUE;
        pChkPnt = nullptr;
        ChkPntSize = 0;
        Status = StatusCode::UnspecifiedError;
        Pid = PartitionId();
    }

    class StackBufferPool : public CoalescingBufferPool
    {
    private:
        // Buffer pool stuff
        //
        SRWLOCK m_srwPoolLock;
        std::stack<std::unique_ptr<CoalescingBuffer>> m_emptyBuffers;
        std::queue<Continuation<HomingEnvelope<CoalescingBuffer>>*> m_bufferWaitList;

        // Helper function to give available empty buffers to continuations on the wait list
        // not thread safe, caller should hold exclusive right to lock
        // m_srwPoolLock
        //
        void ProcessBufferWaitList()
        {
            while (!m_bufferWaitList.empty() && !m_emptyBuffers.empty())
            {
                Continuation<HomingEnvelope<CoalescingBuffer>>* 
                    pJob = m_bufferWaitList.front();
                m_bufferWaitList.pop();
                pJob->Receive(move(m_emptyBuffers.top()));
                m_emptyBuffers.pop();
            }
        }

    public:

        // Construct an empty buffer pool. 
        // To add buffer, call Release
        StackBufferPool(
            size_t bufferSize, 
            uint32_t poolSize, //number of buffers in this pool
            unique_ptr<CoalescingBuffer>(*bufferFactory)(size_t, CoalescingBufferPool&)
            )
        {
            // Init empty buffer pool
            InitializeSRWLock(&m_srwPoolLock);
            for (uint32_t i = 0; i < poolSize; i++)
            {
                unique_ptr<CoalescingBuffer> pb = bufferFactory(bufferSize, *this);
                Audit::Assert(pb->GetPool() == this, "buffer constructed with the wrong pool");
                m_emptyBuffers.push(move(pb));
            }
        }

        virtual ~StackBufferPool()
        {
            // Note: buffers held by others (sweeper, incoming partiton writer) will not be reclaimed.
            // it should be holder's responsibility to release memory of those.
            while (!m_emptyBuffers.empty())
            {
                m_emptyBuffers.pop();
            }
        }

        // Async function for allocating an empty coalescing buffer from pool.
        // bufferHandler will be triggered when buffer is available.
        // thread safe
        //
        void ObtainEmptyBuffer(
            Continuation<HomingEnvelope<CoalescingBuffer> >& bufferHandler
            ) override
        {
            Exclude<SRWLOCK> guard{ m_srwPoolLock };
            m_bufferWaitList.push(&bufferHandler);
            ProcessBufferWaitList();
        }

        // Release a coalescing buffer to pool, all content will be discarded
        // thread safe
        //
        void Release(std::unique_ptr<CoalescingBuffer>& pBuffer) override
        {
            Audit::Assert(pBuffer->GetPool() == this, "buffer returned to the wrong pool");
            pBuffer->ResetBuffer();
            {
                Exclude<SRWLOCK> guard{ m_srwPoolLock };
                m_emptyBuffers.push(std::move(pBuffer));
                ProcessBufferWaitList();
            }
        }
    };

    unique_ptr<CoalescingBufferPool> BufferPoolFactory(
        // size of each buffers in this pool
        size_t bufferSize,

        // number of buffers in this pool
        uint32_t poolSize, 

        // the pool use this function to build buffers
        unique_ptr<CoalescingBuffer>(*bufferFactory)(size_t, CoalescingBufferPool&)
        )
    {
        return make_unique<StackBufferPool>(bufferSize, poolSize, bufferFactory);
    }

    // Large Page Buffer for in memory hash blocks
    // shared among all the partitions and owned by 
    // ExabytesServer    
    template <typename SizeType = uint32_t>
    class InMemoryBlockPool : public CatalogBufferPool<SizeType>
    {
    private:
        // This pool holds hash blocks which are in DRAM.  There can be more spilled to SSD.
        std::unique_ptr<LargePageBuffer> m_blockPool;

        // atomic pool to maintain free blocks
        Utilities::SimpleAtomicPool<SizeType> m_freeIndexes;

        // size of block stored in this buffer
        size_t m_blockSize;        

        // total number of blocks under this pool
        SizeType m_totalBlockCapacity;

        // a low bound of blocks should be in use under normal
        // situation. used as a guide for active reclaimation
        //
        const size_t m_normalUsage;

    public:
        InMemoryBlockPool(uint32_t capacity, size_t blockSize, size_t normalUsage = 0)
            : m_blockSize(blockSize)
            , m_totalBlockCapacity((SizeType)capacity)
            , m_normalUsage(normalUsage)
        {
            Audit::Assert(capacity < std::numeric_limits<SizeType>::max(),
                "Block pool capacity exceeds design limits.");
            // construct the large page buffer based on capacity
            // add all the indexes as free indexes to begin with
            // index range 1 - block capacity  
            size_t actualSize;
            uint32_t actualCount;
            m_blockPool = make_unique<LargePageBuffer>(
                blockSize, m_totalBlockCapacity, actualSize, actualCount);
            Audit::Assert(actualCount >= m_totalBlockCapacity,
                "Failed to allocate memory for catalog.");
            Audit::Assert(actualCount < std::numeric_limits<SizeType>::max(),
                "Block pool capacity exceeds design limits.");
            m_totalBlockCapacity = (SizeType)actualCount;
            ZeroMemory(m_blockPool->PBuffer(), actualSize);

            for (SizeType i = 1; i <= m_totalBlockCapacity; i++)
            {
                // add all the blocks to free pool
                m_freeIndexes.Release(i);
            }
        }

        SizeType AcquireBlkIdx() override
        {
            auto blockIndex = m_freeIndexes.Aquire();
            Audit::Assert(0 < blockIndex && blockIndex <= m_totalBlockCapacity
                , "invalid block from free pool");
            return blockIndex;
        }

        void ReleaseBlock(SizeType index) override
        {
            m_freeIndexes.Release(index);
        }

        void* GetBlock(SizeType index) override
        {
            return m_blockPool->PData((index-1) * m_blockSize);
        }

        bool IsRunningLow() override
        {
            auto available = m_freeIndexes.GetPoolSize();            
            return (available < (m_totalBlockCapacity - m_normalUsage) / 2);
        }
    };

    unique_ptr<CatalogBufferPool<>> SharedCatalogBufferPoolFactory(
        unsigned inMemBlkCount,
        unsigned totalBlkCount,
        size_t numOfPartitions,
        size_t blockSize)
    {
        // During normal time each partition has inMemBlkCount blocks in memory,
        // add totalBlkCount to account for all the blocks that we bring in during GC
        // + 40 for the new chain we create during compaction
        // for 10 Million entries and 512 buckets the max chain length will be 40        
        // Note: GC runs on 1 partition at a time so adding this once is enough        
        auto normalUsage = inMemBlkCount * (numOfPartitions - 1) + totalBlkCount + 40;

        // we add 512 for each partition to compensate for flush queue, and 2k for 
        // read request to use as temps
        auto totalBlockCapacity = normalUsage + numOfPartitions * 512 + 4 * SI::Ki;

        Audit::Assert(totalBlockCapacity < 2 * SI::Gi, 
            "Catalog capacity exceeds design limit.");
        return make_unique<InMemoryBlockPool<uint32_t>>((uint32_t)totalBlockCapacity, blockSize, normalUsage);
    }

    unique_ptr<CatalogBufferPool<>> SharedBlockPoolFactory(unsigned blockCount, size_t blockSize)
    {
        return make_unique<InMemoryBlockPool<uint32_t>>(blockCount, blockSize);
    }

    unique_ptr<CatalogBufferPool<uint16_t>> BlockPoolFactory(unsigned blockCount, size_t blockSize)        
    {
        return make_unique<InMemoryBlockPool<uint16_t>>(blockCount, blockSize);
    }       
}
