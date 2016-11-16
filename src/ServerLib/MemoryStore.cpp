// MemoryStore : an initial in-DRAM storage loop
//
#pragma once

#include "stdafx.h"

#include "Exabytes.hpp"
#include "FileManager.hpp"
#include "TestHooks.hpp"
#include "UtilitiesWin.hpp"

#include <unordered_map>
#include <vector>
#include <queue>
#include <algorithm>

using namespace std;
using namespace Utilities;

namespace Exabytes
{
    class MemoryStore;
}

namespace    // private to this source file
{
    using namespace Exabytes;
    using namespace Datagram;
    using namespace Schedulers;

    static const size_t ALIGNMENT = 64;

    // number of second for which the multi-part items are allowed to
    // stay in memory. sweeper may decide to come in earlier if space
    // is tight
    static const uint64_t MULTI_PART_POOLING_TIME = 15;

    // invalid offset, 
    static const uint32_t UNUSEDOFFSET = ~0UL; 

    // mark an item as staged in some buffer, not yet flushed
    // to partition store, so that later sweeper would not bother
    // with it.
    static const uint64_t ITEM_STAGED = 0xFFFFFFFFFFFFFFFFULL;

    // ratio of data_size/allocated_size
    // if the memory store is almost empty, the sweeper be slow
    // anything
    static const float ALMOST_EMPTY = 0.15f;

    // sweeper would be in a faster pace if the memory store
    // is almost full.
    static const float ALMOST_FULL = 0.75f;
    
    // a blob is stored as one or more parts.  Each part is atomic, given a header,
    // and the multiple parts will be concatinated to form one blob when written to SSD or HDD.
    // While in memory, each part has a header.
    // should be exactly 64 bytes
    //
    struct PartHeader
    {
        Datagram::Description m_description;

        // offset for chaining backwards to prior version(s).
        uint32_t            m_offsetPriorVersion;

        // offset for chaining multi parts together, should point
        // from older one to newer one.
        uint32_t            m_offsetNextPart;

        // a partial read should have about 30seconds (or less)
        // to live in the staging buffer before sweeped to disk
        // -1 means the item is already staged to a file store buffer,
        // waiting to be flushed.
        int64_t               m_expiringTick;
    };
    static_assert(sizeof(PartHeader) == ALIGNMENT, "BufferHeader struct size must align");

    // Memory store needs to provide a function to invalidate value items in a flushed buffer
    typedef void InvalidateProc(_In_ MemoryStore* kvInMem, const CoalescingBuffer& flushedBuffer);

    // The continuation is triggered when a coalescing buffer is flushed to a file store
    // it calles proper function in the memory store to invalidate the value items
    // in the buffer, and advance trailing edge.
    //
    // Runs in the same activity as the sweeper, must be allocated on a seperate arena,
    // as it has much shorter left time as the sweeper itslef. self destruct after finished.
    //
    class BufferCompletion : public Continuation < HomingEnvelope<CoalescingBuffer> >
    {
    private:
        MemoryStore*                                    m_pStore;
        InvalidateProc*                                 m_pInvalidateProc;
        ActionArena&                                    m_arena;

    public:
        BufferCompletion(
            _In_ Activity& activity,
            _In_ ActionArena& arena,
            _In_ MemoryStore& store,
            _In_ InvalidateProc* pInvalidate
            )
            : Continuation(activity)
            , m_pStore(&store)
            , m_pInvalidateProc(pInvalidate)
            , m_arena(arena)
        {}

        void OnReady(
            _In_ WorkerThread&,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t
            ) override
        {
            Audit::Assert((void*)continuationHandle == (void*)&m_postable, "expected handle must be a Coalescing buffer.");

            Audit::Assert(!m_postable.m_pResource->Empty(), "flushed buffer can not be empty.");
            // Buffer flushed, catalog updated, now invalidate the data items
            m_pInvalidateProc(m_pStore, *(m_postable.m_pResource));
            m_postable.Recycle();
            m_arena.Retire();
        }

        void Cleanup() override
        {
            m_postable.Recycle();
        }
    };

    enum class Urgency
    {
        Slow,
        Normal,
        Fast,
        Shutdown
    };

    typedef Urgency SweeperProc(
        _In_ MemoryStore& kvInMem,
        _In_ unique_ptr<CoalescingBuffer>& pEmptyBuffer,
        int64_t tickNow
        );

    // This Actor controls the standard pattern of chained turns, used here for Sweep
    //
    class PersistActor : public Continuation < HomingEnvelope<CoalescingBuffer> >
    {
    private:
        MemoryStore* m_pKVInMem;
        SweeperProc* m_pSweepProc;
        int64_t      m_nextRunTick = 0;

    public:
        PersistActor(Schedulers::Activity& activity, MemoryStore& store, SweeperProc pSweepProc)
            : Continuation(activity)
        {
            m_pKVInMem = &store;
            m_pSweepProc = pSweepProc;
        }

        // This Actor controls the standard pattern of chained turns, used here for Sweep
        //
        void OnReady(_In_ WorkerThread&, _In_ intptr_t, _In_ uint32_t) override
        {
            Audit::Assert((bool)m_postable.m_pResource, "Sweeper need a coalescing buffer to run.");

            // pacing
            LARGE_INTEGER ticksNow;
            Audit::Assert(0 != QueryPerformanceCounter(&ticksNow), "QueryPerformanceCounter failed");
            if (ticksNow.QuadPart < m_nextRunTick - 100)
            {
                this->Post(0, 0, (uint32_t)((m_nextRunTick - ticksNow.QuadPart)/g_frequency.TicksPerMicrosecond()));
                return;
            }
 
            // The retiring loop will be a sequence of turns which chain to each other.
            // Each turn may copy blobs to the SSD staging area, or may flush to SSD,
            // or may do nothing if there is no pending memory.
            // When a turn is finished it posts the next turn, selecting anything from
            // zero to 100ms of delay depending upon estimated congestion.
            Urgency urgency = m_pSweepProc(*m_pKVInMem, m_postable.m_pResource, ticksNow.QuadPart);

            if (urgency != Urgency::Shutdown)
            {
                uint32_t microseconds = (urgency == Urgency::Fast) ? 0
                    : (urgency == Urgency::Normal) ? 1 << 12
                    : 1 << 16;
                m_nextRunTick = ticksNow.QuadPart + (uint64_t) (microseconds * g_frequency.TicksPerMicrosecond());

                if (m_postable.m_pResource)
                {
                    // we still have the buffer, nothing's done then
                    Post((intptr_t)m_pKVInMem, 0, microseconds);
                    return;
                }
                else
                {
                    // buffer is given to a file store to flush
                    // we need a new one
                    ((KeyValueInMemory*)m_pKVInMem)->GetServer()->ObtainEmptyBuffer(*this);
                    return;
                }
            }
            else
            {
                m_postable.Recycle();
                auto status = (StatusCode)::GetLastError();
                m_activity.RequestShutdown(status, L"Sweeper requesting shutdown!");
            }
        }

        void Cleanup()
        {
            m_postable.Recycle();
        }
    };

    // Notification class for IO.
    // This class is used in writing memory store data to file or load data
    // from file. 
    //
    class IoFinish : public ContinuationBase
    {
    protected:
        ContinuationBase* m_pNext;
        StatusCode m_status = StatusCode::OK;
        uint32_t m_size = 0;
    public:
        size_t m_ops = 1;

        IoFinish(Activity& activity, ContinuationBase* pNotify)
            : ContinuationBase(activity)
            , m_pNext(pNotify)
        {}

        void Cleanup() override
        {  /* ignore cleanup action in data recovery. */ }

        // hook for expansion
        virtual void ProcessResult()
        {}

        void OnReady(_In_ WorkerThread&, _In_ intptr_t continuationHandle, _In_ uint32_t length) override
        {
            m_size += length;
            if (continuationHandle != (intptr_t)StatusCode::OK)
            {
                m_status = (StatusCode)continuationHandle;
            }

            m_ops--;
            if (m_ops == 0)
            {
                ProcessResult();
                if (m_pNext != nullptr)
                {
                    m_pNext->Post((intptr_t)m_status, m_size);
                }
            }
        }
    };
}

namespace Exabytes
{
    /* Initially, data is stored in DRAM.  This is replicated to other nodes
    for durability and then allowed to accumulate in DRAM until we have enough
    to write efficiently to SSD.
    Holding in DRAM also allows us to reorder partial appends to create contiguous
    data of better locality on SSD.
    */
    class MemoryStore : public KeyValueInMemory
    {
        friend class TestHooks::LocalStoreTestHooks;
        friend     std::unique_ptr<KeyValueInMemory> MemoryStoreFactory(
            _In_ ExabytesServer& server,

            // the store size is the count of bytes of DRAM we will reserve for
            // the MemoryStore.
            size_t storeSize,

            // file from which store content should be loaded
            AsyncFileIo::FileManager& file,

            // <allocation edge, trailing edge>
            std::pair<uint64_t, uint64_t> edges,

            // Invoked when the load is finished. Caller should take care
            // that the store should only be used after all these finished.
            ContinuationBase* pNotify
            );


    private:
        Scheduler* const m_pScheduler;

        // A large memory (> 128MB) used circularly stages data before persisting.
        unique_ptr<Utilities::LargePageBuffer> m_pMemory;
        size_t m_allocatedSize;

        // key -> address
        unordered_map<Key128, uint32_t> m_keyOffsetMap;

        // Sweeper related fields
        PersistActor* m_pBackgroundSweeper = nullptr;
        Activity*     m_pJanitor = nullptr;
        bool m_isSweeping = false;
        bool m_shuttingdown = false;

        SRWLOCK m_srwMemoryLock;

        // this value perpetually increases and should be modulo the allocated size.
        size_t m_allocationEdge = 0;

        // this value perpetually increases and should be modulo the allocated size.
        size_t m_trailingEdge = 0;

        // Copies a datagram into the memory store, 
        // returns offset of the newly written item
        //
        uint32_t InternalWrite(
            // full description of the blob we are storing
            const Datagram::Description& description,

            // the blob we are sending.  MBZ if and only if descriptor.valueLength == 0
            _In_ const void* pData,

            // must equal descriptor.valueLength.  Required to satisfy automated correctness analysis.
            _In_ size_t dataLength
            )
        {
            Audit::Assert(dataLength == description.ValueLength,
                "Internal Error while writing to memory store: value length mismatch in parameters.");

            // start looking for prior item, actual processing will follow
            uint32_t priorOffset = UNUSEDOFFSET;
            {
                Utilities::Share<SRWLOCK> guard{ m_srwMemoryLock };
                auto iter = m_keyOffsetMap.find(description.KeyHash);
                priorOffset = (iter == m_keyOffsetMap.end()) ? UNUSEDOFFSET : iter->second;
            }

            uint32_t offset = UNUSEDOFFSET;
            Audit::Assert(m_allocatedSize <= (1ul << 30), "buffer offsets limited to 32 bits");
            bool unfinished = true;
            unsigned retryCount = 0;
            bool linked = false;

            PartHeader* pMultiHeader = nullptr;
            if (priorOffset != UNUSEDOFFSET)
            {
                Audit::Assert(priorOffset < m_allocatedSize
                    && priorOffset % ALIGNMENT == 0,
                    "invalid offset of prior version");
                auto pPrevHeader = reinterpret_cast<PartHeader*>
                    (m_pMemory->PData(priorOffset));

                // if this one and the prior one are both multi-part, link them
                // since they have the same key. and in that case it is not 
                // a prior version, its part of current version
                if (pPrevHeader->m_description.Part != 0 && description.Part != 0)
                {
                    priorOffset = UNUSEDOFFSET;
                    pMultiHeader = pPrevHeader;
                    while (pMultiHeader->m_offsetNextPart != UNUSEDOFFSET)
                    {
                        Audit::Assert(pMultiHeader->m_description.KeyHash == description.KeyHash,
                            "multi-part key must match");
                        pMultiHeader = reinterpret_cast<PartHeader*>
                            (m_pMemory->PData(pMultiHeader->m_offsetNextPart));
                    }
                }
            }

            PartHeader* pHeader = nullptr;
            while (unfinished)
            {
                size_t trailingEdge = m_trailingEdge;
                size_t allocationEdge = m_allocationEdge;

                // with those snapshots, do all our preparation.

                size_t available = trailingEdge - allocationEdge;
                Audit::Assert(available <= m_allocatedSize,
                    "available size must not exceed circular buffer");
                size_t totalSize = dataLength + sizeof(PartHeader);                
                if (totalSize > available)
                {
                    // circular buffer failed to keep up with the data. Reject the write request
                    offset = UNUSEDOFFSET;
                    break;
                }

                offset = (uint32_t) (allocationEdge % m_allocatedSize);
                Audit::Assert(0 == (offset % ALIGNMENT),
                    "circular buffer allocation must be aligned");
                pHeader = reinterpret_cast<PartHeader*>(m_pMemory->PData(offset));
                size_t offset2 = (offset + sizeof(PartHeader)) % m_allocatedSize;
                void* pBody = m_pMemory->PData(offset2);

                // Note: the header itself will never be split into two parts,
                // due to the fact its size is the alignment size.
                size_t wrapLimit = m_allocatedSize - offset2;
                size_t length1 = min(dataLength, wrapLimit);
                void* pData2 = (void*)(length1 + (char*)pData);
                size_t length2 = dataLength - length1;
                void* pTrim = m_pMemory->PData((offset2 + dataLength) % m_allocatedSize);
                size_t finalTrimCount = (ALIGNMENT - (totalSize % ALIGNMENT)) % ALIGNMENT;
                totalSize += finalTrimCount;

                // for multi-part item, give it 30 seconds before sweeped to disk
                // it is not enforced, sweeper can do it eariler if space is tight
                // we don't have to be very precise.
                LARGE_INTEGER ticksNow;
                Audit::Assert(0 != QueryPerformanceCounter(&ticksNow),
                    "QueryPerformanceCounter failed");
                uint64_t whenDue = (uint64_t)(ticksNow.QuadPart +
                    (MULTI_PART_POOLING_TIME * Schedulers::g_frequency.TicksPerSecond()));

                // begin locked scope
                {
                    Utilities::Exclude<SRWLOCK> guard{ m_srwMemoryLock };

                    // verify snapshot values unchanged, no other thread made changes before we could get the lock.
                    // do not need to check trailing edge, if it increased, the available size got bigger,
                    // should not affect our operation here.
                    if (allocationEdge == m_allocationEdge)
                    {
                        ZeroMemory(pHeader, sizeof(PartHeader));
                        pHeader->m_description = description;

                        pHeader->m_expiringTick = (uint64_t)whenDue;
                        pHeader->m_offsetPriorVersion = priorOffset;
                        pHeader->m_offsetNextPart = UNUSEDOFFSET;

                        if (0 < length1)
                        {
                            memcpy_s(pBody, length1, pData, length1);
                        }
                        if (0 < length2)
                        {
                            memcpy_s(m_pMemory->PBuffer(), m_allocatedSize, pData2, length2);
                        }
                        if (0 < finalTrimCount)
                        {
                            ZeroMemory(pTrim, finalTrimCount);
                        }
                        m_allocationEdge += totalSize;

                        if (pMultiHeader != nullptr &&
                            pMultiHeader->m_description.KeyHash == description.KeyHash)
                        {
                            // link multiple parts together if the space has not been repurposed
                            while (pMultiHeader->m_offsetNextPart != UNUSEDOFFSET)
                            {
                                pMultiHeader = reinterpret_cast<PartHeader*>
                                    (m_pMemory->PData(pMultiHeader->m_offsetNextPart));
                            }
                            pMultiHeader->m_offsetNextPart = m_pMemory->OffsetOf(pHeader);
                            pHeader->m_offsetPriorVersion = pMultiHeader->m_offsetPriorVersion;
                            linked = true;
                        }
                        unfinished = false;

                        Audit::Assert(pHeader->m_description.KeyHash == description.KeyHash,
                            "Memory changed while still in critical section!");
                        m_keyOffsetMap[description.KeyHash] = offset;
                    }
                    else
                    {
                        ++retryCount;
                        Audit::Assert(retryCount < 10, 
                            "retry count so high there must be a fundamental error");
                    }
                    // lock is released here
                }
            }   // while(unfinished).

            Tracer::LogCounterValue(
                Tracer::EBCounter::MemoryStoreAvailableBytes,
                L"MemStore", m_trailingEdge - m_allocationEdge);
            Tracer::LogCounterValue(Tracer::EBCounter::MemSweepEdge,
                L"AllocEdge", m_allocationEdge);
            return offset;
        }

        // for a key, find out where the item should be write to
        //
        KeyValueOnFile* LookupPersistorFor(const Key128& keyHash) const
        {
            Partition* pPartition = m_pServer->FindPartitionForKey(keyHash);
            return pPartition->Persister();
        }

        void AdvanceTrailingEdge()
        {
            // Should we add memory lock here? maybe not
            // 1. we read allocation edge here, if we read an older value, 
            //    we make a conservative loop exit, no harm done
            // 2. we update trailing edge, while all other places that read
            //    the trailing edge use it to decide how much
            //    free space there is. our change just make their life easier.
            // 3. we read key high, would somebody set it to non-zero? it can
            //    not be. Write always operate on the right hand side of the 
            //    allocation edge, and this happens on the left hand side of it. 

            for (; (m_trailingEdge - m_allocationEdge) < m_allocatedSize;)
            {
                auto pEdge = reinterpret_cast<PartHeader*>(m_pMemory->PData(m_trailingEdge % m_allocatedSize));
                if (pEdge->m_description.KeyHash.ValueHigh() == 0)
                {
                    size_t alignedSize = AlignPower2(ALIGNMENT, 
                        pEdge->m_description.ValueLength + sizeof(PartHeader));
                    m_trailingEdge += alignedSize;
                }
                else
                {
                    break;
                }
            }
            Tracer::LogCounterValue(Tracer::EBCounter::MemoryStoreAvailableBytes,
                L"MemStore", m_trailingEdge - m_allocationEdge);
            Tracer::LogCounterValue(Tracer::EBCounter::MemSweepEdge,
                L"TrailingEdge", m_trailingEdge);
        }

        // Invalidate a bunch of data items
        // This one should be called after a coalescing buffer is flushed to disk store
        void InvalidateSweptItems(const CoalescingBuffer& flushedBuffer)
        {
            for (auto iter = flushedBuffer.cbegin(); iter != flushedBuffer.cend(); iter++)
            {
                uint64_t headerOffset = iter->second.FullAddress();
                uint64_t keyHigh = iter->first.ValueHigh();

                Audit::Assert(headerOffset < m_allocatedSize, 
                    "valid old location expected from sweeper!");
                Audit::Assert(0 == headerOffset % ALIGNMENT,
                    "item location should be aligned");
                PartHeader* pDesc = reinterpret_cast<PartHeader*>
                    (m_pMemory->PData(headerOffset));
                Audit::Assert(pDesc->m_expiringTick == ITEM_STAGED,
                    "Corrupted item state");
                if (pDesc->m_description.Part == 0)
                {
                    Audit::Assert(pDesc->m_offsetNextPart == UNUSEDOFFSET,
                        "single part item should have no chain pointer");
                }

                // in the case of multi-part items, the key->offset
                // map stores the location of the most recent item
                // however the sweeper buffer remembers the location
                // of the oldest item so that we can find all the parts
                // here

                for (;;)
                {    
                    // loop for multi-part items.
                    {
                        Utilities::Exclude<SRWLOCK> guard{ m_srwMemoryLock };
                        if (pDesc->m_description.KeyHash.ValueHigh() == keyHigh)
                        {
                            // only do it when the location is not repurposed for other data
                            // remove map entry if it is pointing here.
                            auto entryIter = m_keyOffsetMap.find(pDesc->m_description.KeyHash);
                            if (entryIter != m_keyOffsetMap.end() 
                                && entryIter->second == headerOffset)
                            {
                                m_keyOffsetMap.erase(entryIter);
                            }
                            pDesc->m_description.KeyHash.Invalidate();
                        }
                    }
                    // next part
                    headerOffset = pDesc->m_offsetNextPart;
                    if (headerOffset < m_allocatedSize && headerOffset % ALIGNMENT == 0)
                    {
                        pDesc = reinterpret_cast<PartHeader*>
                            (m_pMemory->PData(headerOffset));
                        if (pDesc->m_expiringTick != ITEM_STAGED)
                        {
                            // there are maybe new parts added after we staged
                            // the buffer. future sweeper should take care of those.
                            // 
                            break;
                        }
                    }
                    else
                    {
                        break;
                    }
                }
            }
            AdvanceTrailingEdge();
        }

        static void InvalidateHandle(_In_ MemoryStore* pStore, const CoalescingBuffer& flushedBuffer)
        {
            pStore->InvalidateSweptItems(flushedBuffer);
        }

        //    Pick items from trailing edge, and sweep them to file store.
        // items are grouped in a CoalescingBuffer to increase file write efficiency.
        //    As the memory store is shared among all partitions, we sweep by partition.
        // i.e. we identify the partition of the oldest item, and only sweep items that
        // belong to the same partition. We stop if the buffer is full, or there's no
        // item to sweep.
        //
        Urgency SweepToFileStore(unique_ptr<CoalescingBuffer>& pBuffer, int64_t ticksNow)
        {
            // the sweeping logic is single threaded, a posted-loop of actions only one action
            // should ever be running.
            Audit::Assert(false == m_isSweeping, "sweeping should be single threaded");
            Utilities::FlipWhenExist autoFlip(m_isSweeping = true);

            Audit::Assert(pBuffer->Empty(), "need empty buffer to ensure no partition mixup");
            pBuffer->m_origin = CoalescingBuffer::Origin::Memory;
            size_t sweepingEdge = m_trailingEdge;
            PartitionId sweepingPartition; // default value is invalid
            KeyValueOnFile* pStore = nullptr;

            for (;;)
            {
                if (m_shuttingdown)
                {
                    // drop everything and run
                    return Urgency::Shutdown;
                }

                PartHeader* pDesc = nullptr;
                size_t alignedSize = 0;
                while (nullptr == pDesc && (sweepingEdge - m_allocationEdge) < m_allocatedSize)
                {
                    PartHeader* pScan = (PartHeader*)m_pMemory->PData(sweepingEdge % m_allocatedSize);
                    PartitionId part = m_pServer->FindPartitionIdForKey(pScan->m_description.KeyHash);
                    alignedSize = AlignPower2(ALIGNMENT,
                        pScan->m_description.ValueLength + sizeof(PartHeader));

                    // stop at the oldest valid item, or if we already past the oldest
                    // one, next valid item in the same partition as the oldest one
                    if (pScan->m_description.KeyHash.ValueHigh() != 0
                        && pScan->m_expiringTick != ITEM_STAGED
                        && (sweepingPartition.IsInvalid() || part == sweepingPartition)
                        )
                    {
                        pDesc = pScan;
                        if (sweepingPartition.IsInvalid())
                        {
                            sweepingPartition = part;
                            pStore = LookupPersistorFor(pDesc->m_description.KeyHash);
                        }
                    }
                    else
                    {
                        sweepingEdge += alignedSize;
                    }
                }

                if (pDesc == nullptr
                    || (pDesc->m_description.Part !=0 
                        && pDesc->m_expiringTick > ticksNow)
                    )
                {
                    // either there is no item left, or the item is too young
                    // we stop sweeping, and flush what we already have in the buffer
                    break;
                }

                // TODO!! throw item away if deleted or expired, and continue

                // this impose a upper limit on number of parts, but avoid malloc
                void* dataParts[512];
                uint32_t dataSizes[512];
                uint32_t headerOffsets[513];

                uint32_t numParts = 0;
                headerOffsets[numParts] = m_pMemory->OffsetOf(pDesc);
                void* pWrapBuffer = nullptr;

                // looks like code for collecting a multi-part item, should work 
                // for single part ones too
                for (; headerOffsets[numParts] != UNUSEDOFFSET && numParts < 512; numParts++)
                {
                    Audit::Assert(headerOffsets[numParts] < m_allocatedSize,
                        "invalid offset found during sweeping");

                    auto pHeader = reinterpret_cast<PartHeader*>
                                   (m_pMemory->PData(headerOffsets[numParts]));
                    if (numParts > 0)
                    {
                        Audit::Assert(pHeader->m_description.Part != 0
                                      && 
                                        pHeader->m_description.KeyHash.ValueHigh()
                                        == pDesc->m_description.KeyHash.ValueHigh(),
                            "multipart blobs should have same key");
                    }

                    size_t dataOffset = (headerOffsets[numParts] + sizeof(PartHeader))
                                        % m_allocatedSize;
                    dataParts[numParts] = m_pMemory->PData(dataOffset);
                    dataSizes[numParts] = pHeader->m_description.ValueLength;

                    size_t actual = min((size_t)dataSizes[numParts], 
                                        (m_allocatedSize - dataOffset));
                    if (actual < dataSizes[numParts])
                    {
                        //  blob wraps around end of circular buffer
                        Audit::Assert(pWrapBuffer == nullptr,
                            "only one item can possibly wrap around the end");
                        pWrapBuffer = malloc(dataSizes[numParts]);
                        Audit::Assert(pWrapBuffer != nullptr,
                            "can not allocate memory");

                        memcpy_s(pWrapBuffer, actual, dataParts[numParts], actual);
                        memcpy_s((void*)(actual + (char*)pWrapBuffer),
                                 dataSizes[numParts] - actual, 
                                 m_pMemory->PBuffer(), 
                                 dataSizes[numParts] - actual);
                        dataParts[numParts] = pWrapBuffer;
                    }

                    // next part, if any
                    headerOffsets[numParts + 1] = pHeader->m_offsetNextPart;
                }
                Audit::Assert(headerOffsets[numParts] == UNUSEDOFFSET,
                    "number of parts exceeded 512");

                // in the case of multi-part item, we are using the location
                // of the oldest part. this ensures we can invalidates all
                // the parts after the buffer is flushed.
                // 
                AddressAndSize oldLocation{
                    m_pMemory->OffsetOf(pDesc),
                    pDesc->m_description.ValueLength + sizeof(PartHeader) };
                auto error = pBuffer->Append(
                    pDesc->m_description, dataParts, dataSizes, numParts, oldLocation);

                if (pWrapBuffer != nullptr)
                {
                    free(pWrapBuffer);
                    pWrapBuffer = nullptr;
                }

                if (error == StatusCode::OutOfMemory)
                {
                    // now let's flush it and continue to next partition
                    // the last item is left behind, will be picked up later.
                    break; 
                }

                Audit::Assert(StatusCode::OK == error, "unknown error when appending to buffer");

                // we staged that blob, we can advance to the next one.
                sweepingEdge += alignedSize;
                pDesc = nullptr;

                // We need to mark them
                // as staged, so that they won't be sweeped repeatedly.
                for (uint32_t i = 0; i < numParts; i++){
                    auto pHeader = reinterpret_cast<PartHeader*>
                        (m_pMemory->PData(headerOffsets[i]));
                    pHeader->m_expiringTick = ITEM_STAGED;
                }
            }

            if (m_shuttingdown)
            {
                // drop everything and run
                return Urgency::Shutdown;
            }

            // Buffer's full, or we can not sweep any more
            // flush and exist
            if (!pBuffer->Empty()) {
                // TODO if staged data size < 4MB and we are not under memory
                // pressure, give it up. The difficulty here is we need to set
                // m_expirationTick of all the records to 0
                Tracer::LogCounterValue(Tracer::EBCounter::MemSweepEdge,
                    L"Blob", sweepingEdge);

				ActionArena* pMem1 = m_pScheduler->GetNewArena();
                BufferCompletion* pBufferCompletion = 
                    pMem1->allocate<BufferCompletion>(*m_pJanitor, *pMem1, *this, InvalidateHandle);
                pStore->Flush(pBuffer, pBufferCompletion);
            }

            auto available = m_trailingEdge - m_allocationEdge;
            if (available <= m_allocatedSize * (1 - ALMOST_FULL))
                return Urgency::Fast;
            if (available >= m_allocatedSize * (1 - ALMOST_EMPTY))
                return Urgency::Slow;
            return Urgency::Normal;
        }

        // Called after recovery. After we read content from a file, we need
        // to recontruct the index of the data
        //
        void ReconstructIndex()
        {
            Audit::Assert(false == m_isSweeping, 
                "Initializing... sweeping should be not be started yet!");

            for (uint64_t scanEdge = m_trailingEdge; scanEdge < (m_allocationEdge + m_allocatedSize);){

                uint64_t offset = scanEdge % m_allocatedSize;
                Audit::Assert(offset % ALIGNMENT == 0,
                    "Item alignment off during mem store recovery!");
                PartHeader* pScan = (PartHeader*)m_pMemory->PData(offset);

                size_t alignedSize = AlignPower2(ALIGNMENT,
                    pScan->m_description.ValueLength + sizeof(PartHeader));
                scanEdge += alignedSize; // next item

                if (pScan->m_description.KeyHash.IsInvalid())
                {
                    continue;
                    // TODO!! what if pScan->m_expiringTick == ITEM_STAGED
                    // we may have duplicate items with the partition file
                    // what to do?!!!
                }

                // estabilish index
                m_keyOffsetMap[pScan->m_description.KeyHash] = (uint32_t)offset;
            }
        }
        

        static Urgency SweepFunction(MemoryStore& kvInMem, unique_ptr<CoalescingBuffer>& pBuffer, int64_t ticksNow)
        {
            return kvInMem.SweepToFileStore(pBuffer, ticksNow);
        }

        PartHeader* FindHeader(const Key128& key)
        {
            Utilities::Share<SRWLOCK> guard{ m_srwMemoryLock };
            auto entryIter = m_keyOffsetMap.find(key);
            if (entryIter == m_keyOffsetMap.end())
            {
                return nullptr; // NOT FOUND
            }

            uint64_t headerOffset = entryIter->second;
            Audit::Assert(headerOffset < m_allocatedSize
                && headerOffset%ALIGNMENT == 0,
                "only read from valid address");

            PartHeader* pHeader = reinterpret_cast<PartHeader*>
                (m_pMemory->PData(headerOffset));

            Audit::Assert(pHeader->m_description.KeyHash == key,
                "collision not expected in DRAM!");
            return pHeader;
        }

        // Dangerous interface!!! only called during start up, before sweeper
        // starts or any requests can be processed.
        // Try to load memory store content from a file, <allocEdge, trailingEdge>
        //
        // TODO!! check serial number of each partition store to make sure
        // we do not hold two copies of the same data in memory store
        // and file store
        //
        void StartToLoadFromFile(
            AsyncFileIo::FileManager& file, 
            std::pair<uint64_t, uint64_t> edges,
            ContinuationBase* pNotify
            )
        {
            class LoadingFinish : public IoFinish
            {
                MemoryStore& m_store;
            public:
                LoadingFinish(Activity& activity, MemoryStore& store, ContinuationBase* pNotify)
                    : IoFinish(activity, pNotify)
                    , m_store(store)
                {}

                void ProcessResult() override
                {
                    bool failed = false;
                    if (m_status != StatusCode::OK)
                    {
                        failed = true;
                        Tracer::LogError(m_status, L"Recovery of memory store data failed due to I/O error!");
                    }
                    if (m_size < (m_store.m_allocatedSize - (m_store.m_trailingEdge - m_store.m_allocationEdge)))
                    {
                        failed = true;
                        Tracer::LogError(StatusCode::Unexpected, L"Recovery of memory store data failed: not enough data loaded!");
                    }

                    if (failed)
                    {
                        // give up on recoverying
                        m_store.m_allocationEdge = 0;
                        m_store.m_trailingEdge = m_store.m_allocatedSize;
                    }
                    else
                    {
                        // TODO!! tell the server the serial numbers for each partition that we recovered!!!
                        m_store.ReconstructIndex();
                    }
                }
            };
            Audit::Assert(pNotify != nullptr,
                "A completion is expected, most operation can not be performed before this is over!");

            if (file.GetFileSize() != m_allocatedSize)
            {
                // Do not perform loading if the memory store size changed

                // TODO!! maybe we can load it anyway if the file size is smaller
                // than the current memory store size
                pNotify->Post((intptr_t)StatusCode::IncorrectSize, 0);
                return;
            }

            // Note!!! this implementation is tightly coupled with method
            // StartDump, these two must be modified together!!

            m_allocationEdge = edges.first;
            m_trailingEdge = edges.second;

            auto allocEdge = m_allocationEdge % m_allocatedSize;
            auto eraseEdge = m_trailingEdge % m_allocatedSize;
            size_t available = m_trailingEdge - m_allocationEdge;

            if (available == m_allocatedSize)
            {
                // empty store, do not need to do anything.
                pNotify->Post((intptr_t)StatusCode::OK, 0);
                return;
            }

            uint64_t readStart = 0;
            uint64_t readEnd = m_allocatedSize;
            LoadingFinish* pFinish = pNotify->GetArena()->allocate<LoadingFinish>(pNotify->m_activity, *this, pNotify);

            if (available < 4 * SECTORSIZE || available < (m_allocatedSize >> 3))
            {
                // we simply read the entire file
            }
            else
            {
                readStart = eraseEdge & (0 - SECTORSIZE);
                readEnd = AlignPower2(SECTORSIZE, allocEdge);
                if (readEnd == 0)
                    readEnd = m_allocatedSize;
            }

            if (readStart >= readEnd)
            {
                // we need to split the load into two parts, 
                // start -> m_allocatedSize,  0 -> end
                pFinish->m_ops = 2;
                Audit::Assert(readStart < m_allocatedSize, "Alignment operation crossed boarder");
                file.Read(readStart, (uint32_t)(m_allocatedSize - readStart), m_pMemory->PData(readStart), pFinish);
                file.Read(0, (uint32_t)readEnd, m_pMemory->PData(0), pFinish);
            }
            else
            {
                // dump start -> end
                Audit::Assert(readEnd <= m_allocatedSize, "Alignment operation crossed boarder");
                file.Read(readStart, (uint32_t)(readEnd - readStart), m_pMemory->PData(readStart), pFinish);
            }
        }
        
        size_t Size() const override
        {
            return m_allocatedSize;
        }

        // test hooks
        void SetEdges(size_t allocEdge, size_t trailingEdge)
        {
            m_allocationEdge = allocEdge;
            m_trailingEdge = trailingEdge;
        }

        // end test hooks

    public:
        MemoryStore(
            _In_ ExabytesServer& server,

            /* the store size is the count of bytes of DRAM we will reserve for
            the MemoryStore.*/
            size_t storeSize
            )
            : KeyValueInMemory(server)
            , m_pScheduler(&server.GetScheduler())
        {
            // design invariants are checked here
            // alignment will be maintained in the memory buffer.  Blob parts begin with a header.
            Audit::Assert((m_allocatedSize % ALIGNMENT) == 0);
            Audit::Assert((m_allocatedSize % SECTORSIZE) == 0);

            unsigned actualCount;
            m_pMemory = make_unique<Utilities::LargePageBuffer>(storeSize, 1, m_allocatedSize, actualCount);
            Audit::Assert(1 == actualCount);

            m_pJanitor = ActivityFactory(*m_pScheduler, L"MemoryStore Janitor", true);
            m_pBackgroundSweeper = m_pJanitor->GetArena()->allocate<PersistActor>(*m_pJanitor, *this, &SweepFunction);

            // the allocation edge chases the trailing edge, modulo the allocated size
            m_allocationEdge = 0;

            // the trailing edge will be the lesser of the trailing partial or ready blob edges
            m_trailingEdge = m_allocatedSize;

            InitializeSRWLock(&m_srwMemoryLock);
        }

        ~MemoryStore()
        {
            if (m_pJanitor)
            {
                // this will reclaim both the janitor and the sweeper
                m_pJanitor->RequestShutdown(StatusCode::OK);
                m_pJanitor = nullptr;
            }

            m_pMemory->~LargePageBuffer();
            m_pMemory.reset();
        }

        // Start dumping the content of the memory store to a file. 
        // This should only be called upon shutdown to enable recovery
        // after restart.
        //
        // Return a pair of numbers <allocation edge, trailing edge>
        //
        std::pair<uint64_t, uint64_t> StartDump(AsyncFileIo::FileManager& file, ContinuationBase* pNotify) override
        {
            StartCleanup();

            // Note!!! this implementation is tightly coupled with method
            // StartToLoadFromFile, these two must be modified together!!

            uint64_t readStart = 0;
            uint64_t readEnd = m_allocatedSize;
            size_t available = m_trailingEdge - m_allocationEdge;

            auto allocEdge = m_allocationEdge % m_allocatedSize;
            auto eraseEdge = m_trailingEdge % m_allocatedSize;

            // good oppurtunity to shink down edge numbers
            auto ret = std::make_pair(allocEdge, allocEdge + available);

            if (available == m_allocatedSize)
            {
                // empty store, do not need to do anything.
                if (pNotify != nullptr)
                {
                    pNotify->Post((intptr_t)StatusCode::OK, 0);
                }
                return std::make_pair(0, m_allocatedSize);
            }

            IoFinish* pFinish = nullptr;
            if (pNotify != nullptr)
            {
                pFinish = pNotify->GetArena()->allocate<IoFinish>(pNotify->m_activity, pNotify);
            }

            if (available < 4 * SECTORSIZE || available < (m_allocatedSize >> 3))
            {
                // we simply dump the whole thing
            }
            else
            {
                readStart = eraseEdge & (0 - SECTORSIZE); 
                readEnd = AlignPower2(SECTORSIZE, allocEdge);
                if (readEnd == 0)
                    readEnd = m_allocatedSize;
            }

            if (readStart >= readEnd)
            {
                // we need to split the dump into two parts, 
                // start -> m_allocatedSize,  0 -> end
                if (pFinish != nullptr)
                {
                    pFinish->m_ops = 2;
                }
                Audit::Assert(readStart < m_allocatedSize, "Alignment operation crossed boarder");
                file.Write(readStart, (uint32_t)(m_allocatedSize - readStart), m_pMemory->PData(readStart), pFinish);
                file.Write(0, (uint32_t)readEnd, m_pMemory->PData(0), pFinish);
            }
            else
            {
                // dump start -> end
                Audit::Assert(readEnd <= m_allocatedSize, "Alignment operation crossed boarder");
                file.Write(readStart, (uint32_t)(readEnd - readStart), m_pMemory->PData(readStart), pFinish);
            }

            return ret;
        }

        void StartCleanup() override
        {
            m_shuttingdown = true;
        }

        void StartSweeping() override
        {
            GetServer()->ObtainEmptyBuffer(*m_pBackgroundSweeper);
        }


        // Copies a datagram into memory store.  Returns E_SUCCESS if the write was accepted.
        //  Returns an E_ error if there was a problem with validating the write.
        //
        StatusCode Write(
            // full description of the blob we are storing
            _In_ const Description& description,

            // the blob we are storing.  MBZ if and only if descriptor.valueLength == 0
            _In_reads_(dataLength) const void* pData,

            // must equal descriptor.valueLength.
            size_t dataLength
            ) override
        {
            auto success = StatusCode::Unexpected;
            if (Audit::Verify(description.ValueLength == dataLength))
            {
                Tracer::LogActionStart(Tracer::EBCounter::MemStoreWrite, L"");
                uint32_t latest = InternalWrite(description, pData, dataLength);
                if (latest == UNUSEDOFFSET)
                {
                    Tracer::LogActionEnd(Tracer::EBCounter::MemStoreWrite, StatusCode::OutOfMemory);
                    return StatusCode::OutOfMemory;
                }

                success = StatusCode::OK;
                Tracer::LogActionEnd(Tracer::EBCounter::MemStoreWrite, success);
            }
            return success;
        }

        // Synchronous.  Locate the data and returns a buffer with the data
        // via parameter blobEnvelop. Caller responsible for free the buffer
        //
        // Returns an E_ error if there was a problem with validating the Read.
        //
        //
        StatusCode Read(
            int,

            // Key of the blob we are about to retrieve.
            _In_ const PartialDescription& description,

            // Buffer that contains the data blob
            // caller is responsible to release memory 
            // by calling pBlobBuffer->Dispose();
            //
            _Out_ BufferEnvelope& blobEnvelop
            ) override
        {
            Tracer::LogActionStart(Tracer::EBCounter::MemStoreRead, L"");
            auto status = StatusCode::UnspecifiedError;

            auto pHeader = FindHeader(description.KeyHash);
            if (pHeader == nullptr)
            {
                status = StatusCode::NotFound;
                Tracer::LogActionEnd(Tracer::EBCounter::MemStoreRead, status);
                return status;
            }

            uint32_t dataLength = pHeader->m_description.ValueLength;
            if (dataLength == 0){
                status = StatusCode::Deleted;
                Tracer::LogActionEnd(Tracer::EBCounter::MemStoreRead, status);
                return status;
            }

            if (pHeader->m_description.Reader != 0
                && description.Caller != pHeader->m_description.Reader
                && description.Caller != pHeader->m_description.Owner)
            {
                status = StatusCode::AccessDenied;
                Tracer::LogActionEnd(Tracer::EBCounter::MemStoreRead, status);
                return status;
            }

            char* pSource = (char*)pHeader;
            uint32_t blobOffset = sizeof(PartHeader) + m_pMemory->OffsetOf(pHeader);
            pSource += sizeof(PartHeader);

            Utilities::DisposableBuffer* pDBuffer = m_pScheduler->Allocate(dataLength);
            void* pBuffer = pDBuffer->PData();
            blobEnvelop = pDBuffer;
            uint64_t upperHash = description.KeyHash.ValueHigh();
            size_t actual = min((size_t)dataLength, (m_allocatedSize - blobOffset));
            {
                Utilities::Share<SRWLOCK> guard{ m_srwMemoryLock };

                // proceed only if the data remains in place
                if (upperHash == pHeader->m_description.KeyHash.ValueHigh())
                {
                    status = StatusCode::OK;
                    if (0 < actual)
                    {
                        memcpy_s(pBuffer, dataLength, pSource, actual);
                    }
                    if (actual < dataLength)
                    {
                        // wrap around to start of buffer
                        memcpy_s((void*)(actual + (char*)pBuffer), dataLength - actual, m_pMemory->PBuffer(), dataLength - actual);
                    }
                }
                else
                {
                    // we may hit this when the data is sweeped to disk store. Still this is very
                    // unlikely, as the high key is zeroed AFTER the catalog is updated.
                    status = StatusCode::NotFound;
                }
            } // release lock
            if (status == StatusCode::NotFound)
            {
                Tracer::LogWarning(StatusCode::NotFound, L"Memory store reader lost race to sweeper.");
                blobEnvelop.Recycle();
            }

            Tracer::LogActionEnd(Tracer::EBCounter::MemStoreRead, status);
            return status;
        }

        // There are cases we need to inspect an item's header for expiration
        // time, access rights, etc.
        //
        StatusCode GetDescription(
            const Key128& keyHash,

            // header will be returned in this outgoing parameter
            _Out_ Datagram::Description& description
            ) override
        {
            Tracer::LogActionStart(Tracer::EBCounter::MemStoreReadHeader, L"");

            auto pHeader = FindHeader(keyHash);
            if (pHeader == nullptr)
            {
                Tracer::LogActionEnd(Tracer::EBCounter::MemStoreReadHeader, StatusCode::NotFound);
                return StatusCode::NotFound;
            }

            // after all these validation, we set the data
            description = pHeader->m_description;
            if (description.KeyHash.IsInvalid()){
                Tracer::LogActionEnd(Tracer::EBCounter::MemStoreReadHeader, StatusCode::NotFound);
                return StatusCode::NotFound;
            }

            Tracer::LogActionEnd(Tracer::EBCounter::MemStoreReadHeader, StatusCode::OK);
            return StatusCode::OK;
        }        

        size_t GetCheckpointSize() const override
        {
            return 2 * sizeof(size_t);
        }

        StatusCode RetriveCheckpointData(_Out_ void* buffer) const override
        {
            size_t* ptr = reinterpret_cast<size_t*>(buffer);
            *ptr = m_allocationEdge;
            ptr++;
            *ptr = m_trailingEdge;
            return StatusCode::OK;
        }

    };


    /* in DRAM */
    std::unique_ptr<KeyValueInMemory> MemoryStoreFactory(
        _In_ ExabytesServer& server,

        /* the store size is the count of bytes of DRAM we will reserve for
        the MemoryStore.*/
        size_t storeSize
        )
    {
        return make_unique<MemoryStore>(server, storeSize);
    }

    // Construct a memory store, an load the content from a file
    //
    std::unique_ptr<KeyValueInMemory> MemoryStoreFactory(
        _In_ ExabytesServer& server,

        // the store size is the count of bytes of DRAM we will reserve for
        // the MemoryStore.
        size_t storeSize,

        // file from which store content should be loaded
        AsyncFileIo::FileManager& file,

        // <allocation edge, trailing edge>
        std::pair<uint64_t, uint64_t> edges,

        // Invoked when the load is finished. Caller should take care
        // that the store should only be used after all these finished.
        ContinuationBase* pNotify
        )
    {
        auto ret = make_unique<MemoryStore>(server, storeSize);
        ret->StartToLoadFromFile(file, edges, pNotify);
        return std::move(ret);
    }
}

namespace TestHooks
{

    size_t LocalStoreTestHooks::SizeOfMemoryStore(KeyValueInMemory* pstore)
    {
        return dynamic_cast<MemoryStore*>(pstore)->Size();
    }

    void LocalStoreTestHooks::SetEdgesOfMemoryStore(KeyValueInMemory* pstore, size_t allocEdge, size_t trailingEdge)
    {
        dynamic_cast<MemoryStore*>(pstore)->SetEdges(allocEdge, trailingEdge);
    }

}
