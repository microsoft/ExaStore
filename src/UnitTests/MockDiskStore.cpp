#pragma once

#include "stdafx.h"

#include <vector>
#include <queue>
#include <algorithm>

#include "Exabytes.hpp"
#include "Utilities.hpp"
#include "TestUtils.hpp"

namespace
{
    using namespace Datagram;
    using namespace Schedulers;
    using namespace Exabytes;
    using namespace Utilities;

    // A barrier implementation. It is a contination that performces some action
    // after it is triggered a number of times. An example can be that the GC
    // issues 100 different Catalog->Relocate after a buffer flush, and want to
    // update the trailing edge only when all these async Relocate are finished.
    // Here a CounterDownActor can be passed to each of those Relocate and the
    // action can be updating the trailing edge.
    // 
    template<class T>
    class CountDownActor : public ContinuationBase
    {
    private:
        T* m_payload;
        uint32_t m_counter;
    public:
        CountDownActor(
            Activity& activity,
            T*       payload,
            unsigned initial
            )
            : ContinuationBase(activity)
            , m_payload(payload)
            , m_counter(initial)
        {}

        void OnReady(
            _In_ WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            ) override
        {
            m_counter--;
            if (m_counter <= 0)
            {
                if (m_payload != nullptr)
                {
                    m_payload->Action();
                    m_payload = nullptr;
                }
            }
        }

        void Cleanup() override
        {
            if (m_payload != nullptr)
            {
                m_payload->Action();
                m_payload = nullptr;
            }
        }
    };

    class TriggerOrReleaseBuffer
    {
    public:
        Continuation<HomingEnvelope<CoalescingBuffer>>* m_pNext;
        std::unique_ptr<CoalescingBuffer> m_pBuffer;
        ActionArena* m_pArena;

        TriggerOrReleaseBuffer(
            Continuation<HomingEnvelope<CoalescingBuffer>>* pContinuation,
            std::unique_ptr<CoalescingBuffer>& pBuffer,
            ActionArena* pArena
            )
            : m_pNext(pContinuation)
            , m_pBuffer(std::move(pBuffer))
            , m_pArena(pArena)
        {}

        void Action()
        {
            if (m_pNext != nullptr)
            {
                m_pNext->Receive(std::move(m_pBuffer), sizeof(HomingEnvelope<CoalescingBuffer>));
                m_pNext = nullptr;
            }
            else
            {
                m_pBuffer->GetPool()->Release(m_pBuffer);
            }
            if (m_pArena != nullptr){
                m_pArena->Retire();
                m_pArena = nullptr;
            }
        }
    };


}

namespace EBTest
{
    using namespace Exabytes;

    class DummyHandle : public Utilities::DisposableBuffer
    {
    private:
        void* const m_pBlob;
        uint32_t const m_size;
    public:
        DummyHandle(void* pBlob, uint32_t size)
            : m_pBlob(pBlob)
            , m_size(size)
        {}

        void* PData() const override { return m_pBlob; }
        uint32_t Size() const override { return m_size; }
        uint32_t Offset() const override { return  0; }
        void SetOffset(size_t) override {}
        void Dispose() override { delete this; }
    };

    class MockDiskStore;

    class MockStoreBuffer : public CoalescingBuffer
    {
    private:
        size_t m_dataSize = 0;
        size_t m_capacity;
        CoalescingBufferPool& m_pool;
    public:
        std::vector<Description*> m_items;

        MockStoreBuffer(size_t capacity, CoalescingBufferPool& pool)
            : m_capacity(capacity), m_pool(pool)
        {}

        CoalescingBufferPool* GetPool() const override
        {
            return &m_pool;
        }

        void ResetBuffer() override
        {
            m_dataSize = 0;
            m_items.clear();
            CoalescingBuffer::ResetBuffer();
        }

        StatusCode Append(
            // full description of the blob we are storing
            _In_ const Description& description,

            // the blob we are sending.  MBZ if and only if description.valueLength == 0
            _In_reads_(partsCount) void* const* pDataParts,

            // the blob we are sending.  MBZ if and only if description.valueLength == 0
            _In_reads_(partsCount) const uint32_t* pDataSizes,

            // must equal array counts for data parts and sizes.  Required for SAL
            size_t partsCount,

            // record the old location of the item
            _In_ const AddressAndSize& addr
            ) override
        {
            if (partsCount <= 0)
            {
                return StatusCode::InvalidArgument;
            }

            uint32_t totalSize = 0;
            for (auto pIter = pDataSizes; pIter < (pDataSizes + partsCount); ++pIter)
            {
                totalSize += *pIter;
            }

            if (m_dataSize + totalSize > m_capacity)
                return StatusCode::OutOfMemory;

            Description* pData = (Description*)malloc(sizeof(Description) + totalSize);
            *pData = description;
            pData->Part = 0; // multi-parts not allowed on disk;
            pData->ValueLength = totalSize;
            void* pBlob = (void*)(sizeof(Description)+(char*)pData);

            auto pSize = pDataSizes;
            auto ppPart = pDataParts;
            for (unsigned i = 0; i < partsCount; ++i)
            {
                auto size = *pSize++;
                auto pPart = *ppPart++;
                memcpy_s(pBlob, size, pPart, size);
                pBlob = (void*)(size + (char*)pBlob);
            }
            m_items.push_back(pData);
            m_dataSize += totalSize;
            m_oldLocations.push_back(
                std::pair<Key128&, AddressAndSize>(pData->KeyHash, addr)
                );

            return StatusCode::OK;
        }

    };

    class MockDiskStore : public KeyValueOnFile
    {
    private:
        static const uint32_t MAXITEMS = 4096;
        uint32_t m_nextslot = 0;
        Description* m_dataItems[MAXITEMS];
        Activity* m_pActivity;
        Scheduler* m_pScheduler;

        // we need to update catalog when we finished
        // flusing a buffer to file
        Catalog* m_pCatalog = nullptr;

        class BufferFlushTask : public ContinuationWithContext < MockDiskStore, ContextIndex::BaseContext >
        {
        private:
            std::unique_ptr<CoalescingBuffer>  m_pBuffer;
            Continuation<HomingEnvelope<CoalescingBuffer>>* m_pNext;
            ActionArena&                  m_arena;
        public:
            BufferFlushTask(
                // Activity for sequentialize all store maintainance operations
                Activity& activity,

                // Newly allocated arena just for this flushing operation
                ActionArena& arena,

                // Buffer to be flushed to disk
                std::unique_ptr<CoalescingBuffer>& pBuffer,

                // Call back provided by client
                Continuation<HomingEnvelope<CoalescingBuffer>>* pNext
                )
                : ContinuationWithContext(activity)
                , m_pBuffer(std::move(pBuffer))
                , m_pNext(pNext)
                , m_arena(arena)
            { }

            void Cleanup() override
            {}

            void OnReady(
                _In_ WorkerThread&  thread,
                _In_ intptr_t       continuationHandle,
                _In_ uint32_t       messageLength
                )
            {
                MockStoreBuffer* pStoreBuffer = static_cast<MockStoreBuffer*>(m_pBuffer.get());
                MockDiskStore* pStore = GetContext();
                // action should be performed after catalogs are all updated.
                TriggerOrReleaseBuffer* pAction =
                    m_arena.allocate<TriggerOrReleaseBuffer>(m_pNext, m_pBuffer, &m_arena);
                CountDownActor<TriggerOrReleaseBuffer>* pBarrier =
                    m_arena.allocate<CountDownActor<TriggerOrReleaseBuffer>>(m_activity,
                    pAction, (uint32_t)pStoreBuffer->m_items.size());

                CoalescingBuffer::Iterator oldLocationIter = pStoreBuffer->cbegin();
                for (std::vector<Description*>::iterator it = pStoreBuffer->m_items.begin(); it != pStoreBuffer->m_items.end(); ++it, oldLocationIter++) {
                    Audit::Assert(pStore->m_nextslot < MAXITEMS, "DISK STORE can not keep up");
                    pStore->m_dataItems[pStore->m_nextslot] = *it;
                    AddressAndSize location(pStore->m_nextslot << 4, (*it)->ValueLength + sizeof(Description));
                    if (pStoreBuffer->m_origin != CoalescingBuffer::Origin::File)
                    {
                        GetContext()->m_pCatalog->Add((*it)->KeyHash, location);
                        pBarrier->Post(0, 0);
                    }
                    else
                    {
                        Audit::Assert(oldLocationIter->second.FullAddress() != AddressAndSize::INVALIDADDRESS,
                            "invalid address in relocation");
                        GetContext()->m_pCatalog->Relocate((*it)->KeyHash,
                            oldLocationIter->second,
                            location, pBarrier);
                        // Relocate failure means the record is deleted,
                        // too bad we produced more garbage for future gc
                    }

                    pStore->m_nextslot++;
                }

            }
        };

        StatusCode InternalRead(
            // Key of the blob we are about to retrieve.
            _In_ const PartialDescription& description,

            // location of the data item  
            _In_ const AddressAndSize addrAndSize,

            bool headerOnly,

            // context to be used for async completion.
            _In_ Continuation<BufferEnvelope>* pContinuation
            )
        {
            StatusCode success = StatusCode::OK;
            uint32_t index = Reduction::CompressAddress(addrAndSize.FullAddress());
            uint32_t dataLength = 0;
            Description* pHeader = m_dataItems[index];
            void* pSrc = nullptr;

            if (m_nextslot <= index)
            {
                success = StatusCode::UnspecifiedError;
            }
            else if (pHeader->KeyHash != description.KeyHash)
            {
                success = StatusCode::NotFound;
            }
            else if (headerOnly)
            {
                success = StatusCode::OK;
                dataLength = sizeof(Description);
                pSrc = pHeader;
            }
            else if (pHeader->Reader != 0
                && description.Caller != pHeader->Reader
                && description.Caller != pHeader->Owner)
            {
                success = StatusCode::AccessDenied;
            }
            else
            {
                dataLength = pHeader->ValueLength;
                pSrc = (char*)pHeader + sizeof (Description);
                success = StatusCode::OK;
            }

            if (dataLength == 0)
            {
                // dataLength == 0 and success == S_OK, means we got an empty item
                pContinuation->Post((intptr_t)success, 0);
                return success;
            }

            Utilities::DisposableBuffer* pDBuffer = new DummyHandle(pSrc, dataLength);
            pContinuation->Receive(pDBuffer, dataLength);
            return StatusCode::OK;
        }


    public:

        MockDiskStore(
            // Turns are synchronous.  Asynchronous work should be posted using the scheduler.
            _In_ Scheduler& scheduler

            )
            : KeyValueOnFile(AddressSpace::SSD)
            , m_pScheduler(&scheduler)
        {
            m_pActivity = ActivityFactory(scheduler, L"MockDiskStore_IO");
            m_pActivity->SetContext(ContextIndex::BaseContext, this);
        }

        virtual ~MockDiskStore()
        {
            m_pActivity->RequestShutdown(StatusCode::OK);
            for (uint32_t i = 0; i < m_nextslot; i++)
            {
                free(m_dataItems[i]);
            }
        }

        void SetPartition(Partition* pPart) override
        {
            // do we really need to know which partition
            // we are in? currently we only need the catalog
            // for garbage collection, and promoting from HDD
            // to SSD.
            Audit::Assert(m_pCatalog == nullptr, "a file store can only belongs to one partition.");
            m_pCatalog = pPart->GetCatalog();
        }

        size_t GetAvailableBytes() const override
        {
            // This is being called during write operation to decide whether to accept the write operation.
            // correct value would be MAXITEMS - m_nextSlot but this is way less than the estimate which we calculate in EBPartition
            // Since this is a Mock Store and is being used for MemStore testing we just return a very high number here
            //
            return UINT_MAX;
        }

        // Add a full coalescing buffer to flush queue. An async job will write the
        // buffer to disk, then update the Catalog with the new address for all
        // the records. Last, pNotification will be posted, if not NULL
        //
        StatusCode Flush(
            // Buffer to be flushed to disk store, and later returned to pool.
            _In_ std::unique_ptr<CoalescingBuffer>& pBuffer,

            // callback for notification of flush finished.
            _In_ Continuation<HomingEnvelope<CoalescingBuffer>>* pNotification = nullptr
            ) override
        {
			ActionArena* pArena = m_pScheduler->GetNewArena();
            auto pTask = pArena->allocate<BufferFlushTask>(*m_pActivity, *pArena, pBuffer, pNotification);
            pTask->Post(0, 0);
            return StatusCode::OK;
        }

        // Synchronous.  Locate the data and return a description of it.
        // Returns an E_ error if there was a problem with validating the Read.
        //
        StatusCode Read(
            // zero means default.  Negative means infinite.
            int timeoutMillisecs,

            // Key of the blob we are about to retrieve.
            _In_ const PartialDescription& description,

            // location of the data item  
            _In_ const AddressAndSize addrAndSize,

            // context to be used for async completion.
            _In_ Continuation<BufferEnvelope>* pContinuation
            ) override
        {
            return InternalRead(description, addrAndSize, false, pContinuation);
        }

        StatusCode GetDescription(
            const Key128& keyHash,

            const AddressAndSize location,

            // async collection of Description,  When OnReady is called,
            // continuationHandle contains a pointer to Description object
            _In_ Continuation<BufferEnvelope>* pContinuation
            )
        {
            PartialDescription mock{ keyHash, 0 };
            return InternalRead(mock, location, true, pContinuation);
        }

        size_t GetCheckpointSize() const override { return 0; }

        StatusCode RetriveCheckpointData(_Out_ void* buffer) const override
        {
            Audit::NotImplemented("Not supported!");
            return StatusCode::Abort;
        }

        const std::wstring& GetFilePath() const override
        {
            static const std::wstring notFound = L"NotFound!";
            return notFound;
        }

        intptr_t GetFileHandle(void) const override
        {
            return (intptr_t) INVALID_HANDLE_VALUE;
        }

        void RequestShutdown() override
        {}
    };

    class DummyGcActor : public GcActor
    {
        Activity& m_activity;
    public:
        DummyGcActor(ExabytesServer& server,
            Activity& activity,
            PartitionRotor& stores
            )
            : GcActor(server)
            , m_activity(activity)
        {}

        // Prepare the context for fileStore, and start GC. pNext will
        // be posted after GC finishes, or immediately if GC for this
        // store is already underway.
        //
        // returns true when successfully started GC
        // returns false when GC is already underway.
        //
        bool StartGcForStore(KeyValueOnFile& fileStore)
        {
            return true;
        }

        // Set off the collector to observe the GcQue and start whenever
        // there is file store to collect
        void Start() override
        {}

        void RequestShutdown() override
        {
            m_activity.RequestShutdown(StatusCode::OK);
        }
    };

    std::unique_ptr<Exabytes::CoalescingBuffer> MockBufferFactory(
        size_t capacity, 
        _In_ Exabytes::CoalescingBufferPool& pool
        )
    {
        return std::make_unique<MockStoreBuffer>(capacity, pool);
    }

    std::unique_ptr<KeyValueOnFile> MockDiskStoreFactory(
        _In_ Scheduler& scheduler
        )
    {
        return std::make_unique<MockDiskStore>(scheduler);
    }

    GcActor* DummyGcFactory(
        ExabytesServer& server,
        Activity& gcDriver,
        PartitionRotor& stores
        )
    {
		auto pArena = gcDriver.GetScheduler()->GetNewArena();
		return pArena->allocate<DummyGcActor>(server, gcDriver, stores);
    }
}
