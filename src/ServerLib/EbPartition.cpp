// Exabytes Micro Partition
//

#pragma once
#include "stdafx.h"

#include "Exabytes.hpp"
#include "Replicator.hpp"
#include "UtilitiesWin.hpp"

#include <queue>
#include <random>
#include <memory>

namespace
{
    static const unsigned BUCKETBITS = 4;
    static const unsigned BUCKETCOUNT = 1u << BUCKETBITS;
    inline unsigned MapToBucket(const Datagram::Key128& key, uint64_t random)
    {
        uint64_t k = key.ValueHigh() ^ key.ValueLow() ^ random;
        return (BUCKETCOUNT - 1) & (unsigned)
            ((k >> (BUCKETBITS * 12)) ^ (k >> (BUCKETBITS * 10)) ^ (k >> (BUCKETBITS * 8))
            ^ (k >> (BUCKETBITS * 6)) ^ (k >> (BUCKETBITS * 4)) ^ (k >> (BUCKETBITS * 2))
            ^ k);
    }

    static uint64_t GenerateRandom()
    {
        std::random_device seed;
        std::uniform_int_distribution<uint64_t> r;
        return r(seed);
    }
}


namespace Exabytes
{
    using namespace Datagram;
    using namespace Schedulers;
    using namespace Utilities;

    uint64_t Partition::AllocSerial()
    {
        return (uint64_t)InterlockedIncrement64((int64_t*)&m_serial);
    }

    class EbMicroPartition : public Partition
    {
    private:

        // Processing read, we need to query the Catalog, and then
        // read from store to verify we have the right item
        // retry for false collision in Catalog or race condition in store
        // NOTE: code clone with WriteProcessor in MemoryStore class
        //
        class FileStoreReader : public Continuation<BufferEnvelope>
        {
        private:
            AddressAndSize m_addr;

            int m_retry = 6; // max retry
            enum State
            {
                ExpectAddress, ExpectData
            } m_state;

        public:
            // field shared with other continuations in the same
            // read activity, make sure they can not be modified
            //
            EbMicroPartition* const m_pPartition;
            const PartialDescription m_desc;
            Continuation<BufferEnvelope>* m_pNotify;
            const int m_timeout;
            const bool m_headerOnly;

            FileStoreReader(Schedulers::Activity& activity,
                _In_ EbMicroPartition& partition,
                int timeoutMs,
                PartialDescription desc,
                bool headerOnly,
                Continuation<BufferEnvelope>* pNotify
                )
                : Continuation(activity)
                , m_pPartition(&partition)
                , m_pNotify(pNotify)
                , m_desc(desc)
                , m_timeout(timeoutMs)
                , m_headerOnly(headerOnly)
            {
                m_state = ExpectAddress;
            }

            void OnReady(
                _In_ WorkerThread&,
                _In_ intptr_t       continuationHandle,
                _In_ uint32_t       messageLength
                ) override
            {
                switch (m_state)
                {
                case ExpectAddress:
                {
                    // completion called by Catlog->TryGetLocation
                    Audit::Assert(messageLength == 0,
                        "TryGetLocation should not return message");
                    m_addr = AddressAndSize::Reinterpret(
                                  (uint64_t)continuationHandle);

                    if (m_addr.FullAddress() == AddressAndSize::INVALIDADDRESS)
                    {
                        // tell client item not found
                        Tracer::LogActionEnd(Tracer::EBCounter::PartitionRead, StatusCode::NotFound);
                        m_pNotify->Post((intptr_t)StatusCode::NotFound, 0);
                        m_pNotify = nullptr;
                        return;
                    }
                    Audit::Assert(m_addr.Size() > 0, "Can not read empty.");

                    // we have an address, now query file store
                    KeyValueOnFile* pStore = m_pPartition->Persister();
                    m_state = ExpectData;
                    if (m_headerOnly){
                        pStore->GetDescription(m_desc.KeyHash, m_addr, this);
                    }
                    else {
                        pStore->Read(m_timeout, m_desc, m_addr, this);
                    }
                    break;
                }
                case ExpectData:
                    // completion called by store->Read
                    if (messageLength > 0)
                    {
                        // read successful
                        Audit::Assert((void*)continuationHandle == (void*)&m_postable,
                            "invalid handle returned from k-v store read");
                        // client should call "Collect" that copies over the data
                        // the buffer will be reclaimed there. 
                        Tracer::LogActionEnd(Tracer::EBCounter::PartitionRead, StatusCode::OK);
                        m_pNotify->Receive(std::move(m_postable), messageLength);
                        m_pNotify = nullptr;
                    }
                    else {
                        // read failure, retry
                        auto success = (StatusCode)continuationHandle;
                        if (success == StatusCode::NotFound){
                            Audit::Assert(--m_retry > 0, "Too many retry when reading from store.");
                            m_state = ExpectAddress;
                            m_pPartition->m_pCatalog->Locate(m_desc.KeyHash, m_addr, this);
                        } 
                        else if (success == StatusCode::OK)
                        {
                            // an empty item is a "delete" record
                            Tracer::LogActionEnd(Tracer::EBCounter::PartitionRead, success);
                            m_pNotify->Post((intptr_t)StatusCode::Deleted, 0);
                            m_pNotify = nullptr;
                        }
                        else {
                            Tracer::LogActionEnd(Tracer::EBCounter::PartitionRead, success);
                            Tracer::LogError(success, L"unexpected error during partition read ");
                            m_pNotify->Post((intptr_t)success, 0);
                            m_pNotify = nullptr;
                        }
                    }
                    break;
                default:
                    Audit::OutOfLine::Fail("Unexpected state at Partiton read processor");
                }
            }

            void Cleanup() override
            {
                m_postable.Recycle();
                if (m_pNotify != nullptr){
                    m_pNotify->Cleanup();
                    m_pNotify = nullptr;
                }
            }
        };

        // The continuation that kick start a partition read, taking
        // a file store reader as parameter. When triggered, it reads
        // the memory store, and then the file store if the item is
        // not found in the memory store.
        //
        class ReadStarter : public ContinuationBase{
            FileStoreReader* const m_pFileStoreReader;

        public:
            ReadStarter(FileStoreReader* pReader)
                : ContinuationBase(pReader->m_activity)
                , m_pFileStoreReader(pReader)
            {}

            void OnReady(
                _In_ WorkerThread&,
                _In_ intptr_t       continuationHandle,
                _In_ uint32_t
                ) override
            {
                Audit::Assert(continuationHandle == (intptr_t)m_pFileStoreReader,
                    "Unexpected posting of ReadStarter");

                const PartialDescription& desc = m_pFileStoreReader->m_desc;
                EbMicroPartition& part = *m_pFileStoreReader->m_pPartition;
                const int timeout = m_pFileStoreReader->m_timeout;
                Continuation<BufferEnvelope>* const pNotify = m_pFileStoreReader->m_pNotify;

                Tracer::LogActionStart(Tracer::EBCounter::PartitionRead,
                    part.GetPartitionIdStr() );
                BufferEnvelope result;

                // check whether the value item is in DRAM
                auto pMemoryStore = part.GetServer()->GetMemoryStore();
                auto status = StatusCode::UnspecifiedError;

                if (m_pFileStoreReader->m_headerOnly){
                    DisposableBuffer* pBuffer =
                        m_activity.GetScheduler()->Allocate((uint32_t)sizeof(Description));
                    Description* pHeader = reinterpret_cast<Description*>(pBuffer->PData());

                    status = pMemoryStore->GetDescription(desc.KeyHash, *pHeader);
                    result = pBuffer;
                }
                else{
                    status = pMemoryStore->Read(timeout, desc, result);
                }

                if (status == StatusCode::OK)
                {
                    // Data found from memory store, now we can skip m_pReader who reads from file store
                    Tracer::LogActionEnd(Tracer::EBCounter::PartitionRead, status);
                    pNotify->Receive(std::move(result), result.Contents()->Size());
                    return;
                }
                if (status != StatusCode::NotFound)
                {
                    // Error, skip m_pReader and notify client instead
                    Tracer::LogActionEnd(Tracer::EBCounter::PartitionRead, status);
                    pNotify->Post((intptr_t)status, 0);
                    return;
                }

                // could not find it from memory store, 
                // now try file store
                AddressAndSize prior; // default value is invalid

                // read processor will be dispatched by the catalog completion
                part.m_pCatalog->Locate(desc.KeyHash, prior, m_pFileStoreReader);
                return;
            }

            void Cleanup()
            {
                m_pFileStoreReader->Cleanup();
            }
        };

        class WriteProcessor : public ContinuationWithContext<ReplicationReqHeader, ContextIndex::BaseContext>
        {
        private:
            EbMicroPartition* const m_pPart;
            ContinuationBase* m_pNext;

        public:
            WriteProcessor(
                _In_ Activity& activity,
                _In_ EbMicroPartition& part,
                ContinuationBase* pNotify
                )
                : ContinuationWithContext(activity)
                , m_pPart(&part)
                , m_pNext(pNotify)
            { }

            void Cleanup() override
            {
                m_postable.Recycle();
                if (m_pNext != nullptr){
                    Audit::NotImplemented("Aborting update operation may result in in-consistency!");
                    // We have a big problem here. We don't know which stage
                    // the update operation is in.
                    // if it's in replication queue, we need to remove it
                    // from que and abort the replication.
                    // if its finished replication, we need to tell all other
                    // replica to abort.
                    // 

                    ReplicationReqHeader* pReq = GetContext();
                    if (m_pPart != nullptr && pReq != nullptr)
                    {
                        m_pPart->AbortUpdate(pReq->Description.KeyHash, &this->m_activity);
                    }
                    m_pNext->Cleanup();
                    m_pNext = nullptr;
                }
            }

            void OnReady(
                _In_ WorkerThread&,
                _In_ intptr_t       continuationHandle,
                _In_ uint32_t
                ) override
            {
                ReplicationReqHeader* pReq = GetContext();
                Audit::VerifyArgRule(continuationHandle == 0,
                    "invalid parameter for write processor");
                Tracer::LogDebug(StatusCode::OK, L"start store write");

                // Check whether we have enough space in this partition to consume this record
                StatusCode success = StatusCode::UnspecifiedError;
                auto fileStoreAvailableSpce = m_pPart->m_pFileStore->GetAvailableBytes();                                
                auto catalogAvailableSpace = m_pPart->GetCatalog()->GetAvailableSpace();
                               
                if (fileStoreAvailableSpce > m_pPart->m_fileStoreEstimatedWrites && catalogAvailableSpace > m_pPart->m_catalogEstimatedWrites)
                {
                    // Current partition has capacity for this record.
                    // Lets start to add this to memstore. If memstore does not have capacity then 
                    // it will return OUTOFMEMORY
                    KeyValueInMemory* pStore = m_pPart->GetServer()->GetMemoryStore();
                    success = pStore->Write(
                        pReq->Description,
                        (char*)pReq + sizeof(ReplicationReqHeader),
                        pReq->Description.ValueLength
                    );
                }
                else
                {
                    success = StatusCode::OutOfMemory;
                }

                if (m_pNext != nullptr)
                {
                    m_pNext->Post((intptr_t)success, 0);
                    m_pNext = nullptr;
                }
                Tracer::LogActionEnd(Tracer::EBCounter::PartitionWrite, success);
                m_pPart->FinishedUpdate(pReq->Description.KeyHash, &this->m_activity);
            }
        };

        // A continuation triggered when the delete job finished
        // reading the key.
        //
        // In this continuation, if a record with the same key
        // is found, we write a tomb stone to the store to indicate
        // the deletion
        //
        class DeleteProcess : public Continuation<BufferEnvelope>
        {
            EbMicroPartition* const  m_pPart;
            ContinuationBase* m_pNext;
        public:
            DeleteProcess(
                _In_ Activity& activity,
                _In_ EbMicroPartition& partition,
                _In_ ContinuationBase* pNext
                )
                : Continuation(activity)
                , m_pNext(pNext)
                , m_pPart(&partition)
            {}

            void OnReady(
                _In_ WorkerThread&,
                _In_ intptr_t       continuationHandle,
                _In_ uint32_t
                ) override
            {
                auto status = StatusCode::UnspecifiedError;
                Description* pHeader = nullptr;

                if ((void*)continuationHandle == (void*)&m_postable){
                    DisposableBuffer* pBuffer = m_postable.Contents();
                    pHeader = reinterpret_cast<Description*>
                        ((char*)pBuffer->PData() + pBuffer->Offset());

                    status = StatusCode::OK;
                }
                else
                {
                    status = (StatusCode)continuationHandle;
                    // error during reading, or did not find it

                    m_pNext->Post((intptr_t)status, 0);
                    return;
                }

                Activity& updateActivity = m_pNext->m_activity;
                ReplicationReqHeader* pReq = reinterpret_cast<ReplicationReqHeader*>
                    (updateActivity.GetContext(ContextIndex::BaseContext));
                Audit::Assert(pReq->Description.KeyHash == pHeader->KeyHash,
                    "Mismatch key found during delete!");

                if (pHeader->Owner != pReq->Description.Owner)
                {
                    status = StatusCode::AccessDenied;
                }

                if (pHeader->Part != 0)
                {
                    // deletion of multi-part before mature is not allowed
                    status = StatusCode::InvalidState;
                }

                if (pHeader->ValueLength == 0)
                {
                    status = StatusCode::NotFound; // double delete
                }

                if (status != StatusCode::OK)
                {
                    // report error back to caller
                    m_pNext->Post((intptr_t)status, 0);
                }
                else
                {
                    // Construct a stub header to mark the deletion.
                    pReq->Description.Part = 0;
                    pReq->Description.Reader = pHeader->Reader;

                    // write an stub header rec in.
                    m_pPart->InternalWrite(m_pNext);
                }
                m_postable.Recycle();
            }

            void Cleanup() override
            {
                if (m_pNext != nullptr){
                    m_pNext->Cleanup();
                    m_pNext = nullptr;
                }
            }
        };


        // Two updates to the same key can not happen concurrently. Data structure
        // for ensuring this has the side effect of limiting parallelism in a partition
        // 
        const uint64_t m_random;
        std::queue<ContinuationBase*> m_ongoingUpdates[BUCKETCOUNT];
        SRWLOCK m_srwUpdateLock[BUCKETCOUNT];

        size_t m_fileStoreEstimatedWrites = 0;
        size_t m_catalogEstimatedWrites = 0;

        void FinishedUpdate(const Key128& key, Activity const * pUpdate)
        {
            unsigned bucket = MapToBucket(key, m_random);
            ContinuationBase* pNext = nullptr;
            std::queue<ContinuationBase*>& que = m_ongoingUpdates[bucket];
            {
                Utilities::Exclude<SRWLOCK> guard{ m_srwUpdateLock[bucket] };

                // remove current one
                Audit::Assert(!que.empty(), "Ongoing update activity lost!");
                Activity* pFront = &(que.front()->m_activity);
                Audit::Assert(pUpdate == pFront, "mismatching update activity");
                que.pop();

                // trigger next one
                if (!que.empty())
                {
                    pNext = que.front();
                }
            }

            if (pNext != nullptr)
            {
                pNext->Post(0, 0);
            }
        }

        // This is called when an update (delete or write) takes too long and
        // is forced to abort. it may in the update que, need to remove it
        //
        void AbortUpdate(const Key128& key, Activity const * pUpdate)
        {
            ContinuationBase* pNext = nullptr;
            unsigned bucket = MapToBucket(key, m_random);
            std::queue<ContinuationBase*>& que = m_ongoingUpdates[bucket];
            {
                Utilities::Exclude<SRWLOCK> guard{ m_srwUpdateLock[bucket] };

                if (!que.empty()
                    && pUpdate == &(que.front()->m_activity))
                {
                    // happens to be the first on the queue
                    // we need to trigger the next if any
                    que.pop();
                    if (!que.empty())
                    {
                        pNext = que.front();
                    }
                }
                else
                {
                    // going through all items in the queue to remove one
                    // item, really low performance, but this is done only when
                    // something is wrong, and the queue should be very short
                    // (1 to 2 items)
                    auto queue_size = que.size();
                    for (int i = 0; i < queue_size; i++)
                    {
                        ContinuationBase* item = que.front();
                        que.pop();
                        if (&item->m_activity != pUpdate)
                        {
                            que.push(item);
                        }
                    }
                }
            }

            if (pNext != nullptr)
            {
                pNext->Post(0, 0);
            }
        }

        // Process up a Write request, assuming the write request is already
        // in the basic context of the activity.
        // Returns E_SUCCESS if the write was accepted.
        // Returns an E_ error if there was a problem with validating the write.
        // Return is async and does not guarantee the data was sent.  If you need
        // confirmations, design end-to-end confirmation of your service.
        //
        StatusCode InternalWrite(
            _In_ ContinuationBase* pContinuation
            ) 
        {
            Activity& writeActivity = pContinuation->m_activity;
            Tracer::LogActionStart(Tracer::EBCounter::PartitionWrite,
                GetPartitionIdStr(), writeActivity.GetTracer());

            ReplicationReqHeader* pReq = reinterpret_cast<ReplicationReqHeader*>
                (writeActivity.GetContext(ContextIndex::BaseContext));

            ActionArena* pWriteArena = writeActivity.GetArena();
            WriteProcessor* pWriter = pWriteArena->allocate<WriteProcessor>
                (writeActivity, *this, pContinuation);

            // updates to the same key can not happen at the same time
            // 
            unsigned bucket = MapToBucket(pReq->Description.KeyHash, m_random);
            std::queue<ContinuationBase*>& que = m_ongoingUpdates[bucket];
            bool empty_que = false;
            {
                Utilities::Exclude<SRWLOCK> guard{ m_srwUpdateLock[bucket] };

                empty_que = que.empty();
                que.push(pWriter);
            }

            // what if "AbortUpdate" comes in and removed the only item we added?
            // then pTrigger must be the one that called "AbortUpdate"
            // then we have a bigger problem: the data is replicated but now
            // we are aborting? inconsistency!!!

            if (empty_que)
            {
                pWriter->Post(0, 0);
            }

            return StatusCode::OK;
        }

        // Assuming the activity context contains a Description object
        // which describe the item to be deleted.
        //
        StatusCode InternalDelete(
            _In_ ContinuationBase* pContinuation
            )
        {
            Activity& job = pContinuation->m_activity;
            ReplicationReqHeader* pReq = reinterpret_cast<ReplicationReqHeader*>
                (job.GetContext(ContextIndex::BaseContext));
            auto pArena = pContinuation->GetArena();
            PartialDescription desc;
            desc.KeyHash = pReq->Description.KeyHash;
            desc.Caller = pReq->Description.Owner;

            // First read the partition to find the record with the same key
            // call DeleteProcess to write a tomb stone if read is successful
            // ReadStarter -> read memory store;
            // FileStoreReader -> read file store if key not in memory store
            // DeleteProcess -> write tomb stone.
            //
            DeleteProcess* pDeleteProc = pArena->allocate<DeleteProcess>(
                job,
                *this,
                pContinuation
                );

            FileStoreReader* pProc = pArena->allocate<FileStoreReader>(
                job,
                *this,
                100,
                desc,
                true,
                pDeleteProc
                );
            ReadStarter* pStart = pArena->allocate<ReadStarter>(pProc);
            pStart->Post((intptr_t)pProc, 0);
            return StatusCode::OK;
        }

    protected:
        ExabytesServer* const m_pServer;
        std::unique_ptr<Catalog> m_pCatalog;
        std::unique_ptr<KeyValueOnFile> m_pFileStore;
        bool m_shuttingDown = false;

        // Commit the update that has been successfully replicated
        // pContinuation is the handle to the update activity, where
        // the Description is in the base context.
        //
        StatusCode Commit(
            _In_ ContinuationBase* pContinuation
            )
        {
            if (m_shuttingDown)
            {
                pContinuation->Post((intptr_t)StatusCode::Abort, 0);
            }

            Activity& updateActivity = pContinuation->m_activity;
            ReplicationReqHeader* pReq = reinterpret_cast<ReplicationReqHeader*>
                (updateActivity.GetContext(ContextIndex::BaseContext));
            if (pReq->Description.ValueLength == 0) {
                // delete
                return InternalDelete(pContinuation);
            }
            else {
                // write
                return InternalWrite(pContinuation);
            }
        }

    public:
        EbMicroPartition(ExabytesServer& server,
            std::unique_ptr<Catalog>& pCatalog,
            std::unique_ptr<KeyValueOnFile>& pFileStore,
            PartitionId partitionID
            )
            : m_pServer(&server)
            , m_pCatalog(std::move(pCatalog))
            , m_pFileStore(std::move(pFileStore))
            , m_random(GenerateRandom())
            , Partition(partitionID)
        {
            for (int i = 0; i < BUCKETCOUNT; i++)
            {
                InitializeSRWLock(&(m_srwUpdateLock[i]));
            }

            // take care of circular reference: partition <-> (store / catalog)
            m_pFileStore->SetPartition(this);
            m_pCatalog->SetPartition(this);

            // Given below is an offline estimate of amount of data yet to be added to FileStore
            // and Catalog (For Throttling Purpose)
            // During write/delete operations we use this estimation to figure out whether we 
            // accept the incoming request or reject it. More accurate way would be to get 
            // AssignedBytes in MemStore and calculate the values real time when the request arrive

            // MemStore is shared by all partitions. Assuming all the data is equally distributed 
            // among all partitions the amount of data directed towards each partitions can be 
            // estimated to be around memStoreSize / 25. More accurate way would be to get actual
            // number of partitions and data held by MemStore in real time
            m_fileStoreEstimatedWrites = m_pServer->GetMemoryStore()->Size()/25;

            // assuming the data Size is 1K (which is in the ball park for object store)
            // number of entries yet to be added to catalog is following
            m_catalogEstimatedWrites = m_fileStoreEstimatedWrites / 1024;

        }

        virtual ~EbMicroPartition(){
            m_pCatalog.reset();
            m_pFileStore.reset();
        }

        Catalog* GetCatalog() const { return m_pCatalog.get(); }

        ExabytesServer* GetServer() const override { return m_pServer; }

        KeyValueOnFile* Persister() const
        {
            return m_pFileStore.get();
        }

        void RequestShutdown() override
        {
            m_shuttingDown = true;
            m_pFileStore->RequestShutdown();
            m_pCatalog->RequestShutdown();            
        }

        // Start an async read operation, if successful, pContinuation
        // will be triggered, from which Collect should be called to
        // collect the data
        //
        StatusCode Read(
            // zero means default.  Negative means infinite.
            int timeoutMillisecs,

            // full description of the blob we are about to retrieve.
            _In_ const PartialDescription& description,

            // context to be used for async completion.
            _In_ Continuation<BufferEnvelope>* pContinuation
            ) override
        {
            if (m_shuttingDown)
            {
                pContinuation->Post((intptr_t)StatusCode::Abort, 0);
            }

            Activity& readActor = pContinuation->m_activity;
            ActionArena* pArena = pContinuation->GetArena();
            FileStoreReader* pProc = pArena->allocate<FileStoreReader>(
                readActor,
                *this,
                timeoutMillisecs,
                description,
                false,
                pContinuation
                );
            ReadStarter* pStart = pArena->allocate<ReadStarter>(pProc);
            pStart->Post((intptr_t)pProc, 0);
            return StatusCode::OK;
        }

        // When a Continuation follows a Read, the data is ready for synchronous
        //  collection.  The handle must be that passed to the continuation.
        // Returns S_OK if the continuation is valid.
        // Returns an E_ error if there was a problem, for example if the answer has
        //   expired because the service timed out.
        // The handle and its data can only be collected once.  If you collect less
        //  than the full length of the data, the remainder will be uncollectable.
        //
        StatusCode Collect(
            _In_ intptr_t continuationHandle,

            _In_ size_t length,

            _Out_writes_(length) void* pBuffer
            ) override
        {
            if (length == 0)
                return StatusCode::InvalidArgument;

            BufferEnvelope* envelope= reinterpret_cast<BufferEnvelope*>(continuationHandle);
            DisposableBuffer* pIoBuffer = envelope->Contents();
            Audit::Assert(length <= pIoBuffer->Size() - pIoBuffer->Offset(), "data length exceeds buffer boundary!");

            void* pSrc = (char*)pIoBuffer->PData() + pIoBuffer->Offset();
            
            memcpy_s(pBuffer, length, pSrc, length);

            envelope->Recycle();
            return StatusCode::OK;
        }

        // Queue up a Write request, Returns E_SUCCESS if the write was accepted.
        // Returns an E_ error if there was a problem with validating the write.
        // Return is async and does not guarantee the data was sent.  If you need
        // confirmations, design end-to-end confirmation of your service.
        //
        // data is copied to another buffer when request is queued up.
        //
        StatusCode Write(
            // full description of the blob we are storing
            _In_ const Description& description,

            // the blob we are storing.  MBZ if and only if descriptor.valueLength == 0
            _In_reads_(dataLength) const void* pData,

            // must equal descriptor.valueLength.
            size_t dataLength,

            // context to be used for async confirmation of durability.
            _In_ ContinuationBase* pContinuation
            ) override
        {
            if (description.ValueLength != dataLength)
                return StatusCode::InvalidArgument;
            if (dataLength > AddressAndSize::VALUE_SIZE_LIMIT)
                return StatusCode::OutOfBounds;
            if (dataLength > 0 && dataLength < AddressAndSize::ADDRESSALIGN)
                return StatusCode::IncorrectSize;

            if (m_shuttingDown)
            {
                pContinuation->Post((intptr_t)StatusCode::Abort, 0);
            }

            // we need to copy the data and compose a prepare request
            // for replication and commit to local store.
            // We put it on Arena if it's smaller than 56k (assuming
            // 64K Arena size, leaving 8K for Activities and continuations)
            // Larger prepare buffer are allocated from heap.
            Activity& writeActivity = pContinuation->m_activity;
            ActionArena* pWriteArena = writeActivity.GetArena();

            uint32_t reqSize = (uint32_t)(dataLength + 
                sizeof(ReplicationReqHeader));
            ReplicationReqHeader* pReq = nullptr;
            if (reqSize <= 56 * SI::Ki)
            {
                // allocated on arena, auto collected when activity dies
                pReq = reinterpret_cast<ReplicationReqHeader*>
                    (pWriteArena->allocateBytes(reqSize));
            }
            else
            {
                DisposableBuffer* buf = HeapBuffer::Allocate(reqSize);
                // make sure the buffer is disposed when activity shutdown.
                writeActivity.RegisterDisposable(buf);
                pReq = reinterpret_cast<ReplicationReqHeader*>
                    ((char*)buf->PData() + buf->Offset());
            }

            writeActivity.SetContext(ContextIndex::BaseContext, pReq);
            pReq->Decree = 0;
            pReq->Description = description;
            pReq->Description.Timestamp = m_pServer->GetScheduler().GetCurTimeStamp();
            pReq->Description.Serial = AllocSerial();

            void* pBlob = (char*)pReq + sizeof(ReplicationReqHeader);
            memcpy_s(pBlob, dataLength, pData, dataLength);

            return InternalWrite(pContinuation);
        }



        // Queue up a delete request, return E_SUCCESS if the request was accepted
        // First issue a read (header only), write a tomb stone rec only if
        // read is successful.
        // Return is async 
        StatusCode Delete(
            // partial description of the blob we are about to delete.
            _In_ const PartialDescription& description,

            // context to be used for async completion.
            _In_ ContinuationBase* pContinuation
            ) override
        {
            Activity& job = pContinuation->m_activity;
            ActionArena* pArena = job.GetArena();

            // copy request into a buffer and que job
            auto pReq = reinterpret_cast<ReplicationReqHeader*>
                (pArena->allocateBytes(sizeof(ReplicationReqHeader)));
            pReq->Description.KeyHash = description.KeyHash;
            pReq->Description.Owner = description.Caller;
            pReq->Description.ValueLength = 0;
            pReq->Description.Timestamp = m_pServer->GetScheduler().GetCurTimeStamp();
            pReq->Description.Serial = AllocSerial();

            job.SetContext(ContextIndex::BaseContext, pReq);

            return InternalDelete(pContinuation);
        }

        size_t GetCheckpointSize() const override
        {
            return m_pFileStore->GetCheckpointSize();
        }

        StatusCode RetriveCheckpointData(_Out_ void* buffer) const override
        {
            return m_pFileStore->RetriveCheckpointData(buffer);
        }
    };

    class IsolatedPartition : public EbMicroPartition, public NoneReplicator {
    public:
        IsolatedPartition(ExabytesServer& server,
            std::unique_ptr<Catalog>& pCatalog,
            std::unique_ptr<KeyValueOnFile>& pFileStore,
            PartitionId partitionID
            )
            : EbMicroPartition(server, pCatalog, pFileStore, partitionID)
        {}
            
        StatusCode CommitUpdate(
            _In_ Schedulers::ContinuationBase* pWriter) override
        {
            return Commit(pWriter);
        }

        Replicator& GetReplicator() override
        {
            return *this;
        }
    };

    class PartitionReplica : public EbMicroPartition, public VPaxosReplica {
    public:
        PartitionReplica(ExabytesServer& server,
            std::unique_ptr<Catalog>& pCatalog,
            std::unique_ptr<KeyValueOnFile>& pFileStore,
            PartitionId partitionID
            )
            : EbMicroPartition(server, pCatalog, pFileStore, partitionID)
        {}

        StatusCode CommitUpdate(
            _In_ Schedulers::ContinuationBase* pWriter) override
        {
            return Commit(pWriter);
        }

        Replicator& GetReplicator() override
        {
            return *this;
        }

    };

    std::unique_ptr<Partition> IsolatedPartitionFactory(
        _In_ ExabytesServer& server, 
        _In_ std::unique_ptr<Catalog>& pCatalog, 
        _In_ std::unique_ptr<KeyValueOnFile>& pFileStore,
        PartitionId id
        )
    {
        return std::make_unique<IsolatedPartition>(server, pCatalog, pFileStore, id);
    }

    std::unique_ptr<Partition> PartitionReplicaFactory(
        _In_ ExabytesServer& server,
        _In_ std::unique_ptr<Catalog>& pCatalog,
        _In_ std::unique_ptr<KeyValueOnFile>& pFileStore,
        PartitionId id
        )
    {
        return std::make_unique<PartitionReplica>(server, pCatalog, pFileStore, id);
    }

}
