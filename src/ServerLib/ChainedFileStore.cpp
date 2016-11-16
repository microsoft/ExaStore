// ChainedFileStore : a write-sequential, read random journaled log which consists of
// multiple identical sized files forming a circular buffer
//
#include "stdafx.h"

#include "Exabytes.hpp"
#include "FileManager.hpp"
#include "UtilitiesWin.hpp"
#include "ChainedFileStore.hpp"
#include "EbPartition.hpp"

#include "TestHooks.hpp"

#include <vector>
#include <queue>
#include <algorithm>
#include <unordered_set>
#include <atomic>

using namespace std;

using namespace Exabytes;
using namespace Datagram;
using namespace Schedulers;
using namespace Utilities;

namespace    // private to this source file
{

    // Alignment applies to in-memory structs and the start of blobs
    const uint32_t ALIGNMENT = 16;

    // Size of a single file, 
    const int64_t PARTITIONSIZE = 8 * SI::Gi;
    
    // stop GC when the store has only 32 MB left
    const uint32_t GCCUSHION = 32 * SI::Mi;

    size_t COLLECTING_SIZE = 512 * SI::Mi;

    // set the following to 0.01 and 0.001 for testing
    // gc's correctness

    // if (data_size/file_size) is greater than this
    // we need GC immediately, 
    const double ALMOST_FULL = 0.75;

    // if (data_size/file_size) is smaller than this
    // we don't need GC at all.
    double ALMOST_EMPTY = 0.2;

    // if partition is half empty, the GC should check
    // how much the partition have grown in data size
    // to decide whether to collect


    struct BufferHeader
    {
        uint32_t m_blobCount;
        uint32_t m_totalSize;

        // biggest serial number in a journal record
        uint64_t m_serial;
        // 16 bytes
    };
    static_assert(ALIGNMENT == sizeof(BufferHeader),
        "buffer header must fit into 16 bytes");
    static_assert(sizeof(BufferHeader) <= SECTORSIZE,
        "Buffer header should never be split in two by file end");

    // these descriptions are appended to the end of the buffer as a means
    // to fast recovery of the journal.  We can skip through the SSD reading just
    // these and will be able to reinvent the key-location Map.
    //
    struct FlashDescription
    {
        // the unique identifier of the Node.
        Key128 KeyHash;

        // offset from start of the write buffer.
        uint32_t	m_offset;

        // size of the description + blob
        uint32_t	m_size;
    };

    // A barrier implementation. It is a contination that performces some other action
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
            uint32_t initial
            )
            : ContinuationBase(activity)
            , m_payload(payload)
            , m_counter(initial)
        {}

        void OnReady(_In_ WorkerThread&, _In_ intptr_t, _In_ uint32_t) override
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

    // An action, that either post the buffer to pContinuation, or if
    // pContinuation is null, release the buffer
    //
    class TriggerOrReleaseBuffer
    {
    public:
        Continuation<HomingEnvelope<CoalescingBuffer>>* m_pNext;
        unique_ptr<CoalescingBuffer> m_pBuffer;
        ActionArena* m_pArena;

        TriggerOrReleaseBuffer(
            Continuation<HomingEnvelope<CoalescingBuffer>>* pContinuation,
            unique_ptr<CoalescingBuffer>& pBuffer,
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
                auto pa = m_pArena;
                m_pArena = nullptr;
                pa->Retire();
                // we have removed the floor from below our feet. 
                // can not do anything here.
                // Need to make sure this ends the flush sequence:
                // flush start, flush end, end of updating catalog entries
            }
        }
    };

    // Garbage collector uses async read to fill a buffer, this handles completion of the read
    // combine the result of several chunks into one
    //
    class GcReadContinuation : public ContinuationBase
    {
    private:
        uint32_t m_length = 0;
        uint32_t m_numReadOps;
        ContinuationBase* m_pNext;
        ActionArena& m_arena;
    public:
        GcReadContinuation(Activity& activity, ActionArena& arena, uint32_t readOps, ContinuationBase* pNext)
            : ContinuationBase(activity)
            , m_arena(arena)
            , m_numReadOps(readOps)
            , m_pNext(pNext)
        { }

        void Cleanup() override
        {
            if (m_pNext != nullptr)
            {
                m_pNext->Post(0, m_length);
                m_pNext = nullptr;
            }
            m_arena.Retire(); // release memory for flushing continuations.
        }

        void OnReady(
            _In_ Schedulers::WorkerThread&,
            intptr_t       continuationHandle,
            uint32_t       messageLength
            )
        {
            m_numReadOps--;
            // When the read spans two ends of the file, this continuation
            // will be posted twice, only act on the last time
            m_length += messageLength;
            Audit::Assert((StatusCode)continuationHandle == StatusCode::OK, "Failed async reading!");

            if (m_numReadOps == 0) {
                if (m_pNext != nullptr)
                {
                    m_pNext->Post(0, m_length);
                    m_pNext = nullptr;
                }
                m_arena.Retire(); // release memory for flushing continuations.
            }
        }
    };    

    // Continuation triggered when async file reads completes
    // in a ChainedFileStore. these reads are triggered by
    // a store read, either read the whole item or header only
    //
    // this class should be allocated on an arena
    // 
    class FileStoreReadContinuation : public ContinuationBase
    {
        Utilities::DisposableBuffer*  m_pReadBuffer;
        PartialDescription m_partialDesc;
        uint32_t           m_dataSize;
        Continuation<BufferEnvelope>&  m_next;
        uint32_t           m_numReadOps;
        bool               m_headerOnly;

        uint32_t           m_bytesRead;
        StatusCode         m_status;

    public:
        FileStoreReadContinuation(
            Activity&   activity,
            Utilities::DisposableBuffer*  pReadBuffer,
            PartialDescription partialDesc,
            uint32_t    descPlusBlobSize,
            Continuation<BufferEnvelope>& next,
            uint32_t    numReadOps,
            bool        headerOnly
            )
            : ContinuationBase(activity)
            , m_pReadBuffer(pReadBuffer)
            , m_partialDesc(partialDesc)
            , m_dataSize(descPlusBlobSize)
            , m_next(next)
            , m_numReadOps(numReadOps)
            , m_bytesRead(0)
            , m_status(StatusCode::OK)
            , m_headerOnly(headerOnly)
        {
            Audit::Assert(m_numReadOps == 1 || m_numReadOps == 2,
                "One block can only be split into two pieces");
            Audit::Assert(m_pReadBuffer != nullptr,
                "Read buffer missing when constructing FileStoreReadContinuation.");
        }

        void OnReady(
            _In_ WorkerThread&,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            ) override
        {
            m_numReadOps--;
            Audit::Assert(m_numReadOps >= 0 && m_numReadOps < 2,
                "Unexpected read continuation triggered");

            // this is posted by a file I/O queue, continuationHandle holds a status code, 
            if (m_status == StatusCode::OK)
                m_status = (StatusCode)continuationHandle;
            m_bytesRead += length;

            // this may be posted twice when read spans two different files, only act when both finish
            if (m_numReadOps > 0)
                return;

            // Do we really want to recover from disk read error? Bad sector means data corruption, may
            // be its better we just restart
            Audit::Assert(m_status == StatusCode::OK, "read file error is not handled");
            Audit::Assert(m_bytesRead >= m_dataSize + m_pReadBuffer->Offset(), "read file incomplete");
            Description* pHeader = (Description*)((char*)(m_pReadBuffer->PData()) + m_pReadBuffer->Offset());
            Audit::Assert(m_headerOnly || Reduction::RoundUpSize(pHeader->ValueLength + sizeof(Description)) == m_dataSize,
                "Catalog size mismatch with record size on file.");

            auto success = StatusCode::OK;
            int dataLength = 0;
            if (!(m_partialDesc.KeyHash == pHeader->KeyHash))
            {
                success = StatusCode::NotFound;
            }
            else if (m_headerOnly)
            {
                dataLength = sizeof(Description);
                success = StatusCode::OK;
            }
            else if (pHeader->Reader != 0 
                && m_partialDesc.Caller != pHeader->Reader
                && m_partialDesc.Caller != pHeader->Owner)
            {
                success = StatusCode::AccessDenied;
            }
            else
            {
                // return data only, skip header
                dataLength = pHeader->ValueLength;
                m_pReadBuffer->SetOffset(m_pReadBuffer->Offset() + sizeof(Description));
                success = StatusCode::OK;
            }

            if (dataLength == 0)
            {   // if success == S_OK then we have an empty item
                m_pReadBuffer->Dispose();
                m_pReadBuffer = nullptr;
                m_next.Post((intptr_t)success, 0);
            }
            else
            {
                Audit::Assert(success == StatusCode::OK, 
                    "Should not return any data when there is a problem!");
                m_next.Receive(m_pReadBuffer, dataLength);
                m_pReadBuffer = nullptr;
            }
            // this is allocated on read activity arena. the memory will be claimed
            // when the activity finishes
            Tracer::LogActionEnd(Tracer::EBCounter::DiskStoreRead, success);
        }

        void Cleanup() override
        {
            if (m_pReadBuffer != nullptr){
                Audit::OutOfLine::Fail(StatusCode::Unexpected,
                    "Destroying Partition File Read continuation for unexpected reason!");
                m_pReadBuffer->Dispose();
                m_pReadBuffer = nullptr;
                m_next.Cleanup();
            }
        }
    };    
        
    // Keeps track of the current item state
    // only applicable when GCState == InspectRecord
    // GcVersionCHenker continuation sets the keep value
    // default is set to true 
    struct ItemFresshnessState
    {
        Description* m_pDesc;
        AddressAndSize m_location;
        bool m_keep;

        ItemFresshnessState() : ItemFresshnessState(nullptr, AddressAndSize{}, true){};

        ItemFresshnessState(Description* pDesc, AddressAndSize location, bool keep)
            : m_pDesc(pDesc)
            , m_location(location)
            , m_keep(keep)
        {}
    };

    enum class GcState {
        // Starting or just finished with a read buffer
        StartReading, 

        // Collection approaching write edge
        NoWorkToDo, 

        // Waiting for write buffer to be allocated
        ApplyForWriteBuffer, 

        // Waiting for a full write buffer to be flushed to disk
        FlushWriteBuffer, 

        // Waiting for disk read to fill a read buffer
        FillReadBuffer,

        // Waiting for catlog check to get the version 
        // of current record under consideration
        InspectRecord,        

        // Waiting for catalog compaction 
        CatalogCompaction,

        // Waiting for catalog file store gc
        CatalogFileStoreGC
    };

}

namespace Exabytes
{

    class FileGcContinuation;
    class ChainedFileStore;
    class FileStoreGarbageCollector;

    class CfStoreBuffer : public CoalescingBuffer
    {
        friend class ChainedFileStore;
        friend class FileStoreGarbageCollector;
    private:
        unique_ptr<Utilities::LargePageBuffer> m_pMemory;
        size_t  m_bufferSize;

        // the buffer begins with a header, then description + blobs.
        // FlashDescriptions are pushed in reverse order from the end.
        // Finally, an identical then finally a copy of the header.
        // All rounded up to Sector size.
        // Descriptions are clustered separately for faster reconstruction on a restart.
        // Descripion+blob pairs must obey ALIGNMENT.
        //
        BufferHeader* m_pBufferHeader;

        // the blobs grow up from after the header.  Each blob is preceded by its description.
        void* m_pNextBlob;

        // The Descriptions grow down from the end of buffer.
        FlashDescription* m_pDescriptions;

        CoalescingBufferPool& m_pool;

        // Compact a full buffer, ready it for flushing to disk
        //
        void Compact()
        {
            // issue: as a true journal, we need to keep Descriptions of deleted blobs
            //	until the cyclic replacement of the SSD file erases their last body.
            //  Can we just record them as zero length?  Maybe with Owner == zero too?

            // Here is a good place to carefully check the buffer contents.  We do not want to make a mistake durable.
            // It is also infrequent enough that we can afford to be thorough.

            Audit::Assert((((intptr_t)m_pNextBlob) & (ALIGNMENT - 1)) == 0, "blobs need to be aligned");
            Audit::Assert(m_pMemory->PData(sizeof(BufferHeader)) < m_pNextBlob, "blobs must be in the  buffer");
            Audit::Assert((size_t)(((char*)m_pDescriptions) - (char*)(m_pMemory->PData(0))) < m_bufferSize, "descriptions must be in the buffer");

            // We now rearrange the buffer contents to be compact on disk, with the
            // proviso that size rounds up to SECTORSIZE and the Descriptions will
            // be contiguous to the end, with a copy of BufferHeader at the very end.
            // This structure on SSD is efficient to write and to recover.

            auto count = m_pBufferHeader->m_blobCount;
            Audit::Assert((size_t)(((char*)m_pMemory->PData(m_bufferSize - sizeof(BufferHeader))) - ((char*)m_pDescriptions)) == (count * sizeof(FlashDescription)),
                "the number of descriptors needs to match the header blob count");
            auto pSectorEnd = (count * sizeof(FlashDescription)) + sizeof(BufferHeader) + (uintptr_t)m_pNextBlob;
            pSectorEnd = AlignPower2(SECTORSIZE, pSectorEnd);
            auto pNewDescriptions = (FlashDescription*)(pSectorEnd - (count * sizeof(FlashDescription)) - sizeof(BufferHeader));
            Audit::Assert(pNewDescriptions <= m_pDescriptions, "the memory buffer cannot expand");

            // move the descriptions to their new location, finish and duplicate the header.

            if (pNewDescriptions < m_pDescriptions)
            {
                memcpy_s((void*)pNewDescriptions, count * sizeof(FlashDescription), m_pDescriptions, count * sizeof(FlashDescription));
                m_pDescriptions = pNewDescriptions;
            }
            auto pRepeatedHeader = (BufferHeader*)(pNewDescriptions + count);
            m_pBufferHeader->m_totalSize = (uint32_t)(((char*)pSectorEnd) - (char*)(m_pMemory->PData(0)));

            *pRepeatedHeader = *m_pBufferHeader;
        }

    public:

        CfStoreBuffer(size_t bufferSize, CoalescingBufferPool& pool)
            : m_pool(pool)
            , m_bufferSize(bufferSize)
        {
            unsigned actualCount;
            m_pMemory = make_unique<Utilities::LargePageBuffer>(bufferSize, 1, m_bufferSize, actualCount);
            Audit::Assert(0 < actualCount, "Failed to allocate write buffer!");
            ResetBuffer();

            Audit::Assert((m_bufferSize % SECTORSIZE) == 0, "Buffer size must be aligned!");
        }

        void ResetBuffer() override
        {
            ZeroMemory(m_pMemory->PBuffer(), m_bufferSize);
            m_pBufferHeader = (BufferHeader*)m_pMemory->PBuffer();
            m_pNextBlob = m_pMemory->PData(sizeof(BufferHeader));
            m_pDescriptions = (FlashDescription*)m_pMemory->PData(m_bufferSize - sizeof(BufferHeader));
            CoalescingBuffer::ResetBuffer();
        }

        // Copies a datagram into this buffer.  Returns S_OK if the write was accepted.
        // Not thread safe!
        //  Returns an E_ error if there was a problem with validating the write.
        //  Returns E_OUTOFMEMORY if the blob filled the buffer. 
        //  Return is Synchronous.
        //
        StatusCode Append(
            // full description of the blob we are storing
            _In_ const Datagram::Description& description,

            // the blob we are sending.  MBZ if and only if description.valueLength == 0
            _In_reads_(partsCount) void* const* pDataParts,

            // the blob we are sending.  MBZ if and only if description.valueLength == 0
            _In_reads_(partsCount) const uint32_t* pDataSizes,

            // must equal array counts for data parts and sizes.  Required for SAL
            size_t partsCount,

            // original address of the item appended
            _In_ const AddressAndSize& addr
            ) override
        {
            if (partsCount <= 0)
            {
                return StatusCode::InvalidArgument;
            }
            if (m_pBufferHeader->m_totalSize > 0)
            {
                return StatusCode::OutOfMemory; // already compacted, not accepting more write!
            }

            uint32_t totalSize = 0;
            for (auto pIter = pDataSizes; pIter < (pDataSizes + partsCount); ++pIter)
            {
                totalSize += *pIter;
            }

            if (partsCount == 1){
                Audit::Assert(totalSize == description.ValueLength, "size must match");
            }
            auto roundedUp = (totalSize + ALIGNMENT - 1) & ~(ALIGNMENT - 1);

            Audit::Assert(sizeof(Description) + roundedUp <= m_bufferSize - 2 * sizeof(BufferHeader) - sizeof(FlashDescription),
                "Single blob too big for coalecing buffer");

            char* pTrialBlobEnd = sizeof(Description) + roundedUp + (char*)m_pNextBlob;
            char* pTrialDesc = (char*)(m_pDescriptions - 1);
            if (pTrialBlobEnd > pTrialDesc)
            {
                return StatusCode::OutOfMemory;
            }

            void* pBlob = m_pNextBlob;
            m_pNextBlob = pTrialBlobEnd;
            --m_pDescriptions;

            // Write FlashDescription.
            m_pDescriptions->KeyHash = description.KeyHash;
            m_pDescriptions->m_offset = (uint32_t)(((char*)pBlob) - (char*)m_pMemory->PBuffer());
            m_pDescriptions->m_size = (uint32_t)(sizeof(Description) + totalSize);

            *(Description*)pBlob = description;
            ((Description*)pBlob)->Part = 0; // multi-parts not allowed on disk;
            ((Description*)pBlob)->ValueLength = totalSize; // set blob size of multipart description;
            
            m_oldLocations.push_back(
                std::pair<Key128&, AddressAndSize>(((Description*)pBlob)->KeyHash, addr)
                );

            pBlob = (void*)(sizeof(Description) + (char*)pBlob);
            auto pSize = pDataSizes;
            auto ppPart = pDataParts;
            for (unsigned i = 0; i < partsCount; ++i)
            {
                auto size = *pSize++;
                auto pPart = *ppPart++;
                memcpy_s(pBlob, size, pPart, size);
                pBlob = (void*)(size + (char*)pBlob);
            }
            m_pBufferHeader->m_serial = max(m_pBufferHeader->m_serial, description.Serial);
            m_pBufferHeader->m_blobCount++;
            return StatusCode::OK;
        }

        CoalescingBufferPool* GetPool() const override
        {
            return &m_pool;
        }

    };

    // Interface, invoked when a partition file recovery has finished
    //
    class PartitionRecoveryCompletion
    {
    public:
        virtual void operator()(
            // OK when the recovery finished successfully
            StatusCode status,

            // latest serial number recovered
            uint64_t maxSerial,

            // partitions the keys in the file belong to
            std::unordered_set<Datagram::PartitionId> partitions,

            // The data file, from which we recovered index,
            // serve as a hint to recall other context
            // and construct partition
            //
            _In_ AsyncFileIo::FileManager& file

            ) = 0;
    };

    // Context class for recovering index from a partition file. It deletes
    // itself after recoverying is finished.
    //
    // This is tightly coupled with the structure of the coalescing buffer
    // which forms journal records on disk file. There are mini index at
    // the tail of each journal record. We scan the tail to recover index.
    //
    class PartFileRecovery
    {
        PartFileRecovery(const PartFileRecovery&) = delete;
        PartFileRecovery& operator=(const PartFileRecovery&) = delete;

        // Index
        Catalog& m_catalog;

        // Partitions the keys in the file belong to
        std::unordered_set<PartitionId> m_partitions;

        // function to tell which partition a key belongs to
        PartitionId(*const m_partHash)(Key128);

        AsyncFileIo::FileManager& m_file;

        uint64_t m_writeEdge;
        uint64_t m_eraseEdge;
        const size_t   m_storeSize;

        // Memory region to hold the end of the journal record so that we can
        // restore index. 
        // We allocate a big enough buffer, the biggest coalescing buffer
        // is 16M, that could potentially hold 16k keys, 1MB can hold
        // mini index for 32k keys
        //
        static const size_t BUF_SIZE = 1 * SI::Mi;
        char * m_mem = nullptr;
        BufferHeader* m_pHeader = nullptr;
        FlashDescription* m_pIdx = nullptr;

        template<typename T>
        T* GetPtr(uint64_t offset)
        {
            Audit::Assert(offset < BUF_SIZE && offset >= 0,
                "Buffer access out of bound in journal recoverying");
            return reinterpret_cast<T*>(m_mem + offset);
        }

        // Class to handle file read completion
        // 
        class LoadCompletion : public ContinuationBase
        {
            PartFileRecovery& m_recover;

            // contains the header at the front of the 
            // jounal record;
            BufferHeader m_frontEnd;

            uint64_t m_serial = 0;

            uint32_t m_numLoads = 1;

        public:

            LoadCompletion(Activity& job, PartFileRecovery& r)
                : ContinuationBase(job)
                , m_recover(r)
            {}

            void SetNumIOs(uint32_t numIos)
            {
                m_numLoads = numIos;
                Audit::Assert(m_numLoads == 1 || m_numLoads == 2,
                    "Internal error in File recovery ");
            }

            void Cleanup() override
            {
                Audit::NotImplemented("unexpected cancelation of partition recovery!");
            }

            // This is the logic for reading the partition file and restore the
            // catalog. Read this method as a loop, starting from the middle!
            //
            // Each iteration process one journal record. We start with the
            // head header already in memory, and trying to issue file read to
            // load the tail index and tail header into memory. 
            // Then the control goes to the begining of the method, process
            // the tail index, and update catalog, until either data corruption
            // detected or we reached the end.
            // 
            //
            void OnReady(
                _In_ WorkerThread&,
                _In_ intptr_t       continuationHandle,
                _In_ uint32_t
                ) override
            {
                auto status = (StatusCode)continuationHandle;
                if (status != StatusCode::OK)
                {
                    Tracer::LogWarning(status, 
                        L"Disk read error when recoverying from partition file!");
                    goto EndProcessJournalRecord;
                }
                m_numLoads--;
                if (m_numLoads > 0)
                    return;

                if (m_recover.m_pIdx != nullptr)
                {
                    // verify front end header and back end header are the same
                    auto pTailHeader = reinterpret_cast<BufferHeader*>
                        (m_recover.m_pIdx + m_frontEnd.m_blobCount);
                    Audit::Assert(m_recover.m_pHeader == pTailHeader + 1,
                        "Pointer calculation error during partition recovery!");

                    if (pTailHeader->m_blobCount != m_frontEnd.m_blobCount
                        || pTailHeader->m_serial != m_frontEnd.m_serial
                        || pTailHeader->m_totalSize != m_frontEnd.m_totalSize)
                    { 
                        status = StatusCode::Unexpected;
                        Tracer::LogWarning(status,
                            L"Data corruption found in partition file");
                        goto EndProcessJournalRecord;
                    }

                    // Add all index to catalog, scan backwards
                    uint32_t expectedOffset = sizeof(BufferHeader);
                    for (int i = m_frontEnd.m_blobCount - 1; i >= 0; i--)
                    {
                        // TODO!! make max record size 16M and make it
                        // a constant
                        if (m_recover.m_pIdx[i].KeyHash.IsInvalid()
                            || m_recover.m_pIdx[i].m_offset != expectedOffset
                            || m_recover.m_pIdx[i].m_size > 16 * SI::Mi)
                        {
                            status = StatusCode::Unexpected;
                            Tracer::LogWarning(status,
                                L"Data corruption found in partition file");
                            goto EndProcessJournalRecord;
                        }

                        auto addr = (m_recover.m_eraseEdge + m_recover.m_pIdx[i].m_offset)
                            % m_recover.m_storeSize;
                        AddressAndSize addrSize(addr, m_recover.m_pIdx[i].m_size);
                        m_recover.m_catalog.Add(m_recover.m_pIdx[i].KeyHash, addrSize);

                        if (m_recover.m_partHash != nullptr)
                        {
                            PartitionId pid = m_recover.m_partHash(m_recover.m_pIdx[i].KeyHash);
                            m_recover.m_partitions.insert(pid);
                        }

                        auto alignSize = (m_recover.m_pIdx[i].m_size + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
                        expectedOffset += alignSize;
                    }

                    m_serial = max(m_serial, m_frontEnd.m_serial);
                    m_recover.m_pIdx = nullptr;
                    // Get ready for the next record
                    m_recover.m_eraseEdge += m_frontEnd.m_totalSize;
                }

            EndProcessJournalRecord:

                if (status != StatusCode::OK || m_recover.m_eraseEdge >= m_recover.m_writeEdge)
                {
                    // finished!!
                    // TODO!! maybe we can read pass a possible old writing edge until
                    // data corruption is found?!!

                    m_recover.m_onFinish(status, m_serial,
                        std::move(m_recover.m_partitions), m_recover.m_file);
                    m_activity.RequestShutdown(StatusCode::OK);
                    delete &m_recover;
                    return;
                }

                //////////////////////////////////////////////////////////////
                // Process next journal record!!!
                // Now m_recover.m_pHeader should point to the header of the
                // record
                m_frontEnd = *m_recover.m_pHeader;
                m_recover.m_pHeader = nullptr;
                
                // we need to read in the mini idx in this new journal record, and
                // the header of the next record.
                auto readSize = 2 * sizeof(BufferHeader)
                    + m_frontEnd.m_blobCount * sizeof(FlashDescription);

                auto idxStart = m_recover.m_eraseEdge + m_frontEnd.m_totalSize
                    - sizeof(BufferHeader) - m_frontEnd.m_blobCount * sizeof(FlashDescription);
                Audit::Assert(idxStart > m_recover.m_eraseEdge,
                    "Error in computing index position during partition recovery.");

                auto offset = idxStart % m_recover.m_storeSize;
                auto readStart = offset & (0 - SECTORSIZE);
                auto padding = offset - readStart;

                readSize += padding;
                readSize = AlignPower2(SECTORSIZE, readSize);
                Audit::Assert(readSize <= BUF_SIZE,
                    "Read size too big during partition recovery!");

                m_recover.m_pIdx = m_recover.GetPtr<FlashDescription>(padding);
                m_recover.m_pHeader = m_recover.GetPtr<BufferHeader>
                    (padding + sizeof(BufferHeader) + m_frontEnd.m_blobCount * sizeof(FlashDescription));


                // there may be two parts if we exceed file end
                void* pFirst = m_recover.GetPtr<void>(0);
                size_t wrapLimit = m_recover.m_storeSize - readStart;
                uint32_t length1 = (uint32_t)min(readSize, wrapLimit);
                Audit::Assert(length1 > 0,
                    "Read size can not be zero during file recovery!");

                void* pSecond = m_recover.GetPtr<void>(length1);
                uint32_t length2 = (uint32_t)readSize - length1;

                if (0 < length2)
                {
                    SetNumIOs(2);
                }
                else {
                    SetNumIOs(1);
                }

                Audit::Assert(0 != m_recover.m_file.Read(readStart, length1, pFirst, this),
                        "ReadFile failure not yet handled");

                if (0 < length2)
                {
                    Audit::Assert(0 != m_recover.m_file.Read(0, length2, pSecond, this),
                        "ReadFile failure not yet handled");
                }
                // Control goes to the beginning of this method after the I/O finish.
            }

        };

        // Virtual thread for loading from file and processing the index
        Activity* m_pJob;
        LoadCompletion* m_pOnLoadFinish;
        PartitionRecoveryCompletion& m_onFinish;

    public:
        PartFileRecovery(
            _In_ Schedulers::Scheduler& scheduler,
            _Inout_ Catalog& catalog,
            _In_ PartitionId(*partHash)(const Key128),
            _In_ AsyncFileIo::FileManager& file,
            uint64_t writeEdge,
            uint64_t eraseEdge,
            _In_ PartitionRecoveryCompletion& completion
            )
            : m_catalog(catalog)
            , m_partHash(partHash)
            , m_file(file)
            , m_writeEdge(writeEdge)
            , m_eraseEdge(eraseEdge)
            , m_storeSize(file.GetFileSize())
            , m_onFinish(completion)
        {
            // In the partition class, erasing edge is always bigger,
            // making it easy to compute the available space: eraseEdge - writeEdge
            // Here however, we need to scan forward from erase edge to 
            // write edge, so making the erase edge smaller would be easier
            //
            Audit::Assert(m_eraseEdge >= m_storeSize,
                "Unexpected erase edge given to recovery.");
            m_eraseEdge -= m_storeSize;
            Audit::Assert(m_eraseEdge <= m_writeEdge,
                "Ill formed edges given to recovery");
            Audit::Assert(m_writeEdge % ALIGNMENT == 0
                && m_eraseEdge % ALIGNMENT == 0,
                "Partition file edge misalignment found during recovery!");

            m_mem = (char*)_aligned_malloc(BUF_SIZE, SECTORSIZE);
            Audit::Assert(m_mem != nullptr,
                "Failed to allocate memory to recover partition file!");

            m_pJob = ActivityFactory(scheduler, L"Recovery");
            m_pOnLoadFinish = m_pJob->GetArena()->allocate<LoadCompletion>
                (*m_pJob, *this);

            // this class should self destruct at the end of the job
        }

        ~PartFileRecovery()
        {
            if (m_pJob != nullptr)
            {
                m_pJob->RequestShutdown(StatusCode::OK);
                m_pJob = nullptr;
                m_pOnLoadFinish = nullptr;
            }
            if (m_mem != nullptr)
            {
                _aligned_free(m_mem);
                m_mem = nullptr;
            }
        }

        // Kick start the recovery by reading the header from erasing edge:
        // Start from the erasing edge and going forward, read the header so
        // that we know how much in the tail we need to read to recover all
        // the mini index.  The header of the next journal record can be read
        // together with the tail of this record.
        // 
        void StartLoading()
        {
            Tracer::LogDebug(StatusCode::OK, L"Loading journal record for recovery");

            auto readStart = m_eraseEdge % m_storeSize;
            Audit::Assert(readStart % SECTORSIZE == 0,
                "Journal record should align on SECTORSIZE!");
            m_pHeader = GetPtr<BufferHeader>(0);

            m_pOnLoadFinish->SetNumIOs(1);
            m_file.Read(readStart, (uint32_t)SECTORSIZE, GetPtr<void>(0), m_pOnLoadFinish);
        }

    };

    struct FileStoreCheckPoint
    {
        uint64_t WriteEdge;
        uint64_t EraseEdge;
    };

    // Start a job to read a partition file and reconstruct catalog
    //
    void RecoverPartitionFile(
        // Scheduler, used to construct an Activity
        _In_ Schedulers::Scheduler& scheduler,

        // The index, the recovery job will fill it.
        _Inout_ Catalog& catalog,

        // partition hash, optional, used to decide which partition
        // a key belongs to, for extra data verification
        _In_ PartitionId(*partHash)(const Key128),

        // The data file, from which we need to recover index
        _In_ AsyncFileIo::FileManager& file,

        // Check point of of the circular file
        const void* pCheckpoint,
        size_t checkpointSize,

        // Triggered when the recovery is finished.
        _In_ PartitionRecoveryCompletion& completion
        )
    {
        // Checkpoint written by RetriveCheckpointData
        // any change here should also induce the change there

        Audit::Assert(checkpointSize == sizeof(FileStoreCheckPoint), "Error Checkpoint Size for file store");
        auto pEdges = reinterpret_cast<const FileStoreCheckPoint*>(pCheckpoint);

        auto ptr = new PartFileRecovery(scheduler, catalog, partHash, file, 
            pEdges->WriteEdge, pEdges->EraseEdge, completion);
        ptr->StartLoading();

        // we don't need to keep track of ptr, the class will self destruct
        // after job finish.
    }

    // The store is a circular buffer built on top of a file, on either
    // SSD or HDD
    // Assuming all operations will be executed in a single activity
    //
    class ChainedFileStore : public KeyValueOnFile
    {
        friend class TestHooks::LocalStoreTestHooks;
        friend class FileStoreGarbageCollector;

    private:

        // A file write split the buffer flush operation into two continuations,
        // these two need to communicate on the buffer for flushing, and client
        // provided callbacks. We can not use the activity's arena, because the
        // activity is expected to live forever, the arena will run out sooner
        // or later. So we need an extra Arena for each flushing operation.
        //
        class BufferFlushContinuation : public ContinuationWithContext < ChainedFileStore, ContextIndex::BaseContext >
        {
        private:
            unique_ptr<CoalescingBuffer>  m_pBuffer;
            size_t                        m_storeOffset;
            Continuation<HomingEnvelope<CoalescingBuffer>>* m_pNext;
            ActionArena*                  m_pArena;
            uint32_t                      m_length;
            int                           m_numWriteOps=0;
            bool                          m_splitted = false;
        public:
            BufferFlushContinuation(
                // Activity for sequentialize all store maintainance operations
                Activity& activity,

                // Arena for the flushing operation
                ActionArena& arena,

                // Buffer just flushed to disk
                unique_ptr<CoalescingBuffer>& pBuffer,

                // Callback from client
                Continuation<HomingEnvelope<CoalescingBuffer>>* pNext
                )
                : ContinuationWithContext(activity)
                , m_pBuffer(std::move(pBuffer))
                , m_pNext(pNext)
                , m_pArena(&arena)
                , m_length(0)
            { }

            void SetWrittenOffset(size_t offset)
            {
                m_storeOffset = offset;
            }

            void IncrementNumWrites()
            {
                m_numWriteOps++;
            }

            void OnReady(
                _In_ Schedulers::WorkerThread&,
                _In_ intptr_t,
                _In_ uint32_t       messageLength
                )
            {
                m_numWriteOps--; // write op finished
                // there's a possibility this continuation will 
                // be posted twice, only act on the last time
                m_length += messageLength;
                if (m_numWriteOps == 0) {
                    Audit::Assert(m_storeOffset < GetContext()->m_storeSize,
                        "invalid offset for gc buffer");
                    Audit::Assert(m_splitted == m_storeOffset + m_length > GetContext()->m_storeSize,
                            "buffer wrapping must mean two write actions");

                    GetContext()->OnFinishedFlushing(m_pArena,
                        m_pBuffer, m_length, m_storeOffset, m_pNext);
                    m_pNext = nullptr;
                    m_pArena = nullptr;
                }
                else
                {
                    Audit::Assert(m_numWriteOps > 0,
                        "Extra dispatch detected in disk store I/O.");
                    m_splitted = true;
                }
            }

            void Cleanup() override
            {
                if (m_pBuffer){
                    m_pBuffer->GetPool()->Release(m_pBuffer);
                    if (m_pNext != nullptr){
                        m_pNext->Cleanup();
                        m_pNext = nullptr;
                    }
                    if (m_pArena != nullptr){
                        auto pa = m_pArena;
                        m_pArena = nullptr;
                        pa->Retire();
                    }
                }
            }
        };

        class BufferFlushTask : public ContinuationWithContext < ChainedFileStore, ContextIndex::BaseContext >
        {
        private:
            unique_ptr<CoalescingBuffer>  m_pBuffer;
            Continuation<HomingEnvelope<CoalescingBuffer>>* m_pNext;
            ActionArena&                  m_arena;
        public:
            BufferFlushTask(
                // Activity for sequentialize all store maintainance operations
                Activity& activity,

                // Newly allocated arena just for this flushing operation
                ActionArena& arena,

                // Buffer to be flushed to disk
                unique_ptr<CoalescingBuffer>& pBuffer,

                // Call back provided by client
                Continuation<HomingEnvelope<CoalescingBuffer>>* pNext
                )
                : ContinuationWithContext(activity)
                , m_pBuffer(std::move(pBuffer))
                , m_pNext(pNext)
                , m_arena(arena)
            { }

            void OnReady(
                _In_ WorkerThread&,
                _In_ intptr_t,
                _In_ uint32_t
                )
            {
                // ownership of the buffer is passed to BufferFlushContinuation, but we need a copy of the pointer to start the flushing job.
                // why don't we make BufferFlushContinuation a Envelop<CoalescingBuffer> postable continuation? because it
                // has to be posted by the I/O operation, not by us.
                CfStoreBuffer* pStoreBuffer = static_cast<CfStoreBuffer*>(m_pBuffer.get());

                BufferFlushContinuation* onFinishedWriting =
                    m_arena.allocate<BufferFlushContinuation>(m_activity, m_arena, m_pBuffer, m_pNext);
                GetContext()->StartFlushBuffer(pStoreBuffer, *onFinishedWriting);
            }

            void Cleanup(){
                if (m_pBuffer){
                    m_pBuffer->GetPool()->Release(m_pBuffer);
                }
                if (m_pNext != nullptr){
                    m_pNext->Cleanup();
                }
            }
        };

        Scheduler* m_pScheduler;
        // A continuation queue to sequentialize all buffer flushing operations 
        // GC will run on another activity shared by other stores
        // so potential race condition, needs to be careful.
        //
        Activity* m_pActivity;

        // Next use of the file, perpetually increases and should be modulo the allocated size.
        size_t m_writingEdge;

        // GC boundary of file, perpetually increases and should be modulo the allocated size.
        size_t m_erasingEdge;

        // data size after last gc
        size_t m_lastGcSize = 0;

        // the file that back this store
        unique_ptr<AsyncFileIo::FileManager> m_pFile;

        size_t  m_storeSize = PARTITIONSIZE;

        // we need to update catalog when we finished
        // flusing a buffer to file
        Catalog* m_pCatalog;

        // no two GC job should overlap. guaranteed by the fact that there is
        // only one GC worker collecting all file stores.
        // We need a way to guard against accidential overlapping caused by
        // future changes. this is not a good way but better than nothing.
        bool m_underGc = false;

        bool m_shuttingDown = false;

        // The efficient read alignment on an SSD may differ from sector size
        const uint32_t READALIGNMENT;

        // Get file path in c string style
        const wchar_t* GetPathC() const
        {
            return m_pFile->GetFilePath().c_str();
        }

        ////////////////////////////////////////////////////////////////////
        // Below are methods that should only be called by the store 
        // maintainance activity

        // Circular append of the compacted buffer, onto SSD or HDD
        // Not thread safe! No two flush operations should overlap
        //
        void StartFlushBuffer(CfStoreBuffer* pBuffer, BufferFlushContinuation& next)
        {
            size_t trailingEdge = m_erasingEdge;
            size_t allocation = m_writingEdge;

            // with those snapshots, do all our preparation.
            uint32_t totalSize = pBuffer->m_pBufferHeader->m_totalSize;
            size_t available = trailingEdge - allocation;
            Audit::Assert(available <= m_storeSize,
                "available size must not exceed circular store size");
            Audit::Assert(available >= max(totalSize, GCCUSHION), 
                "circular store failed to keep up with the data");
            Audit::Assert(0 == (totalSize % SECTORSIZE), 
                "buffer size must align for unbuffered I/O");

            size_t storeOffset = allocation % m_storeSize;

            // tell <code>next</code> what's the offset for this record.
            // so that it can correctly update Catalog for each blob inside.
            next.SetWrittenOffset(storeOffset);

            // there may be two parts if we exceed the file end
            void* pFirst = pBuffer->m_pMemory->PData(0);
            size_t wrapLimit = m_storeSize - storeOffset;
            uint32_t length1 = (uint32_t) std::min((size_t)totalSize, wrapLimit);
            void* pSecond = pBuffer->m_pMemory->PData(length1);
            uint32_t length2 = totalSize - length1;

            if (0 < length1)
                next.IncrementNumWrites();
            if (0 < length2)
                next.IncrementNumWrites();

            // Trigger async writes
            // GC may overlap with flushing, increasing the trailing edge
            // and available space. this is probably ok
            //
            if (0 < length1)
            {
                m_pFile->Write(storeOffset, (int)length1, pFirst, &next);
            }
            if (0 < length2)
            {
                m_pFile->Write(0, (int)length2, pSecond, &next);
            }

            // we speculatively increase writing edge before we know the write finished
            // to prevent the next "flush" request write to the same place.
            // 
            m_writingEdge += totalSize;
            Tracer::LogCounterValue(Tracer::EBCounter::FileStoreAvailableBytes,
                GetPathC(), m_erasingEdge - m_writingEdge);
            Tracer::LogCounterValue(Tracer::EBCounter::FileStoreWriteEdge,
                GetPathC(), m_writingEdge);
        }

        // Verify all content are written to disk, update catalog and return the buffer to pool
        // This action does not have to be in the maintainance activity.
        // Consider moving this to another async job if we really need more parallelism
        //
        void OnFinishedFlushing(
            _In_ ActionArena* pArena,
            _In_ unique_ptr<CoalescingBuffer>& pBuffer, 
            uint32_t lengthWritten, 
            size_t storeOffset, 
            _In_ Continuation<HomingEnvelope<CoalescingBuffer>>* pNext)
        {
            CfStoreBuffer* pStoreBuffer = static_cast<CfStoreBuffer*>(pBuffer.get());
            // What to do about I/O error? this is disk operation with pre allocated space, so most probably 
            // bad sector. In that case we have corrupted data, recovery is costly, maybe we should just
            // crash
            Audit::Assert(pStoreBuffer->m_pBufferHeader->m_totalSize == lengthWritten,
                          "expected length of write not matched");
            Audit::Assert(m_pCatalog != nullptr,
                "can not locate catalog when relocating data");

            // action should be performed after catalogs are all updated.
            TriggerOrReleaseBuffer* pAction = 
                pArena->allocate<TriggerOrReleaseBuffer>(pNext, pBuffer, pArena);
            CountDownActor<TriggerOrReleaseBuffer>* pBarrier = 
                pArena->allocate<CountDownActor<TriggerOrReleaseBuffer>>(*m_pActivity, 
                                 pAction, pStoreBuffer->m_pBufferHeader->m_blobCount);

            // pDesc iterate all the flash description from bottom up
            FlashDescription* pDesc = pStoreBuffer->m_pDescriptions;
            pDesc += (pStoreBuffer->m_pBufferHeader->m_blobCount - 1);
            void* pEnd = pStoreBuffer->m_pMemory->PData(lengthWritten-sizeof(BufferHeader));
            Audit::Assert((pDesc + 1) == pEnd, "can not find end of flash description");

            CoalescingBuffer::Iterator oldLocationIter = pStoreBuffer->cbegin();
            for (uint32_t i = 0; 
                i < pStoreBuffer->m_pBufferHeader->m_blobCount; 
                i++, pDesc--, oldLocationIter++)
            {
                Audit::Assert(pDesc->m_offset < lengthWritten,
                    "Record offset exceeds buffer boundary!");
                uint64_t newAddr = (storeOffset + pDesc->m_offset) % m_storeSize;
                AddressAndSize newLocation(newAddr, pDesc->m_size);

                if (pStoreBuffer->m_origin != CoalescingBuffer::Origin::File)
                {
                    m_pCatalog->Add(pDesc->KeyHash, newLocation);
                    pBarrier->Post(0, 0);
                }
                else
                {
                    m_pCatalog->Relocate(pDesc->KeyHash, 
                                         oldLocationIter->second,
                                         newLocation, pBarrier);
                    // Relocate failure means the record is deleted,
                    // too bad we produced more garbage for future gc
                }
            }
            Audit::Assert((pDesc + 1) == pStoreBuffer->m_pDescriptions,
                "should be back to start of flash description by now");
        }

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
            Tracer::LogActionStart(Tracer::EBCounter::DiskStoreRead, GetPathC());

            uint64_t storeOffset = addrAndSize.FullAddress();
            uint32_t descPlusBlobSize = (uint32_t)addrAndSize.Size();
            if (headerOnly)
                descPlusBlobSize = sizeof(Description);

            Audit::Assert(storeOffset < m_storeSize, "Invalid offset from Catalog.");
            Audit::Assert(0 == (storeOffset % ALIGNMENT), "offset should always be aligned");

            size_t readOffset = storeOffset & (0 - (uint64_t)READALIGNMENT); // find aligned position
            size_t readSize = (storeOffset - readOffset) + descPlusBlobSize;
            readSize = (readSize + READALIGNMENT - 1) & (0 - (size_t)READALIGNMENT); // round up to alignment
            Audit::Assert(readSize < 32 * SI::Mi, "data item size should be smaller than 16M");

            Utilities::DisposableBuffer* pBuffer = m_pScheduler->Allocate(
                (uint32_t)readSize,
                READALIGNMENT,
                (uint32_t)(storeOffset - readOffset));
            void* pBlob = pBuffer->PData();

            // there may be two parts if we exceed file end
            size_t wrapLimit = m_storeSize - readOffset;
            uint32_t length1 = (uint32_t) min(readSize, wrapLimit);
            void* pSecond = length1 + (char*)pBlob;
            uint32_t length2 = (uint32_t) readSize - length1;
            uint32_t numReadOps = 0;
            if (0 < length1)
                numReadOps++;
            if (0 < length2)
                numReadOps++;

            // schedule the read I/O completion on the same activity with the caller.
            ActionArena* pArena = pContinuation->GetArena();            
            FileStoreReadContinuation* pReadCompletion = pArena->allocate<FileStoreReadContinuation>(
                pContinuation->m_activity,
                pBuffer,
                description,
                descPlusBlobSize,
                *pContinuation,
                numReadOps,
                headerOnly
                );
            pBuffer = nullptr;
            // BUGBUG!! This is rare but possible, after read activity got the address from catalog, and before
            // reaching the actual read in I/O queue, the catalog is updated by GC, then trailing edge is updated.
            // now we are reading from an invalid place

            // To prevent this from happening, one possible solution is to leave enough room between the writing
            // edge and the trailing edge, maybe 2 X size of the coalscing buffer GCCUSHION, so that it takes two or more flush
            // cycles to overwrite the old data. 

            if (0 < length1)
            {
                Audit::Assert(0 != m_pFile->Read(readOffset, length1, pBlob, pReadCompletion),
                    "ReadFile failure not yet handled");
            }
            if (0 < length2)
            {
                Audit::Assert(0 != m_pFile->Read(0, length2, pSecond, pReadCompletion),
                    "ReadFile failure not yet handled");
            }
            return StatusCode::OK;
        }

        size_t Size() const
        {
            return m_storeSize;
        }

        void SetEdges(size_t allocEdge, size_t trailingEdge)
        {
            m_writingEdge = allocEdge;
            m_erasingEdge = trailingEdge;
        }        
        // end of test hooks

    public:
        ChainedFileStore(
            _In_ Scheduler& scheduler,

            // SSD or HDD
            AddressSpace addrSpace,

            // Underlying file to support the store
            unique_ptr<AsyncFileIo::FileManager> pFile,

            // Optional check point for recoverying edges
            const FileStoreCheckPoint* pCheckpoint = nullptr
            )
            : KeyValueOnFile(addrSpace)
            , m_pScheduler(&scheduler)
            , READALIGNMENT((addrSpace != AddressSpace::HDD) ? 512 : SECTORSIZE)
            , m_pFile(std::move(pFile))
        {
            m_storeSize = m_pFile->GetFileSize();
            Audit::Assert(m_storeSize % SECTORSIZE == 0,
                "File store size must be aligned to sector size!");

            // initialize activity for buffer flush, that advances write edge.
            // it maybe invoked by sweeper and GC.
            // GC is in its own activity, thus maybe parallel.
            // GC works to advance the trailing edge.
            // we need to be careful when these two edges get close
            // 
            m_pActivity = ActivityFactory(scheduler, L"ChainedFileLog", true);
            m_pActivity->SetContext(ContextIndex::BaseContext, this);

            m_writingEdge = 0;
            m_erasingEdge = m_storeSize;

            if (pCheckpoint != nullptr)
        {
                m_writingEdge = pCheckpoint->WriteEdge;
                m_erasingEdge = pCheckpoint->EraseEdge;
                Audit::Assert(m_writingEdge < m_erasingEdge && m_writingEdge % SECTORSIZE == 0
                    && m_erasingEdge % SECTORSIZE == 0,
                    "ill formed recovery information provided to file store." );
            }

            Tracer::LogCounterValue(Tracer::EBCounter::FileStoreAvailableBytes,
                GetPathC(), m_erasingEdge - m_writingEdge);
        }

        virtual ~ChainedFileStore()
        {
            if (m_pActivity != nullptr)
            {
                // shutdown activity for buffer flushing
                // since we should have already turned off
                // sweeping and gc, this activity should
                // be idle by now.
                m_pActivity->RequestShutdown(StatusCode::OK);
                m_pActivity = nullptr;
            }
            m_pFile->Close();
            m_pFile.reset();
        }

        void RequestShutdown() override
        {
            m_shuttingDown = true;
        }

        intptr_t GetFileHandle() const override
        {
            return m_pFile->GetFileHandle();
        }

        const wstring& GetFilePath() const override
        {
            return m_pFile->GetFilePath();
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
            size_t availableSize = m_erasingEdge - m_writingEdge;
            Audit::Assert(availableSize <= m_storeSize,
                "available size must not exceed circular store size");                                   

            return availableSize;
        }

        // Add a full coalescing buffer to flush queue. An async job will write the
        // buffer to disk, then update the Catalog with the new address for all
        // the records. Last, pNotification will be posted, if not NULL
        //
        StatusCode Flush(
            // Buffer to be flushed to disk store, and later returned to pool.
            _In_ unique_ptr<CoalescingBuffer>& pBuffer,

            // callback for notification of flush finished.
            _In_ Continuation<HomingEnvelope<CoalescingBuffer>>* pNotification = nullptr
            ) override
        {
            CfStoreBuffer* psBuffer = static_cast<CfStoreBuffer*>(pBuffer.get());
            Audit::Assert(!psBuffer->Empty(), "can only flush none empty buffers");
            Audit::Assert(psBuffer->m_origin != CoalescingBuffer::Origin::Invalid,
                "Unknown buffer origin!");

            psBuffer->Compact();

            // We need a new arena. can not use the arena in m_pActivity, it would run out.
            // need to make sure this arena is returned at the end of the job, i.e. upon finishing
            // disk write
			ActionArena* pArena = m_pScheduler->GetNewArena();
            auto pTask = pArena->allocate<BufferFlushTask>(*m_pActivity, *pArena, pBuffer, pNotification);
            pTask->Post(0, 0);
            return StatusCode::OK;
        }

        // Synchronous.  Locate the data and return a description of it.
        // Returns an E_ error if there was a problem with validating the Read.
        // If errors are encountered async, pContinuation will be called with
        // length = 0 and continuationHandle containing error code.
        //
        StatusCode Read(
            // zero means default.  Negative means infinite.
            int,

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

        // There is a slight chance of hash collision during Catalog look up,
        // to make sure it's the right item, we need to retrieve the header
        // from store.
        // Similar to Read, data should be collected in pContinuation, from
        // BufferEnvelope object.
        //
        StatusCode GetDescription(
            const Key128& keyHash,

            const AddressAndSize location,

            // async collection of Description,  When OnReady is called,
            // continuationHandle contains a pointer to Description object
            _In_ Continuation<BufferEnvelope>* pContinuation
            ) override
        {
            PartialDescription mock{ keyHash, 0 };
            return InternalRead(mock, location, true, pContinuation);
        }

        size_t GetCheckpointSize() const override
        {
            return sizeof(FileStoreCheckPoint);
        }

        // Called by the check pointer to save the info to recovery
        // file.
        StatusCode RetriveCheckpointData(_Out_ void* buffer) const override
        {
            // will be read back by function RecoverPartitionFile
            // any change here should also induce the change there
            auto ptr = reinterpret_cast<FileStoreCheckPoint*>(buffer);
            ptr->WriteEdge = m_writingEdge;
            ptr->EraseEdge = m_erasingEdge;
            return StatusCode::OK;
        }
    };

    // File store garbage collector. It's a continuation that is repeatedly
    // triggered in one thread (activity), going around collecting
    // different stores. 
    // Garbage collectors are designed to live forever
    //
    class FileStoreGarbageCollector : public GcActor, public Continuation < HomingEnvelope<CoalescingBuffer> >
    {
    private:

        // the shortest time an item  must live. 
        // usually the item is deleted after expiration time by the gc.
        // and the gc may also delete a item before expiration time if
        // there is an newer version.
        // but the gc must keep an item alive for at this much time
        // even when there is an newer version
        //
        static const int KEEP_ALIVE_MINUTES = 30;

        // usual GC covers this much data before giving up.
        // for very full partitions the GC covers 4 times as much        

        // Current store under collection
        ChainedFileStore*            m_pFileStore;

        // I/O buffers
        unique_ptr<CoalescingBuffer> m_pReadBuffer;
        unique_ptr<CoalescingBuffer> m_pWriteBuffer;

        // status of collector

        // Edge for reading the next block
        size_t                       m_readingEdge;

        // position where the current reading buffer was from
        size_t                       m_pendingFlushEdge;

        // number of bytes to collect
        size_t                       m_toCollect;

        uint32_t                     m_itemsLeftInReadBuffer;
        GcState                      m_state;

        // only applicable when m_state == InspectRecord
        ItemFresshnessState m_itemFreshnessState;

        PartitionRotor& m_stores;

        bool m_shutdownRequested = false;

        class GcVersionCheckContinuation : public Continuation<BufferEnvelope>
        {
        private:
            ChainedFileStore*   m_fileStore;
            Catalog*            m_catalog;
            ItemFresshnessState*m_pItemState;            
            ContinuationBase*   m_pNext;            
            AddressAndSize      m_retrievedLocation;
			ActionArena*		m_pArena;
            int8_t             m_retry = 6;           
            enum State
            {
                ExpectAddress, ExpectDataHeader
            } m_state;

        public:
            GcVersionCheckContinuation(Activity& activity, ChainedFileStore* fileStore, Catalog* catalog, ItemFresshnessState* itemState, ContinuationBase* pNext, ActionArena& arena)
                : Continuation(activity)
                , m_fileStore(fileStore)
                , m_catalog(catalog)
                , m_pItemState(itemState)                
                , m_pNext(pNext)
				, m_pArena(&arena)
            {
                m_state = ExpectAddress;
            }

            void Cleanup() override
            {
                if (m_pNext != nullptr)
                {
                    m_pNext->Post((intptr_t)StatusCode::OK, 0);
                    m_pNext = nullptr;
                }
				if (m_pArena != nullptr)
				{
                    auto pArena = m_pArena;
                    m_pArena = nullptr;
					pArena->Retire();					
            }
            }

            void OnReady(
                _In_ Schedulers::WorkerThread&,
                intptr_t       continuationHandle,
                uint32_t       messageLength
                )
            {                
                switch (m_state)
                {
                case ExpectAddress:
                {
                    m_retrievedLocation = AddressAndSize::Reinterpret(continuationHandle);

                    // The catalog contains a rounded up estimate of the size.
                    // We need to change the actual size to this same estimate value in order
                    // to compare with the size retrieved from the catalog
                    auto roundedUpExpectedSize = Reduction::RoundUpSize(m_pItemState->m_location.Size());
                    if (m_retrievedLocation.IsVoid())
                    {
                        // We should reach here only after restart data recoevery task is implemented
                        // When this is implemented - only most recent items will be in catalog therefore
                        // there is a chance of key not found... in that case we should just discard this record
                        Tracer::LogError(StatusCode::InvalidState, L"key not found in catalog!");
                        m_pItemState->m_keep = false;
                        m_pNext->Post((intptr_t)StatusCode::OK, 0);
                    }
                    else if (m_pItemState->m_location.FullAddress() == m_retrievedLocation.FullAddress()) 
                    {
                        Audit::Assert(roundedUpExpectedSize == m_retrievedLocation.Size(), 
                            "Since the address matches size should also match. Possible data corruption");

                        // current record is the latest\most updated version for this key in file store
                        m_pItemState->m_keep = true;
                        m_pNext->Post((intptr_t)StatusCode::OK, 0);
                    }
                    else
                    {
                        // we cannot assign older version to current record yet as there is
                        // a slight chance of hash collision in catlog hence we need to fetch the
                        // description from file store to determine this record's fate
                        m_state = ExpectDataHeader;
                        Tracer::LogDebug(StatusCode::OK, L"GCVersionCheck: Looks like there is a chance of collision. Issuing GetDescription");                        
                        m_fileStore->GetDescription(m_pItemState->m_pDesc->KeyHash, m_retrievedLocation, this);
						return;
                    }
                    break;
                }
                case ExpectDataHeader:
                    if (messageLength > 0)
                    {
                        // read successful
                        Audit::Assert((void*)continuationHandle == (void*)&m_postable,
                            "invalid handle returned from k-v store read");

                        DisposableBuffer* pBuffer = m_postable.Contents();
                        auto pRetrievedDesc = reinterpret_cast<Description*>((char*)pBuffer->PData() + pBuffer->Offset());

                        // GetDescription already checks whether key retrived from requested address matches
                        // the key in the request. Otherwise it will return StatusCode::NotFound
                        // So just do an assert here
                        Audit::Assert(m_pItemState->m_pDesc->KeyHash == pRetrievedDesc->KeyHash, "Unexpected result");
                        
                        // key matches but we are here because the AddressAndSize did not match so it means that current record is
                        // an older version for this key
                        m_pItemState->m_keep = false;
                        m_pNext->Post((intptr_t)StatusCode::OK, 0);                        
                    }
                    else {
                        // No data recieved in read. This means key not found at the 
                        // requested address location. This may happen due to hash collision
                        // Initiate another call to catalog locate
                        Audit::Assert((StatusCode)continuationHandle == StatusCode::NotFound,
                            "FileStore->GetDescription returned unexpected state");
                        
                        m_state = ExpectAddress;
                        Tracer::LogDebug(StatusCode::OK, L"GCVersioNCheck: Key not found at the address so it is hash collision indeed. Retrying locate");                       
                        Audit::Assert(--m_retry > 0, "Too many retries while trying to locate a key during GC");                        
                        m_catalog->Locate(m_pItemState->m_pDesc->KeyHash, m_retrievedLocation, this);
						return;
                    }
                    break;
                }

				// finished with this continuation so retire the arena
				if (m_pArena != nullptr)
				{
                    auto pArena = m_pArena;
                    m_pArena = nullptr;
                    pArena->Retire();
                }                                
            }
        };

        // Try to start GC for a file store. 
        // returns true when GC started for this store.
        // returns false when GC is too soon (not needed) for this store
        //
        bool StartGcForStore(KeyValueOnFile& fileStore) override
        {
            if (m_shutdownRequested)
                return false;

            Audit::Assert(m_pFileStore == nullptr,
                "Can not start collecting a store before finishing the previous one!");
            auto pFileStore = dynamic_cast<ChainedFileStore*>(&fileStore);
            Audit::Assert(pFileStore != nullptr,
                "This GC context can only work with ChainedFileStore.");

            // decide whether we are gonna start gc on this store
            // based on data size and data growth since last GC.
            size_t availableSize = pFileStore->m_erasingEdge - pFileStore->m_writingEdge;
            size_t dataSize = pFileStore->m_storeSize - availableSize;
            float full = (float)dataSize / (float)pFileStore->m_storeSize;
            if (dataSize < COLLECTING_SIZE * 2 || full < ALMOST_EMPTY)
            {                 
                return false;
            }

            if (full < ALMOST_FULL)
            {
                // Half empty, look at how much the data grown since last gc
                if (dataSize < pFileStore->m_lastGcSize + (COLLECTING_SIZE / 2)){
                    return false;
                }
                m_toCollect = COLLECTING_SIZE;
            }
            else {
                m_toCollect = min(COLLECTING_SIZE << 2, dataSize/2);
            }

            // collection starts from the erasing edge
            m_pFileStore = pFileStore;
            Audit::Assert(!m_pFileStore->m_underGc, "GC of the same store should not overlap");
            m_pFileStore->m_underGc = true;

            // set GC started in catalog. This will prevent it from flushing blocks to SSD
            m_pFileStore->m_pCatalog->SetGCStart();

            m_pendingFlushEdge = m_readingEdge = m_pFileStore->m_erasingEdge;
            m_itemsLeftInReadBuffer = 0;

            m_state = GcState::StartReading;

            wchar_t logMsg[1024];
            swprintf_s(logMsg, 1023, L"Start GC: %s", m_pFileStore->GetPathC());
            Tracer::LogDebug(StatusCode::OK, logMsg);

            if (!m_pReadBuffer || !m_pWriteBuffer)
            {
                m_pServer->ObtainEmptyBuffer(*this);
            }
            else
            {
                this->Post(0, 0);
            }
            return true;
        }

        void StopGc()
        {
            if (m_pFileStore)
            {
                m_pFileStore->m_underGc = false;
                m_pFileStore->m_lastGcSize = m_pFileStore->m_storeSize -
                    (m_pFileStore->m_erasingEdge - m_pFileStore->m_writingEdge);
                m_pFileStore = nullptr;
            }
            if (m_pReadBuffer)
            {
                m_pReadBuffer->ResetBuffer();
                m_pReadBuffer->m_origin = CoalescingBuffer::Origin::File;
            }
            if (m_pWriteBuffer)
            {
                m_pWriteBuffer->ResetBuffer();
                m_pWriteBuffer->m_origin = CoalescingBuffer::Origin::File;
            }

            m_itemsLeftInReadBuffer = 0;
            m_state = GcState::NoWorkToDo;
        }

        // Garbage collector, should only be executed in the maintainance activity.
        // Not thread safe, 
        //
        void GarbageCollect()
        {
            CfStoreBuffer* pReadBuffer =
                dynamic_cast<CfStoreBuffer*>(m_pReadBuffer.get());
            Audit::Assert(pReadBuffer != nullptr, "Chained file store buffer needed.");

            wchar_t logMsg[1024];
            switch (m_state)
            {
            case GcState::FlushWriteBuffer:
                // called when a write buffer just flushed to disk
                m_pFileStore->m_erasingEdge = m_pendingFlushEdge;
                Tracer::LogCounterValue(Tracer::EBCounter::FileStoreAvailableBytes,
                    m_pFileStore->GetPathC(),
                    m_pFileStore->m_erasingEdge - m_pFileStore->m_writingEdge);

                if (pReadBuffer->m_pBufferHeader->m_blobCount != 0)
                {
                    // we were in the middle of a read buffer, interrupted by write buffer full
                    break;
                }
                m_state = GcState::StartReading;
                // Fall through, as we need to fill a new buffer.

            case GcState::StartReading:
            {
                Audit::Assert(pReadBuffer->m_pBufferHeader->m_blobCount == 0,
                    "read buffer should be empty in the beginning");

                // m_toCollect is an unsigned number, this is testing whether it is smaller than 0;
                if (m_toCollect > m_pFileStore->m_storeSize
                    || m_readingEdge - m_pFileStore->m_writingEdge >= m_pFileStore->m_storeSize - 4096)
                {
                    // we need to stop for a while
                    if (m_pWriteBuffer && !m_pWriteBuffer->Empty())
                    {
                        m_state = GcState::FlushWriteBuffer;
                        m_pFileStore->Flush(m_pWriteBuffer, this);
                        return; // come back after disk write
                    }

                    m_state = GcState::NoWorkToDo;
                    swprintf_s(logMsg, 1023, L"Pause GC: %s", m_pFileStore->GetPathC());
                    Tracer::LogDebug(StatusCode::OK, logMsg);
                    return;
                }

                // fill read buffer
                size_t storeOffset = m_readingEdge % m_pFileStore->m_storeSize;
                size_t readSize = pReadBuffer->m_bufferSize;
                void* pBuffer = pReadBuffer->m_pMemory->PBuffer();

                Audit::Assert(0 == (storeOffset % SECTORSIZE), "journal record on disk should be aligned");
                Audit::Assert(readSize < SI::Gi, "disk buffer size too big");

                // there may be two parts if we exceed file end
                size_t wrapLimit = m_pFileStore->m_storeSize - storeOffset;
                uint32_t length1 = (uint32_t)min(readSize, wrapLimit);
                void* pSecond = length1 + (char*)pBuffer;
                uint32_t length2 = (uint32_t)readSize - length1;

                uint32_t numReads = 0;
                if (0 < length1)
                    numReads++;
                if (0 < length2)
                    numReads++;

				ActionArena* pArena = m_pFileStore->m_pScheduler->GetNewArena();
                GcReadContinuation* pReadCompletion = pArena->allocate<GcReadContinuation>(
                    m_activity, *pArena, numReads, this);

                if (0 < length1)
                {
                    Audit::Assert(0 != m_pFileStore->m_pFile->Read(storeOffset, length1, pBuffer, pReadCompletion),
                        "ReadFile failure not yet handled");
                }
                if (0 < length2)
                {
                    Audit::Assert(0 != m_pFileStore->m_pFile->Read(0, length2, pSecond, pReadCompletion),
                        "ReadFile failure not yet handled");
                }
                m_state = GcState::FillReadBuffer;
                return;
            }

            case GcState::FillReadBuffer:
                Audit::Assert(pReadBuffer->m_pBufferHeader->m_blobCount > 0, "journal record can not be empty");
                m_itemsLeftInReadBuffer = pReadBuffer->m_pBufferHeader->m_blobCount;
                m_readingEdge += pReadBuffer->m_pBufferHeader->m_totalSize;
                m_toCollect -= pReadBuffer->m_pBufferHeader->m_totalSize;
                m_state = GcState::InspectRecord;
                // continue below to append each valid record to write buffer
                break;

            case GcState::ApplyForWriteBuffer:
                Audit::Assert((bool)m_pWriteBuffer, "failed to obtain write buffer");
                m_state = GcState::InspectRecord;
                // continue below to append each valid record to write buffer
                break;

            case GcState::NoWorkToDo:
                Audit::OutOfLine::Fail("why call gc when there is no work to do?");            
            }

            if (!m_pWriteBuffer){
                m_state = GcState::ApplyForWriteBuffer;
                m_pServer->ObtainEmptyBuffer(*this);
                return;
            }            

            // now we should have read buffer filled and write buffer ready to go            
            while (m_itemsLeftInReadBuffer > 0)
            {
                bool keepThisRecord = true;
                Description* pDesc = reinterpret_cast<Description*>(pReadBuffer->m_pNextBlob);
                Audit::Assert(pDesc->Part == 0, "corrupted data record read from disk store");

                uint64_t oldAddr = (
                    m_pendingFlushEdge +
                    ((char*)pDesc - (char*)pReadBuffer->m_pMemory->PBuffer())
                    ) % m_pFileStore->m_storeSize;

                AddressAndSize location(
                    oldAddr,
                    pDesc->ValueLength + sizeof(Description)
                    );

                if (m_itemFreshnessState.m_pDesc == nullptr)
                {                                        
                    uint32_t expirationTime = pDesc->Timestamp + m_pServer->GetPolicy(pDesc->Owner).ExpirationMinutes * 60;
                    auto currentTime = m_activity.GetScheduler()->GetCurTimeStamp() + 30;
                    
                    if (pDesc->ValueLength == 0 && expirationTime >= currentTime)
                    {
                        // this is a deleted but not expired record                    
                        if (pDesc->TaggedForExpiration == 0)
                        {
                            // We are seeing this record for the first time. Mark it for expiration
                            // We will remove it in next cycle. In either case no need to verify its validity
                            // from catalog
                            pDesc->TaggedForExpiration = 1;                         
                        }
                        else
                        {
                            // we are looking at this record 2nd time in GC
                            // now it is safe to discard this record
                            keepThisRecord = false;
                        }
                    }                    
                    else if (expirationTime >= currentTime)
                    {
                        // this is a live valid record entry                        
                        // Check the version of the record. Write it to writebuffer only 
                        // if this is the latest version of the record                      
                        m_itemFreshnessState.m_pDesc = pDesc;
                        m_itemFreshnessState.m_location = location;
                        m_itemFreshnessState.m_keep = true;
                    
                        ActionArena* pArena = m_pFileStore->m_pScheduler->GetNewArena();                        
                        GcVersionCheckContinuation* pVersionCheckCompletion = pArena->allocate<GcVersionCheckContinuation>(
                            m_activity, m_pFileStore, m_pFileStore->m_pCatalog, &m_itemFreshnessState, this, *pArena);

                        AddressAndSize prior{ AddressAndSize::INVALIDADDRESS, 0 };
                        m_pFileStore->m_pCatalog->Locate(pDesc->KeyHash, prior, pVersionCheckCompletion);
                        
                        // come back after version check
                        return;
                    }                    
                    else
                    {
                        // time exceeded expiration time so mark the record as discard
                        keepThisRecord = false;
                    }
                }
                else
                {
                    // we have result from GcVersionChecker
                    keepThisRecord = m_itemFreshnessState.m_keep;
                }
                
                auto roundedUp = AlignPower2(ALIGNMENT, pDesc->ValueLength);                

                if (keepThisRecord == true)
                {                                                 
                    void* pBlobs[1];
                    uint32_t sizes[1];
                    pBlobs[0] = (char*)(pReadBuffer->m_pNextBlob) + sizeof(Description);
                    sizes[0] = pDesc->ValueLength;

                    if (StatusCode::OutOfMemory == m_pWriteBuffer->Append(*pDesc, pBlobs, sizes, 1, location))
                    {
                        m_state = GcState::FlushWriteBuffer;

                        // TODO!!! we should make the updating of trailing edge a sperate continuation
                        // and run together with flushing actors.
                        m_pFileStore->Flush(m_pWriteBuffer, this);
                        return; // come back after disk write
                        // if we need more increase parallelism: flush and forget, and obtain another buffer here
                    }

                    // process the next item in read buffer
                    pReadBuffer->m_pNextBlob =
                        sizeof(Description)+roundedUp + (char*)(pReadBuffer->m_pNextBlob);
                    m_itemsLeftInReadBuffer--;
                    m_itemFreshnessState.m_pDesc = nullptr;
                }
                else
                {
                    // item expired, tell catalog to remove entry
                    m_pFileStore->m_pCatalog->Expire(pDesc->KeyHash, location, this);

                    // advance to next item in read buffer 
                    pReadBuffer->m_pNextBlob =
                        sizeof(Description)+roundedUp + (char*)(pReadBuffer->m_pNextBlob);
                    m_itemsLeftInReadBuffer--;
                    m_itemFreshnessState.m_pDesc = nullptr;

                    // come back after expire finishes
                    return;
                }                                                
            }
            // finished processing a read buffer, 
            pReadBuffer->ResetBuffer();
            m_pendingFlushEdge = m_readingEdge;
            m_state = GcState::StartReading;
            swprintf_s(logMsg, 1023, L"Finished one buffer in GC: %s",
                m_pFileStore->GetPathC());
            Tracer::LogDebug(StatusCode::OK, logMsg);
        }

    public:
        FileStoreGarbageCollector(
            ExabytesServer& server,
            Activity& activity,
            PartitionRotor& stores
            )
            : Continuation(activity)
            , GcActor(server)
            , m_stores(stores)
        {
            m_pFileStore = nullptr;
            m_readingEdge = ~0ULL;
            m_pendingFlushEdge = ~0ULL;
            m_itemsLeftInReadBuffer = 0;
            m_state = GcState::NoWorkToDo;
        }


        // set of the collector to go around the partitions
        void Start() override
        {
            this->Post(0, 0);
        }

        void RequestShutdown() override
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

            if (m_state == GcState::NoWorkToDo)
            {
                Audit::Assert(messageLength == 0 && continuationHandle == 0,
                    "no parameter expected when transition from one store to another");
                auto pPart = m_stores.GetNext();
                if (pPart != nullptr)
                {
                    auto pNextStore = reinterpret_cast<KeyValueOnFile*>(pPart->Persister());
                    if (StartGcForStore(*pNextStore))
                        return; // gc started
                }
                // we dont have any thing to collect
                // check back in 100 milli seconds
                this->Post(0, 0, 100000);
                return;
            }

            if (m_pFileStore->m_shuttingDown)
            {
                StopGc();
            }

            if ((void*)continuationHandle == (void*)&m_postable){
                switch (m_state){
                case GcState::StartReading:
                    if (!m_pReadBuffer)
                    {
                        m_pReadBuffer = std::move(m_postable.m_pResource);
                        m_pReadBuffer->m_origin = CoalescingBuffer::Origin::File;
                    }
                    else if (!m_pWriteBuffer)
                    {
                        m_pWriteBuffer = std::move(m_postable.m_pResource);
                        m_pWriteBuffer->m_origin = CoalescingBuffer::Origin::File;

                    }
                    else {
                        Audit::OutOfLine::Fail("unexpected buffer posted to GC actor.");
                    }
                    break;

                case GcState::ApplyForWriteBuffer:
                    Audit::Assert(!m_pWriteBuffer, "unexpected write buffer posted to Gc actor.");
                    m_pWriteBuffer = std::move(m_postable.m_pResource);
                    m_pWriteBuffer->m_origin = CoalescingBuffer::Origin::File;
                    break;

                case GcState::FlushWriteBuffer:
                    m_postable.Recycle();
                    break;

                default:
                    Audit::OutOfLine::Fail("buffer posted to GC actor in unexpected status");
                }
            }
            if (m_state == GcState::FillReadBuffer)
            {
                CfStoreBuffer* pReadBuffer = static_cast<CfStoreBuffer*>(m_pReadBuffer.get());
                Audit::Assert(messageLength >= pReadBuffer->m_bufferSize, "read incomplete when filling GC read buffer");
            }

            if (m_state == GcState::InspectRecord)
            {
                Audit::Assert((StatusCode) continuationHandle == StatusCode::OK || (StatusCode)continuationHandle == StatusCode::InvalidState,
                    "Unexpected value present in continuationhandle. Expected value is StatusCode::OK or StatusCode::InvalidState");
            }
            if (m_state == GcState::CatalogCompaction)
            {
                auto status = (StatusCode)continuationHandle;
                Tracer::LogActionEnd(Tracer::EBCounter::CatalogCompaction, status);
                Audit::Assert(status == StatusCode::OK, "Catalog Compaction failed");                
                m_state = GcState::CatalogFileStoreGC;
                m_pFileStore->m_pCatalog->StartCatalogFileGC(this);
                return;
            }

            if (m_state == GcState::CatalogFileStoreGC)
            {
                Audit::Assert((StatusCode)continuationHandle == StatusCode::OK, "Catalog file store gc failed");
                Tracer::LogInfo(StatusCode::OK, L"Finished catalog file store gc for current partition");
                m_pFileStore->m_pCatalog->SetGCEnd();

                // sweep the whole catalog
                m_pFileStore->m_pCatalog->SweepToFileStore();

                m_state = GcState::NoWorkToDo;                
                // Done with current partition now stop GC
                StopGc();
                // process next partition in 10 ms
                this->Post(0, 0, 10000);
                return;
            }

            GarbageCollect();

            if (m_state == GcState::NoWorkToDo){
                
                // finished collecting one partition
                // call compaction for the in-memory catlog of this partition
                // before stopping GC for this partition
                Tracer::LogActionStart(Tracer::EBCounter::CatalogCompaction, m_pFileStore->GetPathC());
                m_state = GcState::CatalogCompaction;
                m_pFileStore->m_pCatalog->Compact(this);                                                                
            }
            else if (m_state == GcState::StartReading)
            {
                this->Post(0, 0);
            }
            // other states mean the GC is waiting for async job completion.
        }

        void Cleanup() override{
            StopGc();
            if (m_pReadBuffer)
                m_pReadBuffer->GetPool()->Release(m_pReadBuffer);
            if (m_pWriteBuffer)
                m_pWriteBuffer->GetPool()->Release(m_pWriteBuffer);
            m_postable.Recycle();
        }
    };

	GcActor* FileStoreGcFactory(
        ExabytesServer& server,
        Activity& gcDriver,
        PartitionRotor& stores
        )
    {
		auto pArena = gcDriver.GetScheduler()->GetNewArena();
		return pArena->allocate<FileStoreGarbageCollector>(server, gcDriver, stores);
    }

    std::unique_ptr<CoalescingBuffer> MakeFileCoalescingBuffer(
        size_t bufferSize,
        CoalescingBufferPool& pool
        )
    {
        return make_unique<CfStoreBuffer>(bufferSize, pool);
    }

    // Deprecated!!
    std::unique_ptr<KeyValueOnFile> ChainedFileStoreFactory(
        // Turns are synchronous.  Asynchronous work should be posted using the scheduler.
        _In_ Scheduler& scheduler,

        // SSD or HDD
        AddressSpace addrSpace,

        // the name of the file to be used, the starting portion anyway
        const wchar_t* pathName,

        // Store Size to be created
        // will be rounded up to increments
        size_t size
        )
    {
        if (size == 0)
        {
            size = PARTITIONSIZE;
        }

        auto pFile = AsyncFileIo::FileQueueFactory(pathName, scheduler, size);

        return make_unique<ChainedFileStore>(scheduler, addrSpace, std::move(pFile));
    }

    std::unique_ptr<KeyValueOnFile> ChainedFileStoreFactory(
        _In_ Scheduler& scheduler,

        // SSD or HDD
        AddressSpace addrSpace,

        // the handle of the file to be used
        intptr_t fileHandle
        )
    {
        auto pFile = AsyncFileIo::FileQueueFactory(fileHandle, scheduler);
        return make_unique<ChainedFileStore>(scheduler, addrSpace, std::move(pFile));
    }

    std::unique_ptr<KeyValueOnFile> ChainedFileStoreFactory(
        _In_ Scheduler& scheduler,

        // SSD or HDD
        AddressSpace addrSpace,

        // the name of the file to be used, the starting portion anyway
        std::unique_ptr<AsyncFileIo::FileManager>& fileQue,

        const void* pChkPnt,
        size_t chkPntSize 
        )
    {
        const FileStoreCheckPoint* pC = nullptr;
        if (pChkPnt != nullptr)
        {
            Audit::Assert(chkPntSize == sizeof(FileStoreCheckPoint),
                "Wrong Checkpoint Size for file store");
            pC = reinterpret_cast<const FileStoreCheckPoint*>(pChkPnt);
        }

        return make_unique<ChainedFileStore>(scheduler, addrSpace, std::move(fileQue), pC);
    }


    class CircularLogRecover : public FileRecover, PartitionRecoveryCompletion
    {
        // context for crash recovery
        struct RecoveredData
        {
            uint64_t Serial;
            std::unique_ptr<Catalog> pCatalog;
            std::unique_ptr<AsyncFileIo::FileManager> pFile;

            RecoveredData(const RecoveredData&) = delete;
            const RecoveredData& operator=(const RecoveredData&) = delete;

            RecoveredData(RecoveredData&& other)
                : Serial(other.Serial)
                , pCatalog(std::move(other.pCatalog))
                , pFile(std::move(other.pFile))
            {}

            const RecoveredData& operator=(RecoveredData&& other)
            {
                Serial = other.Serial;
                pCatalog = std::move(other.pCatalog);
                pFile = std::move(other.pFile);
                return *this;
            }

            RecoveredData() {}
        };

        // Information from upper logic about the files from which to recover
        // Data. We need to filling recovered partition if successful.
        //
        std::vector<RecoverRec>* m_pRecoverTable;

        // Extra information we need to keep track of recovery. this vector
        // should run parallel with m_pRecoverTable.
        std::vector<RecoveredData> m_pending;


        std::atomic<size_t> m_numRecovered = 0;
        ExabytesServer* m_pServer = nullptr;
        FileRecoveryCompletion* m_pFinish = nullptr;

    public:
        // Start to recover a partition from a file. 
        // Should be called only once at the startup time.
        //
        void RecoverFromFiles(
            ExabytesServer& server,
            // partition hash, used to decide which partition
            // a key belongs to
            _In_ PartitionId(*partHash)(const Key128),

            // In: file handle and checkpoint, Out: status and partition
            _Inout_ std::vector<RecoverRec>& recTable,

            // Completion callback, parameter past to RecoverFinished
            // should be the same with the one given by the parameter
            // above
            // 
            _In_ FileRecoveryCompletion& notifier
            ) override
        {
            Audit::Assert(m_pServer == nullptr, "Recovery can only be started once!");

            m_pServer = &server;
            m_pRecoverTable = &recTable;
            m_pFinish = &notifier;

            Scheduler& scheduler = server.GetScheduler();
            m_pending.reserve(recTable.size());

            for (auto& fileInfo : *m_pRecoverTable)
            {
                fileInfo.Status = StatusCode::Pending;

                RecoveredData context;
                context.Serial = 0;
                context.pFile = AsyncFileIo::FileQueueFactory
                    (fileInfo.FileHandle, scheduler);

                // get path for the catalog file
                std::wstring path = context.pFile->GetFilePath();
                path.erase(path.find('.')).append(L"-catlogFile");

                context.pCatalog =
                    Exabytes::ReducedMapCatalogFactory(scheduler, path.c_str(),
                    server.GetCatalogBufferPool(), 
                    server.GetCatalogBufferPoolForBloomKey(), 0xBAADF00DADD5C731ull);

                AsyncFileIo::FileManager& file = *context.pFile;
                Catalog& catalog = *context.pCatalog;
                m_pending.push_back(std::move(context));

                RecoverPartitionFile(scheduler, catalog,
                    partHash, file, fileInfo.pChkPnt,
                    fileInfo.ChkPntSize, *this);
            }

        }

        // Invoked when One file recovery is finished. 
        void operator()(
            // OK when the recovery finished successfully
            StatusCode status,

            // latest serial number recovered
            uint64_t maxSerial,

            // partitions the keys in the file belong to
            std::unordered_set<Datagram::PartitionId> partitions,

            // The data file, from which we recovered index,
            // serve as a hint to recall other context
            // and construct partition
            //
            _In_ AsyncFileIo::FileManager& file

            ) override
        {

            for (int i = 0; i < m_pending.size(); i++){
                if (m_pending[i].pFile.get() == &file)
                {
                    // We found the file
                    // there can only be one match, so
                    // no lock needed.
                    Audit::Assert(m_pRecoverTable->at(i).Status == StatusCode::Pending,
                        "Recovery of one partition file called twice!");
                    m_pRecoverTable->at(i).Status = status;
                    m_pending[i].Serial = maxSerial;
                    if (partitions.size() == 1)
                    {
                        auto iter = partitions.begin();
                        Audit::Assert(m_pRecoverTable->at(i).Pid == *iter,
                            "Unexpected partition recovered from file.");
                    }
                    else {
                        Audit::Assert(partitions.size() == 0,
                            "Do not accept multi-partitions yet!");
                    }

                    auto pStore = ChainedFileStoreFactory
                        (m_pServer->GetScheduler(), AddressSpace::SSD,
                        m_pending[i].pFile,
                        m_pRecoverTable->at(i).pChkPnt,
                        m_pRecoverTable->at(i).ChkPntSize);
                    m_pRecoverTable->at(i).pPartition = Exabytes::IsolatedPartitionFactory
                        (*m_pServer, m_pending[i].pCatalog, pStore, 
                        m_pRecoverTable->at(i).Pid);

                    m_numRecovered++;
                    if (m_numRecovered == m_pRecoverTable->size())
                    {
                        // we recovered all the files, report
                        // back to server.
                        m_pFinish->RecoverFinished(*m_pRecoverTable);

                        std::vector<RecoveredData>().swap(m_pending);
                        m_numRecovered = 0;
                        m_pServer = nullptr;
                    }
                    return;
                }
            }

            Audit::OutOfLine::Fail(StatusCode::Unexpected,
                "Unknown file recovered!");
    }

    };

    std::unique_ptr<FileRecover> CircularLogRecoverFactory()
    {
        return std::make_unique<CircularLogRecover>();
    }
}

namespace TestHooks
{
    size_t LocalStoreTestHooks::SizeofChainedFileStore(Exabytes::KeyValueOnFile* pstore)
    {
        return dynamic_cast<ChainedFileStore*>(pstore)->Size();
    }    

    void LocalStoreTestHooks::SetEdgesofChainedFileStore(Exabytes::KeyValueOnFile* pstore, size_t allocEdge, size_t trailingEdge)
    {
        dynamic_cast<ChainedFileStore*>(pstore)->SetEdges(allocEdge, trailingEdge);
    }   

    void LocalStoreTestHooks::EnableQuickGC(Exabytes::KeyValueOnFile* , double almost_empty, size_t collectionSize)
    {
        COLLECTING_SIZE = collectionSize;
        ALMOST_EMPTY = almost_empty;
    }

}
