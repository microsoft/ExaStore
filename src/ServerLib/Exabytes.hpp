// Exabytes.hpp : an iNode system for Exabytes
//

#pragma once

#include "stdafx.h"

#include "Utilities.hpp"
#include "Datagram.hpp"
#include "Scheduler.hpp"
#include "FileManager.hpp"
#include "Catalog.hpp"
#include "ServiceBroker.hpp"

#include <memory>
#include <vector>
#include <unordered_set>

namespace Exabytes
{
    using namespace Utilities;

    // When a write request came with a blob, we need to allocate
    // buffer for that blob. This buffer can be allocated on
    // the activity arena if we can leave enough space for continuations
    //
    const size_t ARENA_BUFFER_MAX = Schedulers::Scheduler::ARENA_SIZE - 4 * SI::Ki;

    // Sectors are assumed for roundup of unbuffered file IO.
    const uint64_t SECTORSIZE = 4096;
    static_assert((SECTORSIZE & (SECTORSIZE - 1)) == 0, "SECTORSIZE must be power of 2");

    extern std::unique_ptr<Catalog> BlobCatalogFactory(
        _In_ Schedulers::Scheduler& scheduler, 
        _In_ const wchar_t* pathName,   // the name of the file to be used
        uint64_t randomizer,            // a random value unique to this service
        uint32_t capacity               // how many values should we allow
        );

    // Huge Buffer shared by catalogs of all partitions
    // for storing in-memory hash blocks
    // Index 0 is invalid value
    // Valid range starts from 1 and can go upto MAX(SizeType)
    template<typename SizeType = uint32_t>
    class CatalogBufferPool
    {    
    public:
        // Acquires a free block from the pool and 
        // return the index
        virtual SizeType AcquireBlkIdx() = 0;

        // Releases block to the pool
        virtual void ReleaseBlock(SizeType index) = 0;

        // Returns a pointer to the request block
        virtual void* GetBlock(SizeType index) = 0;

        // Returns true/false indicating whether its time to 
        // reclaim eligible buffers
        virtual bool IsRunningLow() = 0;
    };

    extern std::unique_ptr<Catalog> ReducedMapCatalogFactory(
        _In_ Schedulers::Scheduler& scheduler,

        // the name of the file to be used 
        _In_ const wchar_t* pathName,

        // shared pool among all catalogs for storing in memory hash blocks
        _In_ Exabytes::CatalogBufferPool<>& hashPoolManager,

        // shared pool among all catalogs for storing in memory bloom key blocks
        _In_ Exabytes::CatalogBufferPool<>& bloomKeyPoolManager,

        // a random value unique to this service
        uint64_t randomizer
        );

    enum class AddressSpace : uint8_t
    {
        DRAM = 0,
        SSD = 1,
        HDD = 2,
        INVALID = 3
    };

    // Replication request, should be followed by update body, including
    // Description and blob.
    // Do we need also include the client address, so the secondary, if
    // takes over as primary can send back ack? No, the client may already
    // timedout.
    //
    struct ReplicationReqHeader
    {
        // The primary tells a secondary whether it is currently active.
        uint64_t Active : 1;

        uint64_t Ballot : 63;

        uint64_t Decree;

        uint64_t Commit;

        // Time stamp (in CPU tick) of when this req is out of the door
        // the time stamp in Description is too coarse grain (second)
        // We don't need to worry about different ticks between machines
        // as this value will simply be lopped back by ACK message.
        uint64_t Tick; 

        Datagram::Description Description;
        // in a write request, this should be followed by blob
    };

    class ExabytesServer;

    // Interface for all things supporting check points. This should
    // include partition files and memory table.
    //
    class CheckPointer
    {
    public:

        virtual size_t GetCheckpointSize() const = 0;
        virtual StatusCode RetriveCheckpointData(_Out_ void* buffer) const = 0;
    };

	// Interface of a DRAM staging buffer, accepting all incoming updates
	// all queries are sync
	class KeyValueInMemory : public CheckPointer
	{
	public:
		KeyValueInMemory(
			ExabytesServer& server
			)
			: m_pServer(&server)
		{}

		// Accept read request, returns an E_ error if there was a problem.
		// return S_OK if successful
		//
		virtual StatusCode Read(
			// zero means default.  Negative means infinite.
			int timeoutMillisecs,

			// Key of the blob we are about to retrieve.
			_In_ const Datagram::PartialDescription& description,

            // Buffer that contains the data blob
			// caller is responsible to release memory 
			// by calling blobEnvelop.Recycle();
			//
			_Out_ Utilities::BufferEnvelope& blobEnvelop
			) = 0;

		// There are cases we need to inspect an item's header for expiration
		// time, versions, etc.
		//
        virtual StatusCode GetDescription(
			const Datagram::Key128& keyHash,

            // header will be returned in this outgoing parameter
			_Out_ Datagram::Description& description
			) = 0;

        // Copies a datagram into memory store.  Returns E_SUCCESS if the write was accepted.
        //  Returns an E_ error if there was a problem with validating the write.
        //  Return is async.  The Continuation will be posted when the data is durable, or rejected
        //  because of security reason
        //
        virtual StatusCode Write(
			// full description of the blob we are storing
            _In_ const Datagram::Description& description,

			// the blob we are storing.  MBZ if and only if descriptor.valueLength == 0
			_In_reads_(dataLength) const void* pData,

			// must equal descriptor.valueLength.
			size_t dataLength
			) = 0;

		// Start the back ground worker that writes old items
		// into partitioned file stores
		//
		virtual void StartSweeping() = 0;

		// Signal the store to prepare for shutdown 
		//
		virtual void StartCleanup() = 0;

		virtual ~KeyValueInMemory() {};

		ExabytesServer* GetServer() const { return m_pServer; }        

        // Returns the allocatedSize for the Memory Store
        virtual size_t Size() const = 0;

        // Start dumping the content of the memory store to a file. 
        // This should only be called upon shutdown to enable recovery
        // after restart.
        //
        // Return a pair of numbers <allocation edge, trailing edge>
        //
        virtual std::pair<uint64_t, uint64_t> StartDump(

            // File to store memory store data
            AsyncFileIo::FileManager& file, 

            // Triggered when writting finished.
            Schedulers::ContinuationBase* pNotify
            ) = 0;

	protected:
		ExabytesServer* const m_pServer;
    private:
        // Assignment not allowed
        KeyValueInMemory& operator=(const KeyValueInMemory&) = delete;
	};

    class KeyValueOnFile;

    // Constraint specification for pooled items and the item pool,
    // the pool must implement a Release function
    // and the item must have a GetPool function
    // http://www.stroustrup.com/bs_faq2.html#constraints
    //
    template<class Item> struct BelongsToPool {
        static private void constraints(std::unique_ptr<Item>& p)  
        { 
            p->GetPool()->Release(p);
        }

        BelongsToPool()  {
            void(*p)(std::unique_ptr<Item>&) = constraints;
            Audit::Assert(p != nullptr);
        }
    };

    // Enable resources that come from a pool to be passed
    // around between continuations: achieving automatic reclaim
    // of resources when a continuation is either
    // abandoned or terminated abnormally.
    // 
    template <class PooledResourceT>
    class HomingEnvelope : BelongsToPool<PooledResourceT>
    {
    public:
        std::unique_ptr<PooledResourceT> m_pResource;

        HomingEnvelope()
        {}

        HomingEnvelope& operator=(std::unique_ptr<PooledResourceT> src)
        {
            Audit::Assert(!m_pResource, "Envelop should be empty.");
            swap(m_pResource, src);
            return *this;
        }

        HomingEnvelope(std::unique_ptr<PooledResourceT> pResource)
        {
            m_pResource = move(pResource);
        }

        HomingEnvelope(HomingEnvelope&& src)
            : m_pResource(move(src.m_pResource))
        { }

        HomingEnvelope& operator=(HomingEnvelope&& src)
        {
            Audit::Assert(!m_pResource, "Envelop should be empty.");
            swap(m_pResource, src.m_pResource);
            return *this;
        }

        void Recycle()
        {
            if (m_pResource){
                m_pResource->GetPool()->Release(m_pResource);
            }
        }
    };

    class CoalescingBufferPool;

    // A coalescing buffer for writing to KeyValueOnFile
    // Must be allocated by calling pool->GetEmptyBuffer(continuation)
    //
    class CoalescingBuffer
    {
    public:
        typedef std::vector<std::pair<Datagram::Key128&, AddressAndSize>> LocationIndexType;

        typedef LocationIndexType::const_iterator Iterator;

        // the buffer maybe filled by DRAM sweeper, file store GC or remote data 
        // transfer
        enum class Origin : uint8_t
        {
            Invalid = 0, Memory, File, Remote
        } m_origin = Origin::Invalid;

        // Copies a datagram into this buffer.  Returns S_OK if the write was accepted.
        // Not thread safe!
        //  Returns an E_ error if there was a problem with validating the write.
        //  Returns E_OUTOFMEMORY if the blob filled the buffer. 
        //  Return is Synchronous.
        //
        virtual StatusCode Append(
            // full description of the blob we are storing
            _In_ const Datagram::Description& description,

            // the blob we are sending.  MBZ if and only if description.valueLength == 0
            _In_reads_(partsCount) void* const* pDataParts,

            // the blob we are sending.  MBZ if and only if description.valueLength == 0
            _In_reads_(partsCount) const uint32_t* pDataSizes,

            // must equal array counts for data parts and sizes.  Required for SAL
            size_t partsCount,

            // record the old location of the item
            _In_ const AddressAndSize& addr
            ) = 0;

        virtual CoalescingBufferPool* GetPool() const = 0;

        bool Empty() const { return m_oldLocations.empty(); }

        // Called by the pool when the buffer is reclaimed.
        // Each sub class should implement its own to clean
        // up its fields, and call super class ResetBuffer
        // 
        virtual void ResetBuffer()
        {
            m_origin = Origin::Invalid;
            m_oldLocations.clear();
        }

        Iterator cbegin() const
        {
            return m_oldLocations.cbegin();
        }

        Iterator cend() const
        {
            return m_oldLocations.cend();
        }

        virtual ~CoalescingBuffer()
        {}

    protected:
        // keep track of data items' original location so that we can move
        // the Catalog pointer to its new location
        //
        LocationIndexType m_oldLocations;
    };

    class CoalescingBufferPool
    {
    public:
        // Async function for allocating an empty coalescing buffer from pool.
        // bufferHandler will be triggered when buffer is available.
        // thread safe
        //
        virtual void ObtainEmptyBuffer(
            Schedulers::Continuation<HomingEnvelope<CoalescingBuffer> >& bufferHandler
            ) = 0;

        // Release a coalescing buffer to pool, all content will be discarded
        //
        virtual void Release(std::unique_ptr<CoalescingBuffer>& pBuffer) = 0;
    };
    
	class KeyValueOnFile : public CheckPointer
	{
    protected:
        AddressSpace m_media;

	public:
        KeyValueOnFile(AddressSpace media)
            : m_media(media)
        {}

		virtual ~KeyValueOnFile() {};

        AddressSpace GetMediaType() const { return m_media; }

        // Accept read request, returns an E_ error if there was a problem
		// with validating the request. continuation will be posted if 
		// return value is S_OK
		//
		// Async job loads the data item from parameter location, verify
		// item key and caller key for access control. and post the result
		// to pContinuation.
		// 
		// When OnReady function of pContinuation is called, if the length
		// is zero, continuation handle contains an error code. other wise
		// data can be retrieved from BufferEnvelop, length indicating size
		// of the data blob
		//
        virtual StatusCode Read(
			// zero means default.  Negative means infinite.
			int timeoutMillisecs,

			// Key of the blob we are about to retrieve.
			_In_ const Datagram::PartialDescription& description,

            // location of the data item  
			_In_ const AddressAndSize location,

			// context to be used for async completion.
            _In_ Schedulers::Continuation<Utilities::BufferEnvelope>* pContinuation
			) = 0;

		// There is a slight chance of hash collision during Catalog look up,
		// to make sure it's the right item, we need to retrieve the header
		// from store.
		// Similar to Read, data should be collected in pContinuation, from
		// BufferEnvelop object.
		//
        virtual StatusCode GetDescription(
			const Datagram::Key128& keyHash,

			const AddressAndSize location,

			// async collection of Description,  When OnReady is called,
			// continuationHandle contains a pointer to Description object
            _In_ Schedulers::Continuation<Utilities::BufferEnvelope>* pContinuation
			) = 0;

		// Add a full coalescing buffer to flush queue. An async job will write the
        // buffer to disk, then update the Catalog with the new address for all
		// the records. Last, pNotification will be posted, if not NULL
        //
        // three activities could write to a disk store, Sweeper, GC and new partition
        // only the last one would introduce new data, all others are moving existing data,
        // currently no mixing of existing and new data allowed in a buffer
		//
        virtual StatusCode Flush(
            // Buffer to be flushed to disk store, and later returned to pool.
            // Caller give up buffer ownership.
            _In_ std::unique_ptr<CoalescingBuffer>& pBuffer,

            // callback for notification of flush finished.
            _In_ Schedulers::Continuation<HomingEnvelope<CoalescingBuffer>>* pNotification = nullptr
            ) = 0;

        // Interfaces for retrieving information about the underlying file
        //
        virtual const std::wstring& GetFilePath() const = 0;
        virtual intptr_t GetFileHandle() const = 0;

        // one file store per partion. So a file store belongs
        // to a partition. This essentially sets a "parent" pointer
        //
        virtual void SetPartition(Partition* pPart) = 0;

        // returns the available bytes in the filestore for this partition
        //
        virtual size_t GetAvailableBytes() const = 0;

        // Initiate the process for partition shutdown
        //
        virtual void RequestShutdown() = 0;
	};

    class Replicator;

    //    Each partition is a collection of data items. A partition is a unit
    // of fail over. A hash function maps item keys to partitions.
    //    Each partition has a catalog, and a file store, either on SSD or
    // on HDD
    class Partition : public CheckPointer
    {
        wchar_t m_IdStr[20];

        // assignment not allowed
        Partition& operator=(const Partition&) = delete;

    protected:
        // Biggest serial number of the committed item
        uint64_t m_serial = 0;
        

    public:
        const Datagram::PartitionId ID; 

        Partition(Datagram::PartitionId id)
            : ID(id)
        {
            swprintf_s(m_IdStr, L"%Xh", id.Value());
        }

        virtual ~Partition() {};

        const wchar_t* GetPartitionIdStr()
        {
            return m_IdStr;
        }

        virtual Catalog* GetCatalog() const = 0;

        virtual ExabytesServer* GetServer() const = 0;

        // Internal facing interface, telling the sweeper in memory store where the
        // persister store is, for sweeping
        //
        virtual KeyValueOnFile* Persister() const = 0;

        uint64_t GetSerial() const
        {
            return m_serial;
        }

        // ONLY to be called by crash/restart data recovery code!!!
        // Update the serial number of the partition
        void SetSerial(uint64_t newSerial)
        {
            m_serial = newSerial;
        }

        uint64_t AllocSerial();

        // Start an async read operation, if successful, pContinuation
        // will be triggered, from which Collect should be called to
        // collect the data
        //
        virtual StatusCode Read(
            // zero means default.  Negative means infinite.
            int timeoutMillisecs,

            // full description of the blob we are about to retrieve.
            _In_ const Datagram::PartialDescription& description,

            // context to be used for async completion.
            _In_ Schedulers::Continuation<Utilities::BufferEnvelope>* pContinuation
            ) = 0;

        // When a Continuation follows a Read, the data is ready for synchronous
        //  collection.  The handle must be that passed to the continuation.
        // Returns S_OK if the continuation is valid.
        // Returns an E_ error if there was a problem, for example if the answer has
        //   expired because the service timed out.
        // The handle and its data can only be collected once.  If you collect less
        //  than the full length of the data, the remainder will be uncollectable.
        //
        virtual StatusCode Collect(
            _In_ intptr_t continuationHandle,

            _In_ size_t length,

            _Out_writes_(length) void* pBuffer
            ) = 0;

        // Queue up a Write request, Returns E_SUCCESS if the write was accepted.
        // Returns an E_ error if there was a problem with validating the write.
        // Return is async and does not guarantee the data was sent.  If you need
        // confirmations, design end-to-end confirmation of your service.
        //
        virtual StatusCode Write(
            // full description of the blob we are storing
            _In_ const Datagram::Description& description,

            // the blob we are storing.  MBZ if and only if descriptor.valueLength == 0
            _In_reads_(dataLength) const void* pData,

            // must equal descriptor.valueLength.
            size_t dataLength,

            // context to be used for async confirmation of durability.
            _In_ Schedulers::ContinuationBase* pContinuation
            ) = 0;

        // Queue up a delete request, return E_SUCCESS if the request was accepted
        // Return is async 
        virtual StatusCode Delete(
            // full description of the blob we are about to retrieve.
            _In_ const Datagram::PartialDescription& description,

            // context to be used for async completion.
            _In_ Schedulers::ContinuationBase* pContinuation
            ) = 0;

        // Return the replication interface
        virtual Replicator& GetReplicator() = 0;

        // Start to shutdown the partition
        virtual void RequestShutdown() = 0;
    };

    // The server should provide a mean to rotate through all partitions.
    // for instance, a garbage collector should go around processing
    // each partition one by one.
    //
    class PartitionRotor
    {
    public:
        virtual Partition* GetNext() = 0;
    };

    // File store garbage collector should be one activity, going around
    // collecting different partitions. 
    class GcActor
    {
    public:
        ExabytesServer* const m_pServer;

        GcActor(ExabytesServer& server)
            :m_pServer(&server)
        {}

        // Prepare the context for fileStore, and start GC. pNext will
        // be posted after GC finishes, or immediately if GC for this
        // store is already underway.
        //
        // returns true when successfully started GC
        // returns false when GC is already underway.
        //
        virtual bool StartGcForStore(KeyValueOnFile& fileStore) = 0;

        // Set off the collector to observe the GcQue and start whenever
        // there is file store to collect
        virtual void Start() = 0;

        virtual void RequestShutdown() = 0;

    private:
        // Assignment not allowed
        GcActor& operator=(const GcActor&) = delete;
    };

    ////////////////////////////////////////////////////////////////////////
    // Data structure to keep track of crash/recovery process. 
    // This is passed to a file recover object, who read the file handle
    // and the check point to creat a partition, and fill the pPartition
    // field
    //
    struct RecoverRec
    {
        // Input to FileRecover
        intptr_t FileHandle;
        void* pChkPnt;
        size_t ChkPntSize;
        Datagram::PartitionId Pid;

        // Output by FileRecover
        Utilities::StatusCode Status;
        std::unique_ptr<Partition> pPartition;

        // Movable, not copiable
        const RecoverRec& operator=(const RecoverRec&) = delete;
        RecoverRec(const RecoverRec&) = delete;

        const RecoverRec& operator=(RecoverRec&& other);
        RecoverRec(RecoverRec&& other);
        RecoverRec();
    };

    // Notification of recovery finished
    //
    class FileRecoveryCompletion
    {
    public:
        virtual void RecoverFinished(
            // For verification purpose
            const std::vector<RecoverRec>& recTable
            ) = 0;
    };

    // Interface: logic for recovery data after crash/recovery
    // One object can only do recovery once, no reentry
    //
    class FileRecover {
    public:
        // Start to recover a partition from a file. 
        // Should be called only once at the startup time.
        //
        virtual void RecoverFromFiles(
            ExabytesServer& server,
            // partition hash, used to decide which partition
            // a key belongs to
            _In_ Datagram::PartitionId (*partHash)(const Datagram::Key128),

            // In: file handle and checkpoint, Out: status and partition
            _Inout_ std::vector<RecoverRec>& recTable,

            // Completion callback, parameter past to RecoverFinished
            // should be the same with the one given by the parameter
            // above
            // 
            _In_ FileRecoveryCompletion& notifier
            ) = 0;

    };
    /////////////////////////////////////////////////////////////////////////

    // Each owner define it's own policy about value retainment
    // Policies are centrally registered with the managers.
    // Exabytes storage server communicates with the manager
    // to retrieve Policies.
    //
    class PolicyManager
    {
    public:
        // Each data owner can have it's own policy, such as
        // expiration time for its data.
        // 
        virtual const Datagram::Policy GetPolicy(Datagram::Key64 ownerId) const = 0;

        // Set policy for certain owner
        virtual void SetPolicy(Datagram::Key64 ownerId, const Datagram::Policy& policy) = 0;

    };

    // Class to assemble pieces together to create a partition
    // Implementation should decide the file store location and size,
    // catalog capacity, etc.
    //
    class PartitionCreater
    {
    public:
        virtual std::unique_ptr<Exabytes::Partition> CreatePartition(
            // to which server the partition belongs to
            // reference provided to satisfy circular reference
            ExabytesServer& server,

            // Id of the new partition
            //
            Datagram::PartitionId id, 

            // open handle of the partition file,
            // must be opened with overlapped flag
            Schedulers::FileHandle fileHandle,

            bool isPrimary
            ) = 0;

        virtual ~PartitionCreater() {}
    };

    // Global state of an Exabytes Server
    // Each server has a single memory store, a set of partitions, each of
    // which has a file store either on SSD or HDD.
    // 
    class ExabytesServer
    {

    protected:
        // I/O handler and scheduler
        //
        std::unique_ptr<ServiceBrokers::ServiceBroker> m_pBroker;

        // file I/O buffer for sweeping and GC
        //
        std::unique_ptr<CoalescingBufferPool> m_pBufferPool;

        // Policy Table that maps owner ID to Policy
        //
        std::unique_ptr<PolicyManager> m_pPolicyTable;

        // One memory store, shared among all partitions
        // All writes are committed to memory store. Data
        // items are later sweeped into file stores of 
        // different partition.
        //
        std::unique_ptr<KeyValueInMemory> m_pMemStore;

        // Large page buffer shared between catalogs of all
        // partitions to store in-memory hash blocks
        std::unique_ptr<CatalogBufferPool<>> m_hashPoolManager;

        // Large page buffer shared between catalogs of all
        // partitions to store in-memory bloom key blocks
        std::unique_ptr<CatalogBufferPool<>> m_bloomKeyPoolManager;

    public:
        ExabytesServer(
            std::unique_ptr<ServiceBrokers::ServiceBroker>& pBroker,
            std::unique_ptr<CoalescingBufferPool>& pBufferPool,
            std::unique_ptr<PolicyManager>& pPolicyManager,
            std::unique_ptr<CatalogBufferPool<>>& pHashPoolManager,
            std::unique_ptr<CatalogBufferPool<>>& pBloomKeyPoolManager
            )
            : m_pBufferPool(std::move(pBufferPool))
            , m_pPolicyTable(std::move(pPolicyManager))
            , m_pBroker(std::move(pBroker))
            , m_hashPoolManager(std::move(pHashPoolManager))
            , m_bloomKeyPoolManager(std::move(pBloomKeyPoolManager))
        {}
  
        // Expected to be called periodically to
        // save checkpoint to a file
        //
        virtual void SaveCheckPoint(
            // Check point file to write to
            AsyncFileIo::FileManager& file,

            // Can be nullptr if fire and forget
            Schedulers::ContinuationBase* pNotify) = 0;

        // Load checkpoint information from a file and save it in
        // recoverTable
        // Must be called before we can serve traffic
        //
        virtual void LoadCheckPoint(
            // Check point file to read from
            _In_ AsyncFileIo::FileManager& file,

            // Decoded checkpoint is saved into recoverTable
            _Out_ std::vector<RecoverRec>& recoverTable,

            // Notification that the async operation finished
            _In_ Schedulers::ContinuationBase& completion
            ) = 0;

        // More functions for setting up server:
        // Ugly: we have circular dependency problems here. for instance, memory
        // store belongs to the server, but memory store also need to 
        // reference the server to get access to hash function and partition
        // table for sweeping value items to partitioned stores.
        //
        // Circular dependency means we need to have some "setup" functions
        // besides the factory. and these setup functions must be called
        // before the server can run.

        void SetMemoryStore(std::unique_ptr<KeyValueInMemory>& pMemStore)
        {
            Audit::Assert(!m_pMemStore, "can not replace existing mem store in a server!");
            m_pMemStore = std::move(pMemStore);
        }

        // Start to move value items from memory store to partitioned file stores
        virtual void StartSweeper() = 0;

        // Start the GC of partitions file stores
        virtual void StartGC() = 0;

        // When a file store gets full, call this
        // to have it garbage collected soon.
        virtual void SendStoreToGc(Datagram::PartitionId pid) = 0;

        virtual bool PartitionExists(Datagram::PartitionId id) const = 0;

        // Create a file on disk to grab some disk space. We want to grab
        // all the space allocated to us upon startup. later we can use
        // these files for partitions.
        virtual StatusCode CreatePartFile(const wchar_t* filePath, size_t size, bool isSSD) = 0;

        // Add an open handle to the server so that a partition can be based on the file.
        // Note the file must be opened with overlapped flag
        //
        virtual void AddFileHandle(Schedulers::FileHandle handle, bool isSSD) = 0;

        // returns how many SSD and HDD partitions that this server can spin up. return value
        // as a paire  <SSD capacity, HDD capacity>
        //
        virtual std::pair<size_t, size_t> SpareCapacity() = 0;

        // Spin up a new partition
        //
        virtual StatusCode StartPartition(Datagram::PartitionId id, bool isPrimary) = 0;

        virtual void StopPartition(Datagram::PartitionId id) = 0;

        virtual Partition* FindPartitionForKey(Datagram::Key128 key) const = 0;

        virtual Datagram::PartitionId FindPartitionIdForKey(Datagram::Key128 key) const = 0;

        // Returns the memory store that are shared among all partitions.
        //
        KeyValueInMemory* GetMemoryStore()
        {
            return m_pMemStore.get();
        }

        // Each data owner can have it's own policy, such as
        // expiration time for its data.
        // 
        const Datagram::Policy GetPolicy(Datagram::Key64 ownerId)
        {
            return m_pPolicyTable->GetPolicy(ownerId);
        }

        // Set policy for certain owner
        // Return true when the owner has existing policy registered
        // and replaced by this call.
        bool SetPolicy(Datagram::Key64 ownerId, const Datagram::Policy& policy)
        {
            m_pPolicyTable->SetPolicy(ownerId, policy);
        }

        // The server acts as a global pool for coalescing buffers
        // these buffers will be shared among memory store sweepers
        // and also file store garbage collectors.
        //
        void ObtainEmptyBuffer(
            Schedulers::Continuation<HomingEnvelope<CoalescingBuffer> >& bufferHandler
            )
        {
            m_pBufferPool->ObtainEmptyBuffer(bufferHandler);
        }

        virtual void StartCleanup()
        {
            if (m_pMemStore)
            {
                m_pMemStore->StartCleanup();
                m_pMemStore = nullptr;
            }
        }

        virtual void Recover(
            _Inout_ std::vector<RecoverRec>& recTable,
            _In_ FileRecoveryCompletion& notifier
            ) = 0;

        virtual StatusCode SetPartition(
            Datagram::PartitionId pid, 
            _In_ std::unique_ptr<Partition>& pPartition) = 0;


        Schedulers::Scheduler& GetScheduler()
        {
            return *(m_pBroker->GetScheduler());
        }

        ServiceBrokers::ServiceBroker& GetBroker()
        {
            return *m_pBroker;
        }

        CatalogBufferPool<>& GetCatalogBufferPool()
        {
            return *m_hashPoolManager;
        }

        CatalogBufferPool<>& GetCatalogBufferPoolForBloomKey()
        {
            return *m_bloomKeyPoolManager;
        }
    };

    extern std::unique_ptr<CoalescingBufferPool> BufferPoolFactory(
        // size of each buffers in this pool
        size_t bufferSize,

        // number of buffers in this pool
        uint32_t poolSize,

        // the pool use this function to build buffers
        std::unique_ptr<CoalescingBuffer>(*bufferFactory)(size_t, CoalescingBufferPool&)
        )
        ;

    extern std::unique_ptr<CatalogBufferPool<>> SharedCatalogBufferPoolFactory(
        // num of in memory blocks
        unsigned inMemBlkCount,

        // total num of hash blocks
        unsigned totalBlkCount,

        // num of partitions hosted by current server
        size_t numOfPartitions,

        size_t blockSize);

    extern std::unique_ptr<CatalogBufferPool<>> SharedBlockPoolFactory(
        unsigned blockCount, size_t blockSize);

    extern std::unique_ptr<CatalogBufferPool<uint16_t>> BlockPoolFactory(
        // total num of blocks
        unsigned blockCount,

        // size of each block
        size_t blockSize
        );
 }
