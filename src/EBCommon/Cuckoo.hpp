///////////////////////////////////////////////////////////////////
// Fixed size Cuckoo hash map to repace stl hash map in some cases.
// Advantages are very low performance variation.
// 1. Find, Delete and Update are all constant time (check 2 buckets)
// 2. Insert may take longer, up to 64 bucket check.
// 3. No resize, no memory churn.
//
// Disadvantages:
// 1. Can not resize once allocated.
// 2. Max load ratio around 90%, vs. 100% for std map
//
// The low performance variation is important for Exabytes store, It can be
// used to hold network sessions and request context. The fixed size naturally
// throttle the incomging requests.
//
// This file provide two implementations. 
// 1. CuckooIndex maps a key to a pointer (or index to an array) where the key
//    and value can be accessed. Max load factor around 95%. This can be used
//    as memory store catalog.
// 2. CuckooTable maps a key to a value type, similar to regular hash map, with
//    max load factor 85%
//

#pragma once

#include "stdafx.h"

#include <memory>
#include <array>
#include <atomic>
#include <vector>
#include <stack>

#include "Utilities.hpp"


namespace Utilities
{
    const size_t SLOT_PER_BUCKET = 4;
    const size_t SLOT_BITS = 2; // take 2 bits to repsents a slot index

    template <typename HandleType>
    struct DummyOp{
        void operator()(HandleType)
        {
        }
    };

    enum class CuckooStatus{
        Ok,
        KeyNotFound,
        KeyDuplicated,
        TableFull
    };

    // A CuckooIndex map a key to a handle. The handle should be either a
    // pointer or integer, via which the key and value can be retrieved.
    // This implementation does not store the key but uses a delegate to
    // retrieve the key from the handle.
    // 
    // This is a 4 way associtive cuckoo hash, with load factor up to 93%.
    // The user provide integer "power", we allocate (2 ** power) buckets
    // so the table as (2 ** power) * 4 positions.
    // 
    // The implementation does not provide any locking. Caller are expected
    // to obtain reader lock for find and delete, writer lock for update and
    // insertion. User also need to make sure update and delete of the same
    // key do not overlap
    //
    // Delete operations do not modify hash structure, so reader
    // lock is enough for the hash table. However, concurrent updating or
    // deleting of the same key may result in inconsistent results.
    //
    // Template Parameters:
    //
    // INV_HANDLE: a constant invalid value, such as nullptr
    // 
    // KeyOfHandle: a delegate to retrieve (const Key&) from a handle
    // 
    // UsePartialKeyTag: whether to use a tag to reduce key fetching
    //                   see SILT paper for detail
    // HandleDestructor: a delegate to receive notification that a certain
    //                   handle is removed from the table.
    //
    template <typename Key, typename Handle, Handle INV_HANDLE,
        class KeyOfHandle, bool UsePartialKeyTag, 
        typename HandleDestructor = DummyOp<Handle>,
        class Hash = std::hash<Key>, class Pred = std::equal_to<Key> >
    class CuckooIndex {
    public:
        ///////////// types ///////////////////////////

        // Two hash functions
        // Currently we can have 2**32 buckets (4G) at most, due to the way
        // two hash functions are constructed. The hash functions are based
        // on higher and lower 32 bits of a std::hash result. To support
        // bigger table, we need new hash functions.
        //
        typedef uint32_t BucketIndex;

        void DoubleHashFuncs(const uint64_t stdHash,
            _Out_ BucketIndex& first, _Out_ BucketIndex& second) const{

            uint32_t lower = (uint32_t)stdHash;
            uint32_t higher = (uint32_t)(stdHash >> 32);

            lower ^= lower >> m_power;
            higher ^= higher >> m_power;

            first = lower & ((1U << m_power) - 1);
            second = higher & ((1U << m_power) - 1);

            if (first == second){
                auto rotation = (higher >> 27) | (lower >> 27);
                higher = (higher << rotation) | (higher >> rotation);
                second = higher & ((1U << m_power) - 1);
                if (first == second){
                    second++;
                    second &= ((1U << m_power) - 1);
                }
            }
        }

        ///////////// functions ///////////////////////////

        // The constructor creates a new hash table 
        // with custom destructor
        //
        CuckooIndex(
            // 2**power number of buckets
            //
            uint64_t power,

            // Delegate to retrieve key from handle
            //
            KeyOfHandle& keyGetter,

            // Delegate to release resources associated with handle
            //
            HandleDestructor& cleanup
            )
            : m_keyring(keyGetter)
            , m_cleanHandle(cleanup)
            , m_power(power)
            , m_buckets(1ULL << power)
            , m_size(0)
        {
            Audit::Assert(m_power <= 32, 
                "Need new hash functions to support large table");
            Audit::Assert(m_power >= 8 || UsePartialKeyTag == false,
                "No point using partial key tag when the table is small");
        }

        // clear removes all the elements in the hash table, calling their
        // destructors.
        void Clear() {
            for (uint64_t i = 0; i < (1ULL << m_power); i++){
                m_buckets[i].ClearBucket(*this);
            }
        }

        size_t Size() const {
            return m_size;
        }

        bool Empty() const {
            return Size() == 0;
        }

        // Locate the handle associated with the key. Returns INV_HANDLE
        // if the key is not found.
        // 
        Handle Find(const Key& key) const
        {
            Handle handle = INV_HANDLE;

            size_t stdhash = m_hash(key);
            BucketIndex i1;
            BucketIndex i2;
            DoubleHashFuncs(stdhash, i1, i2);

            size_t empty;
            size_t foundSlot = m_buckets[i1].
                SearchBucket(*this, key, i2, handle, empty);
            if (foundSlot >= SLOT_PER_BUCKET) {
                foundSlot = m_buckets[i2].
                    SearchBucket(*this, key, i1, handle, empty);
                if (foundSlot >= SLOT_PER_BUCKET){
                    handle = INV_HANDLE;
                }
            }

            return handle;
        }

        bool Contains(const Key& key) const {
            return Find(key) != INV_HANDLE;
        }

        bool Erase(const Key& key) {
            size_t stdhash = m_hash(key);
            BucketIndex i1 ;
            BucketIndex i2 ;
            DoubleHashFuncs(stdhash, i1, i2);

            size_t slot;
            auto updated = m_buckets[i1].UpdateHandle(*this, key, INV_HANDLE, i2, slot);
            if (updated >= SLOT_PER_BUCKET){
                updated = m_buckets[i2].UpdateHandle(*this, key, INV_HANDLE, i1, slot);
            }

            if (updated < SLOT_PER_BUCKET)
                m_size--;

            return (updated < SLOT_PER_BUCKET);
        }

        // Insert a new entry that associates the key with the handle
        // if key already exists, handle will be changed to the value
        // already associated with the existing key
        //
        CuckooStatus Insert(const Key& key, _Inout_ Handle& handle)
        {
            size_t stdhash = m_hash(key);
            BucketIndex b1;
            BucketIndex b2;
            DoubleHashFuncs(stdhash, b1, b2);

            size_t slot1, slot2;
            Handle prevVal;

            auto foundSlot = m_buckets[b1].SearchBucket(*this, key, b2, prevVal, slot1);
            if (foundSlot < SLOT_PER_BUCKET){
                handle = prevVal;
                return CuckooStatus::KeyDuplicated;
            }
            foundSlot = m_buckets[b2].SearchBucket(*this, key, b1, prevVal, slot2);
            if (foundSlot < SLOT_PER_BUCKET) {
                handle = prevVal;
                return CuckooStatus::KeyDuplicated;
            }
            if (slot1 < SLOT_PER_BUCKET) {
                m_buckets[b1].InsertNew(slot1, handle, (uint16_t)b2);
                m_size++;
                return CuckooStatus::Ok;
            }
            if (slot2 < SLOT_PER_BUCKET) {
                m_buckets[b2].InsertNew(slot2, handle, (uint16_t)b1);
                m_size++;
                return CuckooStatus::Ok;
            }

            return InsertInteral(handle, stdhash, b1, b2);
        }

        // Try to update the handle associated with the key. If the key
        // does not exists, it is inserted.
        // returns false when insertion failed (map full)
        //
        CuckooStatus Update(const Key& key, const Handle handle)
        {
            size_t stdhash = m_hash(key);
            BucketIndex b1;
            BucketIndex b2;
            DoubleHashFuncs(stdhash, b1, b2);

            size_t slot1, slot2;

            auto updated = m_buckets[b1].UpdateHandle(*this, key, handle, b2, slot1);
            if (updated < SLOT_PER_BUCKET)
                return CuckooStatus::Ok;

            updated = m_buckets[b2].UpdateHandle(*this, key, handle, b1, slot2);
            if (updated < SLOT_PER_BUCKET)
                return CuckooStatus::Ok;

            // not found, try insert

            if (slot1 < SLOT_PER_BUCKET) {
                m_buckets[b1].InsertNew(slot1, handle, (uint16_t)b2);
                m_size++;
                return CuckooStatus::Ok;
            }
            if (slot2 < SLOT_PER_BUCKET) {
                m_buckets[b2].InsertNew(slot2, handle, (uint16_t)b1);
                m_size++;
                return CuckooStatus::Ok;
            }

            return InsertInteral(handle, stdhash, b1, b2);
        }

    private:

        // Partial key.
        // We use lower 16 bit of the "alternative bucket index" as the
        // tag to reduce the needs for key fetch, see SILT paper
        //
        struct Tag {
        private:
            uint16_t tag_[SLOT_PER_BUCKET];
        public:
            const uint16_t& HashTag(uint64_t i) const {
                return tag_[i];
            }

            uint16_t& HashTag(uint64_t i) {
                return tag_[i];
            }
        };

        // Place holder for when partial key is not used
        struct InvalidTag {
        private:
        public:
            const uint16_t& HashTag(uint64_t i) const {
                Audit::OutOfLine::Fail
                    ("Tag should not be used when UsePartialKeyTag is false");
            }

            uint16_t& HashTag(uint64_t i) {
                Audit::OutOfLine::Fail
                    ("Tag should not be used when UsePartialKeyTag is false");
            }
        };

        // The Bucket type holds SLOT_PER_BUCKET tags and handles.
        //
        struct Bucket : public std::conditional<
            UsePartialKeyTag, Tag, InvalidTag>::type
        {
            std::array<Handle, SLOT_PER_BUCKET > handles_;

            Bucket() {
                handles_.fill(INV_HANDLE);
            }

            bool Occupied(size_t i) const {
                return handles_[i] != INV_HANDLE;
            }

            size_t FindCell(
                _In_ const CuckooIndex& table,
                _In_ const Key &key,
                _In_ BucketIndex other,
                _Out_ size_t& emptyCell
                ) const
            {
                // TODO scan 16 or 32 bytes at a time using AVX instructions.
                emptyCell = SLOT_PER_BUCKET;
                uint64_t j = 0;
                for (; j < SLOT_PER_BUCKET; ++j) {
                    if (!Occupied(j)) {
                        emptyCell = j;
                        continue;
                    }
                    __pragma(warning(push))
                    __pragma(warning(disable:4127))
                    if (UsePartialKeyTag) {
                        if ((uint16_t)other != HashTag(j)) {
                            continue;
                        }
                    }
                    __pragma(warning(pop))

                    Handle h = handles_[j];
                    if (table.m_keyEqual(key, table.m_keyring(h))) {
                        return j;
                    }
                }
                return j;
            }

            //     Search for the given key in this bucket. if found,
            // associated handle is returned via parameter "val".
            //     This function also try to find an empty slot at the same
            // time. Index of the empty cell is returned via parameter 
            // "emptyCell" (SLOT_PER_BUCKET if there is no empty cell).
            //     Return index of the cell containing the key.
            // 
            size_t SearchBucket(
                _In_ const CuckooIndex& table,
                _In_ const Key &key,
                _In_ BucketIndex other,
                _Out_ Handle &val,
                _Out_ size_t& emptyCell
                ) const
            {
                size_t index = FindCell(table, key, other, emptyCell);
                if (index >= SLOT_PER_BUCKET) {
                    return SLOT_PER_BUCKET;
                }
                val = handles_[index];
                return index;
            }

            // Try to update the handle associated with a certain key.
            // Parameter "emptyCell" returns the index of an empty cell in the
            // bucket, or SLOT_PER_BUCKET if there is no empty cell.
            // Returns the cell index where the key is found and updated
            // 
            size_t UpdateHandle(
                _In_ const CuckooIndex& table,
                _In_ const Key &key,
                _In_ const Handle val,
                _In_ BucketIndex other,
                _Out_ size_t& emptyCell
                )
            {
                size_t index = FindCell(table, key, other, emptyCell);
                if (index >= SLOT_PER_BUCKET) {
                    return SLOT_PER_BUCKET;
                }
                if (Occupied(index)){
                    table.m_cleanHandle(handles_[index]);
                }
                handles_[index] = val;
                return index;
            }

            // Insert a new value into a slot
            //
            void InsertNew(size_t slot, Handle val, uint16_t tag)
            {
                Audit::Assert(slot < SLOT_PER_BUCKET && !Occupied(slot),
                    "Invalid slot to insert new item");
                __pragma(warning(push));
                __pragma(warning(disable:4127));
                if (UsePartialKeyTag){
                    HashTag(slot) = tag;
                }
                __pragma(warning(pop));
                handles_[slot] = val;
            }

            void ClearBucket(
                _In_ const CuckooIndex& table
                )
            {
                for (size_t i = 0; i < SLOT_PER_BUCKET; i++){
                    if (Occupied(i)){
                        table.m_cleanHandle(handles_[i]);
                    }
                    handles_[i] = INV_HANDLE;
                }
            }

        };

        // we should have 2 ** m_power number of buckets
        const uint64_t m_power;

        // Buckets of hash cells
        // We expect user to provide locking.
        std::vector<Bucket> m_buckets;

        std::atomic<size_t> m_size;

        // delegate to retrieve the key from handle
        KeyOfHandle const m_keyring;

        // delegate for deciding keys are equal
        Pred m_keyEqual;

        // delegate for hash function
        Hash m_hash;

        // delegate for removing a handle
        mutable HandleDestructor m_cleanHandle;

        // Data structures and functions for insertion. In the case that both
        // of the bucket is full, we need to try to move one cell to its
        // alternative bucket, and recursively do the same when the destination
        // is also full. 
        //
        // Here we translate this recursive relocation to a BFS until an empty
        // slot is identified. 

        // The maximum number of items in a BFS path.
        static const uint8_t MAX_BFS_DEPTH = 3;

        // BfsPath holds the information for a BFS path through the table
        struct BfsPath {
            // a compressed representation of the slots for each of the buckets in
            // the path. pathcode is sort of like a base-SLOT_PER_BUCKET number, and
            // we need to hold at most MAX_BFS_DEPTH slots. Thus we need the
            // maximum pathcode to be at least SLOT_PER_BUCKET^(MAX_BFS_DEPTH)
            size_t pathcode;
            static_assert((SLOT_BITS * MAX_BFS_DEPTH) <
                sizeof(decltype(pathcode)) * 8,
                "pathcode may not be large enough to encode a cuckoo"
                " path");
            static_assert(!std::is_signed<decltype(pathcode)>::value,
                "pathcode should be unsigned for bit operations"
                );

            // The bucket of the last item in the path
            BucketIndex bucket[MAX_BFS_DEPTH];

            // The 0-indexed position in the cuckoo path this slot occupies. It must
            // be less than MAX_BFS_DEPTH, and also able to hold negative values.
            uint8_t depth;
            static_assert(MAX_BFS_DEPTH <=
                ((size_t)1 << (sizeof(decltype(depth)) * 8 - 1)),
                "The depth type must able to hold a value of"
                " MAX_BFS_DEPTH - 1");
            static_assert(!std::is_signed<decltype(depth)>::value,
                "use unsigned for depth index");

            BfsPath() {}
        };

        // Queue built with circular buffer
        class Cqueue {

            // The maximum size of the BFS queue. Unless it's less than
            // SLOT_PER_BUCKET^MAX_BFS_DEPTH, it won't really mean anything.
            static const size_t MAX_QUE_SIZE = 64;

            static_assert((MAX_QUE_SIZE & (MAX_QUE_SIZE - 1)) == 0,
                "MAX_CUCKOO_COUNT should be a power of 2");

            BfsPath m_slots[MAX_QUE_SIZE];

            size_t m_first;

            size_t m_last;

        public:
            Cqueue() : m_first(0), m_last(0) {}

            BfsPath& enqueue() {
                Audit::Assert(!full(),"Cuckoo BFS search out of bound.");
                BfsPath& ret = m_slots[m_last];
                m_last = (m_last + 1) & (MAX_QUE_SIZE - 1);
                return ret;
            }

            BfsPath& dequeue() {
                Audit::Assert(!empty(), "Cuckoo BFS search dequeue exception.");
                auto r = m_first;
                m_first = (m_first + 1) & (MAX_QUE_SIZE - 1);
                return m_slots[r];
            }

            bool empty() {
                return m_first == m_last;
            }

            bool full() {
                return ((m_last + 1) & (MAX_QUE_SIZE - 1)) == m_first;
            }
        };

        // Breath first search to find an empty slot, and a relocation
        // path for insertion of a new item. give up if queue is full
        // or depth limit reached.
        //
        BfsPath SearchRelocPath(const BucketIndex i1, const BucketIndex i2) {
            Cqueue q;
            // The initial pathcode informs cuckoopath_search which bucket the path
            // starts on
            BfsPath& first = q.enqueue();
            first.depth = 0;
            first.bucket[0] = i1;
            first.pathcode = 0;

            BfsPath& second = q.enqueue();
            second.depth = 0;
            second.bucket[0] = i2;
            second.pathcode = 1;

            while (!q.full() && !q.empty()) {
                BfsPath& x = q.dequeue();
                // Picks a (sort-of) random slot to start from
                size_t starting_slot = x.pathcode % SLOT_PER_BUCKET;
                for (size_t i = 0; i < SLOT_PER_BUCKET && !q.full(); i++) {
                    size_t slot = (starting_slot + i) % SLOT_PER_BUCKET;
                    if (!m_buckets[x.bucket[x.depth]].Occupied(slot)) {
                        // Found an empty slot
                        x.pathcode = x.pathcode * SLOT_PER_BUCKET + slot;
                        return x;
                    }

                    // search on, the edge we follow are actually relationships
                    // between two hash functions. 
                    // we use path code to remember all the slots on the search
                    // steps
                    if (x.depth < MAX_BFS_DEPTH - 1) {

                        BucketIndex other;
                        if (UsePartialKeyTag && m_power <= 16){
                            // http://www.cs.cmu.edu/~fawnproj/papers/silt-sosp2011.pdf
                            // We use the lower 16 bit of alternative bucket
                            // index as the partial key tag
                            other = m_buckets[x.bucket[x.depth]].HashTag(slot);
                        }
                        else {
                            Handle h = m_buckets[x.bucket[x.depth]].handles_[slot];
                            const size_t hv = m_hash(m_keyring(h));

                            BucketIndex b1, b2;
                            DoubleHashFuncs(hv, b1, b2);
                            other = (x.bucket[x.depth] == b1) ? b2 : b1;
                        }

                        BfsPath& y = q.enqueue();
                        y = x;
                        y.depth = x.depth + 1;
                        y.bucket[y.depth] = other;
                        y.pathcode = x.pathcode * SLOT_PER_BUCKET + slot;
                    }
                }
            }
            // We didn't find a short-enough cuckoo path, so the queue ran out of
            // space. Return a failure value.
            BfsPath invalid;
            invalid.depth = MAX_BFS_DEPTH + 1;
            return invalid;
        }

        // We have a relocation path, of which the end slot is empty.
        // we need to shift the cell to fill the end slot, and vacate
        // the first slot for insertion.
        //
        void CuckooShift(_In_ const BfsPath& cPath,
            _Out_ BucketIndex& bucket,
            _Out_ size_t& slot)
        {
            auto depth = cPath.depth;
            auto pathCode = cPath.pathcode;
            while (depth > 0)
            {
                BucketIndex fromBucket = cPath.bucket[depth - 1];
                BucketIndex toBucket = cPath.bucket[depth];
                size_t toSlot = pathCode % SLOT_PER_BUCKET;

                pathCode /= SLOT_PER_BUCKET;
                size_t fromSlot = pathCode % SLOT_PER_BUCKET;

                Audit::Assert(!m_buckets[toBucket].Occupied(toSlot),
                    "Cuckoo path corrupted!");
                m_buckets[toBucket].handles_[toSlot] = m_buckets[fromBucket].handles_[fromSlot];
                __pragma(warning(push));
                __pragma(warning(disable:4127));
                if (UsePartialKeyTag){
                    m_buckets[toBucket].HashTag(toSlot) = static_cast<uint16_t>(fromBucket);
                }
                __pragma(warning(pop));

                m_buckets[fromBucket].handles_[fromSlot] = INV_HANDLE;

                bucket = fromBucket;
                slot = fromSlot;
                depth--;
            }
            slot = pathCode % SLOT_PER_BUCKET;
            bucket = cPath.bucket[0];
            // path code should be 0 or 1
        }

        // This should only be called when we have to perform relocation
        // This function invokes an BFS search to find a relocation
        // path, to vacate a cell in one of the two hash locations
        // to insert the new value
        //
        CuckooStatus InsertInteral(
            const Handle val, const size_t stdHash,
            const BucketIndex i1, const BucketIndex i2)
        {
            BucketIndex insert_bucket = ~0UL;
            size_t insert_slot = ~0ULL;

            BfsPath x = SearchRelocPath(i1, i2);
            if (x.depth > MAX_BFS_DEPTH) {
                return CuckooStatus::TableFull;
            }

            CuckooShift(x, insert_bucket, insert_slot);
            Audit::Assert(insert_bucket == i1 || insert_bucket == i2,
                "Cuckoo shift returned wrong bucket!");
            Audit::Assert(insert_slot < SLOT_PER_BUCKET &&
                !m_buckets[insert_bucket].Occupied(insert_slot),
                "CuckooShift returned wrong slot!");

            auto alternative = (uint16_t)((insert_bucket == i1) ? i2 : i1);

            m_buckets[insert_bucket].InsertNew(insert_slot, val, alternative);
            m_size++;

            __pragma(warning(push));
            __pragma(warning(disable:4127));
            if (UsePartialKeyTag){
                BucketIndex b1, b2;
                DoubleHashFuncs(stdHash, b1, b2);

                BucketIndex other = m_buckets[insert_bucket].HashTag(insert_slot);
                Audit::Assert( insert_bucket == b1 && other == b2 
                    || insert_bucket == b2 && other == b1,
                    "Cuckoo tag mismatch!");
            }
            __pragma(warning(pop));

            return CuckooStatus::Ok;
        }


    };

    // CuckooTable is a fixed size hash map using Cuckoo hashing. This
    // implementation uses max load ratio of 70%
    //
    // The table has no locking mechansim. Users should grab writer lock for
    // Insert, Delete, and reader lock for Find.
    //
    // Here key value pairs are stored in a seperate array instead of right in
    // hash buckets. The reason is that Cuckoo Insert may bump things around.
    // We don't want an iterator to be invalidated by an irrelavent insert.
    //
    // Downside of this level of indirection may have slight performance
    // impact, when checking for keys in a bucket, we may have to retrieve keys
    // from 4 different places, which may result in cache misses. Partial
    // key tags are used to reduce number of key accesses.
    // 
    template <typename Key, typename Value, unsigned MAX_LOAD_RATIO = 70,
        bool UsePartialKeyTag = true,
        class Hash = std::hash<Key>,
        class Pred = std::equal_to<Key>
    >
    class CuckooTable {
    private:
        typedef std::pair<Key, Value> entry_type;

        typedef std::allocator<entry_type> EntryAllocator;

        // make it uint32_t if we need a bigger table
        typedef uint16_t IndexType;

        static const IndexType INV_INDEX = static_cast<IndexType>(~0);

        struct KeyOfEntry
        {
            const CuckooTable* const m_table;

            KeyOfEntry(const CuckooTable* table)
                : m_table(table)
            {}

            const Key& operator()(IndexType h) const
            {
                return m_table->GetEntry(h).first;
            }

        private:
            KeyOfEntry& operator=(const KeyOfEntry&) = delete;
        } m_keyDelegate;

        struct CleanEntry
        {
            CuckooTable* m_pTable = nullptr;

            CleanEntry() {}

            CleanEntry(CuckooTable* pTable)
                : m_pTable(pTable)
            {}

            void operator()(IndexType h)
            {
                Audit::Assert(m_pTable->m_busyEntries[h],
                    "Duplicated removal of entry in CuckooTable");
                m_pTable->m_busyEntries[h] = false;
                m_pTable->m_allocator.destroy(&m_pTable->GetEntry(h));
                m_pTable->FreeEntry(h);
            }
        } m_deleter;

        EntryAllocator m_allocator;

        // Vector for all the entries.
        // The reason that we put the entries in a seperate vector, instead of
        // storing them directly inside the hash buckets, is that the cuckoo
        // insertion bumps things around. An iterator may be invalidated by
        // unrelated insertion. We want to provide stable iterators so that
        // the whole table can be traversed
        //
        std::vector< typename std::aligned_storage <
            sizeof(entry_type),
            std::alignment_of<Key>::value>::type>
            m_entryTable;

        std::stack<IndexType> m_freeEntries;
        std::unique_ptr<bool[]> m_busyEntries;

        const size_t m_capacity;

        CuckooIndex < Key, IndexType, INV_INDEX, KeyOfEntry, UsePartialKeyTag, CleanEntry, Hash, Pred >
            m_cuckoo;

        const entry_type& GetEntry(IndexType i) const {
            const entry_type* p = reinterpret_cast<const entry_type*>(&m_entryTable[i]._Val);
            return *p;
        }

        entry_type& GetEntry(IndexType i) {
            entry_type* p = reinterpret_cast<entry_type*>(&m_entryTable[i]._Val);
            return *p;
        }

        IndexType AllocEntry(){
            Audit::Assert(!m_freeEntries.empty(),
                "Expected full table in CuckooTable!");
            IndexType i = m_freeEntries.top();
            m_freeEntries.pop();
            return i;
        }

        void FreeEntry(IndexType i){
            Audit::Assert(m_freeEntries.size() < m_capacity,
                "Double entry free detected in CuckooTable");
            m_freeEntries.push(i);
        }

        CuckooStatus InsertInternal(_In_ const Key& key, _Out_ IndexType& space)
        {
            if (Size() >= m_capacity){
                space = INV_INDEX;
                return CuckooStatus::TableFull;
            }

            IndexType newSlot = AllocEntry();
            Audit::Assert(!m_busyEntries[newSlot],
                "newly allocated entry should not be busy!");

            space = newSlot;

            CuckooStatus status = m_cuckoo.Insert(key, space);

            if (status == CuckooStatus::Ok){
                Audit::Assert(space == newSlot, "Insertion failed!");
                return CuckooStatus::Ok;
            }

            Audit::Assert(status != CuckooStatus::TableFull,
                "Cuckoo table index full before entry array full?");
            FreeEntry(newSlot);

            if (status == CuckooStatus::KeyDuplicated){
                Audit::Assert(space != newSlot && m_busyEntries[space],
                    "Found entry should be busy!");
            }
            else {
                Audit::OutOfLine::Fail("unexpected insertion status!");
            }
            return status;
        }

        // Compute the "real" capacity of the Cuckoo map,
        // return the smallest Power where
        // real_capacity = ((2** Power) / load_ratio) && real_capacity >= capacity;
        // 
        static size_t ComputeCuckooPower(size_t capacity){
            double cells = (double)capacity * 100.0 / (MAX_LOAD_RATIO);

            double p = ceil(log2(cells / (double)SLOT_PER_BUCKET));
            size_t power = (size_t)(p <= 0 ? 1.0 : p);
            Audit::Assert(capacity <= (1ULL << power) * SLOT_PER_BUCKET,
                "Bug in computing cuckoo power");
            return power;
        }

        static size_t ComputeCapacity(size_t power){
            size_t capacity = (size_t)floor((1ULL << power) * SLOT_PER_BUCKET * MAX_LOAD_RATIO / 100.0);
            return capacity;
        }

    public:

        // Iterators
        // The iterator may give out an invalid or even repurposed entry when
        // overlapped with erase
        // 
        class const_iterator {
        protected:
            CuckooTable* m_pTable;
            IndexType m_pos;

            void Advance(){
                m_pos++;
                for (; m_pos < m_pTable->m_capacity; m_pos++){
                    if (m_pTable->m_busyEntries[m_pos]){
                        break;
                    }
                }
                if (m_pos >= m_pTable->m_capacity){
                    m_pos = INV_INDEX;
                }
            }

        public:
            const_iterator(CuckooTable& table, IndexType pos = 0)
                : m_pTable(&table)
                , m_pos(pos)
            {
                if (m_pos >= m_pTable->m_capacity){
                    m_pos = INV_INDEX;
                }
                else if (!m_pTable->m_busyEntries[m_pos]){
                    Advance();
                }
            }

            bool operator==(const const_iterator& other) const {
                return m_pTable == other.m_pTable && m_pos == other.m_pos;
            }

            bool operator!=(const const_iterator& other) const {
                return !operator==(other);
            }

            const entry_type& operator*() const {
                Audit::Assert(m_pos < m_pTable->m_capacity,
                    "Dereference of invalid CuckooTable iterator!");
                return m_pTable->GetEntry(m_pos);
            }

            const entry_type* operator->() {
                return &(operator*());
            }

            // we don't check validity of the current position
            // as it may just be deleted. But we do check for
            // validity of the destination
            //
            const_iterator& operator++(){
                Advance();
                return *this;
            }

        };

        // The iterator may give out an invalid entry in face of a concurrent
        // erase
        class iterator : public const_iterator {
        public:
            iterator(CuckooTable& table, IndexType pos = 0)
                : const_iterator(table, pos)
            { }

            iterator& operator=(const iterator& other){
                m_pTable = other.m_pTable;
                m_pos = other.m_pos;
                return *this;
            }

            entry_type& operator*() {
                Audit::Assert(m_pos < m_pTable->m_capacity,
                    "Dereference of invalid CuckooTable iterator!");
                return m_pTable->GetEntry(m_pos);
            }

            entry_type* operator->() {
                return &(operator*());
            }
        };

        iterator begin() { return iterator(*this, 0); }
        iterator end() { return iterator(*this, INV_INDEX); }

        const_iterator cbegin() { return const_iterator(*this, 0); }
        const_iterator cend() { return const_iterator(*this, INV_INDEX); }

        CuckooTable(size_t capacity)
            : m_capacity(ComputeCapacity(ComputeCuckooPower(capacity)))
            , m_entryTable(ComputeCapacity(ComputeCuckooPower(capacity)))
            , m_keyDelegate(this)
            , m_deleter(this)
            , m_cuckoo(ComputeCuckooPower(capacity), m_keyDelegate, m_deleter)
        {
            Audit::Assert(ComputeCuckooPower(capacity) <= 14, 
                "make IndexType 32 or 64 bit if we need to support bigger table");
            m_busyEntries = std::unique_ptr<bool[]>(new bool[m_capacity]);

            for (IndexType i = 0; i < m_capacity; i++)
            {
                m_freeEntries.push(i);
                m_busyEntries[i] = false;
            }
        }

        // clear removes all the elements in the hash table, calling their
        // destructors.
        void Clear() {
            m_cuckoo.Clear();
            // destruction of entries and releasing of entry spaces
            // would be handled by deleter.
        }

        size_t Size() const {
            return m_cuckoo.Size();
        }

        bool Empty() const {
            return Size() == 0;
        }

        size_t Capacity() const {
            return m_capacity;
        }

        // Locate the value associated with the key. Returns nullptr
        // if the key is not found.
        // TODO!! return iterator after I learn more about type traits
        // 
        iterator Find(const Key& key)
        {
            IndexType i = m_cuckoo.Find(key);

            if (i == INV_INDEX){
                return end();
            }

            Audit::Assert(i < m_capacity && m_busyEntries[i],
                "CuckooTable find invalid item!");
            return iterator(*this, i);
        }

        bool Contains(const Key& key) const {
            return m_cuckoo.Find(key) != INV_INDEX;
        }

        bool Erase(const Key& key) {
            return m_cuckoo.Erase(key);
            // removing of the entry handled by deleter
        }

        std::pair<iterator, CuckooStatus> Insert(entry_type&& entry)
        {
            IndexType space;

            auto status = InsertInternal(entry.first, space);

            if (status == CuckooStatus::Ok){
                GetEntry(space) = std::move(entry);
                m_busyEntries[space] = true;
            }

            return std::make_pair(iterator(*this, space), status);
        }

        // Insert an item into the table, where the Value
        // object is constructed in place. 
        // The first parameter is the key, the rest of parameters
        // are passed into Value's constructor
        //
        // Note that the constructor is only called when
        // insertion is successful.
        //
        template <typename... _Types>
        std::pair<iterator, CuckooStatus> Insert(const Key& key, _Types&&... _args){
            IndexType space;
            auto status = InsertInternal(key, space);
            if (status == CuckooStatus::Ok){
                entry_type& entry = GetEntry(space);
                entry.first = key;
                Value* ptr = &entry.second;
                new (ptr) Value(std::forward<_Types>(_args)...);
                m_busyEntries[space] = true;
            }

            return std::make_pair(iterator(*this, space), status);
        }

    };

};
