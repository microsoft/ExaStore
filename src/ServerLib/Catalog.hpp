#pragma once

#include "stdafx.h"

#include "Utilities.hpp"
#include "Datagram.hpp"
#include "Scheduler.hpp"

namespace Schedulers
{
    class ContinuationBase;
}

namespace Exabytes
{    
    using namespace Schedulers;

	class AddressAndSize
	{
	public:
		static const uint64_t ADDRESSALIGN = 16;
		static const int ADDRESSALIGNBITS = 4;
		static const int ADDRESSBITS = 40;
		static const int SIZEBITS = 24;

        static const uint64_t INVALIDADDRESS = ((1ULL << ADDRESSBITS) - ADDRESSALIGN);

        static const size_t VALUE_SIZE_LIMIT = (1UL << 20);

	private:
		uint64_t m_address : ADDRESSBITS;       // 16TB with 16byte alignment
		uint64_t m_size : SIZEBITS;             // 16MB

    public:

        AddressAndSize(uint64_t address, size_t size)
		{
            static_assert(sizeof(AddressAndSize) == sizeof(uint64_t), "AddressAndSize should fit into 64bit integer.");
			Audit::Assert(0 == (address & (ADDRESSALIGN - 1)), "address needs to align to 16 byte boundary");
			Audit::Assert(address < (ADDRESSALIGN << ADDRESSBITS), "address value overflow");
			Audit::Assert(size < (1UL << SIZEBITS), "size overflow");

			m_size = size;
			m_address = address >> ADDRESSALIGNBITS;
		}

        // construct an invalid value by default
        AddressAndSize()
            : AddressAndSize(INVALIDADDRESS, 0)
        {}
        
        uint32_t Size() const { return m_size; }
		uint64_t FullAddress() const { return m_address << ADDRESSALIGNBITS; }

		uint64_t Raw() const { return *reinterpret_cast<uint64_t const *>(this); }

        static const AddressAndSize& Reinterpret(const uint64_t& val){
            AddressAndSize const* ptr = reinterpret_cast<AddressAndSize const*>(&val);
            return *ptr;
        }

        bool AddressAndSize::operator==(const AddressAndSize& other) const { return this->Raw() == other.Raw(); }
        bool AddressAndSize::operator!=(const AddressAndSize& other) const { return this->Raw() != other.Raw(); }

		bool IsVoid() const { return FullAddress() == INVALIDADDRESS && Size() == 0; }
	};

    class Partition;

    // transformations of hashes to and from smaller bit widths
    //
    namespace Reduction
    {
        // reserve valueS for invalid address and size
        const uint32_t UNUSED_ADDRESS = ~0UL;
        const uint16_t INVALID_SIZE = 0;

        // Key values less than 64k are reserved for non-key purposes.
        const unsigned RESERVEDVALUES = 65536;

        // block lazy ways the seed could be inadequate
        inline bool SeedIsAdequate(uint64_t seed)
        {
            return 0 != (seed >> 32) && (seed >> 32) != (uint32_t) seed;
        }

        // Reduce the key to just the number of bits we keep in the hash pages.
        // The hash pages will be recompressed so these are the only bits allowed.
        // The seed changes with each recompression, to help prevent DOS.
        //
        inline uint64_t HalveKey(const Datagram::Key128& key)
        {
            return key.ValueHigh() ^ key.ValueLow();
        }

		inline uint32_t To28BitKey(uint64_t halfKey, uint64_t seed)
		{
			Audit::ArgRule(SeedIsAdequate(seed), "seed must be a rich random 64 bit pattern");

			auto key = halfKey ^ seed;
			uint32_t rotation = key & 63;
			key = (key << rotation) | (key >> (64 - rotation));
			key ^= key >> 32;
			auto qKey = (uint32_t)key;

			// We need a 28 bit key. Therefore if the MSB 4 bits are not 0000 then 
			// RSH the key by 4 bits otherwise no need to RSH            
			uint8_t KeyMSB = qKey >> 28;
			qKey = (KeyMSB ^ 0) == 0 ? qKey : (qKey >> 4);

			// reserve a small number of key values to serve as special markers            
			return (RESERVEDVALUES < qKey) ? qKey
				: To28BitKey(halfKey, (seed << 1) | (seed >> 63));
		}

		inline uint32_t QuarterKey(uint64_t halfKey, uint64_t seed)
		{
			Audit::ArgRule(SeedIsAdequate(seed), "seed must be a rich random 64 bit pattern");

			auto key = halfKey ^ seed;
			uint32_t rotation = key & 63;
			key = (key << rotation) | (key >> (64 - rotation));
			key ^= key >> 32;
			auto qKey = (uint32_t)key;

			// reserve a small number of key values to serve as special markers
			// it will be extremely rare to need to recurse here.

			return (RESERVEDVALUES < qKey) ? qKey
				: QuarterKey(halfKey, (seed << 1) | (seed >> 63));
		}


        // The address must be a multiple of 16 bytes, and is compressed to 32 bits.
        inline uint32_t CompressAddress(uint64_t address)
        {
            Audit::Assert(0 == (address & 0xFu), "size must be 16 byte aligned");
            address >>= AddressAndSize::ADDRESSALIGNBITS;
            Audit::Assert(address < UNUSED_ADDRESS, "addresses must be smaller than 64 GB - temporary limit");

            return (uint32_t) (address);
        }

        // The rounded-up address is expanded to be a byte offset
        inline size_t ExpandAddress(uint32_t address)
        {
            return (size_t)address << AddressAndSize::ADDRESSALIGNBITS;
        }

        // The size rounds up to a multiple of 16 bytes and is compressed to 16 bits.
        inline uint8_t CompressSize(size_t size)
        {
            // TODO: start accepting sizes upto 16MB. First make the
            // coalescing buffer bigger
            // Audit::Assert(size < (1 << 24), "values may not exceed 16MB size");
            Audit::Assert(size < (1UL << 20), "values may not exceed 1MB size - temporary limit");
            Audit::Assert(size != INVALID_SIZE, "size can not be 0");
                        
            uint8_t size4Bit = 1;
            for (; size4Bit < 16; size4Bit++)
            {
                if (size <= (1 << (size4Bit)) * 1024)
                {
                    return size4Bit;
                }
            }
            Audit::OutOfLine::Fail(StatusCode::Unexpected, 
                "Internal error when compressing item size.");
            return INVALID_SIZE;
        }        

        // The rounded-up size is expanded to be a byte count.        
        inline size_t ExpandSize(uint8_t size)
        {
            if (size == 0)
                return size;

            return (size_t)((1 << (size)) * 1024);
        }

        // Convert the actual size to a rough estimate value
        // starting from 2K which goes up exponentially 32K, 64K...        
        inline size_t RoundUpSize(size_t size)
        {
            return ExpandSize(CompressSize(size));
        }
    };

    // map a hash-key to an actual location in the local storage space.
    // The location 64 bits will typically be divided into a number of different
    // sub-spaces (DRAM, SSD, HDD, for example), but such divisions within
    // the address space are opaque to the catalog.
    // Implementations must be thread-safe.
    //
    class Catalog
    {
    public:
        // Add an address associated with a key.
        // Does not check if the key already exists, the addition becomes the current value
        // and the old address may remain visible as a version.
        // Throws exception for out of memory.
        //
        virtual bool Add(Datagram::Key128 key,
            AddressAndSize addressAndSize) = 0;

        // Async Interface for retrieving the address of the specified key.
        // result AddressAndSize is posted to pContinuation's continuation handle (64bit).
        // possible false conlision may happen. Caller need to verify validity of the 
        // result by checking the store.
        //
        virtual void Locate(Datagram::Key128 key,
            AddressAndSize prior,
            Schedulers::ContinuationBase* pContinuation) = 0;

        // Assign a new address to a key-address instance already in the catalog.
        // The new address may be in the same sub-space or a new sub-space.
        // This operation does not create a new version.
        // Status is returned async in the continuation handle of pContinuation
        // S_OK: successfully relocated
        // E_CHANGED_STATE: old location not valid, nothing done
        //
        virtual void Relocate(Datagram::Key128 key,
            AddressAndSize oldLocation,
            AddressAndSize newLocation,
            Schedulers::ContinuationBase* pContinuation) = 0;

        // Inform the catalog that the item is evicted thus the entry
        // should be invalidated
        //
        virtual void Expire(Datagram::Key128 key,
            AddressAndSize oldLocation,
            Schedulers::ContinuationBase* pContinuation) = 0;

        // One catalog belongs to one partition.
        // this sets a "parent" pointer to its partition. 
        virtual void SetPartition(Partition* pPart) = 0;

        // Initiate the process for catalog shutdown
        // 
        virtual void RequestShutdown() = 0;

        // Garbage collector of the catalog
        //
        virtual void Compact(Schedulers::ContinuationBase* pNotify) = 0;

        // Set GC state = ON for this store
        virtual void SetGCStart() = 0;

        // Sets GC state = OFF for this store
        virtual void SetGCEnd() = 0;

        // Adds all the flushable blocks to catalog 
        // file store flush queue
        virtual void SweepToFileStore() = 0;        

        virtual void StartCatalogFileGC(Schedulers::ContinuationBase* pNotify) = 0;

        // Discards all the eligible hash blocks from in mem
        // hash block pool
        virtual void DiscardHashBlocks() = 0;

        // Returns a conservative estimate of the available space in catalog 
        // in-terms of number of entries
        //
        virtual size_t GetAvailableSpace() = 0;

    };       
}