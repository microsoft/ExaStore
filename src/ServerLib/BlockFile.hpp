// BlockFile : a write-sequential, read random file contains block record of
// the same size. The file forms a circular buffer.
// Used to implemente hash block file and bloom shadow key block file.
//

#pragma once

#include "stdafx.h"

#include "Exabytes.hpp"
#include "FileManager.hpp"
#include "UtilitiesWin.hpp"
#include "ChainedFileStore.hpp"
#include "ReducedKeyMap.hpp"
#include "TestHooks.hpp"

#include <vector>
#include <queue>
#include <algorithm>
#include <string>
#include <unordered_map>

namespace Exabytes{
    static const size_t SSD_READ_ALIGNMENT = 512;

    // Max blocks BlockBuffer can hold
    // The capcity in bytes will vary based on the size of the blocks
    // For example for hash blocks BlockBuffer capacity will be 2MB whereas 
    // for bloom key blocks - 512KB
    static const size_t BlockCapacity = 512;

    struct BlockIdentifier
    {
        uint16_t bucket;
        uint16_t blkId;
        uint32_t fileOffset;

        BlockIdentifier()
            :BlockIdentifier((uint16_t)CATALOGBUCKETCOUNT, INVALID_PAGE, INVALID_FILEOFFSET)
        {}

        BlockIdentifier(uint16_t buck, uint16_t blockId, uint32_t fileoffset = INVALID_FILEOFFSET)
            : bucket(buck)
            , blkId(blockId)
            , fileOffset(fileoffset)
        {}

        bool BlockIdentifier::operator==(const BlockIdentifier& other) const { return this->blkId == other.blkId; }
    };

    enum class BufferType
    {
        HashBlock,
        BloomKeyBlock
    };

    template<typename Block>
    class BlockFile;

    template<typename Block>
    class BlockBuffer
    {
        friend BlockFile<Block>;
    private:        

        // this is set by BlockFile after it finishes 
        // flushing this buffer to SSD
        // block offset is the index in file where the first block
        // from this buffer was written 
        size_t m_storeBlockOffset;

        // number of blocks currently held by this buffer
        uint32_t m_numBlocks;
        
        // header for the block which contains the block identifier        
        BlockIdentifier m_bufferIndex[BlockCapacity];

        void* m_pMemory;

    public:
       
        BlockBuffer()           
        {
            auto bufferCapacity = BlockCapacity * sizeof(Block);
            m_pMemory = ::_aligned_malloc(bufferCapacity, SECTORSIZE);
            Audit::Assert(m_pMemory != nullptr, "Failed to allocated buffer for catalog file read\\write");
            
            ZeroMemory(PData(0), bufferCapacity);

            m_numBlocks = 0;

            // initialize to invalid
            m_storeBlockOffset = Reduction::UNUSED_ADDRESS;
        }

        void* PData(size_t offset)
        {
            Audit::Assert(offset < BlockCapacity * sizeof(Block), "Index out of bounds!");
            return offset + (char*)(m_pMemory);
        }
        bool IsFull()
        {
            return BlockCapacity == m_numBlocks;
        }   

        bool IsEmpty()
        {
            return m_numBlocks == 0;
        }        

        size_t GetAvailableSize()
        {
            return (BlockCapacity - m_numBlocks) * sizeof(Block);
        }

        BlockIdentifier* GetBufferIndex()
        {
            return &m_bufferIndex[0];
        }

        // sets the block offset in file store for the
        // first block in this buffer
        // this is set by file store 
        void SetStoreOffset(size_t storeOffset)
        {
            m_storeBlockOffset = storeOffset / sizeof(Block);
        }

        // gets the file store block offset for the first block in 
        // this buffer
        size_t GetStoreBlockOffset()
        {
            return m_storeBlockOffset;
        }

        // Appends the given block to the buffer and updates the buffer index
        // Not thread safe
        StatusCode Append(void* pBlk, uint16_t bucket, uint16_t blockId)
        {
            if (IsFull())
            {                
                return StatusCode::OutOfMemory;
            }

            auto pDest = this->PData(m_numBlocks * sizeof(Block));
            memcpy_s(pDest, sizeof(Block), pBlk, sizeof(Block));
            m_bufferIndex[m_numBlocks] = BlockIdentifier{ bucket, blockId};
            m_numBlocks++;                                                                            
            return StatusCode::OK;
        }

        // resets the entire buffer
        void ResetBuffer()
        {
            // clear the buffer
            ZeroMemory(this->PData(0), BlockCapacity * sizeof(Block));           

            // reset block count
            m_numBlocks = 0;
        }

        ~BlockBuffer()
        {
            ::_aligned_free(m_pMemory);
            m_pMemory = nullptr;
        }
    };        

    template<typename Block>
    class BlockFile
    {    
    public: 
        virtual ~BlockFile() {};

        // Reads a block from catalog file store
        //
        virtual Utilities::StatusCode ReadBlock(
            // block offset in filestore
            uint32_t blockOffset,
            
            // pointer to destination buffer
            void* pDestBlob,

            // continuaiton to be triggered after read
            // this continuaiton should update catalog 
            _In_ Schedulers::ContinuationBase* pContinuation
            ) = 0;
        
        // Starts block write operation
        //
        virtual void StartBlockWrite(
            // block buffer which needs to be flushed
            BlockBuffer<Block>* pBuffer,

            // continuaiton to trigger after flush finishes
            // this continutaion should update the catalog
            ContinuationBase& next,
            
            // expected GC version. The catalog updater
            // will use this to decide whether to update the catalog
            // or discard this flush
            uint32_t expectedVersion) = 0;

        // Starts GC for catatlog file store
        //
        virtual void StartCatalogStoreGC(Schedulers::ContinuationBase* pNotify) = 0;

    };

    template<typename Block>
    std::unique_ptr<BlockFile<Block>>
        BlockFileFactory(
        // Asynchronous work should be posted using the scheduler.
        _In_ Schedulers::Scheduler& scheduler,

        // the name of the file to be used
        _In_ const wchar_t* pathName,
        //_In_ std::wstring&& pathName,
    
        _In_ size_t fileSize
        );
}
