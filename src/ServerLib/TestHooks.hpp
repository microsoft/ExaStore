
#pragma once

#include "stdafx.h"
#include "Exabytes.hpp"

namespace TestHooks
{
    
    class LocalStoreTestHooks {

    public:
        // initalizer should make sure this can only be initialized 
        // from a test dll
        LocalStoreTestHooks();

        std::unique_ptr<Exabytes::KeyValueOnFile> LocalStoreTestHooks::CreateChainedFileStore(
            // Turns are synchronous.  Asynchronous work should be posted using the scheduler.
            _In_ Schedulers::Scheduler& scheduler,

            // tracks the location of a keyed value in whichever store it currently resides.
            _In_ Exabytes::ExabytesServer& server,

            // the count of bytes of DRAM we will reserve for write buffer
            uint32_t writeBufferSize,

            // the name of the file to be used, the starting portion anyway
            const wchar_t* pathName,

            // Store Size to be created
            // will be rounded up to increments
            int64_t	initialSize = 0,

            // Unit size for growing or shrinking
            // always rounded up to SECTORSIZE
            // default value 10G
            // 
            int64_t increments = 0,

            // test hook
            bool    startGc = true
            );

        size_t SizeofChainedFileStore(Exabytes::KeyValueOnFile* pstore);

        void SetEdgesofChainedFileStore(Exabytes::KeyValueOnFile* pstore, size_t allocEdge, size_t trailingEdge);

        std::unique_ptr<Exabytes::KeyValueInMemory> CreateMemoryStore(
            _In_ Schedulers::Scheduler& scheduler,

            // the maximum number of objects we expect to buffer
            uint32_t bufferCapacityCount,

            // test hook
            bool startSweeper = true
            );

        size_t SizeOfMemoryStore(Exabytes::KeyValueInMemory* pstore);

        void SetEdgesOfMemoryStore(Exabytes::KeyValueInMemory* pstore, size_t allocEdge, size_t trailingEdge);

        // set values of collection size and almost empty to trigger GC faster
        void EnableQuickGC(Exabytes::KeyValueOnFile* pstore, double almost_empty, size_t collectionSize);
    };
}