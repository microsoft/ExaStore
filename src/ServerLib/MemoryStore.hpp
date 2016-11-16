// MemoryStore.hpp : an iNode system for Exabytes
//

#pragma once

#include "stdafx.h"

#include "Exabytes.hpp"

namespace Exabytes
{
    // blob store in DRAM
    extern std::unique_ptr<KeyValueInMemory> MemoryStoreFactory(
        _In_ ExabytesServer& server,

        /* the store size is the count of bytes of DRAM we will reserve for
        the MemoryStore.*/
        size_t storeSize
        );

    // Construct a memory store, an load the content from a file
    //
    extern std::unique_ptr<KeyValueInMemory> MemoryStoreFactory(
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
        Schedulers::ContinuationBase* pNotify
        );

}