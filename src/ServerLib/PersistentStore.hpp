// PersistentStore.hpp : an iNode system for Exabytes
//

#pragma once

#include "stdafx.h"

#include "Exabytes.hpp"

namespace Exabytes
{
    // in SSD
    extern std::unique_ptr<KeyValueStore> PersistentStoreFactory(
        // All work should be synchronous.  Asynchronous work should Schedule turns
        _In_ Schedulers::Scheduler& scheduler,

        // the catalog tracks the location of a keyed value in any store
        _In_ Catalog& catalog,

        // byte count SSD used for persistent primary
        size_t storeSize,

        // the KV store which reliably buffers active data.  Usually a MemoryStore.
        _In_ KeyValueStore& committer
        );
}
