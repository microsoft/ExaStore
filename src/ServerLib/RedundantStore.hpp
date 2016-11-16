// RedundantStore.hpp : an iNode system for Exabytes
//

#pragma once

#include "stdafx.h"

#include "Exabytes.hpp"
namespace Exabytes
{
    // in HDD
    extern KeyValueStore* RedundantBlobStoreFactory(
        // All work should be synchronous.  Asynchronous work should Schedule turns
        _In_ Schedulers::Scheduler& scheduler,

        // the catalog tracks the location of a keyed value in any store
        _In_ Catalog& catalog
        );
}
