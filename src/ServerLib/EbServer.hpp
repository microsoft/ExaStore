#pragma once

#include "stdafx.h"

#include "Exabytes.hpp"

namespace Exabytes
{
    extern std::unique_ptr<ExabytesServer> IsolatedServerFactory(
        std::unique_ptr<ServiceBrokers::ServiceBroker>& pBroker,

        // file I/O buffer pool, used by sweeper and GC 
        std::unique_ptr<CoalescingBufferPool>& pBufferPool,

        // table: owner => data policy
        std::unique_ptr<PolicyManager>& pPolicyManager,

        // Large page buffer shared by catalogs of all partitions
        // for storing in-memory hash blocks
        std::unique_ptr<CatalogBufferPool<>>& pHashPoolManager,

        // Large page buffer shared by catalogs of all partitions
        // for storing in-memory bloom key blocks
        std::unique_ptr<CatalogBufferPool<>>& pBloomPoolManager,

        // class to create catalog and file store to piece
        // together a partition
        //
        std::unique_ptr<PartitionCreater>& pPartitionCreater,

        // class to recover data from files
        std::unique_ptr<FileRecover>& pRecover,

        // partition hash function that maps key to partition
        Datagram::PartitionId (*hashFunc)(const Datagram::Key128),

        // factory for GcActor
		GcActor*(*gcFactory)(
            ExabytesServer& server,
            Schedulers::Activity& gcDriver,
            PartitionRotor& pStores
            )
        );

    extern std::unique_ptr<PolicyManager> PolicyTableFactory();

}
