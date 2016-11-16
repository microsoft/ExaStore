#pragma once

#include "stdafx.h"

#include "Exabytes.hpp"

namespace Exabytes
{
    extern std::unique_ptr<Partition> IsolatedPartitionFactory(
        _In_ ExabytesServer& server,
        _In_ std::unique_ptr<Catalog>& pCatalog,
        _In_ std::unique_ptr<KeyValueOnFile>& pFileStore,
        Datagram::PartitionId id
        );

    extern std::unique_ptr<Partition> PartitionReplicaFactory(
        _In_ ExabytesServer& server,
        _In_ std::unique_ptr<Catalog>& pCatalog,
        _In_ std::unique_ptr<KeyValueOnFile>& pFileStore,
        Datagram::PartitionId id
        );
}