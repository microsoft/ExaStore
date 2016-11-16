#pragma once

#include "stdafx.h"
#include <vector>

#include "Datagram.hpp"
#include "Scheduler.hpp"
#include "Utilities.hpp"

namespace PartitionMaps
{
    // Server rings are scattered across AZs.  We want to have a large enough
    // ring to get balance and availability, small enough for compact map.
    // Each partition hash-maps to a Ring, and then is explicitly assigned
    // within the Ring.
    //
    static const uint32_t RING_MAX_SERVERS = 256;

    // K-V partitions hash-map to a Ring.
    // Within each Ring there are a number of partitions.
    // The partitions will be balanced over the servers.
    //
    static const uint32_t RING_PARTITIONS = 16384;

    struct PartitionNews
    {
        uint32_t PartitionNumber;
        uint8_t CyclicVersion;
        uint8_t Primary;
        uint8_t Secondary;
        uint8_t Tertiary;
    };

    // news items are sent in small sets
    static const uint32_t MAX_NEWS_ITEMS = 16;

    struct GossipNews
    {
        uint32_t Count;
        PartitionNews Items[MAX_NEWS_ITEMS];

        bool IsEmpty() { return Count == 0; }
        bool IsFull() { return MAX_NEWS_ITEMS <= Count; }
    };

    // the gossip network fan-out does not need to be wider than this
    static const uint32_t MAX_NEIGHBORS = 8;

    template <unsigned capacity>
    struct ServerSet
    {
        uint32_t Count;
        Datagram::ServerAddress Addresses[capacity];

        ServerSet() : Count(0) {}
    };
    
    typedef ServerSet<MAX_NEIGHBORS> GossipNeighbors;

    typedef ServerSet<RING_MAX_SERVERS> ServerRing;

    class PartitionMap
    {
    protected:
        uint32_t m_ringCount;
        uint32_t m_ringCountNext;
        uint32_t m_partitionCount;

        PartitionMap(uint32_t ringCount)
            : m_ringCount(ringCount)
            , m_ringCountNext(ringCount)
            , m_partitionCount(ringCount * RING_PARTITIONS)
        {
        }

    public:
        uint32_t MapToPartitionId(Datagram::Key128 key);

        virtual bool TryPrimaryServer(uint32_t partitionId, _Out_ Datagram::ServerAddress& address) = 0;
        virtual bool TrySecondaryServer(uint32_t partitionId, _Out_ Datagram::ServerAddress& address) = 0;
        virtual bool TryTertiaryServer(uint32_t partitionId, _Out_ Datagram::ServerAddress& address) = 0;

        virtual void SetGossipListeners(const GossipNeighbors& neighbors) = 0;

        virtual void UpdateMapWithNews(const GossipNews& news) = 0;
    };

    extern std::unique_ptr<PartitionMap> PartitionMapFactory();

}