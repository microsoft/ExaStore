#include "stdafx.h"

#include <array>

#include "PartitionMaps.hpp"
#include "ServiceBroker.hpp"

#include "UtilitiesWin.hpp"

#include <winsock2.h>
#include <ws2tcpip.h>
#include <Ws2def.h>
#include <Mswsock.h>
#include <iphlpapi.h>

using namespace std;
using namespace Datagram;
using namespace ServiceBrokers;
using namespace Utilities;

namespace  // local private namespace
{
    // lookaside will be used for secondary and tertiary map elements for those
    //   very few partitions currently browned out.  This allows us to keep the
    //   full secondary and tertiary maps only on SSD.
    //
    template <unsigned Size>
    class Lookaside
    {
        // we use a very simple lock-free rotating buffer.  The buffer is short and
        // scanned to the end.  If we ever need it to go faster, use AVX instructions
        // to do the scan of up to 8 values in parallel.  We probably will never
        // need a lookaside with more than 8.

        unsigned m_next = 0;
        uint64_t m_elements[Size];

        uint32_t KeyOf(uint64_t value) { return (uint32_t) value; }

        uint32_t ValueOf(uint64_t value) { return (uint32_t) (value >> 32); }

        // pack the key and value into 32 bits so our updates and reads will be atomic.
        uint64_t KeyAndValue(uint32_t key, uint32_t value) { return key | (((uint64_t) value) << 32); }

        uint64_t KeyAndValueEmpty() { return KeyAndValue(~0u, ~0u); }

    public:
        Lookaside()
        {
            m_next = 0;
            uint64_t empty = KeyAndValueEmpty();
            for (unsigned i = 0; i < Size; ++i)
            {
                m_elements[i] = empty;
            }
        }

        void Append(uint32_t key, uint32_t value)
        {
            uint64_t item = KeyAndValue(key, value);

            // the retry logic does not have to be perfect.  It just has to ensure
            // we NEVER make m_next out of range, and we RARELY will be raced.

        retry:
            unsigned current = m_next;
            unsigned next = (current + 1) % Size;
            if (! TryCompareExchange(&m_next, next, current))
                goto retry;

            // it is only a cache.  We don't care if this value is occasionally redundant
            //  or is occasionally overwritten.  OK, so long as not very often.

            m_elements[current] = item;
        }

        bool TryFind(uint32_t key, _Out_ uint32_t& value)
        {
            for (unsigned i = 0; i < Size; ++i)
            {
                auto item = m_elements[i];    // atomic read into a local copy

                if (key == KeyOf(item))
                {
                    value = ValueOf (item);
                    return true;
                }
            }
            value = 0;
            return false;
        }

        void Remove(uint32_t key, uint32_t /*versionGuard*/)
        {
            auto empty = KeyAndValueEmpty();
            for (unsigned i = 0; i < Size; ++i)
            {
                auto item = m_elements[i];    // atomic read into a local copy

                if (key == KeyOf(item))
                {
                    // Remove is idempotent.  Ignore the return value.
                    TryCompareExchange(m_elements + i, empty, item);
                    return;
                }
            }
        }
    };
}

namespace PartitionMaps
{
    uint32_t PartitionMap::MapToPartitionId(Datagram::Key128 key)
    {
        // something closer to crypto would be better here.  We don't want it to be trivial
        // to get related/predictable key values which map to the same partition.
        // With only 100M partitions and a requirement for this map to be durable, we
        //  cannot avoid deliberate misuse.  But we can make accidents very unlikely.

        return (uint32_t)((key.ValueHigh() >> 32) ^ key.ValueHigh() ^ (key.ValueLow() >> 32) ^ key.ValueLow()) % m_partitionCount;
    }

    typedef std::array<uint8_t, RING_PARTITIONS> RingMap;

    class PartitionMapCompletionQueue
    {
    private:
        SOCKET m_homeSocket{ INVALID_SOCKET };
        sockaddr_in m_homeAddress;
        
        ServiceBrokers::ServiceBroker* m_pBroker;

        unique_ptr<LargePageBuffer> m_pMemory;

        void Cleanup()
        {
            if (m_homeSocket != INVALID_SOCKET)
            {
                closesocket(m_homeSocket);
                m_homeSocket = INVALID_SOCKET;
            }
            m_pMemory = nullptr;
        }

    public:
        PartitionMapCompletionQueue(
            ServiceBrokers::ServiceBroker& broker
        )
        : m_pBroker(&broker)
        {
            ZeroMemory(&m_homeAddress, sizeof(m_homeAddress));
            m_pMemory = nullptr;

            // Create a socket for us to use.
            m_homeSocket = ::WSASocket(AF_INET, SOCK_DGRAM, IPPROTO_UDP, NULL, 0, WSA_FLAG_OVERLAPPED | WSA_FLAG_REGISTERED_IO);
            Audit::Assert(m_homeSocket != INVALID_SOCKET, "could not create local socket");

            Audit::Assert(DiscoverHomeAddress(m_homeSocket, m_homeAddress),
                "DiscoverHomeAddress failed");
        }

        ~PartitionMapCompletionQueue()
        {
            Cleanup();
        }

        void BindVerb(ServiceVerb verb, ServiceBrokers::VerbWorker& actor)
        {
            m_pBroker->BindVerb(verb, actor);
        }
    };

    // Gossip is a fault tolerant protocol.  Values are expected to be propagated through
    // multiple routes, creating redundant delivery and tolerating repetition.
    //
    class GossipOutput
    {
        GossipNeighbors m_neighbors;
        SOCKET m_neighborSockets[MAX_NEIGHBORS];

        GossipNews m_pending;
        GossipNews m_sending;
        SRWLOCK m_appendLock;
        SOCKET m_socket;
        sockaddr_in m_homeAddress;

        uint64_t    m_flushDeadline;
        uint64_t    m_flushIntervalLimit;

        // The broker will use an async pattern around one RIO Completion Queue
        //  and multiple sockets.  Sockets will be attached to the RIO CQ.
        //  - one socket listens for and accepts incoming TCP connections
        //  - one socket will handle the UDP activity for data
        //  - multiple sockets for TCP connections with neighbors and EB manager
        //
        void InitializeSocket()
        {
            ZeroMemory(&m_homeAddress, sizeof(m_homeAddress));

            //----------------------
            // Initialize Winsock
            WSADATA wsaData;
            int iResult = ::WSAStartup(MAKEWORD(2, 2), &wsaData);
            Audit::Assert(iResult == NO_ERROR);

            //----------------------
            // Create a SOCKET for our connections
            m_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
            if (m_socket == INVALID_SOCKET)
            {
                auto error = ::WSAGetLastError();
                WSACleanup();
                Audit::OutOfLine::Fail((StatusCode)error, "Failed to create partition map TCP socket.");
            }

            //----------------------
            // The sockaddr_in structure specifies the address family,
            // IP address, and port of the server to be connected to.
            if (!DiscoverHomeAddress(m_socket, m_homeAddress))
            { 
                auto error = ::WSAGetLastError();
                WSACleanup();
                Audit::OutOfLine::Fail((StatusCode)error, "DiscoverHomeAddress failed. ");
            }

            iResult = ::bind(m_socket, (SOCKADDR*) & m_homeAddress, sizeof(m_homeAddress));
            if (iResult == SOCKET_ERROR)
                {
                auto error = ::WSAGetLastError();
                WSACleanup();
                Audit::OutOfLine::Fail((StatusCode)error, "Failed to bind partition map TCP socket.");
            }

            for (unsigned i = 0; i < m_neighbors.Count; ++i)
            {
                m_neighborSockets[i] = INVALID_SOCKET;
            }
        }

        void ConnectNeighbors()
        {
            // we do not lock this process.  The fault tolerance allows us not
            // to worry if we get glitches due to changing the connections at the
            // same time as we use them for sending updates.

            for (unsigned i = 0; i < m_neighbors.Count; ++i)
            {
                SOCKET neighborSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
                if (neighborSocket == INVALID_SOCKET)
                {
                    auto error = ::WSAGetLastError();
                    WSACleanup();
                    Audit::OutOfLine::Fail((StatusCode)error, "Failed to create neighbor TCP socket.");
                }

                sockaddr_in neighborAddr;
                ZeroMemory(&neighborAddr, sizeof(neighborAddr));
                neighborAddr.sin_family = AF_INET;
                neighborAddr.sin_addr.S_un.S_addr = htonl(m_neighbors.Addresses[i].ServiceIPv4);
                neighborAddr.sin_port = htons(m_neighbors.Addresses[i].ServicePort);

                int iResult = connect(neighborSocket, (SOCKADDR *) & neighborAddr, sizeof(neighborAddr));
                if (iResult == SOCKET_ERROR)
                {
                    auto error = ::WSAGetLastError();
                    WSACleanup();
                    Audit::OutOfLine::Fail((StatusCode)error, "Failed to connect to neighbor socket. ");
                }
                if (m_neighborSockets[i] != INVALID_SOCKET)
                {
                    ::closesocket(m_neighborSockets[i]);
                }
                m_neighborSockets[i] = neighborSocket;
            }
        }

        void DisconnectNeighbors()
        {
            for (unsigned i = 0; i < m_neighbors.Count; ++i)
            {
                if (m_neighborSockets[i] != INVALID_SOCKET)
                {
                    ::closesocket(m_neighborSockets[i]);
                    m_neighborSockets[i] = INVALID_SOCKET;
                }
            }
        }

        void Flush()
        {
            LARGE_INTEGER timeNow;
            ::QueryPerformanceCounter(&timeNow);
            m_flushDeadline = timeNow.QuadPart + m_flushIntervalLimit;
            const void* pMessage = (const void*) &m_sending;
            int length = sizeof(m_sending);
            for (unsigned i = 0; i < m_neighbors.Count; ++i)
            {
                // take a local snapshot of the value so we are decoupled from
                //  simultaneous changes to the neighbor connections.
                // We could still end up trying to send to a neighbor which has
                //  just been disconnected.  We do not check if our send succeeds.
                // We might want to count failures, but so long as the counts
                //  are low there is no need for retry logic.

                auto sock = m_neighborSockets[i];
                if (sock != INVALID_SOCKET)
                {
                    ::send(sock, (const char*) pMessage, length, 0);
                }
            }
            m_sending.Count = 0;
        }

    public:
        GossipOutput()
        {
            InitializeSocket();
            InitializeSRWLock(&m_appendLock);

            // flush every 10th of a second even if not much news
            LARGE_INTEGER frequency;
            ::QueryPerformanceFrequency(&frequency);
            m_flushIntervalLimit = frequency.QuadPart / 10;
            LARGE_INTEGER timeNow;
            QueryPerformanceCounter(&timeNow);
            m_flushDeadline = timeNow.QuadPart + m_flushIntervalLimit;
        }

        ~GossipOutput()
        {
            auto iResult = closesocket(m_socket);
            WSACleanup();
            Audit::Assert(iResult != SOCKET_ERROR);
        }

        void SetNeighbors(const GossipNeighbors& newNeighbors)
        {
            m_neighbors = newNeighbors; 
        }

        void Send(const PartitionNews& item)
        {
            bool flush = false;
            {
                Exclude<SRWLOCK> guard{ m_appendLock };
                if (m_pending.IsFull())
                {
                    // there is a tiny chance flushing has not finished by the
                    // time we get enough new changes to cause another Full state.
                    // That would cause us to get stuck on Full so we test to
                    // see if we can copy to the send buffer yet.

                    if (!m_sending.IsEmpty())
                    {
                        // The protocol is designed to be redundant.  Dropping
                        // occasional messages is tolerated.  That change should
                        // be delivered by alternative routes.

                        return;
                    }
                    m_sending = m_pending;
                    m_pending.Count = 0;
                    flush = true;
                }
                m_pending.Items[m_pending.Count++] = item;
                LARGE_INTEGER timeNow;
                QueryPerformanceCounter(&timeNow);

                // We check for buffer full, or timeout since last message.
                // We do not do a true timeout: we only check time if we have news.  This
                //  will be a problem when the system is too small, we may need to have
                //  some kind of heartbeat to be sure the news flows.

                // circular arithmetic on U64: too big means it wrapped around to negative

                if ((m_pending.IsFull() || ((m_flushDeadline - timeNow.QuadPart) > (1ull << 60)))
                    && m_sending.IsEmpty())
                {
                    m_sending = m_pending;
                    m_pending.Count = 0;
                    flush = true;
                }
            }
            if (flush)
                Flush();
        }
    };

    class DistributedPartitionMap : public PartitionMap
    {
    private:
        static const uint32_t RECENT_NEWS_COUNT = 8 * MAX_NEWS_ITEMS;

        GossipOutput m_outgoingNews;

        vector<ServerRing> Servers;

        unique_ptr<RingMap> PrimaryMap;
        unique_ptr<RingMap> SecondaryMap;
        unique_ptr<RingMap> TertiaryMap;

        // TBD: tanjb, brownouts are still a placeholder
        Lookaside<16> m_brownouts;
        Lookaside<RECENT_NEWS_COUNT> m_recentNews;

        void ParsePartition(uint32_t partitionId, _Out_ unsigned& ringId, _Out_ unsigned& slot)
        {
            Audit::ArgRule(partitionId < m_partitionCount, "partitionID was not modulo partition count");
            ringId = partitionId % m_ringCount;
            slot = partitionId / m_ringCount;
        }

        ServerAddress MapToServer(uint32_t partitionId, RingMap* map)
        {
            unsigned ringId;
            unsigned slot;
            ParsePartition(partitionId, ringId, slot);
            RingMap& ring = map[ringId];
            return Servers[ringId].Addresses[ring[slot]];
        }

    public:
        DistributedPartitionMap(uint32_t ringCount)
            : PartitionMap(ringCount)
            , PrimaryMap(new RingMap[ringCount])
            , SecondaryMap(new RingMap[ringCount])
            , TertiaryMap(new RingMap[ringCount])
        {
            Audit::Assert(m_partitionCount <= m_ringCount * RING_PARTITIONS);
        }

        bool TryPrimaryServer(uint32_t partitionId, _Out_ ServerAddress& address) override
        {
            uint32_t fallback;
            if (m_brownouts.TryFind(partitionId, fallback))
            {
                return false;
            }
            address = MapToServer(partitionId, PrimaryMap.get());
            return true;
        }

        bool TrySecondaryServer(uint32_t partitionId, _Out_ ServerAddress& address) override
        {
            // do not try to "optimize" this as a side effect of finding primary server.
            // when we reach production, the secondary map will usually be kept on SSD.
            // Only a few currently active secondaries will need to be kept in DRAM.

            address = MapToServer(partitionId, SecondaryMap.get());
            return true;
        }

        bool TryTertiaryServer(uint32_t partitionId, _Out_ ServerAddress& address) override
        {
            // do not try to "optimize" this as a side effect of finding primary server.
            // when we reach production, the tertiary map will usually be kept on SSD.
            // Only a few currently active tertiaries will need to be kept in DRAM.

            address = MapToServer(partitionId, TertiaryMap.get());
            return true;
        }

        virtual void SetGossipListeners(const GossipNeighbors& neighbors) override
        {
            m_outgoingNews.SetNeighbors(neighbors);
        }

        virtual void UpdateMapWithNews(const GossipNews& news) override
        {
            for (unsigned i = 0; i < news.Count; ++i)
            {
                auto item = news.Items[i];
                uint32_t knownVersion;
                bool exists = m_brownouts.TryFind(item.PartitionNumber, knownVersion);
                if (!exists || (item.CyclicVersion - knownVersion) < 128)
                {
                    // this is a new version
                    if (exists)
                    {
                        m_recentNews.Remove(item.PartitionNumber, knownVersion);
                    }
                    m_recentNews.Append(item.PartitionNumber, item.CyclicVersion);
                    m_outgoingNews.Send(item);
                }
            }
        }
    };

    extern std::unique_ptr<PartitionMap> PartitionMapFactory()
    {
        return make_unique<DistributedPartitionMap>(32);
    }
}
