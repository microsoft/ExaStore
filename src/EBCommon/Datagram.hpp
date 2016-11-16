/* Copyright (c) Microsoft, 2014.  All rights reserved.

    A datagram transport designed for use with distributed exabyte datasets and
    their related services.  The hashed keys are 128 bits of properly randomized key.

    With 2^54 (10^16) values there is a 1 in a million chance of hash collision.
*/

#pragma once

#include "stdafx.h"

#include <memory>
#include <functional>

#include "Tracer.hpp"

/* We will eventually remove this in favor of a more cloud-worthy error logging mechanism.
   It is a useful bootstrap for us to start with.
*/
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN 1
#endif
#include <winerror.h>

namespace Datagram
{
    /* 64 bits of hash-key, all bits are significant for equality.
       All values less than 2^32 are reserved for special purposes.
    */
    struct Key64
    {
        inline uint64_t Value() const { return m_value; }

        Key64(uint64_t value) { m_value = value; }
        Key64() : Key64(0) {};

        bool Key64::operator==(const Key64 &other) const
        {
            return m_value == other.m_value;
        }

        bool Key64::operator!=(const Key64& other) const
        {
            return m_value != other.m_value;
        }
    private:
        uint64_t m_value;
    };

    /* 128 bits of hash-key, all bits are significant for equality.
       All values less than 2^64 are reserved for special purposes.
    */
    struct Key128
    {
        inline uint64_t ValueLow() const  { return m_value[0]; }
        inline uint64_t ValueHigh() const { return m_value[1]; }

        Key128(uint64_t low, uint64_t high)
        {
            m_value[0] = low;
            m_value[1] = high;
        }

        Key128() : Key128((uint64_t) 0, 0) {}

        Key128(_In_reads_(memSize) void* pMem, size_t memSize)
        {
            Audit::ArgNotNull(pMem, "Key128 pMem cannot be null");
            Audit::ArgRule(sizeof(Key128) <= memSize, "Key128 needs at least 16 bytes to initialize");
            m_value[0] = ((uint64_t*) pMem)[0];
            m_value[1] = ((uint64_t*) pMem)[1];
        }

        bool Key128::operator==(const Key128 &other) const
        {
            return m_value[0] == other.m_value[0]
                && m_value[1] == other.m_value[1];
        }

        bool Key128::operator!=(const Key128& other) const
        {
            return !(*this == other);
        }

        void Invalidate() { m_value[1] = 0; }
        bool IsInvalid() const { return m_value[1] == 0; }

    private:
        uint64_t m_value[2];
    };

    enum class ServiceVerb : uint8_t
    {
        None = 0,

        // ack used for flow control in multi-packets msg
        // is not visible to upper level logic outside of
        // the ServiceBroker
        Ack = 1,

        // remote writes are accepted from the network socket.
        RemoteWrite = 2,

        // remote reads are accepted from the network socket.
        RemoteRead = 3,

        RemoteDelete = 4,

        // Ack to remote write, 
        // Ack to replication request
        // Error response to remote read
        Status = 5,

        // Return value to remote read
        Value = 6,

        AddPartition = 7,

        // Messages used in vertical paxos
        // consult TLA+ spec
        NewBallot,

        ActivateBallot,

        CompleteBallot,

        // replication request from primary to secondaries
        PreparUpdate,

        // A primary replica complains to manager that a secondry
        // timedout
        SecondaryTimeout,

        // A secondary complain to manager that the primary timeout
        PrimaryTimeout,

        // A primary tells the manager an inactive replica comes back
        // and caught up
        PromoteInactive,

        // Acknowledge receiving of replication
        // req or beacon, 
        AckUpdate,

        // Acknowledge receiving of manager command.
        AckCmd
    };

    struct Request
    {
        /* The ServiceID changes over time.  Authorized clients are expected to be
        subscribed to an update service which issues tham a current valid ID.
        */
        uint32_t    ServiceID;

        /* ClientID changes over time.  A server should probe the security
        service to verify that a given client and IP combination is valid.
        */
        uint32_t    ClientID;

        // Destination address and port when trying to send. Sender's address
        // and port when received.
        // We are designed exclusively to run within the DC, where IPv4 will persist.
        //
        uint32_t    RemoteIPv4;
        uint16_t    RemotePort;

        // Sequence number in multi-packet messages
        // The first packet should always carry a negative number, representing
        // the total number of packets in this message
        // 0 means this is a single packet message
        // Should never be -1
        //
        // when service verb is Ack, it's the sequence number
        // of the packet the receiver is expecting
        int16_t     Sequence;

        /* The action we are requesting.
        */
        ServiceVerb     Verb;

        // Dapper style trace ID. 
        // reused to identify retries of the same operation.
        // reused to identify multi-packet communication session
        // if a retry comes before a send session is finished, we can ignore it.
        // put at last to avoid alignment problems
        struct Tracer::TraceRec Tracer;

        // Constructor
        Request(
            // Ip and port are used on the wire, so use ntoh when passing native values.
            uint32_t wire_ipv4, uint16_t wire_port, 
            int16_t seq, ServiceVerb verb, 
            const Tracer::TraceRec& trace
            )
            : RemoteIPv4(wire_ipv4)
            , RemotePort(wire_port)
            , Sequence(seq)
            , Verb(verb)
            , Tracer(trace)
        {}
    };

    // A partial descriptor is used for reads.
    struct PartialDescription
    {
        // the unique identifier of the Node.
        Key128 KeyHash;

        // A service identifier used to verify access rights.
        Key64 Caller;
    };

    // Descriptor for a Value to be written into the Exabytes store.
    struct Description
    {
        // the unique identifier of the Node.
        Key128 KeyHash;

        // Only the owner of the data may replace or delete a node.
        // Consider generating an owner Key64 from the owner Key128.
        Key64 Owner;

        // If reader zero is allowed, the data is public.  Otherwise, the reader
        // Key64 value must be matched to verify permission to read.
        Key64 Reader;

        // Reserved for server use
        uint64_t Serial;

        // Byte count.
        // In early implementations Node restrictions may be quite short,
        // a limit is supplied when connecting to the storage service.
        uint32_t ValueLength : 30;

        // Blobs written as multiple appends must use non-zero Part.
        // The app may choose assign any non-zero value.  We will write parts
        // in the order they happen to arrive, not sorted, and without any
        // significance to repetition and gaps.  If a zero-value Part is
        // mixed with non-zero Parts the result is undefined.  Don't do that.
        //
        uint32_t Part : 1;
        
        // Reserved for Server use
        uint32_t TaggedForExpiration : 1;

        // Timestamp is also used as the version of the data item,
        // this is generated when the first write arrives at the primary
        // server
        uint32_t Timestamp;
    };
    static_assert(sizeof(Description) == 48,
        "Header too big may break alignment in memory store.");

    // Policy for a set of data,
    // usually identified by an ower ID
    struct Policy
    {
        //   Time before the data expire
        // Although the unit here one minute, we only offer granularity
        // of a day or so.
        // 
        // 0 means the data should never be committed to store.
        // max value 30240 (two weeks)
        //
        uint16_t ExpirationMinutes : 15;

        // if true, Read operation can retrive data from secondary 
        // servers. This achieves a little better availability
        // while sacrafies a little consistency.
        //
        uint16_t CanReadFromSecondary : 1;

        Policy()
            : ExpirationMinutes(0)
            , CanReadFromSecondary(0)
        {}

        Policy(uint16_t expireInHour, bool readFromSecondary)
            : ExpirationMinutes(expireInHour * 60)
            , CanReadFromSecondary(readFromSecondary? 1 : 0)
        {
            Audit::Assert(ExpirationMinutes <= 30240, 
                "expiration must be smaller than three weeks");
        }
    };

    struct PartitionId
    {
        uint32_t Value() const { return m_value; }

        PartitionId(uint32_t value) { m_value = value; }
        PartitionId() : PartitionId(~0UL) {};

        bool PartitionId::operator==(const PartitionId &other) const
        {
            return m_value == other.m_value;
        }

        bool PartitionId::operator!=(const PartitionId& other) const
        {
            return m_value != other.m_value;
        }

        bool IsInvalid()
        {
            return m_value == ~0UL;
        }

    private:
        uint32_t m_value;
    };

    struct ServerAddress
    {
        // We are designed exclusively to run within the DC, where IPv4 will persist.
        //
        uint32_t    ServiceIPv4;
        uint16_t    ServicePort;

        ServerAddress() : ServerAddress(0, 0){}

        ServerAddress(uint32_t IPv4, uint16_t port)
            : ServiceIPv4(IPv4)
            , ServicePort(port)
        {}
    };

    // Data grams used in Vertical Paxos Protocol:
    // These data structures are shared between the server and the manager.
    
    enum class ReplicaRole : uint8_t{
        Invalid,

        Primary,

        Secondary,

        // Timed out replica is marked as Inactive. If it does not come
        // back within 5 minutes they are completely kicked out
        Inactive, 

        // Newly joined machine is marked as Candidate as it accept data
        // transfer from other machines. 
        Candidate
    };

    struct ReplicaState{
        ServerAddress Node;
        ReplicaRole   Role = ReplicaRole::Invalid;
    };

    struct PartitionConfig {
        uint64_t Ballot = 0;
        uint64_t PrevBallot = 0;
        ReplicaState Replicas[4];
    };
}

// provide specializations of std:: functors
namespace std
{
    // for std:: hash trait
    template<>
    struct hash<Datagram::Key64>
    {
        inline uint64_t operator()(const Datagram::Key64& value) const
        {
            return (uint64_t) value.Value();
        }
    };

    // for std:: hash trait
    template<>
    struct hash<Datagram::Key128>
    {
        inline uint64_t operator()(const Datagram::Key128& value) const
        {
            return value.ValueHigh() ^ value.ValueLow();
        }
    };

    template<>
    struct hash<Datagram::PartitionId>
    {
        inline uint64_t operator()(const Datagram::PartitionId& value) const
        {
            return (uint64_t) value.Value();
        }
    };

    // for std:: equality trait
    template<>
    struct equal_to<Datagram::Key64>
    {
        inline bool operator()(const Datagram::Key64& x, const Datagram::Key64& y) const { return (x.Value() == y.Value()); }
    };

    // for std:: equality trait
    template<>
    struct equal_to<Datagram::Key128>
    {
        inline bool operator()(const Datagram::Key128& x, const Datagram::Key128& y) const
        {
            return (x.ValueHigh() == y.ValueHigh()) && (x.ValueLow() == y.ValueLow());
        }
    };
}