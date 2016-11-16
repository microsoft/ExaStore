// NetBursts.cpp : Defines the entry point for the console application.
//

#pragma once

#include "stdafx.h"

#include "Datagram.hpp"
#include "Scheduler.hpp"

namespace ServiceBrokers
{
    // Message size must align to 8 byte boundary;
    static const uint32_t MSG_ALIGN = 8;
    static_assert(MSG_ALIGN & MSG_ALIGN,
        "Alignment must be power of two for easy roundup.");

    // We use two crc32, not necessarily safer, but hopefuly faster.
    static const uint32_t CHKSUM_SIZE = 2 * sizeof(uint32_t);

    // Action to dispatch incoming network request called synchronously.
    // so please return promptly.
    // Absolutely no sync I/O should be called from here
    //
    class VerbWorker
    {
    public:
        // Called when the underlying network received a message, and
        // the worker need to provide a buffer to carry that message
        // the worker needs to set proper offset if it want leave
        // room to add extra headers.
        //
        // If nullptr is returned from this function, the message
        // will be discarded.
        // 
        // Rarely the message is corruptted, then the buffer will be disposed.
        // Or else the buffer containing the message is given back to the
        // worker with the next function.
        //
        virtual Utilities::BufferEnvelope AcquireBuffer(size_t msgSize) = 0;

        // Called when the network copied a message to the disposible
        // buffer provided in the above function, and give back the buffer
        // for processing. The message always start with a Request header
        virtual void DoWork(Utilities::BufferEnvelope pMsg) = 0;
    };

    /* The service broker is an agent which manages a distributed service associated with a Socket.
    */
    class ServiceBroker : public Schedulers::Work
    {
    public:

        // biggest UDP packet size is 
        // 65535 - (sizeof(IP Header)+ sizeof(UDP Header))=65535-(20+8) = 65507
        // 
        // In registered I/O buffer, we have data gram followed by IP info.
        // Try to make it 64bit align, to prevent out of alignment fetch of the IP info
        // 
        static const size_t DATAGRAMSIZE = 65472;

        virtual std::string HomeAddressIP() = 0;

        virtual std::string HomeAddressPort() = 0;

        virtual Schedulers::Scheduler* GetScheduler() = 0;

        virtual void BindVerb(Datagram::ServiceVerb verb, VerbWorker& acceptor) = 0;

        virtual size_t GetSliceStandardLength() = 0;

        // sync function to issue a send to a remote machine. 
        // usually fire n forget, no ack requested.
        // It is safe for the caller to release buffer afterwards
        //
        virtual Utilities::StatusCode Send(_In_ Datagram::Request& request, _In_ const void* blob, uint32_t blobLength) = 0;
        
        virtual ~ServiceBroker() {};
    };

    template<typename T, uint32_t bound>
    inline uint32_t ComputeCRC(T(&dst)[bound], _In_ const void* src, const uint32_t size, _Inout_ uint32_t& leftcrc, _Inout_ uint32_t& rightcrc)
    {
        ComputeCRC(dst, bound, src, size, leftcrc, rightcrc);
        return size;
    }

    inline void ComputeCRC(_Out_ void* dst, const uint32_t bound, _In_ const void* src, const uint32_t size, _Inout_ uint32_t& leftcrc, _Inout_ uint32_t& rightcrc)
    {
        Audit::Assert(size % MSG_ALIGN == 0, "Need alignment for crc computation");
        Audit::Assert(bound >= size, "Write memory over bound");

        auto dSrc = reinterpret_cast<const uint32_t*>(src);
        auto dDst = reinterpret_cast<uint32_t*>(dst);

        for (size_t i = 0; i < size / sizeof(uint32_t); i += 2)
        {
            dDst[i] = dSrc[i];
            dDst[i + 1] = dSrc[i + 1];
            leftcrc = _mm_crc32_u32(leftcrc, dSrc[i]);
            rightcrc = _mm_crc32_u32(rightcrc, dSrc[i + 1]);
        }
    }

}
