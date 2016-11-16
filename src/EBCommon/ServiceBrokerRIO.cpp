#pragma once

#include "stdafx.h"

#include <utility>
#include <algorithm>
#include <vector>
#include <stack>

#include "ServiceBrokerRIO.hpp"
#include "SchedulerIOCP.hpp"

#include "UtilitiesWin.hpp"
#include "UdpSession.hpp"

#include <winsock2.h>
#include <ws2tcpip.h>
#include <Ws2def.h>
#include <Mswsock.h>
#include <iphlpapi.h>

namespace    // private to this source file
{
    using namespace Datagram;
    using namespace Schedulers;

    // Get function table for the Registered IO Functions
    //
    bool InitializeRIOtable(SOCKET socket, __out _RIO_EXTENSION_FUNCTION_TABLE& RIO)
    {
        GUID functionTableId = WSAID_MULTIPLE_RIO;
        DWORD dwBytes = 0;
        int iResult = WSAIoctl(
            socket,
            SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER,
            &functionTableId,
            sizeof(GUID),
            (void**)&RIO,
            sizeof(RIO),
            &dwBytes,
            0,
            0);
        return (0 == iResult);
    }

    // create an IOCP and an associated completion queue, for use with
    // both sends and receives on this socket.
    //
    bool CreateIOCPandCQ(_In_ _RIO_EXTENSION_FUNCTION_TABLE& RIO, _In_ OVERLAPPED* pOverlapped, const int CQ_COUNT, __out HANDLE& hIOCP, __out RIO_CQ& completionQueue)
    {
        completionQueue = 0;
        hIOCP = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 0);
        if (hIOCP == NULL)
            return false;
        RIO_NOTIFICATION_COMPLETION completionType;
        {
            completionType.Type = RIO_IOCP_COMPLETION;
            completionType.Iocp.IocpHandle = hIOCP;
            completionType.Iocp.CompletionKey = (void*)1;
            completionType.Iocp.Overlapped = pOverlapped;
        }
        completionQueue = RIO.RIOCreateCompletionQueue(CQ_COUNT, &completionType);
        return (completionQueue != RIO_INVALID_CQ);
    }

    class UnimplementedActivity : public ServiceBrokers::VerbWorker
    {
    public:
        BufferEnvelope AcquireBuffer(size_t) override
        {
            wchar_t msg[1024];
            swprintf_s(msg, L"ServiceVerb not set!");
            Tracer::LogError(StatusCode::Unexpected, msg);
            return BufferEnvelope();
        }

        void DoWork(
            BufferEnvelope pMsg
            ) override
        {
            Audit::OutOfLine::Fail(StatusCode::Unexpected,
                "Should not get here as we did not provide any buffer for containing message!");
        }
    };

    // Currently only used in sending slice only, essentially we need
    // two bits: Sending means the slice is given to the OS for send
    // InChain means the slice is part of a long message.
    enum class SliceState : uint16_t
    {
        Free = 0,
        Sending = 1,
        InChain = 2,
        InChainAndSending = 3
    };

}

namespace Utilities
{
    template<>
    struct ExcludedRegion < CRITICAL_SECTION >
    {
        static void Exclude(_In_ CRITICAL_SECTION& lock)
        {
            ::EnterCriticalSection(&lock);
        }
        static void Release(_In_ CRITICAL_SECTION& lock)
        {
            ::LeaveCriticalSection(&lock);
        }
    };
}

namespace ServiceBrokers
{
    using namespace Utilities;

    struct BufferSlice
    {
        _RIO_BUF m_RB;
    };

    /* The derived class in a nested anonymous namespace serves two purposes.
    The primary purpose is it allows us to define a public interface requiring no Windows.h or related headers.
    The secondary but related purpose is to entirely hide non-portable implementation details.
    */
    namespace
    {

        class ServiceBrokerRIO : public ServiceBroker
        {
            struct SRWInit{
                void operator()(SRWLOCK* lock) {
                    InitializeSRWLock(lock);
                }
            };
            typedef CuckooSessionMap<ServiceBrokerRIO, SRWLOCK, SRWInit> SessionMapType;
            typedef UdpSession<SRWLOCK, SRWInit> SessionType;

        public:
            // Constructor for the local agent of a service.
            // Synchronous.
            //
            ServiceBrokerRIO(
                _In_ const std::wstring& name,  // the name of the service
                uint16_t port = 0
                )
            {
                ZeroMemory(&m_homeAddress, sizeof(m_homeAddress));
                m_serviceName = name;
                m_pMemory = nullptr;

                // Initialize the critical section one time only.
                Audit::Assert(0 != InitializeCriticalSectionAndSpinCount(&m_requestQueueCriticalSection, 0x00000400),
                    "InitializeCriticalSectionAndSpinCount");

                // Initialize Winsock v2.2
                WSADATA wsaData;
                int iResult = ::WSAStartup(MAKEWORD(2, 2), &wsaData);
                Audit::Assert(iResult == 0, "WSAStartup failed");

                // Create a socket for us to use.
                m_homeSocket = ::WSASocket(AF_INET, SOCK_DGRAM, IPPROTO_UDP, NULL, 0, WSA_FLAG_OVERLAPPED | WSA_FLAG_REGISTERED_IO);
                Audit::Assert(m_homeSocket != INVALID_SOCKET, "could not create local socket");

                Audit::Assert(DiscoverHomeAddress(m_homeSocket, m_homeAddress, port),
                    "DiscoverHomeAddress failed");

                // Get function table for the Registered IO Functions
                Audit::Assert(InitializeRIOtable(m_homeSocket, m_RIO),
                    "WSAIoctl failed to get the RIO function pointers");

                // create an IOCP and an associated completion queue

                Audit::Assert(CreateIOCPandCQ(m_RIO, &m_overlapped, CQ_COUNT, m_hIOCP, m_sendReceiveCQ),
                    "RIOCreateCompletionQueue");

                // attach the CQ to the socket and set limits on outstanding requests

                const ULONG maxOutstandingReceive = (2 * CQ_COUNT) / 3;
                const ULONG maxOutstandingSend = CQ_COUNT - maxOutstandingReceive;

                void *pContext = nullptr;

                m_requestQueue = m_RIO.RIOCreateRequestQueue(
                    m_homeSocket,
                    maxOutstandingReceive,
                    1,
                    maxOutstandingSend,
                    1,
                    m_sendReceiveCQ,
                    m_sendReceiveCQ,
                    pContext);

                SliceAndQueueBuffers();

                // OK, now we are ready to get the party started.
                // use 1 thread for early debugging.  Change to 4 regularily to find concurrency issues.

                m_pScheduler = Schedulers::SchedulerIOCPFactory((IOCPhandle)m_hIOCP, 8, this);

                for (int i = 0; i < 256; ++i)
                    m_verbBindings[i] = &m_unimplementedActor;

                // we want to be notified of completion related to the socket.
                INT notifyResult = m_RIO.RIONotify(m_sendReceiveCQ);
                Audit::Assert(notifyResult == ERROR_SUCCESS, "RIONotify");

                InitializeSRWLock(&m_srwSendSlicesLock);

                m_sessions.SetVisitor(*this);
                m_sessions.StartPolling(*m_pScheduler);
            }

            virtual ~ServiceBrokerRIO()
            {
                Cleanup();
            }

            std::string HomeAddressIP()
            {
                const int COUNT = 1024;
                wchar_t chars[COUNT];
                if (nullptr == ::InetNtop(AF_INET, &m_homeAddress.sin_addr, chars, COUNT))
                    return std::string{ "home address error" };
                char utf8[2 * COUNT];
                ZeroMemory(utf8, sizeof(utf8));
                WideCharToMultiByte(CP_UTF8, 0, chars, 1 + (int)wcsnlen(chars, COUNT - 1), utf8, 2 * COUNT, NULL, NULL);
                return std::string{ utf8 };
            }

            std::string HomeAddressPort()
            {
                const int COUNT = 16;
                char utf8[COUNT];
                ZeroMemory(utf8, sizeof(utf8));
                _itoa_s(ntohs(m_homeAddress.sin_port), utf8, 16, 10);
                return std::string{ utf8 };
            }

            Schedulers::Scheduler* GetScheduler() { return &(*m_pScheduler); }

            bool BindSocketToIOCP(
                SOCKET sock,
                HANDLE hExistingIOCP
                )
            {
                HANDLE h2 = ::CreateIoCompletionPort((HANDLE)sock, hExistingIOCP, 0, 0);
                return h2 != NULL;
            }

            // Add a verb which will match requests from the network, and an actor to perform the request.
            //
            void BindVerb(ServiceVerb verb, VerbWorker& actor)
            {
                auto currentAcceptor = m_verbBindings[(uint8_t)verb];
                Audit::Assert(currentAcceptor == &m_unimplementedActor || currentAcceptor == &actor,
                    "redefining a known verb is not allowed");
                m_verbBindings[(uint8_t)verb] = &actor;
            }

            // all slices are this length.  Network-wide constant.
            //
            size_t GetSliceStandardLength() override { return DATAGRAMSIZE; }

            // This is the Primary Action assigned on the IOCP, dequeue RIO completions.
            // The socket event tells us to expect 1 or more completions in the RIO buffers.
            // Select the actor bound to perform network verbs such as Read or Write.
            // Also, retires buffers which were used for outgoing requests.
            //
            virtual void Run(WorkerThread&, intptr_t, uint32_t)
            {
                RIORESULT results[CQ_COUNT];

                // pOverlapped from ICOP is ignored
                // length is ignored.
                // The event sending us here was just a wake-up call.
                // We need to drain the RIO completions to find out what is really happening.

                // Collect multiple results from the Socket completion.

                ULONG numResults = m_RIO.RIODequeueCompletion(m_sendReceiveCQ, results, CQ_COUNT);
                Audit::Assert(0 < numResults && RIO_CORRUPT_CQ != numResults,
                    "RIODequeueCompletion");

                /* Re-enable socket completions on the IOCP
                */
                INT notifyResult = m_RIO.RIONotify(m_sendReceiveCQ);
                Audit::Assert(notifyResult == ERROR_SUCCESS, "RIONotify");

                for (unsigned i = 0; i < numResults; ++i)
                {
                    // a request occupies one or more slices of the Registered IO buffer.

                    _RIO_BUF *pSlice = reinterpret_cast<_RIO_BUF *>(results[i].RequestContext);

                    unsigned sliceIndex = pSlice->Offset / BUFFERSLICESIZE;
                    Audit::Assert(0 == pSlice->Offset % BUFFERSLICESIZE);
                    uint32_t messageLength = results[i].BytesTransferred;
                    Audit::Assert(messageLength <= DATAGRAMSIZE, "incoming message too big");

                    pSlice->Length = messageLength;
                    PktSlice* pData = SliceBody(sliceIndex);

                    // we have incoming data, and we have outgoing data.  The low-numbered
                    // slices are dedicated to incoming requests from the Socket

                    if (sliceIndex < m_receiveSliceCount)
                    {
                        pData->Next = (uint16_t)sliceIndex;
                        InterlockedDecrement(&m_availableRcvSlices);

                        bool reclaim = true;
                        if (pData->Req.Verb == ServiceVerb::Ack)
                        {
                            OnRcvAck(&pData->Req, messageLength);
                        }
                        else if (pData->Req.Sequence != 0)
                        {
                            reclaim = OnRcvPacket((uint16_t)sliceIndex);
                        }
                        else
                        {
                            // look up the requested Verb and pass the request to an Actor which
                            //  will perform the request.
                            auto pWork = m_verbBindings[(uint8_t)(pData->Req.Verb)];
                            auto envelope = pWork->AcquireBuffer(messageLength - CHKSUM_SIZE);
                            if (envelope.Contents() != nullptr)
                            {
                                uint32_t leftcrc = 0;
                                uint32_t rightcrc = 0;

                                char* dstPtr = (char*)envelope.Contents()->PData() + envelope.Contents()->Offset();
                                auto dstSize = envelope.Contents()->Size() - envelope.Contents()->Offset();

                                ComputeCRC(dstPtr, dstSize,  &pData->Req, messageLength - CHKSUM_SIZE,
                                    leftcrc, rightcrc);

                                // TODO!!! Paranoid, remove
                                SOCKADDR_INET* pSenderAddr = (SOCKADDR_INET*)(m_pMemory->PData(m_pAddresses[sliceIndex].Offset));
                                Audit::Assert(pSenderAddr == &pData->Addr,
                                    "Address slice points to the wrong place.");
                                auto pReq = reinterpret_cast<Request*>(dstPtr);
                                pReq->RemoteIPv4 = ntohl(pSenderAddr->Ipv4.sin_addr.S_un.S_addr);
                                pReq->RemotePort = ntohs(pSenderAddr->Ipv4.sin_port);

                                auto plcrc = reinterpret_cast<const uint32_t*>
                                    (&pData->Blob[messageLength - offsetof(PktSlice, Blob) - CHKSUM_SIZE]);
                                auto prcrc = plcrc + 1;
                                if (leftcrc != *plcrc || rightcrc != *prcrc)
                                {
                                    Tracer::LogWarning(StatusCode::InvalidState, 
                                        L"CRC error found in remote message");
                                }
                                else
                                {
                                    pWork->DoWork(std::move(envelope));
                                }
                            }
                        }
                        if (reclaim)
                        {
                            // Buffer reclaimed, verb must copy the package to somewhere else.
                            RecycleSlice(*pSlice);
                            pSlice = nullptr;
                            pData = nullptr;
                            sliceIndex = INV_NEXT;
                        }
                    }
                    else
                    {
                        // should outgoing completions dispatch to an Actor too, for confirmation?
                        // No, the transport is inherently unreliable.  Confirmation must be an explicit
                        // incoming message which will be delivered to the application.

                        // the high numbered slices held data for sends, which the socket is giving
                        //  back since the send is completed.  if the next pointer points to itself
                        //  then this is a loner, we can return to the pool.
                        //  or else this is part of a chain, we need to wait for ack to reclaim it.

                        SliceState oldState = (SliceState)
                            InterlockedCompareExchange16(
                            (short*)&pData->State,
                            (uint16_t)SliceState::InChain,
                            (uint16_t)SliceState::InChainAndSending);

                        if (oldState != SliceState::InChainAndSending)
                        {
                            // a buffer can change from InChainAndSending
                            // to Sending, when it is released from a chain
                            // But it should never change from Sending to
                            // InChainSending.
                            Audit::Assert(oldState == SliceState::Sending,
                                "expect slice sending state");
                            pData->State = SliceState::Free;
                            RecycleSendSlice(pData);
                            pSlice = nullptr;
                            pData = nullptr;
                            sliceIndex = INV_NEXT;
                        }
                    }
                }
            }


            // Async send, but data are copied to RIO buffers, so it is safe for the caller
            // to release caller's buffer after the call
            //
            StatusCode Send(_In_ Request& request, _In_ const void* blob, uint32_t blobLength) override
            {
                Audit::Assert(blobLength % MSG_ALIGN == 0,
                    "Message length needs to be aligned.");
                uint32_t totalSize = blobLength + sizeof(Request) + CHKSUM_SIZE;

                request.Sequence = 0;
                if (totalSize > DATAGRAMSIZE)
                {
                    return SendMultiPackets(request, blob, blobLength);
                }
                else
                {
                    return SendSinglePacket(request, blob, blobLength);
                }
            }

            // Poll udp sessions, required by session map
            void PollSession(SessionType& session)
            {
                uint16_t slices[2];
                uint16_t toSend = 0;
                auto seq = session.m_seq;
                slices[0] = session.m_firstSlice;

                if (session.m_status != SessionStatus::Sending
                    || slices[0] == INV_NEXT
                    || seq >= session.m_totalPck
                    )
                {
                    return;
                }

                if (seq > 0 && seq < session.m_totalPck - 1)
                {
                    // we need to send out two packets
                    slices[1] = SliceBody(slices[0])->Next;
                }
                else
                {
                    slices[1] = INV_NEXT;
                }

                { ////// critical section //////
                    Utilities::Exclude<SRWLOCK> guard{ session.m_lock };
                    if (session.m_status == SessionStatus::Sending
                        && session.m_tickDown < SESSIONTICKS - 1
                        && session.m_seq == seq
                        && session.m_firstSlice == slices[0])
                    {
                        for (int i = 0; i < 2; i++)
                        {
                            if (slices[i] == INV_NEXT)
                                break;
                            // these slices are still in session
                            // the terminator must took them off (with a
                            // critical section) before releasing them
                            // (change their state). So here we don't need
                            // a interlocked state change
                            auto pS = SliceBody(slices[i]);
                            SliceState state = pS->State;
                            if (state == SliceState::InChainAndSending){
                                // send operation took longer than POLLINTERVAL
                                // we don't try to resend here, will come back 
                                // next polling period.
                                slices[i] = INV_NEXT;
                                break;
                            }
                            Audit::Assert(state == SliceState::InChain,
                                "Resend: slice should be in chain!");
                            pS->State = SliceState::InChainAndSending;
                            toSend++;
                        }
                    }
                    else {
                        Audit::Assert(toSend == 0,
                            "Failed to send some slices");
                        return;
                    }
                } ////// critical section //////

                // on termination, or received ack, they may try to release
                // these buffers. the state "InChainAndSending" would make
                // them only change the state to Sending, without actually
                // recycle these buffers
                // when the OS is done with these buffers, it would recycle
                // them only if Sending. for buffers with state 
                // InChainAndSending, it would change them to InChain
                for (int i = 0; i < 2; i++)
                {
                    if (slices[i] == INV_NEXT)
                        break;
                    auto res = SendSlice(slices[i]);
                    toSend--;
                    if (res != StatusCode::OK){
                        Audit::OutOfLine::Fail(res, "Failed to send slice");
                    }
                }
                Audit::Assert(toSend == 0, "failed to send all slices");
            }

            // try to clean up a session and release all buffers
            // returns true if successful
            bool CleanupSession(SessionType& session)
            {
                uint16_t head;
                uint16_t tail;
                { ////// critical section //////
                    Utilities::Exclude<SRWLOCK> guard{ session.m_lock };
                    if (session.m_tickDown > 0)
                    {
                        return false;
                    }

                    if (session.m_status == SessionStatus::Initialing)
                    {
                        Tracer::LogError(StatusCode::Unexpected, 
                            L"UDP session creation took too long, ttl has to be extended!");
                        return false;
                    }

                    head = session.m_firstSlice;
                    tail = session.m_lastSlice;
                    session.m_firstSlice = INV_NEXT;
                    session.m_lastSlice = INV_NEXT;
                    session.m_seq = session.m_totalPck + 1;
                    session.m_status = SessionStatus::Dead;
                }////// critical section //////

                if (head < m_receiveSliceCount)
                {
                    RecycleRcvChain(head, tail);
                }
                else if (head < CQ_COUNT)
                {
                    RecycleSendChain(head, tail);
                }
                else
                {
                    Audit::Assert(head == INV_NEXT, "unknown RIO buffer index");
                }
                return true;
            }

        private:

            // should be power of 2
            static const int BUFFER_ALIGN = 64;

            // number of registered I/O buffers we allocate for network
            // A generous quantity of message buffers allows multiple requests to be
            // holding buffers while the action is being completed.
            //
            static const int BUFFERCOUNT = 1024;
            static_assert(BUFFERCOUNT < USHRT_MAX, "Slice count too big for uint16_t type index");

            // size of each buffer. We need DATAGRAMSIZE for data, followed by IP
            // info, and then a 16bit int for chaining. 16bit enum for state
            //
            static const int BUFFERSLICESIZE =
                ((DATAGRAMSIZE + sizeof(SOCKADDR_INET) + sizeof(uint16_t))
                + sizeof(uint16_t)
                + BUFFER_ALIGN - 1) & ~(BUFFER_ALIGN - 1);

            // An interpretation of packet slice structure.
            struct PktSlice 
            {
                Request       Req;
                char          Blob[DATAGRAMSIZE - sizeof(Request)];
                SOCKADDR_INET Addr;

                // we use an 16 bit integer as the next pointer to chain
                // up slices. next points to itself when this slices is
                // all by itself. INV_NEXT means end of the chain
                //
                // this is important, as we would know whether it is
                // safe to reclaim an slices after a send finishes.
                // slices that are part of a chain should not be reclaimed
                // right away.
                //
                uint16_t      Next;
                SliceState    State;
                PktSlice() = delete;
            };
            static_assert(sizeof(PktSlice) <= BUFFERSLICESIZE,
                "Packet slice size exceed design value.");
            static const size_t BLOB_SIZE = sizeof(decltype(PktSlice::Blob));
            static_assert( BLOB_SIZE % MSG_ALIGN == 0,
                "Message length must align to avoid split crcs into two packet.");

            /* It is extremely unlikely that there will be a completion for every
            buffer in one event, but it is cheap and simple to permit.
            */
            static const int CQ_COUNT = BUFFERCOUNT;

            // we use a 16 bit number as a next pointer to chain up
            // slices when needed. using 65535 as an invalid value
            // 
            static const uint16_t INV_NEXT = static_cast<uint16_t>(~0);

            SOCKET m_homeSocket{ INVALID_SOCKET };
            sockaddr_in m_homeAddress;
            HANDLE m_hIOCP;
            OVERLAPPED m_overlapped;
            RIO_CQ m_sendReceiveCQ;
            RIO_RQ m_requestQueue;
            CRITICAL_SECTION m_requestQueueCriticalSection;
            size_t m_registeredSize;
            unsigned m_sliceCount;
            std::unique_ptr<LargePageBuffer> m_pMemory;

            std::unique_ptr<_RIO_BUF[]> m_pSlices;
            _RIO_BUF* m_pAddresses;
            unsigned m_receiveSliceCount;
            unsigned m_sendSliceCount;
            std::stack<unsigned> m_sendSlices;
            SRWLOCK m_srwSendSlicesLock;
            unsigned m_availableRcvSlices;


            std::wstring m_serviceName;
            _RIO_EXTENSION_FUNCTION_TABLE m_RIO;

            std::unique_ptr<Scheduler> m_pScheduler;

            UnimplementedActivity m_unimplementedActor;
            VerbWorker* m_verbBindings[256];

            SessionMapType m_sessions;


            // Register the IO buffers and associate them with the socket.
            // RIODequeueCompletion() must not be called prior to completing this.
            //
            void SliceAndQueueBuffers()
            {
                static_assert(BUFFERCOUNT < INV_NEXT, "Too many buffers.");
                Audit::Assert(offsetof(PktSlice, Addr) == DATAGRAMSIZE,
                    "Structure PktSlice does not reflect intended slice structure.");
                Audit::Assert(offsetof(PktSlice, Blob) == sizeof(Request),
                    "Structure PktSlice does not reflect intended slice structure.");

                m_pMemory = std::make_unique<Utilities::LargePageBuffer>(BUFFERSLICESIZE, BUFFERCOUNT, m_registeredSize, m_sliceCount);
                Audit::Assert(m_sliceCount >= CQ_COUNT, "can not allocate enough memory for slices");

                RIO_BUFFERID id = m_RIO.RIORegisterBuffer((char*)m_pMemory->PBuffer(), (DWORD)m_registeredSize);
                Audit::Assert(id != RIO_INVALID_BUFFERID, "RIORegisterBuffer");

                // for each slice for data, we have a slice for address.
                // the slice address is neccessary to provide remote address when
                // trying to send a packet (with RIOSendEx). 
                m_pSlices = std::make_unique<_RIO_BUF[]>(CQ_COUNT * 2);
                m_pAddresses = &m_pSlices[CQ_COUNT];

                for (size_t i = 0; i < CQ_COUNT; ++i)
                {
                    _RIO_BUF *pBuffer = &m_pSlices[i];

                    pBuffer->BufferId = id;
                    pBuffer->Offset = (ULONG)(i * BUFFERSLICESIZE);
                    pBuffer->Length = DATAGRAMSIZE;

                    _RIO_BUF* pAddress = &m_pAddresses[i];
                    pAddress->BufferId = id;
                    pAddress->Offset = (ULONG)(i * BUFFERSLICESIZE + DATAGRAMSIZE);
                    pAddress->Length = sizeof(SOCKADDR_INET);
                }

                const DWORD recvFlags = 0;

                m_receiveSliceCount = (2 * CQ_COUNT) / 3;
                for (uint16_t i = 0; i < m_receiveSliceCount; ++i)
                {
                    // this queues the receive buffer.  It does not wait for incoming data.
                    SliceBody(i)->Next = i;
                    if (!m_RIO.RIOReceiveEx(
                        m_requestQueue,
                        &m_pSlices[i],
                        1,
                        nullptr,
                        &m_pAddresses[i],
                        nullptr,
                        nullptr,
                        recvFlags,
                        &m_pSlices[i])
                        )
                    {
                        Audit::OutOfLine::Fail((StatusCode)::WSAGetLastError(), "failed to initially assign receive buffers to the socket");
                    }
                }
                m_availableRcvSlices = m_receiveSliceCount;
                Tracer::LogCounterValue(Tracer::EBCounter::RIOAvailableRcv, L"", m_availableRcvSlices);

                m_sendSliceCount = CQ_COUNT - m_receiveSliceCount;
                for (uint16_t i = static_cast<uint16_t>(m_receiveSliceCount);
                    i < CQ_COUNT; ++i)
                {
                    // we keep a stack (for MRU order) of empty slices for sending
                    m_sendSlices.push(i);
                    SliceBody(i)->Next = i;
                }
                Tracer::LogCounterValue(Tracer::EBCounter::RIOAvailableSend, L"", m_sendSlices.size());

                // RIODequeueCompletion() must not be called prior to reaching here.
            }

            PktSlice* SliceBody(unsigned sliceHandle)
            {
                Audit::Assert(sliceHandle < CQ_COUNT,
                    "invalid slice handle");
                auto offset = m_pSlices[sliceHandle].Offset;

                // TODO!! Paranoid check, remove
                Audit::Assert(offset == sliceHandle * BUFFERSLICESIZE,
                    "RIO slice structure corruptted!");

                return reinterpret_cast<PktSlice*>(m_pMemory->PData(offset));
            }

            uint16_t AllocSendSlicesHandle()
            {
                Utilities::Exclude<SRWLOCK> guard{ m_srwSendSlicesLock };
                Audit::Assert(!m_sendSlices.empty(),
                    "exhausted all send buffers");
                unsigned sliceIndex = m_sendSlices.top();
                m_sendSlices.pop();
                auto pS = SliceBody(sliceIndex);
                Audit::Assert(pS->State == SliceState::Free
                    && pS->Next == sliceIndex,
                    "Slice state changed while in the pool.");

                return static_cast<uint16_t>(sliceIndex);
            }

            // When an Action finishes working with a buffer slice it must be
            // returned to be used for receiving another message.
            //
            void RecycleSendSlice(_In_ PktSlice* slice)
            {
                size_t offset = ((char*)slice) - (char*)(m_pMemory->PBuffer());
                size_t sliceIndex = offset / BUFFERSLICESIZE;
                Audit::Assert(sliceIndex < CQ_COUNT
                    && sliceIndex >= m_receiveSliceCount,
                    "Expect to reclaim only slices for send.");
                Audit::Assert(0 == (offset % BUFFERSLICESIZE));
                Audit::Assert(slice->State == SliceState::Free,
                    "Recycle of busy slice is not allowed!");

                ZeroMemory(slice, BUFFERSLICESIZE);
                slice->State = SliceState::Free;
                slice->Next = (uint16_t)sliceIndex;

                m_pSlices[sliceIndex].Length = DATAGRAMSIZE;

                {
                    Utilities::Exclude<SRWLOCK> guard{ m_srwSendSlicesLock };
                    m_sendSlices.push((unsigned)sliceIndex);
                    Audit::Assert(m_sendSlices.size() <= CQ_COUNT - m_receiveSliceCount,
                        "Double reclaim of send slice detected!");
                }
            }

            // When an Action finishes working with a buffer slice it must be
            // returned to be used for receiving another message.
            //
            bool RecycleSlice(_In_ _RIO_BUF& slice)
            {
                slice.Length = DATAGRAMSIZE;
                unsigned bufIndex = slice.Offset / BUFFERSLICESIZE;
                Audit::Assert(bufIndex < m_receiveSliceCount,
                    "Expect to reclaim only slices for receive.");

                const DWORD recvFlags = 0;
                bool keepRunning = true;
                ZeroMemory(m_pMemory->PData(slice.Offset), BUFFERSLICESIZE);
                InterlockedIncrement(&m_availableRcvSlices);
                Audit::Assert(m_availableRcvSlices <= m_receiveSliceCount,
                    "Double reclaim of receive slice detected!");
                {
                    Utilities::Exclude < CRITICAL_SECTION > guard{ m_requestQueueCriticalSection };
                    // this queues the receive buffer.  It does not wait for incoming data.
                    // For incoming data, see RIODequeueCompletion().

                    keepRunning = FALSE != m_RIO.RIOReceiveEx(
                        m_requestQueue,
                        &slice,
                        1,
                        nullptr,
                        &m_pAddresses[bufIndex],
                        nullptr,
                        nullptr,
                        recvFlags,
                        &slice);

                }
                return keepRunning;
            }

            StatusCode SendSlice(unsigned sliceHandle)
            {
                auto state = SliceBody(sliceHandle)->State;
                if (state != SliceState::InChainAndSending && state != SliceState::Sending)
                {
                    char msg[128];
                    sprintf_s(msg, "SendSlice expect slice state to be sending, found: %d", state);
                    Audit::OutOfLine::Fail(StatusCode::Unexpected, msg);
                }

                auto status = StatusCode::UnspecifiedError;
                _RIO_BUF* pSendSlice = &m_pSlices[sliceHandle];
                _RIO_BUF* pAddressSlice = &m_pAddresses[sliceHandle];
                {
                    Utilities::Exclude < CRITICAL_SECTION > guard{ m_requestQueueCriticalSection };

                    auto success = m_RIO.RIOSendEx(m_requestQueue, pSendSlice, 1, nullptr, pAddressSlice, nullptr, nullptr, 0, pSendSlice);
                    if (success)
                    {
                        status = StatusCode::OK;
                    }
                    else
                    {
                        status = (StatusCode)::WSAGetLastError();
                    }
                }
                return status;
            }


            void Cleanup()
            {
                m_sessions.RequestShutdown();
                m_pScheduler->RequestShutdown(StatusCode::OK, L"Destructing Service Broker.");

                if (m_homeSocket != INVALID_SOCKET)
                {
                    closesocket(m_homeSocket);
                    m_homeSocket = INVALID_SOCKET;
                }
                m_pMemory = nullptr;

                // Release resources used by the critical section object.
                DeleteCriticalSection(&m_requestQueueCriticalSection);

                WSACleanup();
                m_serviceName.clear();
            }

            StatusCode SendSinglePacket(_In_ const Request& request, _In_ const void* blob, uint32_t blobLength)
            {
                uint16_t sliceHandle = AllocSendSlicesHandle();
                auto pSend = SliceBody(sliceHandle);

                pSend->Addr.Ipv4.sin_family = AF_INET;
                pSend->Addr.Ipv4.sin_port = htons(request.RemotePort);
                pSend->Addr.Ipv4.sin_addr.S_un.S_addr = htonl(request.RemoteIPv4);

                if (blobLength > 0)
                {
                    uint32_t totalSize = blobLength + sizeof(Request) + CHKSUM_SIZE;
                    Audit::Assert(totalSize <= DATAGRAMSIZE, "packet too big!");
                    m_pSlices[sliceHandle].Length = totalSize;

                    uint32_t leftcrc = 0;
                    uint32_t rightcrc = 0;
                    ComputeCRC(&pSend->Req, sizeof(pSend->Req),
                        &request, sizeof(Request), leftcrc, rightcrc);
                    ComputeCRC(pSend->Blob,
                        blob, blobLength, leftcrc, rightcrc);

                    auto pCrc = reinterpret_cast<uint32_t*>(&(pSend->Blob[blobLength]));
                    *pCrc = leftcrc;
                    pCrc++;
                    *pCrc = rightcrc;
                    Audit::Assert(pCrc < (uint32_t*)&pSend->Addr,
                        "Buffer overrun when writing RIO slice.");
                }
                else
                {
                    // this is a control packet with empty body
                    m_pSlices[sliceHandle].Length = sizeof(Request);
                    pSend->Req = request;
                }

                pSend->Next = sliceHandle;
                pSend->State = SliceState::Sending;

                auto res = SendSlice(sliceHandle);
                if (res != StatusCode::OK){
                    Audit::OutOfLine::Fail("Send slice fail.");
                }
                return res;
            }

            StatusCode SendMultiPackets(
                _In_ const Request& request,
                _In_ const void* blob,
                uint32_t blobLength
                )
            {
                Audit::Assert(blobLength + sizeof(Request) > DATAGRAMSIZE, "Message too small!");

                // compute how many packet we need, remember to add Request header on
                // each packet

                uint32_t totalPck = (blobLength + CHKSUM_SIZE) / BLOB_SIZE;
                totalPck += ((blobLength + CHKSUM_SIZE) % BLOB_SIZE) > 0 ? 1 : 0;
                Audit::Assert(totalPck < CQ_COUNT/3,
                    "Message too long, choking the ServiceBrokerRIO.");

                SessionMapType::EntryType* ptr = nullptr;
                bool newSession = m_sessions.CreateSendSession
                    (request.Tracer, static_cast<uint16_t>(totalPck), ptr);

                if (!newSession){
                    // a retry comes before we finish sending
                    return StatusCode::OK;
                }

                // Now we have a new session created, so we need to worry about
                // race conditions. So set status last

                Audit::Assert(ptr != nullptr,
                    "unable to locate newly created udp send session!");
                SessionType& sendSession = ptr->second;
                Audit::Assert(sendSession.m_seq == 0 
                    || sendSession.m_status == SessionStatus::Initialing,
                    "Newly created UDP send session corruptted.");

                uint32_t leftcrc = 0;
                uint32_t rightcrc = 0;

                // copy data into RIO slices so the caller don't have to keep the data,
                for (uint16_t seq = 0; seq < totalPck; seq++){
                    uint16_t sliceHandle = AllocSendSlicesHandle();
                    auto pData = SliceBody(sliceHandle);

                    // keep refreshing our lifetime in case we stuck behind a lock 
                    sendSession.m_tickDown = SESSIONTICKS;

                    // TODO!! Paranoid check, remove.
                    _RIO_BUF* pAddressSlice = &m_pAddresses[sliceHandle];
                    Audit::Assert(&pData->Addr == m_pMemory->PData(pAddressSlice->Offset),
                        "Address slice points to the wrong place!");

                    pData->Addr.Ipv4.sin_family = AF_INET;
                    pData->Addr.Ipv4.sin_port = htons(request.RemotePort);
                    pData->Addr.Ipv4.sin_addr.S_un.S_addr = htonl(request.RemoteIPv4);

                    if (seq == 0){
                        // request header is replicated to all slices, but we only
                        // compute crc for it once
                        ComputeCRC(&pData->Req, sizeof(pData->Req),
                            &request, sizeof(Request), leftcrc, rightcrc);
                    }
                    else {
                        pData->Req = request;
                    }

                    size_t dataOffset = seq * BLOB_SIZE;
                    uint32_t dataSize = (uint32_t)std::min(BLOB_SIZE,
                        blobLength - dataOffset);
                    Audit::Assert(dataOffset + dataSize <= blobLength,
                        "Packet data exceeds boundary!");
                    m_pSlices[sliceHandle].Length = dataSize + sizeof(Request);

                    ComputeCRC(pData->Blob,
                        (char*)blob + dataOffset, dataSize, leftcrc, rightcrc);

                    if (seq == totalPck - 1){
                        // last packet, append crc;
                        auto pCrc = reinterpret_cast<uint32_t*>(&pData->Blob[dataSize]);
                        *pCrc = leftcrc;
                        pCrc++;
                        *pCrc = rightcrc;
                        Audit::Assert(pCrc < (uint32_t*)&pData->Addr,
                            "Buffer overrun when writing RIO slice.");
                        m_pSlices[sliceHandle].Length += CHKSUM_SIZE;
                    }
                    pData->Next = INV_NEXT;
                    pData->State = SliceState::InChain;

                    if (seq == 0)
                    {
                        sendSession.m_firstSlice = sliceHandle;
                        sendSession.m_lastSlice = sliceHandle;
                        pData->Req.Sequence = 0 - static_cast<int16_t>(totalPck);
                    }
                    else
                    {
                        SliceBody(sendSession.m_lastSlice)->Next = (uint16_t)sliceHandle;
                        sendSession.m_lastSlice = (uint16_t)sliceHandle;
                        pData->Req.Sequence = seq;
                    }
                }
                if (sendSession.m_tickDown < SESSIONTICKS / 2){
                    Tracer::LogWarning(StatusCode::Unexpected, L"UDP send preparation took too long.");
                }
                SliceBody(sendSession.m_firstSlice)->State = SliceState::InChainAndSending;
                sendSession.m_tickDown = SESSIONTICKS;

                // Now we open the door so race with Poll could happen.
                Audit::Assert(sendSession.m_status == SessionStatus::Initialing,
                    "UDP send session corruption at the end of creation.");
                sendSession.m_status = SessionStatus::Sending;

                auto res = SendSlice(sendSession.m_firstSlice);
                if (res != StatusCode::OK){
                    Audit::OutOfLine::Fail(res, "send slice fail");
                }
                return res;
            }


            // An ack is received over the network
            //
            void OnRcvAck(
                _In_ Request const * pPckt,
                size_t pcktSize
                )
            {
                uint16_t numToSend = 0;
                if (sizeof(Request) != pcktSize)
                {
                    // TODO!! Change to error logging in production, 
                    // don't crash server by user's mistake
                    Audit::OutOfLine::Fail(StatusCode::Unexpected, "ack packet should have no content");
                    return;
                }

                SessionMapType::EntryType* pKS = m_sessions.FindSendSession(pPckt->Tracer);

                if (pKS == nullptr
                    || pKS->second.m_status != SessionStatus::Sending){
                    return;
                }

                SessionType& sendSession = pKS->second;
                auto seq = sendSession.m_seq;
                int16_t ack = pPckt->Sequence;

                // We only want to react to expected ack and ignore the rest.
                // As these communications are plain text, an attacker can
                // easily fake ack packets. If we respond only to expected ack,
                // under normal network condition, the receiver has a high
                // chance of receiving all the packets.
                //     The expected ack is:
                // if we are sending first or last packet, then ack should be seq+1
                // else (0 < seq < total-1) then ack should be seq + 2
                if (ack == (seq + 1) && (seq == 0 || seq == (sendSession.m_totalPck - 1))
                    || ack == (seq + 2) && 0 < seq && seq < (sendSession.m_totalPck - 1)
                    )
                {
                    // slices to be recycled
                    auto head = sendSession.m_firstSlice;
                    if (head == INV_NEXT)
                        return;

                    auto tail = head;
                    if (ack == seq + 2)
                    {
                        tail = SliceBody(tail)->Next;
                    }
                    if (tail == INV_NEXT){
                        return; // race, another thread freed the slices
                    }

                    uint16_t toSend[2] = { SliceBody(tail)->Next, INV_NEXT };
                    if (ack > 0 && ack < sendSession.m_totalPck - 1 && toSend[0] != INV_NEXT)
                    {
                        toSend[1] = SliceBody(toSend[0])->Next;
                    }

                    { ////// critical section //////
                        Utilities::Exclude<SRWLOCK> guard{ sendSession.m_lock };

                        // check session id here, it is possible
                        // the session space has been reclaimed and repurposed
                        if (sendSession.m_status != SessionStatus::Sending
                            || sendSession.m_seq != seq
                            || sendSession.m_firstSlice != head
                            || (pPckt->Tracer != pKS->first))
                        {
                            Audit::Assert(numToSend == 0, "unable to send some slice");
                            return;
                        }
                        sendSession.m_tickDown = SESSIONTICKS;
                        sendSession.m_seq = ack;

                        // get rid of confirmed packets
                        sendSession.m_firstSlice = toSend[0];

                        if (ack == sendSession.m_totalPck)
                        {
                            Audit::Assert(toSend[0] == INV_NEXT,
                                "slices leaked from send session");
                            // we are finished. 
                            sendSession.m_tickDown = 0;
                            sendSession.m_status = SessionStatus::Finished;
                        }
                        else
                        {
                            // stage next two packets for sending
                            for (int i = 0; i < 2; i++)
                            {
                                if (toSend[i] == INV_NEXT)
                                    break;

                                // since we checked at the beginning of the 
                                // critical section, these two packets
                                // are still in the session. At this
                                // point terminator can not come in and
                                // change their state to Free.
                                // so no need to use interlock
                                auto pData = SliceBody(toSend[i]);
                                Audit::Assert(pData->State == SliceState::InChain,
                                    "unexpected slice state");
                                pData->State = SliceState::InChainAndSending;
                                numToSend++;
                            }
                        }
                    } ////// critical section //////

                    Audit::Assert(head != INV_NEXT,
                        "slice for acknowledged segment got lost during send");
                    SliceBody(tail)->Next = INV_NEXT;
                    auto numReleased = RecycleSendChain(head, tail);
                    Audit::Assert(numReleased == ((head == tail) ? 1 : 2),
                        "Partial send chain release failed.");

                    for (int i = 0; i < 2; i++)
                    {
                        if (toSend[i] == INV_NEXT)
                            break;
                        // terminator might have freed them, since their
                        // state was InChainAndSending, freeing makes
                        // them Sending, no harm done.
                        Audit::Assert(SliceBody(toSend[i])->Req.Sequence == ack + i,
                            "unexpected sequence number found in RIO buffer");
                        auto res = SendSlice(toSend[i]);
                        numToSend--;
                        if (res != StatusCode::OK){
                            Audit::OutOfLine::Fail(res, "send slice fail!");
                        }
                    }
                }
                Audit::Assert(numToSend == 0, "unable to send some slice");
            }

            void SendAck(int16_t seq, int16_t totalPkt, const Tracer::TraceRec& id, uint32_t ip, uint16_t port)
            {
                if (seq < totalPkt && seq % 2 == 1 || seq == totalPkt)
                {
                    // send ack
                    Request ack{ ntohl(ip), ntohs(port), seq, ServiceVerb::Ack, id };
                    SendSinglePacket(ack, nullptr, 0);
                }
            }

            // Called when we receive a packet as part of a long message. 
            // Invoke worker if the whole thing is received.
            // Returns true if the packet needs to be reclaimed
            // Returns false if the packet is linked into a long message
            // thus should not be reclaimed
            //
            bool OnRcvPacket(uint16_t sliceHandle)
            {
                auto pPckt = SliceBody(sliceHandle);

                // TODO!!! change into error logging in production
                // assertion is used for testing.
                Audit::Assert(pPckt->Req.Sequence != 0 && pPckt->Req.Sequence != -1,
                    "Invalid sequence number from an incoming message!");

                if (pPckt->Req.Sequence < 0)
                {
                    return OnRcvFirstPacket(pPckt, sliceHandle);
                }
                else{
                    return OnRcvSubsequentPacket(pPckt, sliceHandle);
                }
            }

            // Called when we received a packet, and it is the first of 
            // a long message
            // return true when the packet is free to be recycled
            //
            bool OnRcvFirstPacket(PktSlice* pPckt, uint16_t sliceHandle)
            {
                SessionMapType::EntryType* pKS = nullptr;
                auto remoteIPv4 = pPckt->Addr.Ipv4.sin_addr.S_un.S_addr;
                auto remotePort = pPckt->Addr.Ipv4.sin_port;

                // This is the first packet, sequence is actually
                // 0 - total_packets
                bool newSession = m_sessions.CreateRcvSession(
                    pPckt->Req.Tracer, 0 - pPckt->Req.Sequence, pKS);
                if (newSession)
                {
                    // TODO!!! change into error logging in production
                    // assertion is used for testing.
                    // we should not merge if the this condition fail
                    size_t pcktSize = m_pSlices[sliceHandle].Length;
                    Audit::Assert(pcktSize == ServiceBroker::DATAGRAMSIZE,
                        "First packet expected to be full size");

                    // now we have a new session, with no lock, but there
                    // shoudl be no race condition. the remote should
                    // only send the second packet after we send back
                    // the ack.
                    if (pKS->second.m_tickDown < SESSIONTICKS / 2)
                    {
                        Tracer::LogWarning(StatusCode::Unexpected, L"UDP rcv session creation took too long");
                    }
                    pKS->second.m_seq = 1;
                    pKS->second.m_tickDown = SESSIONTICKS;
                    pPckt->Next = INV_NEXT;
                    pKS->second.m_firstSlice = sliceHandle;
                    pKS->second.m_lastSlice = sliceHandle;
                    pKS->second.m_status = SessionStatus::Receiving;

                    Request ack = pPckt->Req;
                    ack.Sequence = 1;
                    ack.Verb = ServiceVerb::Ack;
                    ack.RemoteIPv4 = ntohl(remoteIPv4);
                    ack.RemotePort = ntohs(remotePort);

                    SendSinglePacket(ack, nullptr, 0);

                    return false; // packet chained, should not recycle
                }
                else
                {
                    SendAck(pKS->second.m_seq, pKS->second.m_totalPck, 
                        pPckt->Req.Tracer, remoteIPv4, remotePort);
                    return true; // packet should be recycled
                }
            }

            // Called when we receive a part of a long message, but it is
            // not the first packet
            // return true when the packet is free to be recycled
            bool OnRcvSubsequentPacket(PktSlice* pPckt, uint16_t sliceHandle)
            {
                bool reclaimPckt = true;
                SessionMapType::EntryType* pKS = 
                    m_sessions.FindRcvSession(pPckt->Req.Tracer);
                if (pKS == nullptr)
                    return reclaimPckt;

                size_t pcktSize = m_pSlices[sliceHandle].Length;
                if (pcktSize != ServiceBroker::DATAGRAMSIZE
                    && pPckt->Req.Sequence < pKS->second.m_totalPck - 1
                    )
                {
                    // TODO!! Change to error logging, don't let user error crash server.
                    Audit::OutOfLine::Fail(StatusCode::Unexpected, "Non-tail packet must be full");
                    return reclaimPckt;
                }

                auto seq = pKS->second.m_seq;
                auto totalPkt = pKS->second.m_totalPck;
                auto remoteIPv4 = pPckt->Addr.Ipv4.sin_addr.S_un.S_addr;
                auto remotePort = pPckt->Addr.Ipv4.sin_port;

                // collect the slice chain if this is the last packet
                bool lastPacket = false;
                uint16_t head = INV_NEXT;
                uint16_t tail = INV_NEXT;

                for (int retry = 4; retry > 0; retry--)
                {
                    if (pKS->second.m_status != SessionStatus::Receiving)
                    {
                        break;
                    }
                    if (pKS->second.m_seq != pPckt->Req.Sequence
                        && pKS->second.m_seq != pPckt->Req.Sequence+1)
                    {
                        break;
                    }

                    { ////// critical section //////
                        Utilities::Exclude<SRWLOCK> guard{ pKS->second.m_lock };
                        SessionType& rcvSession = pKS->second;
                        // it is possible the space has been repurposed for another session.
                        // check session ID here!
                        if (!std::equal_to<Tracer::TraceRec>()(pKS->first, pPckt->Req.Tracer)){
                            return reclaimPckt;
                        }
                        if (rcvSession.m_status == SessionStatus::Receiving
                            && pPckt->Req.Sequence == rcvSession.m_seq
                            )
                        {
                            Audit::Assert(rcvSession.m_lastSlice != INV_NEXT,
                                "Corrupted chain found during receiving!");

                            auto lastTailPckt = SliceBody(rcvSession.m_lastSlice);
                            Audit::Assert(((lastTailPckt->Req.Sequence == 0 - rcvSession.m_totalPck) && (pPckt->Req.Sequence == 1))
                                || lastTailPckt->Req.Sequence == pPckt->Req.Sequence - 1,
                                "Sequence Not continous during receiving!");
                            Audit::Assert(lastTailPckt->Next == INV_NEXT,
                                "Rcv Chain is not null terminated.");

                            rcvSession.m_seq++;
                            reclaimPckt = false;

                            pPckt->Next = INV_NEXT;
                            lastTailPckt->Next = sliceHandle;
                            rcvSession.m_lastSlice = sliceHandle;
                            rcvSession.m_tickDown = SESSIONTICKS;

                            if (rcvSession.m_seq == rcvSession.m_totalPck)
                            {
                                lastPacket = true;
                                head = rcvSession.m_firstSlice;
                                tail = rcvSession.m_lastSlice;
                                totalPkt = rcvSession.m_totalPck;

                                rcvSession.m_firstSlice = INV_NEXT;
                                rcvSession.m_lastSlice = INV_NEXT;
                                rcvSession.m_seq = rcvSession.m_totalPck + 1;
                                rcvSession.m_status = SessionStatus::Finished;
                                // reclaim soon but leave time to send ack
                                rcvSession.m_tickDown = 2; 
                            }
                            seq = rcvSession.m_seq;
                            break;
                        }
                    } ////// critical section //////
                }
                SendAck(seq, totalPkt, pPckt->Req.Tracer, remoteIPv4, remotePort);
                pKS = nullptr;

                if (lastPacket)
                {
                    HarvestSlices(head, tail, totalPkt);
                }
                return reclaimPckt;
            }

            void HarvestSlices(uint16_t head, uint16_t tail, int16_t totalPkt)
            {
                // need to count the Request size exactly once.
                size_t tailSize = m_pSlices[tail].Length;
                auto msgSize = (uint32_t)((totalPkt - 1) * BLOB_SIZE + tailSize);

                auto headPtr = SliceBody(head);
                auto pWork = m_verbBindings[(uint8_t)(headPtr->Req.Verb)];
                auto envelope = pWork->AcquireBuffer(msgSize - CHKSUM_SIZE);

                if (envelope.Contents() == nullptr)
                {
                    RecycleRcvChain(head, tail);
                    return;
                }

                char* dstPtr = (char*)envelope.Contents()->PData() + envelope.Contents()->Offset();
                auto dstLen = envelope.Contents()->Size() - envelope.Contents()->Offset();

                // Compute crc of the first Request header, as we did it in send.
                uint32_t leftcrc = 0;
                uint32_t rightcrc = 0;
                headPtr->Req.Sequence = 0;
                ComputeCRC(dstPtr, dstLen, 
                    &headPtr->Req, (uint32_t)sizeof(headPtr->Req),
                    leftcrc, rightcrc);

                // TODO!!! Paranoid, remove
                SOCKADDR_INET* pSenderAddr = (SOCKADDR_INET*)(m_pMemory->PData(m_pAddresses[head].Offset));
                Audit::Assert(pSenderAddr == &headPtr->Addr,
                    "Address slice points to the wrong place.");
                auto pReq = reinterpret_cast<Request*>(dstPtr);
                pReq->RemoteIPv4 = ntohl(pSenderAddr->Ipv4.sin_addr.S_un.S_addr);
                pReq->RemotePort = ntohs(pSenderAddr->Ipv4.sin_port);

                dstPtr += sizeof(headPtr->Req);
                dstLen -= sizeof(headPtr->Req);

                uint16_t seq = 0;
                auto index = head;
                auto prev = head;
                while (index != INV_NEXT && index != tail)
                {
                    auto pckt = SliceBody(index);

                    Audit::Assert(pckt->Req.Sequence == seq,
                        "Invalid packet sequence found during collection.");
                    seq++;

                    Audit::Assert(m_pSlices[index].Length == DATAGRAMSIZE,
                        "Non-tail packet should be full.");
                    ComputeCRC(dstPtr, dstLen, pckt->Blob, BLOB_SIZE, leftcrc, rightcrc);
                    dstPtr += BLOB_SIZE;
                    Audit::Assert(dstLen >= BLOB_SIZE,
                        "Buffer overrun when harvesting received message.");
                    dstLen -= BLOB_SIZE;

                    prev = index;
                    index = pckt->Next;
                }

                Audit::Assert(index == tail, "slice link corrupted!");
                auto tailPtr = SliceBody(tail);
                Audit::Assert(tailPtr->Req.Sequence == seq && seq == totalPkt - 1,
                    "Invalid packet sequence found during collection.");

                auto pcktLen = (uint32_t)(tailSize - sizeof(Request) - CHKSUM_SIZE);
                ComputeCRC(dstPtr, dstLen, tailPtr->Blob, pcktLen, leftcrc, rightcrc);
                dstPtr += pcktLen;
                Audit::Assert(dstLen >= pcktLen,
                    "Buffer overrun when harvesting received message.");
                dstLen -= pcktLen;

                // verify crc
                auto plcrc = reinterpret_cast<const uint32_t*>
                    (&tailPtr->Blob[pcktLen]);
                auto prcrc = plcrc + 1;
                Audit::Assert(prcrc < (uint32_t*)&tailPtr->Addr, "Buffer overrun.");
                bool crcCorrect = leftcrc == *plcrc && rightcrc == *prcrc;

                RecycleRcvChain(head, tail);
                head = INV_NEXT;
                tail = INV_NEXT;

                if (!crcCorrect)
                {
                    Tracer::LogWarning(StatusCode::InvalidState,
                        L"CRC error found in remote message");
                }
                else
                {
                    pWork->DoWork(std::move(envelope));
                }
            }

            // recycle all the RIO buffers in a receiving chain
            // assume the chain is INV_NEXT terminated
            //
            void RecycleRcvChain(uint16_t head, uint16_t tail)
            {
                uint16_t old = INV_NEXT;
                while (head != INV_NEXT)
                {
                    old = head;
                    head = SliceBody(head)->Next;
                    RecycleSlice(m_pSlices[old]); // receive
                }
                Audit::Assert(old == tail,
                    "corrupted slice chain detected");
            }

            // recycle all the RIO buffers in a chain
            // assume the chain is INV_NEXT terminated
            int16_t RecycleSendChain(uint16_t head, uint16_t tail)
            {
                uint16_t old = INV_NEXT;
                int16_t num = 0;
                while (head != INV_NEXT)
                {
                    old = head;
                    auto pS = SliceBody(head);
                    head = pS->Next;
                    ReleaseSendSegment(pS);
                    num++;
                }
                Audit::Assert(old == tail,
                    "corrupted slice chain detected");
                return num;
            }

            void ReleaseSendSegment(PktSlice* pS)
            {

                SliceState oldState = (SliceState)
                    InterlockedCompareExchange16(
                    (short*)&pS->State,
                    (uint16_t)SliceState::Sending,
                    (uint16_t)SliceState::InChainAndSending);

                if (oldState != SliceState::InChainAndSending)
                {
                    // this slice has been taken off from the session
                    // thus nobody can get hold of him and change his
                    // state any more
                    Audit::Assert(oldState == SliceState::InChain,
                        "expect slice to be in chain");
                    pS->State = SliceState::Free;
                    RecycleSendSlice(pS);
                }
            }

        };

    }

    std::unique_ptr<ServiceBroker> ServiceBrokerRIOFactory(
        _In_ const std::wstring& name,           // the name of the service
        uint16_t port
        )
    {
        return std::make_unique<ServiceBrokerRIO>(name, port);
    }

}
