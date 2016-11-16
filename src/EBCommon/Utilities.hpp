// Utilities
//

#pragma once

#include "stdafx.h"

#include <memory>
#include <string>
#include <atomic>

#include "StatusWin.hpp"

/* named constants for self documenting code
(Systeme Internationale)
*/
namespace SI
{
    const int Ki = 1024;
    const int Mi = Ki * Ki;
    const int64_t Gi = Mi * Ki;

    const double milli = 1e-3;
    const double micro = 1e-6;
}

namespace Audit
{
    using namespace Utilities;

    /* Assertions designed for use in production code (not limited to debug builds).
       Avoid the evaluation of arguments except in failure paths.  The only
       cost for normal paths should be the overhead of the predicate and
       a predictable branch.
       Audit APIs should support message internationalization.  Until then
       Audit is only useful in places where non-translation is acceptable.
       */
    struct OutOfLine
    {
        static void Fail(const char* utf8message);

        static void FailArgNull(const char* utf8message);

        static void FailArgRule(const char* utf8message);

        static void Log(const StatusCode status, const char* utf8message);

        static void Log(const char* utf8message);

        static void LogArgNull(const char* utf8message);

        static void LogArgRule(const char* utf8message);

        static void Fail(const StatusCode status);

        static void Fail(const StatusCode status, const char* const message);
    };

    // evaluate conditions inline to avoid evaluating arguments until necessary

    inline void Assert(StatusCode status)
    {
        if (status != StatusCode::OK)
        {
            OutOfLine::Fail(status);
        }
    }

    inline void Assert(bool invariant)
    {
        if (!invariant)
        {
            OutOfLine::Fail("assert");
        }
    }

    inline void Assert(bool invariant, const char* utf8message)
    {
        if (!invariant)
        {
            OutOfLine::Fail(utf8message);
        }
    }

    inline void ArgNotNull(void* value, const char* utf8message)
    {
        if (value == nullptr)
        {
            OutOfLine::FailArgNull(utf8message);
        }
    }

    inline void ArgRule(bool invariant, const char* utf8message)
    {
        if (!invariant)
        {
            OutOfLine::FailArgRule(utf8message);
        }
    }

    inline void NotImplemented()
    {
        OutOfLine::Fail("not implemented");
    }

    inline void NotImplemented(const char* utf8message)
    {
        OutOfLine::Fail(utf8message);
    }

    // Verifications are used by senders when validating requests from elswhere.
    // As defensive code, they should log errors, not Fail the server.

    inline bool Verify(bool invariant)
    {
        if (!invariant)
        {
            OutOfLine::Log("assert");
        }
        return invariant;
    }

    inline bool Verify(bool invariant, const char* utf8message)
    {
        if (!invariant)
        {
            OutOfLine::Log(utf8message);
        }
        return invariant;
    }

    inline bool VerifyArgNotNull(void* value, const char* utf8message)
    {
        bool invariant = (value != nullptr);
        if (!invariant)
        {
            OutOfLine::LogArgNull(utf8message);
        }
        return invariant;
    }

    inline bool VerifyArgRule(bool invariant, const char* utf8message)
    {
        if (!invariant)
        {
            OutOfLine::LogArgRule(utf8message);
        }
        return invariant;
    }

    inline void VerifyNotImplemented(const char* utf8message)
    {
        OutOfLine::Log(utf8message);
    }
}

namespace Utilities
{
    // Provide an interface for reclaiming stuff, usually for
    // resources stuck around in activities
    // 
    class Disposable
    {
    public:
        virtual void Dispose() = 0;
    };

    // an aligned memory buffer to be passed around should only be passed
    // around as pointers. Dispose should be suicidle
    //
    // Unbuffered I / O require aligned memory buffer. But the actual blob
    // starting point might not be aligned, thus an offset is needed
    //
    class DisposableBuffer : public Disposable
    {
    public:
        virtual void* PData() const = 0;
        virtual uint32_t Size() const = 0;
        virtual uint32_t Offset() const = 0;
        virtual void SetOffset(size_t v) = 0;
    };

    // An aligned buffer allocated from heap. Not copiable
    //
    class HeapBuffer : public DisposableBuffer
    {
    private:
        uint32_t const m_blobSize;
        uint32_t m_offset;

        HeapBuffer(uint32_t size, uint32_t offset)
            : m_blobSize(size)
            , m_offset(offset)
        {}

        HeapBuffer& operator=(const HeapBuffer&) = delete;
        HeapBuffer(const HeapBuffer&) = delete;
    public:
        void Dispose() override
        {
            ::_aligned_free(PData());
        }

        void* PData() const override
        {
            return (char*)this - m_blobSize;
        }

        uint32_t Offset() const override { return m_offset; }
        void SetOffset(size_t v) override
        {
            Audit::Assert(v < (1ull << 32));
            m_offset = (uint32_t) v;
        }

        uint32_t Size() const override { return m_blobSize; }

        static DisposableBuffer* Allocate(uint32_t dataBlobSize, uint32_t alignment = 8, uint32_t offset = 0)
        {
            Audit::ArgRule(((alignment - 1) ^ (0 - alignment)) == ~0, "alignment must be a power of 2");

            void* buf = ::_aligned_malloc(dataBlobSize + sizeof(HeapBuffer), alignment);
            Audit::Assert(buf != nullptr, "can not allocate memory from heap!");

            // stick the recycler at the end of the buffer to keep data buffer aligned
            HeapBuffer* ret = new ((char*)buf + dataBlobSize)HeapBuffer(dataBlobSize, offset);
            return ret;
        }
    };

    // Allow a disposible buffer to pass around between continuations
    // only one continuation can have ownership of the buffer at a time
    // enforced by non-copyable
    //
    class BufferEnvelope 
    {
        DisposableBuffer* m_pBuffer = nullptr;

        BufferEnvelope(const BufferEnvelope& other) = delete;
        BufferEnvelope& operator=(const BufferEnvelope&) = delete;

    public:

        BufferEnvelope()
        {}

        BufferEnvelope& operator=(DisposableBuffer*& src)
        {
            Audit::Assert(!m_pBuffer, "Envelope full!.");
            std::swap(m_pBuffer, src);
            return *this;
        }

        BufferEnvelope(DisposableBuffer*& src)
        {
            Audit::Assert(!m_pBuffer, "Envelope full!.");
            std::swap(m_pBuffer, src);
        }

        BufferEnvelope(BufferEnvelope&& src)
        {
            Audit::Assert(!m_pBuffer, "Envelope full!.");
            std::swap(m_pBuffer, src.m_pBuffer);
        }

        BufferEnvelope& operator=(BufferEnvelope&& src)
        {
            Audit::Assert(!m_pBuffer, "Envelop should be empty.");
            std::swap(m_pBuffer, src.m_pBuffer);
            return *this;
        }

        void Recycle() 
        {
            if (m_pBuffer != nullptr){
                m_pBuffer->Dispose();
                m_pBuffer = nullptr;
            }
        }

        DisposableBuffer* Contents() const
        {
            return m_pBuffer;
        }

        // giving away the buffer, this evelope becomes empty
        // likely leak, use with caution
        //
        DisposableBuffer* ReleaseContents()
        {
            auto pBuffer = m_pBuffer;
            m_pBuffer = nullptr;
            return pBuffer;
        }

        virtual ~BufferEnvelope()
        {
            Recycle();
        }
    };

    // Utility to automatically flip a boolean when exit a scope
    // 
    class FlipWhenExist
    {
        bool& m_value;

        FlipWhenExist(const FlipWhenExist&) = delete;
        FlipWhenExist& operator=(const FlipWhenExist&) = delete;

    public:
        FlipWhenExist(bool& v): m_value(v)
        {
        }

        ~FlipWhenExist()
        {
            m_value = !m_value;
        }
    };

    /* converst a wide string to Utf8 chars
    */
    int wsToCharBuf(const std::wstring& s, char* buf, int bufLen);

    /**** use RAII pattern for Try..Finally.  C++ spec section 6.6, 6.6.3. ****/
    /*
    This seems a lot of work to get a Try..Finally pattern and I wonder how efficient the code is.
    But Stroustrup says it is the best!  And C++ has no "finally", so, let's experiment and see.
    */
    template<typename Guard>
    struct ExcludedRegion
    {
        static void Exclude(_In_ Guard& lock)
        {
            throw std::runtime_error{ "must be specialized - did you include UtilitiesWin?" };
        }
        static void Release(_In_ Guard& lock)
        {
            throw std::runtime_error{ "must be specialized - did you include UtilitiesWin?" };
        }
    };

    template<typename Guard>
    struct SharedRegion
    {
        static void Share(_In_ Guard& lock)
        {
            throw std::runtime_error{ "must be specialized - did you include UtilitiesWin?" };
        }
        static void Release(_In_ Guard& lock)
        {
            throw std::runtime_error{ "must be specialized - did you include UtilitiesWin?" };
        }
    };

    // Exclusive region. Acquire a exclusive lock when contruct,
    // releasing it when destructed. 
    //
    template<typename Guard>
    class Exclude
    {
        Guard* m_guard;
    public:
        Exclude(_In_ Guard& guard)
        {
            m_guard = nullptr;
            ExcludedRegion<Guard>::Exclude(guard);
            m_guard = &guard;
        }

        ~Exclude()
        {
            if (nullptr != m_guard)
                ExcludedRegion<Guard>::Release(*m_guard);
            m_guard = nullptr;
        }
    };

    template<typename Guard>
    class Share
    {
        Guard* m_guard;
    public:
        Share(
            // an object supporting shared reader lock pattern
            _In_ Guard& guard
            )
        {
            m_guard = nullptr;
            SharedRegion<Guard>::Share(guard);
            m_guard = &guard;
        }

        ~Share()
        {
            if (nullptr != m_guard)
                SharedRegion<Guard>::Release(*m_guard);
            m_guard = nullptr;
        }
    };

    /*********** end of the Try..Finally region *********************/

    // A lock free (interlocked) queue for thread safe operation.
    // The capacity of the queue is bounded and must be power of 2.
    // Example usage : a resources pool
    //
    template <typename BT>
    class BoundedBuf
    {
        BT* m_buf;

        const uint32_t m_capacity;

        std::atomic<uint32_t> m_write = 0;
        std::atomic<uint32_t> m_read = 0;

        uint32_t Plus(uint32_t a, uint32_t b)
        {
            return (a + b) & (2 * m_capacity - 1);
        }

        uint32_t Minus(uint32_t a, uint32_t b)
        {
            return (a - b) & (2 * m_capacity - 1);
        }

    public:
        // Capacity must be power of 2, we do not need an extra
        // item to find out whehter the ring buffer is empty
        // or full, 
        // See Dr. Leslie Lamport bounded buffer discussion in TLA+ hand book.
        // 
        BoundedBuf(uint32_t capacity)
            :m_capacity(capacity)
        {
            Audit::Assert(m_capacity > 0 &&
                ((m_capacity & (m_capacity-1)) == 0 ),
                "BoundedBuf capacity must be power of 2");

            Audit::Assert(m_capacity <= (1 << 30),
                "Do not support capacity this large. Use 64b integer for bigger buffer");
            m_buf = new BT[m_capacity];
        }

        size_t GetSize()
        {
            auto writeEdge = m_write.load(std::memory_order_relaxed);
            auto readEdge = m_read.load(std::memory_order_relaxed);
            return Minus(writeEdge, readEdge);
        }

        uint32_t GetCapacity() { return m_capacity; }

        void Add(const BT& v)
        {
            auto writeEdge = m_write.load(std::memory_order_relaxed);
            auto readEdge = m_read.load(std::memory_order_relaxed);
            for (int retry = 0; retry < 32; retry++)
            {
                if (Minus(writeEdge, readEdge) >= m_capacity){
                    readEdge = m_read.load(std::memory_order_seq_cst);
                    Audit::Assert(Minus(writeEdge, readEdge) < m_capacity, "Bounded Buffer Exceeds Capacity!");
                }

                auto newWriteEdge = Plus(writeEdge, 1);
                if (!m_write.compare_exchange_weak(writeEdge, newWriteEdge))
                {
                    continue;
                }

                m_buf[writeEdge & (m_capacity - 1)] = v;
                return;
            }
            Audit::OutOfLine::Fail(Utilities::StatusCode::Unexpected, "Bounded Buffer insertion failed with too many retry.");
        }

        BT Get()
        {
            auto writeEdge = m_write.load(std::memory_order_relaxed);
            auto readEdge = m_read.load(std::memory_order_relaxed);

            for (int retry = 0; retry < 32; retry++)
            {
                if (writeEdge == readEdge)
                {
                    writeEdge = m_write.load(std::memory_order_seq_cst);
                    Audit::Assert(writeEdge != readEdge, "Trying to get from an empty bounded buffer!");
                }

                // can not use reference here, or else this value maybe modified after we finished
                // compare and exchange but before we return.
                BT ret = m_buf[readEdge & (m_capacity - 1)];

                auto newReadEdge = Plus(readEdge, 1);
                if (m_read.compare_exchange_weak(readEdge, newReadEdge))
                {
                    return ret;
                }
            }
            Audit::OutOfLine::Fail(Utilities::StatusCode::Unexpected, "Bounded Buffer Get failed with too many retry.");
            return BT{};
        }

        ~BoundedBuf() { delete m_buf; }
    };

    class LargePageBuffer
    {
        void* m_pBuffer;
        size_t m_allocatedSize;

    public:
        // Allocate multiple units of buffers contiguously in large pages which will stay resident
        LargePageBuffer(
            // the caller may consider the memory to be an array of buffers of this size.
            // Must be > 0.
            const size_t oneBufferSize,

            // The number of unit buffers in the array.
            const unsigned bufferCount,

            // The allocated size will be rounded up to the next Windows Large-Page boundary.
            // Large pages are typically 2MB on x64, or 64kB on ARM.
            __out size_t& allocatedSize,

            // Due to round up, it is possible you will recieve more unit buffers than requested.
            __out unsigned& allocatedBufferCount
            );

        void* PBuffer() { return m_pBuffer; }

        void* PData(size_t offset) {
            Audit::Assert( offset <= m_allocatedSize, "Index out of bounds!");
            return offset + (char*)(m_pBuffer);
        }

        uint32_t OffsetOf(void * pInternalItem)
        {
            intptr_t dif = ((intptr_t)pInternalItem) - (intptr_t)m_pBuffer;
            Audit::Assert((0 <= dif) && (dif < (int64_t)m_allocatedSize), "the argument did not point inside the buffer");
            return (uint32_t)dif;
        }

        size_t Capacity() { return m_allocatedSize; }

        // free the contiguous memory obtained via AllocateLargePageBuffer
        ~LargePageBuffer();
    };

    // Placeholder, replace with something stronger from our existing library
    extern uint32_t PsuedoCRC(void* pBuffer, const size_t size);

    inline uint32_t AlignPower2(uint32_t ALIGN, uint32_t value)
    {
        return (value + ALIGN - 1) & (0 - ALIGN);
    }

    inline uint64_t AlignPower2(uint64_t ALIGN, uint64_t value)
    {
        return (value + ALIGN - 1) & (0 - ALIGN);
    }

    // Disposabuffers are allocated to carry network messages,
    // and read data from SSD. Most of the time these data are
    // small and transient. We use these buffer pools to reduce
    // memory churn, only go to heap for big ones.
    //
    // Worth it to implement a buddy system? Either allocation
    // or free would be slow.
    //
    template <uint32_t BUF_SIZE, uint32_t NUM_BUFS>
    class SmallBufferPool
    {
    private:
        static_assert((BUF_SIZE & (BUF_SIZE - 1)) == 0,
            "Buffer size must be power of 2.");

        struct SmallBuffer : public DisposableBuffer
        {
            SmallBufferPool* m_pPool = nullptr;
            uint32_t m_size = 0;
            uint32_t m_offset = 0;

            SmallBuffer& operator=(const SmallBuffer&) = delete;
            SmallBuffer(const SmallBuffer&) = delete;

            SmallBuffer() {}

            void Dispose() override
            {
                m_pPool->Release(this);
            }

            void* PData() const override
            {
                return m_pPool->GetData(this);
            }

            uint32_t Offset() const override { return m_offset; }
            void SetOffset(size_t v) override
            {
                Audit::Assert(v < m_size);
                m_offset = (uint32_t)v;
            }

            uint32_t Size() const override { return m_size; }
        };

        SmallBuffer m_body[NUM_BUFS];
        BoundedBuf<uint32_t> m_free;
        std::unique_ptr<LargePageBuffer> m_pMemory;

        void* GetData(const SmallBuffer* pBuf)
        {
            Audit::Assert(pBuf->m_size != 0, "Access Released Buffer!");
            auto index = pBuf - m_body;
            Audit::Assert(index < m_free.GetCapacity(), "Buffer released to wrong pool!");

            return m_pMemory->PData(index * BUF_SIZE);
        }

        void Release(SmallBuffer* pBuf)
        {
            Audit::Assert(pBuf->m_size != 0, "Double buffer release detected!");
            pBuf->m_size = 0;
            pBuf->m_offset = 0;

            auto index = pBuf - m_body;
            Audit::Assert(&m_body[index] == pBuf, "Internal error in pointer arithmetic.");
            Audit::Assert(index < m_free.GetCapacity(), "Buffer released to wrong pool!");
            m_free.Add((uint32_t)index);
        }

    public:
        SmallBufferPool()
            : m_free(NUM_BUFS)
        {
            size_t allocatedSize;
            uint32_t actualCount;
            m_pMemory = std::make_unique<Utilities::LargePageBuffer>
                (BUF_SIZE, NUM_BUFS, allocatedSize, actualCount);
            Audit::Assert(actualCount == NUM_BUFS,
                "Ill formed large page size for buffer pool");

            for (uint32_t i = 0; i < NUM_BUFS; i++) {
                m_body[i].m_pPool = this;
                m_free.Add(i);
            }
        }

        uint32_t BufSizeLimit()
        {
            return BUF_SIZE;
        }

        DisposableBuffer* Allocate(uint32_t dataBlobSize, uint32_t alignment = 8, uint32_t offset = 0)
        {
            Audit::Assert(dataBlobSize > 0, "Can not allocate empty buffer.");
            Audit::Assert(dataBlobSize <= BUF_SIZE, "Allocation buffer size too big.");
            Audit::Assert(alignment <= BUF_SIZE && ((alignment & (alignment - 1)) == 0),
                "Buffer aligenment requirement can not be met!");
            auto index = m_free.Get();
            m_body[index].m_offset = offset;
            m_body[index].m_size = dataBlobSize;
            return &m_body[index];
        }
    };

    template <uint32_t NUM_BUFS>
    class TinyBufferPool
    {
    private:
        static const uint32_t TINY_SIZE = 13;
        struct TinyBuffer : public DisposableBuffer
        {
            uint64_t m_data[TINY_SIZE];
            TinyBufferPool* m_pPool = nullptr;
            uint32_t m_size = 0;
            uint32_t m_offset = 0;

            TinyBuffer& operator=(const TinyBuffer&) = delete;
            TinyBuffer(const TinyBuffer&) = delete;

            TinyBuffer() {};

            void Dispose() override
            {
                m_pPool->Release(this);
            }

            void* PData() const override
            {
                return (void*)&(m_data[0]);
            }

            uint32_t Offset() const override { return m_offset; }
            void SetOffset(size_t v) override
            {
                Audit::Assert(v < m_size);
                m_offset = (uint32_t)v;
            }

            uint32_t Size() const override { return m_size; }

        };
        static_assert(sizeof(TinyBuffer) == 128, "Object size should be power of 2");
        TinyBuffer m_body[NUM_BUFS];
        BoundedBuf<uint32_t> m_free;

        void Release(TinyBuffer* pBuf)
        {
            Audit::Assert(pBuf->m_size != 0, "Double buffer release detected!");
            pBuf->m_size = 0;
            pBuf->m_offset = 0;

            auto index = pBuf - m_body;
            Audit::Assert(index < m_free.GetCapacity(), "Buffer released to wrong pool!");
            Audit::Assert(&m_body[index] == pBuf, "Internal error in pointer arithmetic.");
            m_free.Add((uint32_t)index);
        }

    public:
        TinyBufferPool()
            : m_free(NUM_BUFS)
        {
            for (uint32_t i = 0; i < NUM_BUFS; i++) {
                m_body[i].m_pPool = this;
                m_free.Add(i);
            }
        }

        uint32_t BufSizeLimit()
        {
            return uint32_t(TINY_SIZE*sizeof(uint64_t));
        }

        DisposableBuffer* Allocate(uint32_t dataBlobSize, uint32_t alignment = 8, uint32_t offset = 0)
        {
            Audit::Assert(dataBlobSize > 0, "Can not allocate empty buffer.");
            Audit::Assert(dataBlobSize <= BufSizeLimit(), "Allocation buffer size too big.");
            Audit::Assert(alignment <= 8 && ((alignment & (alignment - 1)) == 0),
                "Buffer aligenment requirement can not be met!");
            auto index = m_free.Get();
            m_body[index].m_offset = offset;
            m_body[index].m_size = dataBlobSize;
            return &m_body[index];
        }

    };

}
