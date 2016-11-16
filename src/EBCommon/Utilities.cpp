// Procrastinate : an initial storage loop
//
#pragma once

#include "stdafx.h"
#include "Scheduler.hpp"
#include "Utilities.hpp"
#include "UtilitiesWin.hpp"

#include <memory>
#include <unordered_map>
#include <utility>
#include <algorithm>

#include <cstdio>

using namespace std;

namespace Audit
{
    using namespace Utilities;

    // write to stderr for experimenting on AP machines.
    // TODO!! to be merged with ETW Tracing, need to figure out
    // how to collect the trace from AP machines though.
    void OutOfLine::Fail(const StatusCode status, const char* const message)
    {
        static const int BufferSize = SI::Ki;

        char buffer[BufferSize];

        ::memset(buffer, 0, sizeof(char)* BufferSize);

        if (message) sprintf_s ( buffer , BufferSize , "%d: %s: %s" , status , GetSystemErrorMessage(status).c_str(), message );
        else         sprintf_s ( buffer , BufferSize , "%d: %s"     , status , GetSystemErrorMessage(status).c_str()          );

        fprintf_s(stderr, buffer);
        //throw std::runtime_error{ buffer };
        char* errPtr = nullptr;
        *errPtr = 'c'; // trigger null pointer exception to generate memory dump on AP.
    }

    void OutOfLine::Fail(const char* utf8message)
    {
        Fail((StatusCode)::GetLastError(), utf8message);
    }

    void OutOfLine::FailArgNull(const char* utf8message)
    {
        Fail((StatusCode)::GetLastError(), utf8message);
    }

    void OutOfLine::FailArgRule(const char* utf8message)
    {
        Fail((StatusCode)::GetLastError(), utf8message);
    }

    // TBD: for the moment, we assert failure rather than logging, to help local debugging
    void OutOfLine::Log(const StatusCode status, const char* utf8message)
    {
        Fail(status, utf8message);
    }

    void OutOfLine::Log(const char* utf8message)
    {
        Log((StatusCode)::GetLastError(), utf8message);
    }

    void OutOfLine::LogArgNull(const char* utf8message)
    {
        Log((StatusCode)::GetLastError(), utf8message);
    }

    void OutOfLine::LogArgRule(const char* utf8message)
    {
        Log((StatusCode)::GetLastError(), utf8message);
    }

    void OutOfLine::Fail(const StatusCode status)
    {
        Fail(status, nullptr);
    }
}

namespace Utilities
{

    /* converst a wide string to Utf8 chars
    */
    int wsToCharBuf(const std::wstring& s, char* buf, int bufLen)
    {
        return WideCharToMultiByte(CP_UTF8, 0, s.c_str(), (int) s.length(), buf, bufLen, NULL, NULL);
    }

    void EnableLargePages()
    {
        HANDLE      hToken;
        TOKEN_PRIVILEGES tp;

        if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY, &hToken))
        {
            Audit::OutOfLine::Fail((StatusCode)::GetLastError());
        }
        // the app needs to run at elevated privilege, with local security policy allowing memory locking.
        auto closer = AutoClose (hToken,
            [&]{
            if (!LookupPrivilegeValue(NULL, TEXT("SeLockMemoryPrivilege"), &tp.Privileges[0].Luid))
            {
                Audit::OutOfLine::Fail((StatusCode)::GetLastError());
            }
            tp.PrivilegeCount = 1;
            tp.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED;
            AdjustTokenPrivileges(hToken, FALSE, &tp, 0, nullptr, 0);
            // AdjustToken silently fails, so we always need to check last status.
            auto status = (StatusCode)::GetLastError();
            if (status != StatusCode::OK)
            {
                if (status == StatusCode::PermissionError)
                    Audit::OutOfLine::Fail("run at elevated privilege, with local security policy allowing memory locking");
                else
                    Audit::OutOfLine::Fail((StatusCode)status);
            }
        });
    }

    /* Allocate multiple units of buffers contiguously in large pages which will stay resident
    */
    LargePageBuffer::LargePageBuffer(
        const size_t oneBufferSize,
        const unsigned bufferCount,
        __out size_t& allocatedSize,
        __out unsigned& allocatedBufferCount
        )
    {
        m_pBuffer = nullptr;
        m_allocatedSize = 0;
        const size_t granularity = ::GetLargePageMinimum();
        const size_t minimumSize = oneBufferSize * bufferCount;
        allocatedSize = (minimumSize + granularity - 1) & (0 - granularity);
        allocatedBufferCount = 0;

        m_pBuffer = ::VirtualAlloc(
            nullptr,
            allocatedSize,
            MEM_COMMIT | MEM_RESERVE | MEM_LARGE_PAGES,
            PAGE_READWRITE
            );

        if (m_pBuffer != nullptr)
        {
            m_allocatedSize = allocatedSize;
            allocatedBufferCount = (int) (allocatedSize / oneBufferSize);
        }
        else
        {
            Audit::OutOfLine::Fail((StatusCode)::GetLastError(), "Cannot allocate large page! ");
        }
    }

    LargePageBuffer::~LargePageBuffer()
    {
        if (m_pBuffer != nullptr)
        {
            ::VirtualFreeEx(GetCurrentProcess(), m_pBuffer, m_allocatedSize, 0);
            m_pBuffer = nullptr;
        }
    }

    // Placeholder, replace with something stronger from our existing library
    uint32_t PsuedoCRC(void* pBuffer, const size_t size)
    {
		uint8_t* pIter = (uint8_t*) pBuffer;
		uint8_t* pLast = size + (uint8_t*) pBuffer;
        uint64_t value = 0;
		while (pIter < pLast)
        {
			value = (value >> 23) ^ (value << 41) ^ (uint64_t) (*pIter++);
        }
        return (uint32_t) ((value >> 32) ^ value);
    }

}
