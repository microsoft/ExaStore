// Non-portable utilities.

/*  This should only be #included into a .cpp file so as to avoid making
    other header files non-portable.
    (and because getting windows.h and other headers to play nice is tricky)
*/

#pragma once

#include "Utilities.hpp"
#include "StatusWin.hpp"
#include <vector>

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN 1
#endif

#define NOMINMAX 

#include <windows.h>
#include <WinSock2.h>

#include <string>

namespace Utilities
{

    template<typename Action>
    class HandleAutoCloser
    {
        HANDLE m_handle;
    public:
        HandleAutoCloser(_In_ HANDLE handle, _In_ Action act) {
            m_handle = handle;
            act();
        }
        ~HandleAutoCloser() { CloseHandle(m_handle); };
    };

    template<typename Action>
    HandleAutoCloser<Action> AutoClose(_In_ HANDLE h, _In_ Action act)
    {
        return HandleAutoCloser<Action>(h, act);
    }

    void EnableLargePages();

    template<>
    struct SharedRegion < SRWLOCK >
    {
        static void Share(_In_ SRWLOCK& lock)
        {
            ::AcquireSRWLockShared(&lock);
        }
        static void Release(_In_ SRWLOCK& lock)
        {
            ::ReleaseSRWLockShared(&lock);
        }
    };

    template<>
    struct ExcludedRegion < SRWLOCK >
    {
        static void Exclude(_In_ SRWLOCK& lock)
        {
            ::AcquireSRWLockExclusive(&lock);
        }
        static void Release(_In_ SRWLOCK& lock)
        {
            ::ReleaseSRWLockExclusive(&lock);
        }
    };

    template<typename Atom>
    bool TryCompareExchange(_Inout_ Atom* pTarget, Atom newValue, Atom currentValue)
    {
        return currentValue == InterlockedCompareExchange(pTarget, newValue, currentValue);
    }    

    template <typename T>
    class SimpleAtomicPool
    {
    private:
        std::vector<T> m_freeItems;
        SRWLOCK   m_lock;

    public:
        SimpleAtomicPool()
        {
            InitializeSRWLock(&m_lock);
        }

        T Aquire()
        {
            Utilities::Exclude < SRWLOCK >  guard{ m_lock };
            Audit::Assert(0 < m_freeItems.size(),
                "free pool overflow not yet implemented");
            T freshBlockIndex = m_freeItems.back();
            m_freeItems.pop_back();
            return freshBlockIndex;
        }

        void Release(T item)
        {
            Utilities::Exclude<SRWLOCK> guard{ m_lock };
            m_freeItems.push_back(item);
        }

        size_t GetPoolSize()
        {
            Utilities::Share<SRWLOCK> guard{ m_lock };
            return m_freeItems.size();
        }

    };

    inline errno_t GetFileInfoFromHandle(intptr_t handle, _Out_ std::wstring& path, _Out_ size_t& fileSize)
    {
        wchar_t fileNameBuf[MAX_PATH + 5];
        auto pathSize = GetFinalPathNameByHandleW((HANDLE)handle, fileNameBuf, MAX_PATH + 4, 0);
        Audit::Assert(pathSize < MAX_PATH, "Partition file path too long.");

        auto err = GetLastError();
        if ((StatusCode)err != StatusCode::OK){
            return err;
        }

        path = fileNameBuf;

        LARGE_INTEGER actualSize;
        if (FALSE == GetFileSizeEx((HANDLE)handle, &actualSize)){
            return GetLastError();
        }
        fileSize = actualSize.QuadPart;
        return S_OK;
    }

    // Open a file if it already exists
    // Exclusive read/write/delete access, overlapped and un-buffered IO
    // Returns StatusCode::OK if successful
    //
    inline StatusCode OpenExistingFile(_In_ const wchar_t* filePath, _Out_ intptr_t& fileHandle)
    {
        HANDLE& handle = (HANDLE&)fileHandle;
        handle = ::CreateFile(filePath,
            GENERIC_READ | GENERIC_WRITE | DELETE,
            0,								// exclusive, not shared
            nullptr,						// not passed to any child process
            OPEN_EXISTING,					// create if not yet existing
            FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED,			// do not write-through, we don't want forced metadata updates
            nullptr							// no template
            );
        auto ret = (StatusCode)GetLastError();
        if (ret == StatusCode::OK || ret == StatusCode::FileExists)
        {
            ret = StatusCode::OK;
        }
        else 
        {
            handle = INVALID_HANDLE_VALUE;
        }
        return ret;
    }

    // Open a file if it exists, create a new one if it does not exists or
    // size is wrong.
    // Exclusive read/write/delete access, overlapped and un-buffered IO
    // Returns StatusCode::OK if successful
    // 
    inline StatusCode OpenFile(_In_ const wchar_t* filePath, _In_ size_t size, _Out_ intptr_t& fileHandle)
    {
        HANDLE& handle = (HANDLE&)fileHandle;
        handle = INVALID_HANDLE_VALUE;
        StatusCode ret = StatusCode::OK;
        bool failed = false;

        for (int retry = 4; retry > 0; retry--){
            if (failed){
                if (handle != INVALID_HANDLE_VALUE){
                    ::CloseHandle(handle);
                }
                ::DeleteFile(filePath);
            }

            handle = ::CreateFile(filePath,
                GENERIC_READ | GENERIC_WRITE | DELETE,
                0,								// exclusive, not shared
                nullptr,						// not passed to any child process
                OPEN_ALWAYS,					// create if not yet existing
                FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED,			// do not write-through, we don't want forced metadata updates
                nullptr							// no template
                );
            ret = (StatusCode)GetLastError();
            if (ret == StatusCode::OK || ret == StatusCode::FileExists)
            {
                ret = StatusCode::OK;
                LARGE_INTEGER requestedSize;
                requestedSize.QuadPart = size;
                failed = (FALSE == SetFilePointerEx(handle, requestedSize, &requestedSize, FILE_BEGIN));
                if (failed){
                    ret = (StatusCode)GetLastError();
                    continue;
                }
                failed = (requestedSize.QuadPart != (long long)size);
                if (failed){
                    ret = StatusCode::IncorrectSize;
                    continue;
                }
                failed = (FALSE == SetEndOfFile(handle));
                if (failed){
                    ret = (StatusCode)GetLastError();
                    continue;
                }
                failed = false;
                break;
            }
            else {
                failed = true;
            }
            Audit::Assert(failed, "retry not needed!");
        }

        if (failed){
            if (handle != INVALID_HANDLE_VALUE){
                ::CloseHandle(handle);
            }
            ::DeleteFile(filePath);
            return ret;
        }
        Audit::Assert(handle != INVALID_HANDLE_VALUE);
        LARGE_INTEGER actualSize;
        Audit::Assert(FALSE != GetFileSizeEx(handle, &actualSize), "failed GetFileSize");
        Audit::Assert(actualSize.QuadPart == (long long)size, "actual file size does not match requested size");

        return StatusCode::OK;
    }

    extern bool DiscoverHomeAddress(const SOCKET homeSocket, __out sockaddr_in& homeAddress, uint16_t port=0);

    std::string GetSystemErrorMessage(const StatusCode sysErr);

}
