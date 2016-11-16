#pragma once

#include "stdafx.h"
#include "Scheduler.hpp"

namespace AsyncFileIo
{
    // Interface for a file I/O queue, which sequentialize all 
    // operations to a file.
    //
    class FileManager : public Schedulers::Work
    {
        FileManager& operator=(const FileManager&) = delete;
        FileManager(const FileManager&) = delete;

    protected:
        const size_t m_size;
        const std::wstring m_path;
        FileManager(const wchar_t* path, size_t size)
            : m_size(size)
            , m_path(path)
        {}

    public:
        size_t GetFileSize() const { return m_size; }
        const std::wstring& GetFilePath() const { return m_path; }
        virtual intptr_t GetFileHandle() const = 0;

        // Enqueue a read request. When the read operation finished
        // parameter callback will be called with
        //  pCallback->OnReady(thread, statusCode, numBytesRead)
        // Fire and forget when pCallback is set to NULL
        //
        virtual bool Read(
            _In_  size_t   offset,
            _In_  uint32_t numBytesToRead,
            _Out_ void*    pBuffer,
            _In_ Schedulers::ContinuationBase* pCallback
            ) = 0;

        // Enqueue a write request. When the write operation finished
        // parameter callback will be called with
        //  callback->OnReady(thread, statusCode, numBytesWritten)
        // Fire and forget when pCallback is set to NULL
        //
        virtual bool Write(
            _In_  size_t   offset,
            _In_  uint32_t numBytesToWrite,
            _In_  void*    pBuffer,
            _In_ Schedulers::ContinuationBase* pCallback
            ) = 0;

        // Close the file and release all resources
        // The file is deleted when toDelete is true
        //
        virtual void Close(bool toDelete = false) = 0;

        virtual ~FileManager() = default;
    };

    // Create a file and associate the handle with IOCP
    std::unique_ptr<FileManager> FileQueueFactory(const wchar_t* path, Schedulers::Scheduler& scheduler, size_t initialSize = 0);

    // Associate an existing handle with IOCP, the handle have to be created with exclusive read/write/delete access
    // with no buffer and overlapped flag
    //
    std::unique_ptr<FileManager> FileQueueFactory(intptr_t fileHandle, Schedulers::Scheduler& scheduler);
}