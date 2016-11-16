#include "stdafx.h"

#include "FileManager.hpp"
#include "SchedulerIOCP.hpp"
#include "UtilitiesWin.hpp"

#include <queue>

namespace  // private to local file
{
    // TODO!!! Duplicated constant, also defined in FlashStore
    // Need to find a good way to centralize all these windows related
    // including FileHandle, POverlapped, etc.
    static const size_t SECTORSIZE = 4096;

    struct FileOperation
    {
        size_t   m_offset;
        uint32_t m_numBytesRequested;
        void*    m_pBuffer;
        Schedulers::ContinuationBase* m_pCallback;

        FileOperation()
        {}

        FileOperation(
            _In_  size_t   offset,
            _In_  uint32_t numBytesToOp,
            _Out_ void*    pBuffer,
            _In_ Schedulers::ContinuationBase* pCallback
            )
            : m_offset(offset)
            , m_numBytesRequested(numBytesToOp)
            , m_pBuffer(pBuffer)
            , m_pCallback(pCallback)
        { }
    };

    struct OpQue
    {
        std::queue<FileOperation> Que;
        // guard when requests are added to or removed from the queue.
        SRWLOCK Lock;

        // one structure is enough as we don't allow concurrent operations 
        FileOperation CurrentOp;

        OVERLAPPED Overlap;
        const Tracer::EBCounter Operation; // either DiskRead or DiskWrite

        // in the function that starting an async IO operation
        bool Starting = false; 

        // in the function that ending an async IO operation
        bool Ending = false;

        OpQue(bool isRead)
            : Operation(isRead? Tracer::EBCounter::DiskRead : Tracer::EBCounter::DiskWrite)
        {
            InitializeSRWLock(&Lock);
            CurrentOp.m_pBuffer = nullptr;
        }

        const Tracer::TraceRec* GetTopTracer()
        {
            return (CurrentOp.m_pCallback == nullptr) ?
                nullptr : CurrentOp.m_pCallback->m_activity.GetTracer();
        }

        OpQue& operator=(const OpQue&) = delete;
        OpQue(const OpQue&) = delete;
    };

}


namespace AsyncFileIo
{
    using namespace Utilities;

    class FileManagerIOCP : public FileManager
    {

    public:
        // Constructor should only be called from the factory method, which ensure the file name/size match.
        //
        FileManagerIOCP(intptr_t fileHandle, Schedulers::Scheduler& scheduler, const wchar_t* path, size_t fileSize)
            : m_handle(fileHandle)
            , m_pScheduler(&scheduler)
            , FileManager(path, fileSize)
        {
            Audit::Assert(m_pScheduler->RegisterFileWorker(m_handle, this) == (intptr_t)this, "Register file worker failed!");
        }

        bool Read(
            _In_  size_t   offset,
            _In_  uint32_t numBytesToRead,
            _Out_ void*    pBuffer,
            _In_ Schedulers::ContinuationBase* pCallback
            ) override
        {
            Audit::Assert(pBuffer != nullptr,
                "Buffer missing when calling disk read.");
            FileOperation op{ offset, numBytesToRead, pBuffer, pCallback };
            EnqueueOp(op, true);
            return true;
        }

        bool Write(
            _In_  size_t   offset,
            _In_  uint32_t numBytesToWrite,
            _In_  void*    pBuffer,
            _In_ Schedulers::ContinuationBase* pCallback
            ) override
        {
            Audit::Assert(0 == (offset % SECTORSIZE), "write offset must be aligned");
            Audit::Assert(0 == (numBytesToWrite % SECTORSIZE), "write offset must be aligned");
            // Experiments show pBuffer does not have to be aligned with SECTORSIZE, can we generalize this??

            Audit::Assert(pBuffer != nullptr,
                "Buffer missing when calling disk write.");

            FileOperation op{ offset, numBytesToWrite, pBuffer, pCallback };
            EnqueueOp(op, false);
            return true;
        }

        void Close(bool toDelete = false) override
        {
            wchar_t msg[MAX_PATH + 12];
            const wchar_t* format = toDelete ? L"Deleting: %s" : L"Closing: %s";
            swprintf_s(msg, format, m_path.c_str());
            Tracer::LogInfo(StatusCode::OK, msg);

            Audit::Assert(m_handle != (intptr_t)INVALID_HANDLE_VALUE, "Double closing!");
            if (!m_readQue.Que.empty() || m_readQue.CurrentOp.m_pBuffer != nullptr
                || !m_writeQue.Que.empty() || m_writeQue.CurrentOp.m_pBuffer != nullptr){
                Tracer::LogError(StatusCode::OK, L"Closing a file with pending operations!");
            }

            if (toDelete)
            {
                FILE_DISPOSITION_INFO fdi;
                fdi.DeleteFileW = true;
                if (!SetFileInformationByHandle((HANDLE)m_handle, FileDispositionInfo, &fdi, sizeof(FILE_DISPOSITION_INFO)))
                {
                    // DELETE file failed, so we could not release disk space for others, but do we blow up?
                    auto status = (StatusCode)::GetLastError();
                    Tracer::LogError(status, L"Failed to delete file!");
                }
            }

            if (!CloseHandle((HANDLE)m_handle))
            {
                Audit::OutOfLine::Fail((StatusCode)::GetLastError(), "Failed to close file handle");
            }
            m_handle = (intptr_t)INVALID_HANDLE_VALUE;
        }

        intptr_t GetFileHandle() const override
        {
            return m_handle;
        }

        // This function is called when a completion is posted to the file
        // Since only one operation is allowed at a time, this method can never be
        // called concurrently.
        //
        void Run(
            _In_ Schedulers::WorkerThread&,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            ) override
        {
            Audit::Assert(continuationHandle == (intptr_t)(&m_readQue.Overlap)
                || continuationHandle == (intptr_t)(&m_writeQue.Overlap),
                "Mismatched overlap data structure!");

            EndAsyncIo((continuationHandle == (intptr_t)(&m_readQue.Overlap))? m_readQue : m_writeQue,
                length);
        }

    private:
        intptr_t m_handle;
        Schedulers::Scheduler* m_pScheduler;
        OpQue m_readQue{ true } ;
        OpQue m_writeQue{ false };

        // This function issue async read or write. completion of these
        // operations will be posted to IOCP, which will trigger Run function
        // of this class
        //
        void TriggerAsyncIo(OpQue& opque)
        {
            Audit::Assert(!opque.Starting,
                "Race condition in starting an disk io operation!");

            opque.Starting = true;
            Utilities::FlipWhenExist(opque.Starting);

            ZeroMemory(&(opque.Overlap), sizeof(OVERLAPPED));
            opque.Overlap.Offset = (int32_t)opque.CurrentOp.m_offset;
            opque.Overlap.OffsetHigh = (int32_t)(opque.CurrentOp.m_offset >> 32);

            Audit::Assert(opque.CurrentOp.m_pBuffer != nullptr,
                "Invalid buffer provided to file IO.");
            BOOL result = 1;
            Tracer::LogActionStart(opque.Operation, m_path.c_str(), opque.GetTopTracer());

            if (opque.Operation == Tracer::EBCounter::DiskRead){
                result = ReadFile((HANDLE)m_handle, opque.CurrentOp.m_pBuffer, opque.CurrentOp.m_numBytesRequested, NULL, &opque.Overlap);
            } 
            else if (opque.Operation == Tracer::EBCounter::DiskWrite){
                result = WriteFile((HANDLE)m_handle, opque.CurrentOp.m_pBuffer, opque.CurrentOp.m_numBytesRequested, NULL, &opque.Overlap);
            }
            else {
                char msg[100];
                _snprintf_s(msg, 99, "Unknown operation in file operation queue: %d", (int)opque.Operation);
                Audit::OutOfLine::Fail(msg);
            }

            Audit::Assert(!result, "File must be opened with async mode!");
            auto error = ::GetLastError();
            if (error != ERROR_IO_PENDING){
                Audit::OutOfLine::Fail((StatusCode)error, "I/O error recovery not implemented!");
            }

        }

        // Queue up an operation, 
        // 
        void EnqueueOp(FileOperation& op, bool isRead)
        {
            OpQue& opque = isRead ? m_readQue : m_writeQue;

            {
                Utilities::Exclude<SRWLOCK> guard{ opque.Lock };
                opque.Que.push(op);

                if (opque.CurrentOp.m_pBuffer != nullptr)
                {
                    // one operation pending
                    return;
                }

                // idle, trigger next operation
                opque.CurrentOp = opque.Que.front();
                opque.Que.pop();
            }

            TriggerAsyncIo(opque);
        }

        // called when completion notice received.
        void EndAsyncIo(OpQue& opque, uint32_t length)
        {
            Tracer::LogActionEnd(opque.Operation, 
                (StatusCode)(opque.Overlap.Internal), opque.GetTopTracer());

            Audit::Assert(!opque.Ending,
                "Race condition detecting in ending file io.");
            opque.Ending = true;
            Audit::Assert(opque.CurrentOp.m_pBuffer != nullptr,
                "Async op corrpution detected!");

            // pass back the status code and number of bytes transfered
            if (opque.CurrentOp.m_pCallback != nullptr)
            {
                opque.CurrentOp.m_pCallback->Post((intptr_t)(opque.Overlap.Internal), length);
            }

            // trigger next operation, if there is any
            {
                Utilities::Exclude<SRWLOCK> guard{ opque.Lock };

                if (opque.Que.empty())
                {
                    // set current status to idle
                    opque.CurrentOp.m_pBuffer = nullptr;
                    opque.Ending = false;
                    return;
                }

                // we have more operation pending, trigger one
                opque.CurrentOp = opque.Que.front();
                opque.Que.pop();
            }
            opque.Ending = false;

            TriggerAsyncIo(opque);

        }

        // Assignment not allowed
        FileManagerIOCP& operator=(const FileManagerIOCP&) = delete;
    };


    std::unique_ptr<FileManager> FileQueueFactory(const wchar_t* path, Schedulers::Scheduler& scheduler, size_t fileSize)
    {
        intptr_t handle;
        StatusCode status = OpenFile(path, fileSize, handle);

        if (status != StatusCode::OK)
        {
            Audit::OutOfLine::Fail(status, "Failed to create a file");
        }
        return std::make_unique<FileManagerIOCP>(handle, scheduler, path, fileSize);
    }

    std::unique_ptr<FileManager> FileQueueFactory(intptr_t fileHandle, Schedulers::Scheduler& scheduler)
    {
        LARGE_INTEGER actualSize;
        Audit::Assert(FALSE != GetFileSizeEx((HANDLE)fileHandle, &actualSize), "failed GetFileSize");
        Audit::Assert(actualSize.QuadPart > 0, "Invalid file size!");

        wchar_t fileNameBuf[MAX_PATH + 5];
        auto pathSize = GetFinalPathNameByHandleW((HANDLE)fileHandle, fileNameBuf, MAX_PATH + 4, 0);
        Audit::Assert(pathSize < MAX_PATH, "Partition file path too long.");

        auto err = GetLastError();
        if ((StatusCode)err != StatusCode::OK){
            Audit::OutOfLine::Fail((StatusCode)err, "Failed to create partition.");
        }

        return std::make_unique<FileManagerIOCP>(fileHandle, scheduler, fileNameBuf, (size_t)actualSize.QuadPart);
    }

}

