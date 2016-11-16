
#pragma once

#include "stdafx.h"

#include "Datagram.hpp"
#include "Scheduler.hpp"

namespace ExaBroker
{
    using namespace Utilities;

    // Async completion for collecting result of a remote write
    //
    class WriteCompletion : public Schedulers::ContinuationBase
    {
    public:
        WriteCompletion(Schedulers::Activity& activity)
            : ContinuationBase(activity)
        {}

        // this function is called when a remote write is finished
        // and the result is given 
        // 
        virtual void ProcessWriteResult(StatusCode status) = 0;

        // provided by the client broker, DO NOT OVERRIDE
        //
        void OnReady(
            _In_ Schedulers::WorkerThread&,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t
            ) override
        {
            ProcessWriteResult((StatusCode)continuationHandle);
        }

    };

    // Async completion for collecting result of a remote read
    //
    class ReadCompletion : public Schedulers::Continuation<Utilities::BufferEnvelope>
    {
    public:
        ReadCompletion(Schedulers::Activity& activity)
            : Continuation(activity)
        {}

        // this function is called when a remote read is finished with
        // results passed to it 
        // 
        virtual void ProcessReadResult(StatusCode status, Utilities::DisposableBuffer* buffer) = 0;

        // provided by the client broker, DO NOT OVERRIDE
        //
        void OnReady(
            _In_ Schedulers::WorkerThread&,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            ) override
        {
            if ((void*)continuationHandle == (void*)&m_postable)
            {
                // we have data
                Audit::Assert(length == m_postable.Contents()->Size() - m_postable.Contents()->Offset(),
                    "data buffer size mismatch");
                ProcessReadResult(StatusCode::OK, m_postable.ReleaseContents());
            }
            else
            {
                auto status = (StatusCode)continuationHandle;

                Audit::Assert(length == 0, "no data expected");
                Audit::Assert(status != StatusCode::OK,
                    "expect error code when there is no data");
                ProcessReadResult(status, nullptr);
            }
        }
    };

    class DeleteCompletion : public Schedulers::ContinuationBase
    {
    public:
        DeleteCompletion(Schedulers::Activity& activity)
            : ContinuationBase(activity)
        {}

        // this function is called when a remote write is finished
        // and the result is given 
        // 
        virtual void ProcessDeleteResult(StatusCode status) = 0;

        // provided by the client broker, DO NOT OVERRIDE
        //
        void OnReady(
            _In_ Schedulers::WorkerThread&,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t
            ) override
        {
            ProcessDeleteResult((StatusCode)continuationHandle);
        }
    };

    class ClientBroker
    {
    public:

        // Sync interface for writing
        //
        virtual StatusCode Write(
            // header of the value item, including key and owner info
            //
            Datagram::Description& header,
            
            // value blob
            //
            _In_ void* blob, 

            // size of the blob, must be same with the length
            // in the header
            //
            size_t blobSize
            ) = 0;

        // Async interface for writing
        //
        virtual StatusCode Write(
            // header of the value item, including key and owner info
            //
            Datagram::Description& header,

            // value blob
            //
            _In_ void* blob,

            // size of the blob, must be same with the length
            // in the header
            //
            size_t blobSize,

            // Completion of write, override ProcessWriteResult
            // to get the result code
            //
            WriteCompletion* pCompletion
            ) = 0;
            
        // Sync interface for reading
        //
        virtual StatusCode Read(
            Datagram::PartialDescription keyAndCaller,

            // buffer containing the value. caller should call
            // (*buffer)->Dispose() when done
            //
            _Out_ Utilities::DisposableBuffer** buffer
            ) = 0;

        // Sync interface for reading, for better compatibility with
        // DBBench
        //
        virtual StatusCode Read(
            Datagram::PartialDescription keyAndCaller,

            _Out_ std::string& buffer
            ) = 0;

        // Async interface for reading
        virtual StatusCode Read(
            Datagram::PartialDescription keyAndCaller,

            // Completion of read, override ProcessReadResult
            // to get result
            //
            ReadCompletion* pCompletion
            ) = 0;

        // Sync interface for deleting
        //
        virtual StatusCode Delete(
            Datagram::PartialDescription keyAndCaller
            ) = 0;

        // Async interface for reading
        virtual StatusCode Delete(
            Datagram::PartialDescription keyAndCaller,

            // Completion of read, override ProcessReadResult
            // to get result
            //
            DeleteCompletion* pCompletion
            ) = 0;
    };
}
