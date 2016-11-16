
#pragma once

#include "stdafx.h"

#include "Exabytes.hpp"

namespace Exabytes
{
    // Create a file store by creating a file with designated name and size
    //
    extern std::unique_ptr<KeyValueOnFile> ChainedFileStoreFactory(
        _In_ Schedulers::Scheduler& scheduler,

        // SSD or HDD
        AddressSpace addrSpace,

        // the handle of the file to be used
        intptr_t fileHandle
        );

    // Create a file store with an open file
    //
    extern std::unique_ptr<KeyValueOnFile> ChainedFileStoreFactory(
        _In_ Schedulers::Scheduler& scheduler,

        // SSD or HDD
        AddressSpace addrSpace,

        // the name of the file to be used, the starting portion anyway
        std::unique_ptr<AsyncFileIo::FileManager>& fileQue,

        const void* pChkPnt = nullptr,
        size_t chkPntSize = 0
        );

	extern GcActor* FileStoreGcFactory(
        ExabytesServer& server,
        Schedulers::Activity& gcDriver,
        PartitionRotor& stores
        );

    extern std::unique_ptr<CoalescingBuffer> MakeFileCoalescingBuffer(
        size_t bufferSize,
        CoalescingBufferPool& pool
        );

    extern std::unique_ptr<FileRecover> CircularLogRecoverFactory();

}
