// NetBursts.cpp : Defines the entry point for the console application.
//

#pragma once

#include "stdafx.h"

#include "Scheduler.hpp"

namespace Schedulers
{
    /* Avoid any inclusion of Windows headers in our *.hpp
    They cause complexity, difficulties in reconciling versions,
    and are generally archaic in style.  This project is aiming
    for a more modern C++ style and standard data types.

    We therefore wrap some unavoidable Windows types.
    */
    typedef intptr_t IOCPhandle;

    // The worker threads run non-blocking tasks off the IOCP
    //
    extern std::unique_ptr<Scheduler> SchedulerIOCPFactory(
        /* One IO Completion Port will schedule all activity for these threads
        */
        IOCPhandle hIOCP,

        /* These threads will be dedicated to the IO Completions.
        */
        int numThreads,

        /* There are two dedicated completion key values on the IOCP:
        0: shutdown
        1: call the Primary WorkerAction
        Other completion keys may be added by registering WorkerActions.
        */
        _In_ Work* pPrimaryAction
        );

}