#pragma once
#include "stdafx.h"

#include <thread>
#include <unordered_set>
#include <condition_variable>

#include "UtilitiesWin.hpp"
#include "CppUnitTest.h"
#include "TestUtils.hpp"


using namespace Microsoft::VisualStudio::CppUnitTestFramework;

using namespace Utilities;


namespace
{
    const size_t NUM_THREADS = 8;
    const size_t NUM_ITEMS = 16384;

    BoundedBuf<uint32_t> g_seq(4);
    BoundedBuf<uint64_t> g_que(NUM_THREADS * NUM_ITEMS);
    std::unordered_set<uint64_t> g_result;
    SRWLOCK g_lock = SRWLOCK_INIT;

    std::mutex g_bar;
	size_t g_barCount = 0;
    std::condition_variable g_cv;

    // Producer thread
    void ProducerProc(size_t thread_id)
    {
        size_t base = thread_id * NUM_ITEMS;
        for (size_t i = 0; i < NUM_ITEMS; i++)
        {
            g_que.Add(i + base);
            if ((i & 31) == 0)
            {
                EBTest::Trace("Adding %d number, value: %d", i,  i + base);
            }

            if (i == (NUM_ITEMS - (NUM_ITEMS / 2)))
            {
                std::unique_lock<std::mutex> l(g_bar);
				g_barCount++;
                g_cv.notify_all();
            }
        }
    }

    // consumer thread.
    // need to run twice to deplete all in bounded buffer
    void ConsumerProc()
    {
        for (size_t i = 0; i < (NUM_ITEMS/2); i++)
        {
            auto res = g_que.Get();

            {
                Exclude<SRWLOCK> guard{ g_lock };
                auto inserted = g_result.insert(res);
                if (!inserted.second){
                    Audit::OutOfLine::Fail("Same item get twice from bounded buffer!");
                }
            }

        }
    }

    
}

namespace EBTest
{
    TEST_CLASS(BoundedBufTest)
    {
    public:

        TEST_METHOD(BoundedBufSeqSimple)
        {
            g_seq.Add(4);
            g_seq.Add(3);

            Audit::Assert(g_seq.Get() == 4);
            Audit::Assert(g_seq.Get() == 3);

            g_seq.Add(4);
            g_seq.Add(3);
            g_seq.Add(2);
            g_seq.Add(1);
            Audit::Assert(g_seq.Get() == 4);
            Audit::Assert(g_seq.Get() == 3);
            Audit::Assert(g_seq.Get() == 2);
            Audit::Assert(g_seq.Get() == 1);

            g_seq.Add(4);
            g_seq.Add(3);
            g_seq.Add(2);
            g_seq.Add(1);
            Audit::Assert(g_seq.Get() == 4);
            Audit::Assert(g_seq.Get() == 3);
            Audit::Assert(g_seq.Get() == 2);
            Audit::Assert(g_seq.Get() == 1);
        }

        TEST_METHOD(BoundedBufReadAfterWrite)
        {
            // spin up 32 threads, each write 1024 items
            // then another 32 thread read, each read 1024 items.

            std::thread producers[NUM_THREADS];
            std::thread consumers[NUM_THREADS];

            for (int i = 0; i < NUM_THREADS; i++){
                producers[i] = std::thread(ProducerProc, i);
            }

			while (g_barCount < NUM_THREADS - 2)
            {
                std::unique_lock<std::mutex> l(g_bar);
                g_cv.wait(l);
            }

            // consumers, overlapping with producers.
            for (int i = 0; i < NUM_THREADS; i++){
                consumers[i] = std::thread(ConsumerProc);
            }

            for (int i = 0; i < NUM_THREADS; i++){
                producers[i].join();
                consumers[i].join();
            }

            // Second wave of consumers
            for (int i = 0; i < NUM_THREADS; i++){
                consumers[i] = std::thread(ConsumerProc);
            }
            for (int i = 0; i < NUM_THREADS; i++){
                consumers[i].join();
            }
        }

    };
}