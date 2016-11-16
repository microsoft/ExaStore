#include "stdafx.h"

//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string.h>
#include <io.h>
#include <sys\stat.h>
#include <time.h>
#include <algorithm>
#include <mutex>

#include "env.h"
#include "slice.h"
#include "random.h"
#include <signal.h>
#include <thread>

#include "UtilitiesWin.hpp"

namespace db_bench
{
    namespace
    {

        std::string GetWindowsErrSz(DWORD err)
        {
            LPSTR lpMsgBuf;
            FormatMessageA(
                FORMAT_MESSAGE_ALLOCATE_BUFFER |
                FORMAT_MESSAGE_FROM_SYSTEM |
                FORMAT_MESSAGE_IGNORE_INSERTS,
                NULL,
                err,
                0, // Default language
                reinterpret_cast<LPSTR>(&lpMsgBuf),
                0,
                NULL
                );
            std::string Err = lpMsgBuf;
            LocalFree(lpMsgBuf);
            return Err;
        }

        ////////////////////////////////////////////////////////////////////////////////////

        class WinEnv : public Env
        {
        public:
            WinEnv() {};

            virtual ~WinEnv()
            {
                for (int i = 0; i<threads_to_join_.size(); ++i)
                {
                    threads_to_join_[i]->join();
                }
            }

            virtual void StartThread(void(*function)(void* arg), void* arg);

            virtual void WaitForJoin();

            /*!
            *  Given the name or type of a particular database, this function
            *  constructs a path/directory name. It does not create this directory.
            *
            *  \param  path_base       The base path.
            *  \param  database_name   Name of the database for which we need to
            *                          create this directory.
            *  \param  path            The return path.
            */

            static uint64_t gettid()
            {
                std::thread::id tid = std::this_thread::get_id();
                uint64_t thread_id = 0;
                memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
                return thread_id;
            }


            virtual uint64_t NowMicros() override
            {
                auto dura = std::chrono::system_clock::now().time_since_epoch();
                return std::chrono::duration_cast<std::chrono::microseconds>(dura).count();
            }


        private:
            std::mutex mu_;
            std::vector<std::thread*> threads_to_join_;
        };


        namespace
        {
            struct StartThreadState
            {
                void(*user_function)(void*);
                void* arg;
            };

            void WinthreadCall(const char* label, std::error_code result)
            {
                if (0 != result.value())
                {
                    char buf[512];
                    strerror_s(buf, 512, result.value());
                    fprintf(stderr, "pthread %s: %s\n", label, buf);
                    exit(1);
                }
            }
        }

        static void* StartThreadWrapper(void* arg)
        {
            StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
            state->user_function(state->arg);
            delete state;
            return nullptr;
        }

        void WinEnv::StartThread(void(*function)(void* arg), void* arg)
        {
            StartThreadState* state = new StartThreadState;
            state->user_function = function;
            state->arg = arg;
            try
            {
                std::thread* p_t = new  std::thread(&StartThreadWrapper, state);
                mu_.lock();
                threads_to_join_.push_back(p_t);
                mu_.unlock();
            }
            catch (std::system_error& ex)
            {
                WinthreadCall("start thread", ex.code());
            }
        }

        void WinEnv::WaitForJoin()
        {
            for (int i = 0; i < threads_to_join_.size(); ++i)
            {
                threads_to_join_[i]->join();
            }
            for (int i = 0; i < threads_to_join_.size(); ++i)
            {
                delete threads_to_join_[i];
            }
            threads_to_join_.clear();
        }

    }  // namespace

    Env* Env::Default()
    {
        static WinEnv default_env;
        return &default_env;
    }

}  // namespace rocksdb
