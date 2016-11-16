/***************************
 * db_bench.cpp
 * port from rocksdb and leveldb benchmark
 */

#include "stdafx.h"

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <mutex>
#include <condition_variable>

#include "env.h"
#include "slice.h"
#include "histogram.h"
#include "random.h"
#include "testutil.h"

#include "UtilitiesWin.hpp"
#include "Datagram.hpp"
#include "FixedServerBroker.hpp"

#include <winsock2.h>
#include <Ws2tcpip.h>

// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//      fillseq       -- write N values in sequential key order in async mode
//      fillrandom    -- write N values in random key order in async mode
//      overwrite     -- overwrite N values in random key order in async mode
//      fillsync      -- write N/100 values in random key order in sync mode
//      fill100K      -- write N/1000 100K values in random order in async mode
//      readseq       -- read N times sequentially
//      readreverse   -- read N times in reverse order
//      readrandom    -- read N times in random order
//      readhot       -- read N times in random order from 1% section of DB
//      crc32c        -- repeated crc32c of 4K of data
//      acquireload   -- load N*1000 times
//   Meta operations:
//      compact     -- Compact the entire DB
//      stats       -- Print DB stats
//      heapprofile -- Dump a heap profile (if supported by this port)
static const char* FLAGS_benchmarks =
"fillrandom,"
"overwrite,"
"readrandom,"
"readrandom,"  // Extra run to allow previous compactions to quiesce
;

// Ip and Port of the Exabyte Server
static uint32_t FLAGS_server_ip = 0;

static uint16_t FLAGS_server_port = 0;

// base of the exabytes key used by the tests
static uint64_t FLAGS_key_high = 0xc502899af9710b65;

static uint64_t FLAGS_key_low = 0xd91c836500000000;

static uint64_t FLAGS_owner_key = 0xCafeF00d0000ULL;


// Number of key/values to place in database
static int FLAGS_num = 1000000;

// Number of read operations to do.  If negative, do FLAGS_num reads.
static int FLAGS_reads = -1;

// Number of concurrent threads to run.
static int FLAGS_threads = 1;

// Size of each value
static int FLAGS_value_size = 30000;

// Print histogram of operation timings
static bool FLAGS_histogram = false;

namespace db_bench {
    namespace {
        using namespace Utilities;

        // Helper for quickly generating random data.
        class RandomGenerator {
        private:
            std::string data_;
            int pos_;

        public:
            RandomGenerator() {
                // We use a limited amount of data over and over again and ensure
                // that it is larger than the compression window (32KB), and also
                // large enough to serve all typical value sizes we want to write.
                Random rnd(301);
                std::string piece;
                while (data_.size() < 134217728) {
                    // Add a short fragment that is as compressible as specified
                    // by FLAGS_compression_ratio.
                    test::RandomString(&rnd, 100, &piece);
                    data_.append(piece);
                }
                pos_ = 0;
            }

            Slice Generate(int len) {
                if (pos_ + len > data_.size()) {
                    pos_ = 0;
                    assert(len < data_.size());
                }
                pos_ += len;
                return Slice(data_.data() + pos_ - len, len);
            }
        };

        static void AppendWithSpace(std::string* str, Slice msg) {
            if (msg.empty()) return;
            if (!str->empty()) {
                str->push_back(' ');
            }
            str->append(msg.data(), msg.size());
        }

        class Stats {
        private:
            double start_;
            double finish_;
            double seconds_;
            int done_;
            int next_report_;
            int64_t bytes_;
            HistogramImpl hist_;
            std::string message_;

        public:
            Stats() { Start(); }

            void Start() {
                next_report_ = 100;
                hist_.Clear();
                done_ = 0;
                bytes_ = 0;
                seconds_ = 0;
                start_ = (double) Env::Default()->NowMicros();
                finish_ = start_;
                message_.clear();
            }

            void Merge(const Stats& other) {
                hist_.Merge(other.hist_);
                done_ += other.done_;
                bytes_ += other.bytes_;
                seconds_ += other.seconds_;
                if (other.start_ < start_) start_ = other.start_;
                if (other.finish_ > finish_) finish_ = other.finish_;

                // Just keep the messages from one thread
                if (message_.empty()) message_ = other.message_;
            }

            void Stop() {
                finish_ = (double) Env::Default()->NowMicros();
                seconds_ = (finish_ - start_) * 1e-6;
            }

            void AddMessage(Slice msg) {
                AppendWithSpace(&message_, msg);
            }

            void FinishedSingleOp(uint64_t micros) {
                if (FLAGS_histogram) {
                    hist_.Add(micros);
//                    if (micros > 20000) {
//                        fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
//                        fflush(stderr);
//                    }
                }

                done_++;
                if (done_ >= next_report_) {
                    if (next_report_ < 1000)   next_report_ += 100;
                    else if (next_report_ < 5000)   next_report_ += 500;
                    else if (next_report_ < 10000)  next_report_ += 1000;
                    else if (next_report_ < 50000)  next_report_ += 5000;
                    else if (next_report_ < 100000) next_report_ += 10000;
                    else if (next_report_ < 500000) next_report_ += 50000;
                    else                            next_report_ += 100000;
                    fprintf(stderr, "... finished %d ops%30s\r", done_, "");
                    fflush(stderr);
                }
            }

            void AddBytes(int64_t n) {
                bytes_ += n;
            }

            void Report(const Slice& name) {
                // Pretend at least one op was done in case we are running a benchmark
                // that does not call FinishedSingleOp().
                if (done_ < 1) done_ = 1;
                double elapsed = (finish_ - start_) * 1e-6;

                std::string extra;
                if (bytes_ > 0) {
                    // Rate is computed on actual elapsed time, not the sum of per-thread
                    // elapsed times.
                    char rate[256];
                    _snprintf_s(rate, sizeof(rate), 255, "%6.1f MB/s",
                        (bytes_ / 1048576.0) / elapsed);
                    extra = rate;
                }
                AppendWithSpace(&extra, message_);

                fprintf(stdout, "%-12s : %11.3f micros/op; elapsed %f seconds;%s%s\n",
                    name.ToString().c_str(),
                    seconds_ * 1e6 / done_,
                    elapsed,
                    (extra.empty() ? "" : " "),
                    extra.c_str());
                if (FLAGS_histogram) {
                    fprintf(stdout, "Microseconds per op:\n%s\n", hist_.ToString().c_str());
                }
                fflush(stdout);
            }
        };

        // State shared by all concurrent executions of the same benchmark.
        struct SharedState {
            std::mutex mu;
            std::condition_variable cv;
            int total;

            // Each thread goes through the following states:
            //    (1) initializing
            //    (2) waiting for others to be initialized
            //    (3) running
            //    (4) done

            int num_initialized;
            int num_done;
            bool start;

            SharedState() { }
        };

        // Per-thread state for concurrent executions of the same benchmark.
        struct ThreadState {
            int tid;             // 0..n-1 when running in n threads
            Random rand;         // Has different seeds for different threads
            Stats stats;
            SharedState* shared;

            ThreadState(int index)
                : tid(index),
                rand(1000 + index) {
            }
        };

    }  // namespace

    class Benchmark {
    private:
        uint32_t server_ip_;
        uint16_t server_port_;
        std::unique_ptr<ExaBroker::ClientBroker> exaBroker_;

        int num_;
        int value_size_;
        int reads_;
        int heap_counter_;

        void PrintHeader() {
            const int kKeySize = 16;
            PrintEnvironment();
            fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
            fprintf(stdout, "Values:     %d bytes each\n", FLAGS_value_size);
            fprintf(stdout, "Entries:    %d\n", num_);
            fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
                ((static_cast<int64_t>(kKeySize + FLAGS_value_size) * num_)
                / 1048576.0));
            PrintWarnings();
            fprintf(stdout, "------------------------------------------------\n");
        }

        void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
            fprintf(stdout,
                "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n"
                );
#endif
#ifndef NDEBUG
            fprintf(stdout,
                "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif

        }

        void PrintEnvironment() {
            fprintf(stderr, "Exabytes:    Single Server Test\n");

#if defined(__linux)
            time_t now = time(NULL);
            fprintf(stderr, "Date:       %s", ctime(&now));  // ctime() adds newline

            FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
            if (cpuinfo != NULL) {
                char line[1000];
                int num_cpus = 0;
                std::string cpu_type;
                std::string cache_size;
                while (fgets(line, sizeof(line), cpuinfo) != NULL) {
                    const char* sep = strchr(line, ':');
                    if (sep == NULL) {
                        continue;
                    }
                    Slice key = TrimSpace(Slice(line, sep - 1 - line));
                    Slice val = TrimSpace(Slice(sep + 1));
                    if (key == "model name") {
                        ++num_cpus;
                        cpu_type = val.ToString();
                    }
                    else if (key == "cache size") {
                        cache_size = val.ToString();
                    }
                }
                fclose(cpuinfo);
                fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
                fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
            }
#endif
        }

    public:
        Benchmark()
            : 
            server_ip_(FLAGS_server_ip),
            server_port_(FLAGS_server_port),
            num_(FLAGS_num),
            value_size_(FLAGS_value_size),
            reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
            heap_counter_(0) 
        {
            exaBroker_ = ExaBroker::TestBrokerFactory(server_ip_, server_port_);
        }

        ~Benchmark() {
        }

        void Run() {
            PrintHeader();

            const char* benchmarks = FLAGS_benchmarks;
            while (benchmarks != NULL) {
                const char* sep = strchr(benchmarks, ',');
                Slice name;
                if (sep == NULL) {
                    name = benchmarks;
                    benchmarks = NULL;
                }
                else {
                    name = Slice(benchmarks, sep - benchmarks);
                    benchmarks = sep + 1;
                }

                // Reset parameters that may be overriddden bwlow
                num_ = FLAGS_num;
                reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
                value_size_ = FLAGS_value_size;

                void (Benchmark::*method)(ThreadState*) = NULL;
                bool fresh_db = false;
                int num_threads = FLAGS_threads;

                if (name == Slice("fillseq")) {
                    fresh_db = true;
                    method = &Benchmark::WriteSeq;
                }
                else if (name == Slice("fillrandom")) {
                    fresh_db = true;
                    method = &Benchmark::WriteRandom;
                }
                else if (name == Slice("overwrite")) {
                    fresh_db = false;
                    method = &Benchmark::WriteRandom;
                }
                else if (name == Slice("fillsync")) {
                    fresh_db = true;
                    num_ /= 1000;
                    method = &Benchmark::WriteRandom;
                }
                else if (name == Slice("fill100K")) {
                    fresh_db = true;
                    num_ /= 1000;
                    value_size_ = 100 * 1000;
                    method = &Benchmark::WriteRandom;
                }
                else if (name == Slice("readrandom")) {
                    method = &Benchmark::ReadRandom;
                }
                else if (name == Slice("readmissing")) {
                    method = &Benchmark::ReadMissing;
                }
                else if (name == Slice("readhot")) {
                    method = &Benchmark::ReadHot;
                }
                else if (name == Slice("readrandomsmall")) {
                    reads_ /= 1000;
                    method = &Benchmark::ReadRandom;
                }
                else if (name == Slice("deleteseq")) {
                    method = &Benchmark::DeleteSeq;
                }
                else if (name == Slice("deleterandom")) {
                    method = &Benchmark::DeleteRandom;
                }
                else if (name == Slice("readwhilewriting")) {
                    num_threads++;  // Add extra thread for writing
                    method = &Benchmark::ReadWhileWriting;
                }
                else {
                    if (name != Slice()) {  // No error message for empty name
                        fprintf(stderr, "unknown benchmark '%s'\n", name.ToString().c_str());
                    }
                }

                if (method != NULL) {
                    RunBenchmark(num_threads, name, method);
                }
            }
        }

    private:
        struct ThreadArg {
            Benchmark* bm;
            SharedState* shared;
            ThreadState* thread;
            void (Benchmark::*method)(ThreadState*);
        };

        static void ThreadBody(void* v) {
            ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
            SharedState* shared = arg->shared;
            ThreadState* thread = arg->thread;
            {
                std::unique_lock<std::mutex> l(shared->mu);
                shared->num_initialized++;
                if (shared->num_initialized >= shared->total) {
                    shared->cv.notify_all();
                }
                while (!shared->start) {
                    shared->cv.wait(l);
                }
            }

            thread->stats.Start();
            (arg->bm->*(arg->method))(thread);
            thread->stats.Stop();

            {
                std::unique_lock<std::mutex> l(shared->mu);
                shared->num_done++;
                if (shared->num_done >= shared->total) {
                    shared->cv.notify_all();
                }
            }
        }

        void RunBenchmark(int n, Slice name,
            void (Benchmark::*method)(ThreadState*)) {
            SharedState shared;
            shared.total = n;
            shared.num_initialized = 0;
            shared.num_done = 0;
            shared.start = false;

            ThreadArg* arg = new ThreadArg[n];
            for (int i = 0; i < n; i++) {
                arg[i].bm = this;
                arg[i].method = method;
                arg[i].shared = &shared;
                arg[i].thread = new ThreadState(i);
                arg[i].thread->shared = &shared;
                Env::Default()->StartThread(ThreadBody, &arg[i]);
            }

            {
                std::unique_lock<std::mutex> lock(shared.mu);
                while (shared.num_initialized < n) {
                    shared.cv.wait(lock);
                }

                shared.start = true;
                shared.cv.notify_all();
                while (shared.num_done < n) {
                    shared.cv.wait(lock);
                }
            }

            for (int i = 1; i < n; i++) {
                arg[0].thread->stats.Merge(arg[i].thread->stats);
            }
            arg[0].thread->stats.Report(name);

            for (int i = 0; i < n; i++) {
                delete arg[i].thread;
            }
            delete[] arg;
        }


        void WriteSeq(ThreadState* thread) {
            DoWrite(thread, true);
        }

        void WriteRandom(ThreadState* thread) {
            DoWrite(thread, false);
        }

        void DoWrite(ThreadState* thread, bool seq) {
            RandomGenerator gen;
            thread->stats.Start();
            if (num_ != FLAGS_num) {
                char msg[100];
                _snprintf_s(msg, sizeof(msg), 100, "(%d ops)", num_);
                thread->stats.AddMessage(msg);
            }

            int64_t bytes = 0;
            for (int i = 0; i < num_; i++) {
                int64_t key_rand = seq ? i : (thread->rand.Next() % FLAGS_num);
                Datagram::Description header;
                header.KeyHash = Datagram::Key128(FLAGS_key_high, FLAGS_key_low | key_rand);
                header.Owner = FLAGS_owner_key;
                header.Part = 0;
                header.Reader = 0;
                header.ValueLength = value_size_;

                Slice holder = gen.Generate(value_size_);
                bytes += value_size_;

                auto start = Env::Default()->NowMicros();
                auto status = exaBroker_->Write(header, (void*)holder.data_, value_size_);
                auto end = Env::Default()->NowMicros();
                thread->stats.FinishedSingleOp(end - start);

                if (status != StatusCode::OK) {
                    fprintf(stderr, "put error: %d\n", status);
                    fprintf(stderr, "latency: %lld microseconds\n", end - start);
                    exit(1);
                }
            }
            thread->stats.AddBytes(bytes);
        }


        void ReadRandom(ThreadState* thread) {
            int found = 0;
            for (int i = 0; i < reads_; i++) {
                Datagram::PartialDescription header;
                const int key_rand = thread->rand.Next() % FLAGS_num;
                header.Caller = 0;
                header.KeyHash = Datagram::Key128(FLAGS_key_high, FLAGS_key_low | key_rand);
                std::string value;

                auto start = Env::Default()->NowMicros();
                auto s = exaBroker_->Read(header, value);
                auto end = Env::Default()->NowMicros();
                if (s == StatusCode::OK)
                {
                    found++;
                }
                else if (s != StatusCode::NotFound)
                {
                    fprintf(stderr, "read error: %d\n", s);
                    exit(1);
                }
                thread->stats.FinishedSingleOp(end - start);
            }
            char msg[100];
            _snprintf_s(msg, sizeof(msg), 100, "(%d of %d found)", found, num_);
            thread->stats.AddMessage(msg);
        }

        void ReadMissing(ThreadState* thread) {
            for (int i = 0; i < reads_; i++) {
                Datagram::PartialDescription header;
                const int key_rand = thread->rand.Next() % FLAGS_num;
                header.Caller = 0;
                // make sure we this key is never used by write
                header.KeyHash = Datagram::Key128(FLAGS_key_high, (FLAGS_key_low ^ (1ull << 62)) | key_rand);
                std::string value;

                auto start = Env::Default()->NowMicros();
                auto s = exaBroker_->Read(header, value);
                auto end = Env::Default()->NowMicros();
                if (s != StatusCode::NotFound)
                {
                    fprintf(stderr, "read error: %d\n", s);
                    exit(1);
                }
                thread->stats.FinishedSingleOp(end - start);
            }
        }

        void ReadHot(ThreadState* thread) {
            const int range = (FLAGS_num + 99) / 100;
            for (int i = 0; i < reads_; i++) {
                std::string value;
                Datagram::PartialDescription header;
                const int key_rand = thread->rand.Next() % range;
                header.Caller = 0;
                header.KeyHash = Datagram::Key128(FLAGS_key_high, FLAGS_key_low | key_rand);

                auto start = Env::Default()->NowMicros();
                exaBroker_->Read(header, value);
                auto end = Env::Default()->NowMicros();
                thread->stats.FinishedSingleOp(end-start);
            }
        }

        void DoDelete(ThreadState* thread, bool seq) {
            RandomGenerator gen;
            for (int i = 0; i < num_; i ++) {
                const int key_rand = seq ? i : (thread->rand.Next() % FLAGS_num);
                Datagram::PartialDescription header;
                header.Caller = FLAGS_owner_key;
                header.KeyHash = Datagram::Key128(FLAGS_key_high, FLAGS_key_low | key_rand);

                auto start = Env::Default()->NowMicros();
                auto s = exaBroker_->Delete(header);
                auto end = Env::Default()->NowMicros();
                if ((s != StatusCode::OK) && (s != StatusCode::NotFound)) {
                    fprintf(stderr, "del error: %d\n", s);
                    exit(1);
                }
                thread->stats.FinishedSingleOp(end - start);
            }
        }

        void DeleteSeq(ThreadState* thread) {
            DoDelete(thread, true);
        }

        void DeleteRandom(ThreadState* thread) {
            DoDelete(thread, false);
        }

        void ReadWhileWriting(ThreadState* thread) {
            if (thread->tid > 0) {
                ReadRandom(thread);
            }
            else {
                // Special thread that keeps writing until other threads are done.
                RandomGenerator gen;
                size_t nreq = 0;
                for (;;nreq++) {
                    {
                        std::unique_lock<std::mutex> l(thread->shared->mu);
                        if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
                            // Other threads have finished
                            break;
                        }
                    }

                    int64_t key_rand = thread->rand.Next() % FLAGS_num;
                    Datagram::Description header;
                    header.KeyHash = Datagram::Key128(FLAGS_key_high, FLAGS_key_low | key_rand);
                    header.Owner = FLAGS_owner_key;
                    header.Part = 0;
                    header.Reader = 0;
                    header.ValueLength = value_size_;

                    Slice holder = gen.Generate(value_size_);

                    auto s = exaBroker_->Write(header, (void*)holder.data_, value_size_);
                    if ((nreq & 0x0F) == 0)
                        Sleep(1);
                    if (s != StatusCode::OK) {
                        fprintf(stderr, "put error: %d\n", s);
                        exit(1);
                    }
                }

                // Do not count any of the preceding work/delay in stats.
                thread->stats.Start();
                printf("Number of writes: %zd\n", nreq);
            }
        }

    };

}  // namespace leveldb

int main(int argc, char** argv) {

    sockaddr_in server;
    server.sin_family = AF_INET;

    for (int i = 1; i < argc; i++) {
        int n;
        char junk;
        if (db_bench::Slice(argv[i]).starts_with("--benchmarks=")) {
            FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
        }
        else if (db_bench::Slice(argv[i]).starts_with("--serverip="))
        {
            const char* ipstr = argv[i] + strlen("--serverip=");
            inet_pton(AF_INET, ipstr, &server.sin_addr);
            FLAGS_server_ip = ntohl(server.sin_addr.S_un.S_addr);
        }
        else if (sscanf_s(argv[i], "--serverport=%d%c", &n, &junk, 1) == 1) {
            FLAGS_server_port = static_cast<uint16_t>(n);
        }
        else if (sscanf_s(argv[i], "--histogram=%d%c", &n, &junk, 1) == 1 &&
            (n == 0 || n == 1)) {
            FLAGS_histogram = n != 0;
        }
        else if (sscanf_s(argv[i], "--num=%d%c", &n, &junk, 1) == 1) {
            FLAGS_num = n;
        }
        else if (sscanf_s(argv[i], "--reads=%d%c", &n, &junk, 1) == 1) {
            FLAGS_reads = n;
        }
        else if (sscanf_s(argv[i], "--threads=%d%c", &n, &junk, 1) == 1) {
            FLAGS_threads = n;
        }
        else if (sscanf_s(argv[i], "--value_size=%d%c", &n, &junk, 1) == 1) {
            FLAGS_value_size = n;
        }
        else {
            fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
            exit(1);
        }
    }

    if (FLAGS_server_ip == 0 || FLAGS_server_port == 0)
    {
        fprintf(stderr, "Server Ip and Port must be provided, for example: \n");
        fprintf(stderr, "--serverip=10.24.75.189 --serverport=54460\n");
        exit(1);
    }

    db_bench::Benchmark benchmark;
    benchmark.Run();
    return 0;
}
