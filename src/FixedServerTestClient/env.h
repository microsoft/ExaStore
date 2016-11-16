// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An Env is an interface used by the rocksdb implementation to access
// operating system functionality like the filesystem etc.  Callers
// may wish to provide a custom Env object when opening a database to
// get fine gain control; e.g., to rate limit file system operations.
//
// All Env implementations are safe for concurrent access from
// multiple threads without any external synchronization.

#pragma once

#ifndef STORAGE_ROCKSDB_INCLUDE_ENV_H_
#define STORAGE_ROCKSDB_INCLUDE_ENV_H_

#include <cstdarg>
#include <string>
#include <memory>
#include <vector>
#include <stdint.h>

#ifdef GetCurrentTime
#undef GetCurrentTime
#endif

namespace db_bench {

class FileLock;
class Logger;
class RandomAccessFile;
class SequentialFile;
class Slice;
class WritableFile;
class RandomRWFile;
class Directory;
struct DBOptions;
class RateLimiter;

using std::unique_ptr;
using std::shared_ptr;


// Options while opening a file to read/write
struct EnvOptions {

  // construct with default Options
  EnvOptions();

  // construct from Options
  explicit EnvOptions(const DBOptions& options);

  // If true, then allow caching of data in environment buffers
  bool use_os_buffer = true;

   // If true, then use mmap to read data
  bool use_mmap_reads = false;

   // If true, then use mmap to write data
  bool use_mmap_writes = true;

  // If true, set the FD_CLOEXEC on open fd.
  bool set_fd_cloexec = true;

  // Allows OS to incrementally sync files to disk while they are being
  // written, in the background. Issue one request for every bytes_per_sync
  // written. 0 turns it off.
  // Default: 0
  uint64_t bytes_per_sync = 0;

  // If true, we will preallocate the file with FALLOC_FL_KEEP_SIZE flag, which
  // means that file size won't change as part of preallocation.
  // If false, preallocation will also change the file size. This option will
  // improve the performance in workloads where you sync the data on every
  // write. By default, we set it to true for MANIFEST writes and false for
  // WAL writes
  bool fallocate_with_keep_size = true;

  // If not nullptr, write rate limiting is enabled for flush and compaction
  RateLimiter* rate_limiter = nullptr;
};

class Env {
 public:
  Env() { }
  virtual ~Env() {};

  // Return a default environment suitable for the current operating
  // system.  Sophisticated users may wish to provide their own Env
  // implementation instead of relying on this default environment.
  //
  // The result of Default() belongs to rocksdb and must never be deleted.
  static Env* Default();

  // Start a new thread, invoking "function(arg)" within the new thread.
  // When "function(arg)" returns, the thread will be destroyed.
  virtual void StartThread(void (*function)(void* arg), void* arg) = 0;

  // Wait for all threads started by StartThread to terminate.
  virtual void WaitForJoin() {}

  // Returns the number of micro-seconds since some fixed point in time. Only
  // useful for computing deltas of time.
  virtual uint64_t NowMicros() = 0;

private:
  // No copying allowed
  Env(const Env&);
  void operator=(const Env&);
};


}  // namespace rocksdb

#endif  // STORAGE_ROCKSDB_INCLUDE_ENV_H_
