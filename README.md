# Exabyte Store

A Key-Value pair storage engine that is the basic building block of large scale low cost
storage solutions. 

## Why Another Store?

Log Structured Merge tree (LSM) is the dominant choice of data organization in storage engines.
LSMs insist on storing records sequentially by constantly sorting them, racking up write
amplifications, reducing available I/O bandwidth and hurting SSD life.

ExaStore uses an innovative hash based index for high performance and low cost. ExaStore
consumes on average 3 bytes DRAM per key, and achives much lower write amplifications
since we don't have to sort the data all the time.

## Code

Unlike RocksDB, ExaStore is not a linkable libary in its current form. It is built as a
stand alone exe accepting read and write request via UDP. 

Currently ExaStore is Windows based, where IOCP provides is pretty nice in keeping CPU
usage down. All the code is under directory `src`. The easiest way to compile the code is
open Exabytes.sln using Visual Studio.

Under `Solution Exabytes`, `ExaServer` project compiles into the stand alone exe which is the
storage engine. `ExaBroker` compiles into a library that knows how to communicate with
the storage engine using UDP protocol. `FixedServerTestClient` is a modified db_bench (derived
from RocksDB) that serves as a performance test engine.  `EBTest` contains unit tests. 
`UdpTestApp` and `UdpTestClient` are test programs for a network transportation protocol
built on top of UDP.

## Documents



