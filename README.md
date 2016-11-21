# Exabyte Store

A Key-Value pair storage engine that is the basic building block of large scale low cost
storage solutions. 

## Why Another Store?

Log Structured Merge tree (LSM) is the dominant choice of data organization in storage engines.
LSMs insist on storing records sequentially by constantly sorting them, racking up high write
amplifications, wasting vast majority of the available I/O bandwidth and hurting SSD life.

Exabyte Store uses an innovative hash based index for high performance and low cost. ExaStore
consumes on average 3 bytes DRAM per key, and achives much lower write amplifications
since we don't have to sort the data all the time.

More detailed discussion can be found in the blogs below.

- [Overview](https://blogs.msdn.microsoft.com/chenfucsperfthoughts/2016/09/12/sorting_cost/)
- [LSM Tree Compaction is Costly](https://blogs.msdn.microsoft.com/chenfucsperfthoughts/2016/09/12/lsm_compaction/)
- [An Low Cost Alternative](https://blogs.msdn.microsoft.com/chenfucsperfthoughts/2016/09/12/why-pay-for-soring-if-you-dont-need-it-3-of-3/)

## Code

Our goal is to build a low cost, distributed and replicated storage system. Currently
only the single node storage engine is finished.

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

## Specification

To ensure the correctness of the network related component, we rely on formal specification
and model checking tool TLA+. There are two specs under directory `spec`, one is an UDP based
network protocol that allows you to transport data larger than a single UDP packet. The second
is the replication protocol.

## Documents

Design documents can be found in `design` directory.  `Store.md` file describe high
level design of the storage server. Documents under `Manager` directory are related
to planed Exabyte Manager component that orchestrate a distributed storage solution.

