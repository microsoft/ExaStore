# ExaStore

A Key-Value pair storage engine that is the basic building block of large scale low cost
storage solutions. 

ExaStore reduce overall storage cost and improves performance at the same time by reducing
write amplification comparing with widely used LSM based storage engines such as RocksDB
and HBase. Write amplifications not only hurt SSD life, but also occupies majority of the
I/O bandwith, leaving only a tiny portion to the client.

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
the storage engine using UDP protocol. `FixedServerTestClient` is a modified db_bench (stolen
from RocksDB) that serves as a performance test engine.  `EBTest` contains unit tests. 
`UdpTestApp` and `UdpTestClient` are test programs for a network transportation protocol
built on top of UDP.





