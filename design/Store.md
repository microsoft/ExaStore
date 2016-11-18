#Single Node Storage Engine Design

Our altimate goal is to provide a low cost distributed and replicated storage
system. This document explains the design of the most important component:
the single node storage engine.

## Key Value API

The Storage Engine is a stand alone exe accepting requests via UDP. We provide
a library `ExaBroker` that knows how to communicate with the storage engine.

### Data Record

The data record should have the following fields:

- Key: 128 bits.  Custom keys can be hashed into 128b integer with minimum collision. 
- Owner: a 64 bit hash key.  1..232-1 reserved. Only an owner can overwrite or delete.
- Reader: a 64 bit hash key.  0 means anyone.  1..232-1 reserved.
- Time Stamp: the time when the written request arrives primary server. The store supports versioning based on time stamps.
- Parts: boolean. Any non-zero value indicating the data is one part of a series of write partial requests.
- Values: data blobs, currently limited to 16 MB in 1MB parts.

### POLICIES

Policies regarding data retention and consistency are maintained based on 64 bit Owner ID.

**Expiration lifetime:**  unsigned integer limited to 2047 hours.  Zero means Zero
(will not be written to SSD). In Exabyte scale we cannot allow data to live forever.
All data expires in a finite time.  In future we may make provisions for extending
lifetime, for discard prior to schedule, for copying to archive, etc.  These functions
will revolve around the capabilities associated with the Owner ID.

**Allow Secondary Read:** Boolean. When true, on the rare occasion of Primary server
timeout, the read operation is sent to a secondary server.

### API

Write.

> Atomic operation: Write a blob, under a key, with specified owner and permitted reader, size.
  
Read.

> Always reads the whole value.  
> To be implemented. Reader can provide a time stamp in order to retrieve an earlier version.

 
Delete.

> Atomic operation. Allow the key-value to expire.  Do not retrieve the value if key is presented and the
request has no time stamp earlier than the deletion.

WritePartial/WriteFinal.

> Atomic operation, for opportunistic collection of a burst of small related data with multiple parallel
writers, e.g. in distributed tracing. Results will be atomic but multiple blobs concatenated with
undefined order.  Timestamp and expiration lifetime will relate to the first part accepted.
Multiple Finals are idempotent.  If none is seen, the Final is implied by a timeout.

> Opportunistic in the sense that we provide no guarantee. In most cases all partial writes within
5 seconds of the first part accepted will be collected and merged. After merging, all subsequent
partial write to the same key will be ignored . Under unusual circumstances they might drop at 1
second. We aim at achieving 5 seconds 90% of the time. Time window of the session (from the first
accepted part to the merging time) will be reported to read requests.

> Example scenario for this case is session based logging, where all writes (originating from
multiple servers) would send events keyed on a session ID and they concatenate.  Allowing for 
logging to include pathological slow cases this would probably work find with a 5 second window 
(even if we discard beyond 5 seconds, we probably captured what the pathology was).
