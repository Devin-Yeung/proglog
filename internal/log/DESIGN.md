# Logging

This document describes the design of the logging package used in our system.

## Store

`store` is the physical layer of the logging package. It is responsible for the on disk storage of log entries.
Since the length of log entries is variable, we use a length-prefixed format to store log entries:

```text
+----------------------------+--------------+
| length of record (8 bytes) | record bytes |
+----------------------------+--------------+
```

To optimize for write performance, the `store` is **append-only** and log entries are **buffered in memory** and flushed
to disk in batches.

## Index

`index` is designed to provide fast lookups of log entries. As `store` is optimized for writes, the performance of random
reads is poor. To solve this problem, we maintain an in-memory index that maps log entry logical *offsets* (relative to 
the starting point of segment) to their physical *positions* in the file:

```text
0          4 bytes                     12 bytes
+----------+---------------------------+
|  Offset  |         Position          |
+----------+---------------------------+
|  uint32  |          uint64           |
+----------+---------------------------+
|<-- 4B -->|<----------- 8B ---------->|
|<-------- entryWidth (12B) ---------->|
```
