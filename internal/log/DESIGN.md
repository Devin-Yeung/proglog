# Logging (internal/log)

This document describes the on-disk and in-memory design of the `internal/log` package.

At a high level, a `Log` is an append-only sequence of protobuf records persisted to disk. The log is split into
independent *segments*; each segment has:

- a `.store` file with the serialized records
- a `.index` file mapping logical offsets to positions in the store

## Terminology

- **Offset (absolute)**: The global, monotonically increasing record offset returned by `Log.Append`.
- **Base offset**: The first absolute offset in a segment (also used in segment filenames).
- **Relative offset**: `absoluteOffset - baseOffset` (stored in the index as a `uint32`).
- **Position**: The byte offset (from the beginning) of a record inside a segment's `.store` file.

## On-disk layout

The log directory contains pairs of files per segment:

```text
<baseOffset>.store
<baseOffset>.index
```

On startup, `Log` scans the directory, extracts base offsets from filenames, deduplicates them (because each segment has
two files), and recreates segments in offset order.

## Store

`store` is the persistence layer for record payloads. It is append-only and uses a length-prefixed framing format because
records are variable length. All integer encoding uses little-endian byte order.

```text
+----------------------------+--------------+
| length of record (8 bytes) | record bytes |
+----------------------------+--------------+
```

Writes are buffered via `bufio.Writer` to reduce syscalls. `Read`/`ReadAt` flush the write buffer before reading to ensure
read-after-write correctness.

## Index

`index` maps a segment's relative offsets to store positions. The index is persisted on disk and memory-mapped (mmap) to
provide fast random access while still being recoverable across process restarts.

Each entry is fixed width:

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

Implementation details:

- The index file is pre-sized to `Config.segment.maxIndexBytes` before mapping (mmap can’t grow files).
- `index.size` tracks the number of bytes actually used for valid entries.
- On `Close`, the mapping is synced, the file is truncated down to `index.size`, and resources are released.
- `Read(-1)` reads the last entry (used during segment initialization to recover the segment’s `nextOffset`).

## Segment

A `segment` combines a store and an index for a contiguous range of offsets.

- `baseOffset` is the first absolute offset in the segment.
- `nextOffset` is the next absolute offset to be assigned (right boundary, exclusive).

Append path:

1. Set `record.Offset = nextOffset`
2. Marshal the protobuf record and append to the store, capturing the store position
3. Write `(relativeOffset, position)` to the index
4. Increment `nextOffset`

A segment is considered full when either:

- `store.size >= Config.segment.maxStoreBytes`, or
- `index.size >= Config.segment.maxIndexBytes` (used bytes; the underlying file may be larger due to preallocation)

## Log

`Log` manages a slice of segments and a single active segment (the one that receives new appends).

- `Append` writes to the active segment and rolls to a new segment when it becomes full. The new segment’s base offset is
  the previous segment’s `nextOffset`.
- `Read` finds the segment whose range satisfies `baseOffset <= offset < nextOffset` and delegates to `segment.Read`.
- `Truncate(lowest)` deletes entire segments whose highest offset is lower than `lowest`. Truncating the active segment is
  rejected (`ErrSegmentActive`) to avoid removing offsets that might still be appended to.

## Concurrency and durability notes

- `Log` uses an `RWMutex` to synchronize segment selection and lifecycle operations.
- `store` guards its buffered writer and size with a mutex.
- `Append` does not fsync; buffered writes are flushed on `Read`/`ReadAt`/`Close`. If callers need stronger durability
  guarantees, they must arrange an explicit flush/sync policy (not currently exposed by the package).
