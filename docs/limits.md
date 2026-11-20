# Limits

Guarantees, bounds and caveats of kolo-store. This page is reference material: the README does not repeat it.

## Guarantees

- `Append` stores the entry, its key and the new signed head in one atomic
  batch, synced to disk; `Replica.Update` stores a whole growth step the
  same way. A crash never leaves an entry without its head or a head
  past the stored entries. `Create`, `Delete(indexes...)`, `OpenReplica` (when
  it creates), `Replica.AddKeys`, `Replica.ApplyDelete`, `Store.Delete` and `Store.Adopt` are
  single batches too.
- `Open` reads the header and the signed head first, then only the
  entries the head covers, in one pass, and checks each one against its
  signed record as it reads it, keeping only the record and tree hash. It
  stops at the first problem and returns no log. It never repairs data.
  `Open` and `OpenReplica` therefore take time and I/O linear in the size
  of the log: they read every stored entry once, so a large log opens
  slowly (about 250 bytes of memory per entry stays afterwards).
- After `Open`, every read (`Get`, `Read`, `Entry`, `Key`, `AddKey`, `AddKeys`)
  fetches the entry from the backend and checks it against the record and
  flags in memory: the ciphertext hash, the key commitment, whether a
  marker and a key are stored. A change on disk returns `ErrCorrupt`
  wrapping `kololog.ErrStorageMismatch`.
- Memory is bounded by the number of entries, not their size, except in
  `Adopt`: the open log or replica keeps about 250 bytes per entry, and a
  read, append or update holds the entries it handles. Under `GOMEMLIMIT=64MiB`, a 10 GiB
  log of 1 MiB entries is written, replicated, reopened and read back with
  a peak Go heap of 95 to 125 MB across runs, most of it the garbage
  collector's headroom and Pebble's own buffers, and the open log and
  replica keep about 250 bytes per entry. `EntryInfo` and `Key` of every
  entry read only the keys, 32 bytes per entry, and no ciphertext
  (`TestLargeLogBoundedMemory`, run with
  `KOLO_STORE_LARGE_BYTES=10737418240`; a normal run uses 256 MiB).
- `Adopt` holds the entries it adds in its batch, and Pebble holds a
  second copy while it commits: adopting 256 MiB of entries took a peak
  heap of 560 MB, 2.1 times their size, in the same test. It counts its
  batch as Pebble does and returns `ErrTooLarge` for 2 GiB or more (1 GiB on
  32-bit), half of Pebble's batch limit, so both copies fit in that limit;
  nothing is written. It reads the stored keys and the markers of entries
  only the store deleted, never the stored ciphertext, then reopens the
  whole log like `Open`.
- A store holds one log per log ID, whatever its owner: keys are per log
  ID. `Open` or `OpenReplica` of an ID the store already holds for another
  owner returns `kololog.ErrBadLogHeader`, changes nothing on disk and
  never returns that owner's log; a replica of another owner's log with
  the same ID needs another store.
- A log ID can be open only once per backend in a process. A second
  `OpenPebble` on the same directory fails, also through another path, so
  another process cannot write the same logs.

## Bounds

| What | Value |
|---|---|
| Memory per entry of an open log or replica | about 250 bytes (tree hashes and a compact record); entries are read from disk |
| Stored key of a live entry | 32 bytes, read without the ciphertext |
| Logs per log ID in one store | one, whatever its owner |
| Open handles per log ID and backend in a process | one; `ErrLogInUse` otherwise |
| `OpenPebble` on one directory | one at a time, also through another path |
| Batch of one `Adopt` | under 2 GiB, 1 GiB on 32-bit, counted as Pebble counts a batch; `ErrTooLarge` from 2 GiB on. Half of Pebble's batch limit, since Adopt and Pebble each hold a copy |
| Pebble batch | under 4 GiB - 1 bytes, 2 GiB - 1 on 32-bit: Pebble's `maxBatchSize`, min(MaxUint32, MaxInt) (Pebble v2.1.7 `open.go:52`, `internal/constants/constants.go:16`); `ErrTooLarge` otherwise, where Pebble itself would panic |
| `PebbleOptions.MemTableSize` | 0 (Pebble's 4 MiB) or 1 MiB to 1 GiB; `ErrInvalidOptions` otherwise. Memory: up to twice the size |
| Pebble file mode | 0600 for every file Pebble creates in the directory, so the entry keys stored in them are readable by the owner only; `OpenPebble` also changes the files already in the directory to 0600 |
| Store directory mode | 0700 on creation; a directory that group or others can access, that another user owns, or that is swapped while it opens returns `ErrInsecureDir` |

- Permissions and owner of the store directory, and the file modes, are not checked on Windows.
- `Log.Header`, `Log.ID`, `Log.Len` and the same methods of `Replica` return their value after `Close` too; every other method returns `ErrClosed`.
- The directory is checked when it is opened (same directory, owner, permissions). Changing what the path points to afterwards requires write access to a parent directory, and the store does not defend against that.
- Pebble links the C zstd library when cgo is enabled. Build with `CGO_ENABLED=0` for a pure Go binary.
- The storage is trusted to be the node's own disk. `WithTrustedHead` is the anchor against a rollback of the head, and the replica kind byte is not signed.
