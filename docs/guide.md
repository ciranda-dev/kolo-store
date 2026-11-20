# Guide

Walkthrough of the API. The on-disk forms are in [formats.md](formats.md) and the bounds and guarantees in [limits.md](limits.md). A complete program is in the README, and [examples/basic](../examples/basic/main.go) is a runnable example.

## Stores and logs

- `OpenPebble(dir)` opens a store directory on disk, and `NewMemory()`
  gives an in-memory one for tests. Both return a `Backend`, a small
  interface (Get, Put, Delete, ordered Scan, atomic Batch) that you can
  also implement yourself. `OpenPebble` creates the directory with mode
  0700 and refuses one that group or others can access, that another user
  owns, or that is swapped while it opens. Permissions and owner are not
  checked on Windows. The directory is checked when it is opened (same
  directory, owner, permissions). Changing what the path points to
  afterwards requires write access to a parent directory, and the store
  does not defend against that.
- `OpenPebbleWith(dir, PebbleOptions{MemTableSize, NoCompression})` is
  `OpenPebble` for large values, such as 1 MiB chunks. `MemTableSize`
  (1 MiB to 1 GiB, zero for Pebble's 4 MiB) makes Pebble flush and compact
  less often; it costs up to twice that size in memory, one memtable being
  filled and one waiting to be flushed. `NoCompression` skips compressing
  blocks, which is wasted work on ciphertext. Every commit is still
  synced, so durability is the same, and a directory opens with any
  options. A `MemTableSize` out of bounds returns `ErrInvalidOptions` and
  creates nothing. On one Mac, 1 GiB in 1 MiB values took 14.9 s with the
  defaults and 7.4 s with 256 MiB and no compression; reproduce with
  `KOLO_STORE_BENCH=1 go test -run PebbleWriteSpeed -v`.
- `New(b)` wraps a backend in a `Store`. Stores on the same backend value
  share their open logs, so a custom `Backend` should be a comparable type
  such as a pointer.
- `Create(id, opts...)` starts a new log. `Open(id, logID, opts...)` loads
  a stored log and rebuilds it with `kololog.Restore`. Each entry's key is
  stored next to its ciphertext, so a reopened log reads its old entries.
  `Open` refuses a stored replica with `ErrReplica`, so a replica never
  becomes a writable log that could fork its owner's history. Pass
  `kololog.WithTrustedHead(h)` with the last head you published, so an
  older stored snapshot returns `kololog.ErrRollback`. The storage is
  trusted to be the node's own disk, and `WithTrustedHead` is the anchor
  against a rollback of the head. The replica kind byte is not signed
  either: a write to the disk that marks a grown replica as an own log lets
  `Open` accept it with the owner's identity.
- `OpenReplica(pub, logID, header)` opens the stored read-only replica of
  someone else's log, or creates it from the owner's public key, the log ID
  and the signed log header (`kololog.LogHeader.Encode()`). It never needs
  a private key. An existing replica is verified like `Open`; a non-empty
  `header` must then equal the stored one. It refuses one of your own logs
  with `ErrNotReplica`.
- `List()` returns the stored log and replica IDs. `Owner(logID)` returns
  the owner key named in the stored header and whether the log is a
  replica, so after a restart you know whether to call `Open` or
  `OpenReplica`, and with which key. That key is checked only against the
  header's own signature; `Open` and `OpenReplica` verify everything
  against the key you pass them.
- `Delete(logID)` removes a whole log or replica in one batch, and refuses
  a log open anywhere in the process. Deleting a replica also returns
  `ErrLogInUse` while the owner's writable log with that ID is open in the
  same process. `Close()` closes every open `Log` and `Replica` of the
  store, then the backend.
- `Adopt(id, rep, opts...)` brings the closed own log of `id` up to the
  replica `rep` of it, which may live in any store, and opens the result
  like `Open`. It is for a device whose own log is behind the copy others
  hold: pull the missing entries with their keys into a replica, then adopt
  it, and the log appends at the replica's length. `rep` needs a head, no
  pending target (`ErrIncomplete`) and the key of every live entry past the
  stored head (`kololog.ErrKeyUnavailable`); the entries the store already
  holds keep their stored keys. It must belong to `id`, with the header
  this store holds (`kololog.ErrBadLogHeader`), and this store must hold an
  own log with its ID (`kololog.ErrMissingHeader` when there is none,
  `ErrReplica` for a replica). The stored log's head must be a prefix of
  the replica's, so a shorter or forked replica returns
  `kololog.ErrRollback`; a log open in this process returns `ErrLogInUse`.
  One batch adds the replica's entries past the stored head with their
  keys and its head, so a crash leaves the old log or the new one, never a
  mix. Deletions are kept from both sides: an entry the replica deleted is
  stored with its marker and its key removed, and an entry the store
  deleted stays deleted even when the replica still holds it, so a
  deletion is never undone. Before the batch, Adopt reads the stored keys,
  no ciphertext, and checks the marker of every entry only the store
  deleted; a stored entry without a key and without a valid marker returns
  `ErrCorrupt`. It does not repair the stored entries it keeps: other
  damage there fails the final open, as it fails `Open`. The batch holds
  the entries it adds, and Pebble a second copy: Adopt refuses a batch over
  of 2 GiB or more (1 GiB on 32-bit) with `ErrTooLarge`, and 256 MiB of entries took a
  peak heap of 560 MB. An error from the final open, such as
  `kololog.ErrEncodingMismatch` for a missing `opts`, leaves the adopted
  log stored and `Open` opens it.
- A `Log` has the methods of `kololog.Log` (`Append`, `Delete`, `Read`,
  `Get`, `Entry`, `Key`, `EntryInfo`, `Head`, `Header`, `ID`, `Len`,
  `Record`, `InclusionProof`, `ConsistencyProof`) plus `Close`, and is safe
  for concurrent use. It is the only way to write to a stored log. The
  store is the log's `kololog.Storage`: kolo-log commits every change as
  one batch before it shows it, so a failed write changes nothing and the
  log keeps working. `Delete(indexes...)` stores the delete markers, signed
  by the log key, in place of the entries' ciphertexts and removes their
  keys in one synced batch: deleting 1,000 entries takes about 34 ms in
  one call against about 3.6 s one at a time; `Open` rejects a deleted entry without a valid marker.
  `Entry(i)` returns the stored form of an entry with its key, and `Key(i)`
  and `EntryInfo(i)` return its key and whether it is deleted or keyed
  without reading its ciphertext, for a sync layer.
- A `Replica` grows only through `Update(h, proof, entries)`, which runs
  `kololog.Replica.Update` (head, every entry and its key, the consistency
  proof against the rebuilt root), which stores the new entries and keys,
  the head and any unfinished target with its proof in one batch. The entries may be
  a chunk that stops short of `h`, so a replica far behind catches up in
  steps and each step is one bounded batch. `OpenReplica` re-verifies a
  stored target by replaying it through the checks of `Update`, so a
  restart in the middle of a catch-up resumes where it stopped; `Target()`
  returns the head being filled and its proof. A refused update or a failed
  write changes nothing in memory or on disk. `AddKey(i, key)` gives an entry that came
  without its key the key, checked against the record's commitment, in one
  write; the key it already has writes nothing. `AddKeys(start, keys)`
  checks many keys at once with `kololog.Replica.AddKeys` (at most
  `kololog.MaxAddKeys` per call) and stores every key that passes in one
  batch; a failed write stores none.
  `ApplyDelete(markers...)` verifies owner delete markers and stores them
  in place of the entries' ciphertexts and keys in one write; one bad
  marker applies none. Reads are those of
  a `Log` (`Read`, `Get`, `Entry`, `Key`, `EntryInfo`, `Head`, `Header`,
  `ID`, `Len`, `Record`, `InclusionProof`, `ConsistencyProof`) plus
  `Close`. There is no `Append` and no `Delete` by index. Errors from
  kolo-log pass through, so `errors.Is` tells `kololog.ErrRollback`,
  `kololog.ErrDiverged`, `kololog.ErrBadProof`, `kololog.ErrBadEntry` and
  `kololog.ErrInvalidHead` apart. A failed write returns the backend's
  error and changes nothing; try again.

## Deletion

Deletion is best effort. On this disk, the key is removed in the same
write that drops the ciphertext and stores the marker. After that write
Pebble no longer references the old value, and compaction later drops it
from its files, but the bytes are not erased: a backup, a filesystem
snapshot, the write-ahead log or freed disk blocks from before the delete
keep a working key. The open log knows the entry is deleted and refuses an
old value put back, but after a reopen that value and its key make the
entry readable again with no error. Across peers, deletion holds
on every peer that follows the protocol and applies the marker; a peer
that already received a key can keep it, and nothing can take it back.

## Errors

Errors, for `errors.Is`:

- `ErrLogInUse`: the log or replica is already open on this backend.
- `ErrIncomplete`: `Adopt` got a replica with no head or an unfinished
  catch-up.
- `ErrTooLarge`: a write too large for one batch, an `Adopt` batch of 2 GiB
  or more or a Pebble batch of 4 GiB - 1 bytes or more (1 GiB and
  2 GiB - 1 on 32-bit). Nothing is written.
- `ErrReplica`: `Open` found a read-only replica. `ErrNotReplica`:
  `OpenReplica` found a log that is not a replica.
- `ErrCorrupt`: stored data is missing, has extra keys, does not decode or
  does not match its signed header, heads, records and delete markers, or
  an own log's live entry lost its key, at `Open`, `Adopt` or a later
  read. The error also wraps the kolo-log error that found it, such as
  `kololog.ErrRestoreMismatch`, `kololog.ErrStorageMismatch` or
  `kololog.ErrInvalidHead`, so `errors.Is` finds both.
- `ErrClosed`: the store, backend or log is closed.
- `ErrNotFound`: `Backend.Get` found no value.
- `ErrInsecureDir`: `OpenPebble` found a directory open to group or
  others, owned by another user, or swapped while opening.
- `ErrInvalidOptions`: `OpenPebbleWith` got a `MemTableSize` that is not 0
  or between 1 MiB and 1 GiB.
- Other kolo-log errors from `Open` pass through wrapped, without
  `ErrCorrupt`, such as `kololog.ErrMissingHeader`, `kololog.ErrRollback`
  and `kololog.ErrEncodingMismatch`. `kololog.ErrRestoreMismatch` always
  comes inside `ErrCorrupt`, since it means damaged stored data.
