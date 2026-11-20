# kolo-store

Persistent storage for Kolo logs and read-only replicas, on one machine.

[![status: experimental](https://img.shields.io/badge/status-experimental-orange)](#status)
[![license: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)
[![go: 1.26+](https://img.shields.io/badge/go-1.26%2B-00ADD8)](go.mod)

## What it is

kolo-store keeps [kolo-log](https://github.com/ciranda-dev/kolo-log) logs on disk and brings them back after a restart, checking every signature, hash and Merkle root again. An open log keeps only its tree hashes and records in memory, about 250 bytes per entry, and reads entries from disk, so memory does not grow with the size of what is stored. `Adopt` is the one exception: it holds the entries it adds about twice over, and refuses a batch of 2 GiB or more. One store holds many logs, your own and read-only replicas of other people's. It keeps a log replica the way a [Certificate Transparency monitor (RFC 9162, section 8.2)](https://www.rfc-editor.org/info/rfc9162) keeps a log, and deletes by destroying keys, the cryptographic erase of [NIST SP 800-88 Rev. 1](https://doi.org/10.6028/NIST.SP.800-88r1).

It uses [Pebble](https://github.com/cockroachdb/pebble) on disk, and a small `Backend` interface you can implement yourself. There is no network code.

## Install

```sh
go get github.com/ciranda-dev/kolo-store
```

Pebble links the C zstd library when cgo is enabled. Build with `CGO_ENABLED=0` for a pure Go binary.

## Quick start

Create a log in a store directory, append an entry, close it, reopen it and append again. The program takes the store directory as its only argument; use a new directory in a temporary location. Save it outside the repository and run it with `go run . <store dir>` in a module that requires kolo-store, kolo-log and kolo-id.

```go
package main

import (
	"fmt"
	"log"
	"os"

	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: myapp <store dir>")
	}
	me, err := koloid.Generate()
	check(err)
	b, err := kolostore.OpenPebble(os.Args[1])
	check(err)
	s := kolostore.New(b)
	defer s.Close()

	l, err := s.Create(me, kololog.WithValueEncoding(kololog.UTF8))
	check(err)
	_, err = l.Append("hello")
	check(err)
	logID := l.ID()
	check(l.Close())

	l, err = s.Open(me, logID, kololog.WithValueEncoding(kololog.UTF8))
	check(err)
	defer l.Close()
	i, err := l.Append("again")
	check(err)
	var text string
	check(l.Read(i, &text))
	fmt.Println(l.Len(), text)
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
```

```text
$ go run . "$(mktemp -d)/store"
2 again
```

In a real program, keep `me` with [kolo-id](https://github.com/ciranda-dev/kolo-id) `Save` and `Load`: only the identity that created a log can open it again.

## API

| Symbol | What it does |
|---|---|
| `OpenPebble(dir)`, `NewMemory()` | A `Backend` on disk, or in memory for tests |
| `OpenPebbleWith(dir, PebbleOptions{MemTableSize, NoCompression})` | `OpenPebble` tuned for large values; commits stay synced |
| `New(b)` | Wrap a backend in a `Store` |
| `Create(id, opts...)` | Start a new log |
| `Open(id, logID, opts...)` | Load a stored log and rebuild it with `kololog.Restore`; `kololog.WithTrustedHead(h)` rejects an older snapshot |
| `OpenReplica(pub, logID, header)` | Open, or create, the read-only replica of someone else's log; it never needs a private key |
| `Adopt(id, rep, opts...)` | Bring your closed log up to a newer replica of it in one batch, keeping every deletion, and open it |
| `List()`, `Owner(logID)` | The stored IDs, and the owner key and kind of one |
| `Delete(logID)` | Remove a whole log or replica in one batch |
| `Log` | The methods of `kololog.Log` plus `Close`; every change is one atomic, synced batch |
| `Replica` | Grows only through `Update`; `AddKey`, `AddKeys` and `ApplyDelete` also change it |

`Open` refuses a stored replica with `ErrReplica`, so a replica never becomes a writable log that could fork its owner's history.

An append stores the entry, its key and the new signed head in one atomic batch, so a crash never leaves an entry without its head or a head past the stored entries. `Open` checks everything it reads and returns no log at the first problem. It never repairs data.

Errors work with `errors.Is`: `ErrLogInUse`, `ErrIncomplete`, `ErrReplica`, `ErrNotReplica`, `ErrCorrupt`, `ErrClosed`, `ErrNotFound`, `ErrTooLarge`, `ErrInsecureDir` and `ErrInvalidOptions`. Errors from kolo-log pass through.

Deletion is best effort. The key is removed in the same write that drops the ciphertext, but a backup, a filesystem snapshot or freed disk blocks from before the delete can keep a working key. See [docs/guide.md](docs/guide.md).

## More

- [docs/guide.md](docs/guide.md): stores, logs, replicas, deletion and errors.
- [docs/formats.md](docs/formats.md): what is stored on disk, and references.
- [docs/limits.md](docs/limits.md): guarantees, bounds and caveats.
- [examples/basic](examples/basic/main.go): a runnable example.
- [SECURITY.md](SECURITY.md): security policy.

## Status

Experimental. The API may change before v1.0.0.

## License

Licensed under either of the [Apache License, Version 2.0](LICENSE) or the [MIT license](LICENSE-MIT), at your option.
