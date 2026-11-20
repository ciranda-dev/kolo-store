// Command basic stores two Kolo logs in Pebble, reopens one after closing the store and deletes the other.
package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"
)

func main() {
	dir, err := os.MkdirTemp("", "kolo-store-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "db")

	me, err := koloid.Generate()
	if err != nil {
		log.Fatal(err)
	}

	b, err := kolostore.OpenPebble(path)
	if err != nil {
		log.Fatal(err)
	}
	s := kolostore.New(b)
	notes, err := s.Create(me, kololog.WithValueEncoding(kololog.UTF8))
	if err != nil {
		log.Fatal(err)
	}
	scratch, err := s.Create(me)
	if err != nil {
		log.Fatal(err)
	}
	for _, text := range []string{"first", "second", "third"} {
		if _, err := notes.Append(text); err != nil {
			log.Fatal(err)
		}
	}
	if _, err := scratch.Append([]byte("temporary")); err != nil {
		log.Fatal(err)
	}
	if err := notes.Delete(1); err != nil {
		log.Fatal(err)
	}
	before, err := notes.Head()
	if err != nil {
		log.Fatal(err)
	}
	notesID, scratchID := notes.ID(), scratch.ID()
	notes.Close()
	scratch.Close()
	if err := s.Close(); err != nil {
		log.Fatal(err)
	}

	// A new process: open the directory again and restore the log from disk.
	b, err = kolostore.OpenPebble(path)
	if err != nil {
		log.Fatal(err)
	}
	s = kolostore.New(b)
	defer s.Close()
	ids, err := s.List()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("stored logs:", len(ids))

	notes, err = s.Open(me, notesID, kololog.WithValueEncoding(kololog.UTF8))
	if err != nil {
		log.Fatal(err)
	}
	defer notes.Close()
	after, err := notes.Head()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("restored entries:", notes.Len(), "same root:", after.Root == before.Root)
	head, err := kololog.VerifyHead(me.PublicKey(), notesID, after)
	if err != nil {
		log.Fatal(err)
	}
	rec, err := notes.Record(0)
	if err != nil {
		log.Fatal(err)
	}
	proof, err := notes.InclusionProof(0, head.Size())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("entry 0 included:", kololog.VerifyInclusion(head, rec, proof))

	_, err = notes.Get(1)
	fmt.Println("entry 1 deleted:", errors.Is(err, kololog.ErrDeleted))
	var first string
	if err := notes.Read(0, &first); err != nil {
		log.Fatal(err)
	}
	fmt.Println("entry 0 after restore:", first)

	i, err := notes.Append("fourth")
	if err != nil {
		log.Fatal(err)
	}
	var text string
	if err := notes.Read(i, &text); err != nil {
		log.Fatal(err)
	}
	fmt.Println("appended after restore:", i, text)

	_, err = s.Open(me, notesID, kololog.WithValueEncoding(kololog.UTF8))
	fmt.Println("second open refused:", errors.Is(err, kolostore.ErrLogInUse))

	if err := s.Delete(scratchID); err != nil {
		log.Fatal(err)
	}
	ids, err = s.List()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("stored logs after delete:", len(ids))
}
