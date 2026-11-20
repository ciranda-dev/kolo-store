package kolostore

import (
	"sync"
	"testing"
)

func TestHookIsSafeToSetWhileRead(t *testing.T) {
	var h hook[func() int]
	if h.get() != nil {
		t.Fatal("an unset hook returned a function")
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 1000 {
				if f := h.get(); f != nil {
					f()
				}
			}
		})
	}
	for range 1000 {
		h.set(func() int { return 1 })
		h.set(nil)
	}
	wg.Wait()
	h.set(func() int { return 7 })
	if got := h.get()(); got != 7 {
		t.Fatalf("hook returned %d, want 7", got)
	}
}
