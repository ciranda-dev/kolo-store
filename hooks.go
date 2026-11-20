package kolostore

import "sync/atomic"

// hook holds a function that tests install; the code that calls it reads it without a data race.
type hook[F any] struct{ p atomic.Pointer[F] }

func (h *hook[F]) set(f F) { h.p.Store(&f) }

// get returns the installed function, or nil.
func (h *hook[F]) get() (f F) {
	if p := h.p.Load(); p != nil {
		f = *p
	}
	return f
}
