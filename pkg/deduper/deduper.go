package deduper

import (
	"hash/fnv"
	"sync"
)

// Deduper in memory LRU uniquer with limited lifetime.  Test will return true
// if the message has been (recently) seen.  Each time Trim() is called, it
// advances the generation.  Messages from the nth and nth-1 generation are
// considered to be seen.
//
// This offers a low overhead crude LRU uniquing of streams over time with a
// relatively bounded memory footprint but without allowing duplications across
// batch boundaries that a naive reset of the deduper state could allow or
// needing to maintain eviction lists.
type Deduper struct {
	sync.Mutex
	seen    map[uint64]bool
	lastgen map[uint64]bool
}

// Test determines if the Deduper has seen a key via another Test within the
// last calls of Trim.
func (m *Deduper) Test(b []byte) bool {
	h := fnv.New64a()
	// hash.Hash says Write will never return an error.
	_, _ = h.Write(b)
	v := h.Sum64()

	m.Lock()
	// Allow this gen and the previous gen.
	res := m.seen[v] || m.lastgen[v]
	m.seen[v] = true
	m.Unlock()

	return res
}

// Trim demotest the current generation of keys and removes knowledge of the
// nth-2 generation of keys upon return.
func (m *Deduper) Trim() {
	m.Lock()
	m.lastgen = m.seen
	m.seen = map[uint64]bool{}
	m.Unlock()
}

// New creates a new Deduper with no keys in it.  The zero value Deduper
// will panic due to not having allocated maps; use NewDeduper.
func New() *Deduper {
	return &Deduper{
		seen:    map[uint64]bool{},
		lastgen: map[uint64]bool{},
	}
}
