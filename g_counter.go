package crdt

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	uuid "github.com/satori/go.uuid"
)

// GCounter represent a G-counter in CRDT, which is
// a state-based grow-only counter that only supports
// increments.
type GCounter struct {
	mtx sync.RWMutex

	// ident provides a unique identity to each replica.
	ident string

	// counter maps identity of each replica to their
	// entry values i.e. the counter value they individually
	// have.
	counter map[string]int
}

// NewGCounter returns a *GCounter by pre-assigning a unique
// identity to it.
func NewGCounter() *GCounter {
	return &GCounter{
		ident:   uuid.NewV4().String(),
		counter: make(map[string]int),
	}
}

// NewGCounterFromJSONByte returns a *GCounter from previously
// serialized json
func NewGCounterFromJSONBytes(in []byte) *GCounter {

	in_struct := gcounterJSON{}
	err := json.Unmarshal(in, &in_struct)

	if err != nil {

		panic(fmt.Sprintf("failed to import GCounter from JSON: %+v\n", string(in)))
	}

	return &GCounter{
		ident:   in_struct.I,
		counter: in_struct.C,
	}
}

// Inc increments the GCounter by the value of 1 everytime it
// is called.
func (g *GCounter) Inc() {
	g.IncVal(1)
}

// IncVal allows passing in an arbitrary delta to increment the
// current value of counter by. Only positive values are accepted.
// If a negative value is provided the implementation will panic.
func (g *GCounter) IncVal(incr int) {
	if incr < 0 {
		panic("cannot decrement a gcounter")
	}

	g.mtx.Lock()
	g.counter[g.ident] += incr
	g.mtx.Unlock()

}

// Count returns the total count of this counter across all the
// present replicas.
func (g *GCounter) Count() (total int) {
	g.mtx.RLock()

	for _, val := range g.counter {
		total += val
	}

	g.mtx.RUnlock()
	return
}

func (g *GCounter) String() string {
	return strconv.Itoa(g.Count())
}

// Merge combines the counter values across multiple replicas.
// The property of idempotency is preserved here across
// multiple merges as when no state is changed across any replicas,
// the result should be exactly the same everytime.
func (g *GCounter) Merge(c *GCounter) {
	g.mtx.Lock()
	c.mtx.Lock()

	for ident, val := range c.counter {
		if v, ok := g.counter[ident]; !ok || v < val {
			g.counter[ident] = val
		}
	}

	g.mtx.Unlock()
	c.mtx.Unlock()
}

type gcounterJSON struct {
	I string         `json:"i"`
	C map[string]int `json:"e"`
}

// MarshalJSON will be used to generate a serialized output
// of a given GCounter.
func (g *GCounter) MarshalJSON() ([]byte, error) {
	g.mtx.RLock()

	b, e := json.Marshal(&gcounterJSON{
		I: g.ident,
		C: g.counter,
	})

	g.mtx.RUnlock()
	return b, e
}
