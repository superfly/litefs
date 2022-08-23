package litefs

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// RWMutexInterval is the time between reattempting lock acquisition.
const RWMutexInterval = 10 * time.Microsecond

// RWMutex is a reader/writer mutual exclusion lock. It wraps the sync package
// to provide additional capabilities such as lock upgrades & downgrades. It
// only supports TryLock() & TryRLock() as that is what's supported by our
// FUSE file system.
type RWMutex struct {
	mu      sync.Mutex
	sharedN int           // number of readers
	excl    *RWMutexGuard // exclusive lock holder
}

// State returns whether the mutex has a exclusive lock, one or more shared
// locks, or if the mutex is unlocked.
func (rw *RWMutex) State() RWMutexState {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	if rw.excl != nil {
		return RWMutexStateExclusive
	} else if rw.sharedN > 0 {
		return RWMutexStateShared
	}
	return RWMutexStateUnlocked
}

// Lock attempts to obtain a exclusive lock on rw. Returns an error if ctx is done.
func (rw *RWMutex) Lock(ctx context.Context) (*RWMutexGuard, error) {
	if guard := rw.TryLock(); guard != nil {
		return guard, nil
	}

	ticker := time.NewTicker(RWMutexInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			if guard := rw.TryLock(); guard != nil {
				return guard, nil
			}
		}
	}
}

// TryLock tries to lock the mutex for writing and returns a guard if it succeeds.
func (rw *RWMutex) TryLock() *RWMutexGuard {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	if !rw.canLock() {
		return nil
	}
	guard := newRWMutexGuard(rw, RWMutexStateExclusive)
	rw.excl = guard
	return guard
}

// CanLock returns true if the write lock could be acquired.
func (rw *RWMutex) CanLock() bool {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return rw.canLock()
}

func (rw *RWMutex) canLock() bool {
	return rw.sharedN == 0 && rw.excl == nil
}

// TryRLock tries to lock rw for reading and reports whether it succeeded.
func (rw *RWMutex) TryRLock() *RWMutexGuard {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	if !rw.canRLock() {
		return nil
	}
	g := newRWMutexGuard(rw, RWMutexStateShared)
	rw.sharedN++
	return g
}

// RLock attempts to obtain a shared lock on rw. Returns an error if ctx is done.
func (rw *RWMutex) RLock(ctx context.Context) (*RWMutexGuard, error) {
	if guard := rw.TryRLock(); guard != nil {
		return guard, nil
	}

	ticker := time.NewTicker(RWMutexInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			if guard := rw.TryRLock(); guard != nil {
				return guard, nil
			}
		}
	}
}

// CanRLock returns true if the read lock could be acquired.
func (rw *RWMutex) CanRLock() bool {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return rw.canRLock()
}

func (rw *RWMutex) canRLock() bool {
	return rw.excl == nil
}

// RWMutexGuard is a reference to a held lock.
type RWMutexGuard struct {
	rw    *RWMutex
	state RWMutexState
}

func newRWMutexGuard(rw *RWMutex, state RWMutexState) *RWMutexGuard {
	return &RWMutexGuard{
		rw:    rw,
		state: state,
	}
}

// TryLock upgrades the lock from a shared lock to an exclusive lock.
// This is a no-op if the lock is already an exclusive lock.
func (g *RWMutexGuard) TryLock() bool {
	g.rw.mu.Lock()
	defer g.rw.mu.Unlock()

	assert(g.state != RWMutexStateUnlocked, "attempted exclusive lock of unlocked guard")

	switch g.state {
	case RWMutexStateShared:
		assert(g.rw.excl == nil, "exclusive lock already held while upgrading shared lock")
		if g.rw.sharedN > 1 {
			return false // another shared lock is being held
		}

		assert(g.rw.sharedN == 1, "invalid shared lock count on guard upgrade")
		g.rw.sharedN, g.rw.excl = 0, g
		g.state = RWMutexStateExclusive
		return true

	case RWMutexStateExclusive:
		return true // no-op

	default:
		panic(fmt.Sprintf("invalid guard state: %d", g.state))
	}
}

// CanLock returns true if the guard can become an exclusive lock.
func (g *RWMutexGuard) CanLock() bool {
	g.rw.mu.Lock()
	defer g.rw.mu.Unlock()

	assert(g.state != RWMutexStateUnlocked, "attempted exclusive lock check of unlocked guard")

	switch g.state {
	case RWMutexStateShared:
		return g.rw.sharedN == 1
	case RWMutexStateExclusive:
		return true
	default:
		panic(fmt.Sprintf("invalid guard state: %d", g.state))
	}
}

// RLock downgrades the lock from an exclusive lock to a shared lock.
// This is a no-op if the lock is already a shared lock.
func (g *RWMutexGuard) RLock() {
	g.rw.mu.Lock()
	defer g.rw.mu.Unlock()

	assert(g.state != RWMutexStateUnlocked, "attempted shared lock of unlocked guard")

	switch g.state {
	case RWMutexStateShared:
		return // no-op
	case RWMutexStateExclusive:
		assert(g.rw.excl == g, "attempted downgrade of non-exclusive guard")
		g.rw.sharedN, g.rw.excl = 1, nil
		g.state = RWMutexStateShared
	default:
		panic(fmt.Sprintf("invalid guard state: %d", g.state))
	}
}

// Unlock unlocks the underlying mutex. Guard must be discarded after Unlock().
func (g *RWMutexGuard) Unlock() {
	g.rw.mu.Lock()
	defer g.rw.mu.Unlock()

	switch g.state {
	case RWMutexStateUnlocked:
		return // double unlocks are no-op
	case RWMutexStateShared:
		assert(g.rw.sharedN > 0, "invalid shared lock state on unlock")
		g.rw.sharedN--
		g.state = RWMutexStateUnlocked
	case RWMutexStateExclusive:
		assert(g.rw.excl == g, "attempted unlock of non-exclusive guard")
		g.rw.sharedN, g.rw.excl = 0, nil
		g.state = RWMutexStateUnlocked
	default:
		panic(fmt.Sprintf("invalid guard state: %d", g.state))
	}
}

// RWMutexState represents the lock state of an RWMutexGuard.
type RWMutexState int

// String returns the string representation of the state.
func (s RWMutexState) String() string {
	switch s {
	case RWMutexStateUnlocked:
		return "unlocked"
	case RWMutexStateShared:
		return "shared"
	case RWMutexStateExclusive:
		return "exclusive"
	default:
		return fmt.Sprintf("<unknown(%d)>", s)
	}
}

const (
	RWMutexStateUnlocked = iota
	RWMutexStateShared
	RWMutexStateExclusive
)
