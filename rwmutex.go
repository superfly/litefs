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

	// If set, this function is called when the state transitions.
	// Must be set before use of the mutex or its guards.
	OnLockStateChange func(prevState, newState RWMutexState)
}

// Guard returns an unlocked guard for the mutex.
func (rw *RWMutex) Guard() RWMutexGuard {
	return RWMutexGuard{rw: rw, state: RWMutexStateUnlocked}
}

// State returns whether the mutex has a exclusive lock, one or more shared
// locks, or if the mutex is unlocked.
func (rw *RWMutex) State() RWMutexState {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return rw.state()
}

func (rw *RWMutex) state() RWMutexState {
	if rw.excl != nil {
		return RWMutexStateExclusive
	} else if rw.sharedN > 0 {
		return RWMutexStateShared
	}
	return RWMutexStateUnlocked
}

// RWMutexGuard is a reference to a mutex. Locking, unlocking, upgrading, &
// downgrading operations are all performed via the guard instead of directly
// on the RWMutex itself as this works similarly to how POSIX locks work.
type RWMutexGuard struct {
	rw    *RWMutex
	state RWMutexState
}

// State returns the current state of the guard.
func (g *RWMutexGuard) State() RWMutexState {
	g.rw.mu.Lock()
	defer g.rw.mu.Unlock()
	return g.state
}

// Lock attempts to obtain a exclusive lock for the guard. Returns an error if ctx is done.
func (g *RWMutexGuard) Lock(ctx context.Context) error {
	if g.TryLock() {
		return nil
	}

	ticker := time.NewTicker(RWMutexInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			if g.TryLock() {
				return nil
			}
		}
	}
}

// TryLock upgrades the lock from a shared lock to an exclusive lock.
// This is a no-op if the lock is already an exclusive lock. This function will
// trigger OnLockStateChange on the mutex, if set, and if state changes.
func (g *RWMutexGuard) TryLock() bool {
	g.rw.mu.Lock()
	prevState := g.rw.state()
	v := g.tryLock()
	fn, newState := g.rw.OnLockStateChange, g.rw.state()
	g.rw.mu.Unlock()

	if fn != nil && prevState != newState {
		fn(prevState, newState)
	}
	return v
}

func (g *RWMutexGuard) tryLock() bool {
	switch g.state {
	case RWMutexStateUnlocked:
		if g.rw.sharedN != 0 || g.rw.excl != nil {
			return false
		}
		g.rw.sharedN, g.rw.excl = 0, g
		g.state = RWMutexStateExclusive
		return true

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
		panic("RWMutexGuard.TryLock(): unreachable")
	}
}

// CanLock returns true if the guard can become an exclusive lock.
// Also returns the current state of the underlying mutex to determine if the
// lock is blocked by a shared or exclusive lock.
func (g *RWMutexGuard) CanLock() (canLock bool, mutexState RWMutexState) {
	g.rw.mu.Lock()
	defer g.rw.mu.Unlock()

	switch g.state {
	case RWMutexStateUnlocked:
		return g.rw.sharedN == 0 && g.rw.excl == nil, g.rw.state()
	case RWMutexStateShared:
		return g.rw.sharedN == 1, g.rw.state()
	case RWMutexStateExclusive:
		return true, g.rw.state()
	default:
		panic("RWMutexGuard.CanLock(): unreachable")
	}
}

// RLock attempts to obtain a shared lock for the guard. Returns an error if ctx is done.
func (g *RWMutexGuard) RLock(ctx context.Context) error {
	if g.TryRLock() {
		return nil
	}

	ticker := time.NewTicker(RWMutexInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			if g.TryRLock() {
				return nil
			}
		}
	}
}

// TryRLock attempts to obtain a shared lock on the mutex for the guard. This will upgrade
// an unlocked guard and downgrade an exclusive guard. Shared guards are a no-op.
func (g *RWMutexGuard) TryRLock() bool {
	g.rw.mu.Lock()
	prevState := g.rw.state()
	v := g.tryRLock()
	fn, newState := g.rw.OnLockStateChange, g.rw.state()
	g.rw.mu.Unlock()

	if fn != nil && prevState != newState {
		fn(prevState, newState)
	}
	return v
}

func (g *RWMutexGuard) tryRLock() bool {
	switch g.state {
	case RWMutexStateUnlocked:
		if g.rw.excl != nil {
			return false
		}
		g.rw.sharedN++
		g.state = RWMutexStateShared
		return true

	case RWMutexStateShared:
		return true // no-op

	case RWMutexStateExclusive:
		assert(g.rw.excl == g, "attempted downgrade of non-exclusive guard")
		g.rw.sharedN, g.rw.excl = 1, nil
		g.state = RWMutexStateShared
		return true

	default:
		panic("RWMutexGuard.TryRLock(): unreachable")
	}
}

// CanRLock returns true if the guard can become a shared lock.
func (g *RWMutexGuard) CanRLock() bool {
	g.rw.mu.Lock()
	defer g.rw.mu.Unlock()

	switch g.state {
	case RWMutexStateUnlocked:
		return g.rw.excl == nil
	case RWMutexStateShared, RWMutexStateExclusive:
		return true
	default:
		panic("RWMutexGuard.CanRLock(): unreachable")
	}
}

// Unlock unlocks the underlying mutex.
func (g *RWMutexGuard) Unlock() {
	g.rw.mu.Lock()
	prevState := g.rw.state()
	g.unlock()
	fn, newState := g.rw.OnLockStateChange, g.rw.state()
	g.rw.mu.Unlock()

	if fn != nil && prevState != newState {
		fn(prevState, newState)
	}
}

func (g *RWMutexGuard) unlock() {
	switch g.state {
	case RWMutexStateUnlocked:
		return // already unlocked, skip
	case RWMutexStateShared:
		assert(g.rw.sharedN > 0, "invalid shared lock state on unlock")
		g.rw.sharedN--
		g.state = RWMutexStateUnlocked
	case RWMutexStateExclusive:
		assert(g.rw.excl == g, "attempted unlock of non-exclusive guard")
		g.rw.sharedN, g.rw.excl = 0, nil
		g.state = RWMutexStateUnlocked
	default:
		panic("RWMutexGuard.Unlock(): unreachable")
	}
}

// RWMutexState represents the lock state of an RWMutex or RWMutexGuard.
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
	RWMutexStateUnlocked = RWMutexState(iota)
	RWMutexStateShared
	RWMutexStateExclusive
)
