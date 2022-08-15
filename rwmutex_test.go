package litefs_test

import (
	"context"
	"testing"
	"time"

	"github.com/superfly/litefs"
)

func TestRWMutex_TryLock(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		var mu litefs.RWMutex
		g := mu.TryLock()
		if g == nil {
			t.Fatal("expected lock")
		} else if mu.TryLock() != nil {
			t.Fatal("expected lock failure")
		}
		g.Unlock()
	})

	t.Run("Relock", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.TryLock()
		if g0 == nil {
			t.Fatal("expected lock")
		} else if mu.TryLock() != nil {
			t.Fatal("expected lock failure")
		}
		g0.Unlock()

		g1 := mu.TryLock()
		if g1 == nil {
			t.Fatal("expected lock after unlock")
		}
		g1.Unlock()
	})

	t.Run("BlockedBySharedLock", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.TryRLock()
		if g0 == nil {
			t.Fatal("expected lock")
		} else if mu.TryLock() != nil {
			t.Fatal("expected lock failure")
		}
		g0.Unlock()

		g1 := mu.TryLock()
		if g1 == nil {
			t.Fatal("expected lock after shared unlock")
		}
		g1.Unlock()
	})
}

func TestRWMutex_Lock(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		var mu litefs.RWMutex
		g0, err := mu.Lock(context.Background())
		if err != nil {
			t.Fatal(err)
		} else if g0 == nil {
			t.Fatal("expected lock")
		}

		ch := make(chan int, 0)
		go func() {
			g1, err := mu.Lock(context.Background())
			if err != nil {
				t.Fatal(err)
			} else if g1 == nil {
				t.Fatal("expected lock")
			}
			close(ch)
		}()

		select {
		case <-ch:
			t.Fatal("lock obtained too soon")
		case <-time.After(100 * time.Millisecond):
		}

		g0.Unlock()

		select {
		case <-ch:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timeout waiting for lock")
		}
	})

	t.Run("ContextCanceled", func(t *testing.T) {
		var mu litefs.RWMutex
		g0, err := mu.Lock(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer g0.Unlock()

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			if _, err := mu.Lock(ctx); err != context.Canceled {
				t.Fatal(err)
			}
		}()

		time.Sleep(100 * time.Millisecond)
		cancel()
		time.Sleep(100 * time.Millisecond)
	})
}

func TestRWMutex_CanLock(t *testing.T) {
	t.Run("WithExclusiveLock", func(t *testing.T) {
		var mu litefs.RWMutex
		if !mu.CanLock() {
			t.Fatal("expected to be able to lock")
		}
		g := mu.TryLock()
		if mu.CanLock() {
			t.Fatal("expected to not be able to lock")
		}
		g.Unlock()

		if !mu.CanLock() {
			t.Fatal("expected to be able to lock again")
		}
	})

	t.Run("WithSharedLock", func(t *testing.T) {
		var mu litefs.RWMutex
		if !mu.CanLock() {
			t.Fatal("expected to be able to lock")
		}
		g := mu.TryRLock()
		if mu.CanLock() {
			t.Fatal("expected to not be able to lock")
		}
		g.Unlock()

		if !mu.CanLock() {
			t.Fatal("expected to be able to lock again")
		}
	})
}

func TestRWMutex_TryRLock(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.TryRLock()
		if g0 == nil {
			t.Fatal("expected lock")
		}
		g1 := mu.TryRLock()
		if g1 == nil {
			t.Fatal("expected another lock")
		}
		if mu.TryLock() != nil {
			t.Fatal("expected lock failure")
		}
		g0.Unlock()
		g1.Unlock()

		g2 := mu.TryLock()
		if g2 == nil {
			t.Fatal("expected lock after unlock")
		}
		g2.Unlock()
	})

	t.Run("BlockedByExclusiveLock", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.TryLock()
		if g0 == nil {
			t.Fatal("expected lock")
		}
		if mu.TryRLock() != nil {
			t.Fatalf("expected lock failure")
		}
		g0.Unlock()

		g1 := mu.TryLock()
		if g1 == nil {
			t.Fatal("expected lock after unlock")
		}
		g1.Unlock()
	})

	t.Run("AfterDowngrade", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.TryLock()
		if g0 == nil {
			t.Fatal("expected lock")
		}
		if mu.TryRLock() != nil {
			t.Fatalf("expected lock failure")
		}
		g0.RLock()

		g1 := mu.TryRLock()
		if g1 == nil {
			t.Fatal("expected lock after downgrade")
		}
		g0.Unlock()
		g1.Unlock()
	})

	t.Run("AfterUpgrade", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.TryRLock()
		if !g0.TryLock() {
			t.Fatal("expected upgrade")
		}
		if mu.TryRLock() != nil {
			t.Fatalf("expected lock failure")
		}
		g0.RLock() // downgrade

		g1 := mu.TryRLock()
		if g1 == nil {
			t.Fatal("expected lock after downgrade")
		}
		g0.Unlock()
		g1.Unlock()
	})
}

func TestRWMutex_RLock(t *testing.T) {
	t.Run("MultipleSharedLocks", func(t *testing.T) {
		var mu litefs.RWMutex
		g0, err := mu.RLock(context.Background())
		if err != nil {
			t.Fatal(err)
		} else if g0 == nil {
			t.Fatal("expected lock")
		}

		g1, err := mu.RLock(context.Background())
		if err != nil {
			t.Fatal(err)
		} else if g1 == nil {
			t.Fatal("expected lock")
		}

		g0.Unlock()
		g1.Unlock()
	})

	t.Run("Blocked", func(t *testing.T) {
		var mu litefs.RWMutex
		g0, err := mu.Lock(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		go func() {
			g1, err := mu.RLock(context.Background())
			if err != nil {
				t.Fatal(err)
			} else if g1 == nil {
				t.Fatal("expected lock")
			}
			g1.Unlock()
		}()

		time.Sleep(100 * time.Millisecond)
		g0.Unlock()
		time.Sleep(100 * time.Millisecond)
	})

	t.Run("ContextCanceled", func(t *testing.T) {
		var mu litefs.RWMutex
		g0, err := mu.Lock(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer g0.Unlock()

		ch := make(chan int)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			if _, err := mu.RLock(ctx); err != context.Canceled {
				t.Fatalf("unexpected error: %v", err)
			}
			close(ch)
		}()

		time.Sleep(100 * time.Millisecond)
		cancel()
		<-ch
	})
}

func TestRWMutex_CanRLock(t *testing.T) {
	t.Run("WithExclusiveLock", func(t *testing.T) {
		var mu litefs.RWMutex
		if !mu.CanRLock() {
			t.Fatal("expected to be able to lock")
		}
		g := mu.TryLock()
		if mu.CanRLock() {
			t.Fatal("expected to not be able to lock")
		}
		g.Unlock()

		if !mu.CanRLock() {
			t.Fatal("expected to be able to lock again")
		}
	})

	t.Run("WithSharedLock", func(t *testing.T) {
		var mu litefs.RWMutex
		if !mu.CanRLock() {
			t.Fatal("expected to be able to lock")
		}
		g := mu.TryRLock()
		if !mu.CanRLock() {
			t.Fatal("expected to be able to lock")
		}
		g.Unlock()

		if !mu.CanRLock() {
			t.Fatal("expected to be able to lock again")
		}
	})
}

func TestRWMutexGuard_TryLock(t *testing.T) {
	t.Run("DoubleLock", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.TryLock()
		if !g0.TryLock() { // no-op
			t.Fatal("expected true for no-op")
		}
		g0.Unlock()
	})

	t.Run("WithSharedLock", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.TryRLock()
		g1 := mu.TryRLock()
		if g0.TryLock() {
			t.Fatal("expected upgrade failure")
		}
		g1.Unlock()

		if !g0.TryLock() {
			t.Fatal("expected upgrade success")
		}
		g0.Unlock()
	})
}

func TestRWMutexGuard_TryRLock(t *testing.T) {
	t.Run("DoubleLock", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.TryRLock()
		g0.RLock() // no-op
		g0.Unlock()
	})
}
