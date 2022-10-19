package litefs_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/superfly/litefs"
	"golang.org/x/sync/errgroup"
)

func TestRWMutexGuard_TryLock(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		var mu litefs.RWMutex
		g0, g1 := mu.Guard(), mu.Guard()
		if !g0.TryLock() {
			t.Fatal("expected lock")
		} else if g1.TryLock() {
			t.Fatal("expected lock failure")
		}
		g0.Unlock()
	})

	t.Run("Relock", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.Guard()
		if !g0.TryLock() {
			t.Fatal("expected lock")
		}
		g0.Unlock()

		g1 := mu.Guard()
		if !g1.TryLock() {
			t.Fatal("expected lock after unlock")
		}
		g1.Unlock()
	})

	t.Run("BlockedBySharedLock", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.Guard()
		if !g0.TryRLock() {
			t.Fatal("expected lock")
		}
		g0.Unlock()

		g1 := mu.Guard()
		if !g1.TryLock() {
			t.Fatal("expected lock after shared unlock")
		}
		g1.Unlock()
	})
}

func TestRWMutexGuard_Lock(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.Guard()
		if err := g0.Lock(context.Background()); err != nil {
			t.Fatal(err)
		}

		ch := make(chan int)
		var g errgroup.Group
		g.Go(func() error {
			g1 := mu.Guard()
			if err := g1.Lock(context.Background()); err != nil {
				return err
			}
			close(ch)
			return nil
		})

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

		if err := g.Wait(); err != nil {
			t.Fatalf("goroutine failed: %s", err)
		}
	})

	t.Run("ContextCanceled", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.Guard()
		if err := g0.Lock(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer g0.Unlock()

		ctx, cancel := context.WithCancel(context.Background())
		var g errgroup.Group
		g.Go(func() error {
			g1 := mu.Guard()
			if err := g1.Lock(ctx); err != context.Canceled {
				return err
			}
			return nil
		})

		time.Sleep(100 * time.Millisecond)
		cancel()
		time.Sleep(100 * time.Millisecond)

		if err := g.Wait(); err != nil {
			t.Fatalf("goroutine failed: %s", err)
		}
	})
}

func TestRWMutexGuard_CanLock(t *testing.T) {
	t.Run("WithExclusiveLock", func(t *testing.T) {
		var mu litefs.RWMutex
		guard0 := mu.Guard()
		if canLock, _ := guard0.CanLock(); !canLock {
			t.Fatal("expected to be able to lock")
		}
		g := mu.Guard()
		g.TryLock()
		guard1 := mu.Guard()
		if canLock, mutexState := guard1.CanLock(); canLock {
			t.Fatal("expected to not be able to lock")
		} else if got, want := mutexState, litefs.RWMutexStateExclusive; got != want {
			t.Fatalf("mutex=%s, expected %s", got, want)
		}
		g.Unlock()

		guard2 := mu.Guard()
		if canLock, _ := guard2.CanLock(); !canLock {
			t.Fatal("expected to be able to lock again")
		}
	})

	t.Run("WithSharedLock", func(t *testing.T) {
		var mu litefs.RWMutex
		guard0 := mu.Guard()
		if canLock, _ := guard0.CanLock(); !canLock {
			t.Fatal("expected to be able to lock")
		}
		g := mu.Guard()
		g.TryRLock()
		guard1 := mu.Guard()
		if canLock, mutexState := guard1.CanLock(); canLock {
			t.Fatal("expected to not be able to lock")
		} else if got, want := mutexState, litefs.RWMutexStateShared; got != want {
			t.Fatalf("mutex=%s, want %s", got, want)
		}
		g.Unlock()

		guard2 := mu.Guard()
		if canLock, _ := guard2.CanLock(); !canLock {
			t.Fatal("expected to be able to lock again")
		}
	})
}

func TestRWMutexGuard_TryRLock(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.Guard()
		if !g0.TryRLock() {
			t.Fatal("expected lock")
		}
		g1 := mu.Guard()
		if !g1.TryRLock() {
			t.Fatal("expected another lock")
		}
		if guard := mu.Guard(); guard.TryLock() {
			t.Fatal("expected lock failure")
		}
		g0.Unlock()
		g1.Unlock()

		g2 := mu.Guard()
		if !g2.TryLock() {
			t.Fatal("expected lock after unlock")
		}
		g2.Unlock()
	})

	t.Run("BlockedByExclusiveLock", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.Guard()
		if !g0.TryLock() {
			t.Fatal("expected lock")
		}
		if guard := mu.Guard(); guard.TryRLock() {
			t.Fatalf("expected lock failure")
		}
		g0.Unlock()

		g1 := mu.Guard()
		if !g1.TryLock() {
			t.Fatal("expected lock after unlock")
		}
		g1.Unlock()
	})

	t.Run("AfterDowngrade", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.Guard()
		if !g0.TryLock() {
			t.Fatal("expected lock")
		}
		if guard := mu.Guard(); guard.TryRLock() {
			t.Fatalf("expected lock failure")
		}
		if !g0.TryRLock() {
			t.Fatal("expected downgrade")
		}

		g1 := mu.Guard()
		if !g1.TryRLock() {
			t.Fatal("expected lock after downgrade")
		}
		g0.Unlock()
		g1.Unlock()
	})

	t.Run("AfterUpgrade", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.Guard()
		if !g0.TryRLock() {
			t.Fatal("expected lock")
		}
		if !g0.TryLock() {
			t.Fatal("expected upgrade")
		}
		if guard := mu.Guard(); guard.TryRLock() {
			t.Fatalf("expected lock failure")
		}
		if !g0.TryRLock() { // downgrade
			t.Fatal("expected downgrade")
		}

		g1 := mu.Guard()
		if !g1.TryRLock() {
			t.Fatal("expected lock after downgrade")
		}
		g0.Unlock()
		g1.Unlock()
	})
}

func TestRWMutexGuard_RLock(t *testing.T) {
	t.Run("MultipleSharedLocks", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.Guard()
		if err := g0.RLock(context.Background()); err != nil {
			t.Fatal(err)
		}

		g1 := mu.Guard()
		if err := g1.RLock(context.Background()); err != nil {
			t.Fatal(err)
		}

		g0.Unlock()
		g1.Unlock()
	})

	t.Run("Blocked", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.Guard()
		if err := g0.Lock(context.Background()); err != nil {
			t.Fatal(err)
		}

		var g errgroup.Group
		g.Go(func() error {
			g1 := mu.Guard()
			if err := g1.RLock(context.Background()); err != nil {
				return err
			}
			g1.Unlock()
			return nil
		})

		time.Sleep(100 * time.Millisecond)
		g0.Unlock()
		time.Sleep(100 * time.Millisecond)

		if err := g.Wait(); err != nil {
			t.Fatalf("goroutine failed: %s", err)
		}
	})

	t.Run("ContextCanceled", func(t *testing.T) {
		var mu litefs.RWMutex
		g0 := mu.Guard()
		if err := g0.Lock(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer g0.Unlock()

		ch := make(chan int)
		ctx, cancel := context.WithCancel(context.Background())
		var g errgroup.Group
		g.Go(func() error {
			g1 := mu.Guard()
			if err := g1.RLock(ctx); err != context.Canceled {
				return fmt.Errorf("unexpected error: %v", err)
			}
			close(ch)
			return nil
		})

		time.Sleep(100 * time.Millisecond)
		cancel()
		<-ch

		if err := g.Wait(); err != nil {
			t.Fatalf("goroutine failed: %s", err)
		}
	})
}

func TestRWMutexGuard_CanRLock(t *testing.T) {
	t.Run("WithExclusiveLock", func(t *testing.T) {
		var mu litefs.RWMutex
		if guard := mu.Guard(); !guard.CanRLock() {
			t.Fatal("expected to be able to lock")
		}
		g := mu.Guard()
		g.TryLock()
		if guard := mu.Guard(); guard.CanRLock() {
			t.Fatal("expected to not be able to lock")
		}
		g.Unlock()

		if guard := mu.Guard(); !guard.CanRLock() {
			t.Fatal("expected to be able to lock again")
		}
	})

	t.Run("WithSharedLock", func(t *testing.T) {
		var mu litefs.RWMutex
		if guard := mu.Guard(); !guard.CanRLock() {
			t.Fatal("expected to be able to lock")
		}
		g := mu.Guard()
		g.TryRLock()
		if guard := mu.Guard(); !guard.CanRLock() {
			t.Fatal("expected to be able to lock")
		}
		g.Unlock()

		if guard := mu.Guard(); !guard.CanRLock() {
			t.Fatal("expected to be able to lock again")
		}
	})
}
