package lockout

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

func testLock(t *testing.T, devices ...model.DeviceKind) model.MaintenanceLock {
	t.Helper()
	lock, err := model.NewMaintenanceLock("chamber-1", devices, "seal replacement", "tech-12", time.Now())
	if err != nil {
		t.Fatalf("new lockout: %v", err)
	}
	return lock
}

func TestRegistryLifecycleAndGuard(t *testing.T) {
	ctx := context.Background()
	var pending int32
	registry := NewRegistry(func(string, model.DeviceKind) bool { return atomic.LoadInt32(&pending) > 0 })
	lock := testLock(t, model.DeviceUpstreamGate, model.DeviceFillValve)
	if err := registry.Request(ctx, lock); err != nil {
		t.Fatalf("request: %v", err)
	}

	commits := 0
	commit := func() error { commits++; return nil }

	// Requested but not approved: commands still flow.
	if _, err := registry.GuardDevice(ctx, "chamber-1", model.DeviceUpstreamGate, commit, nil); err != nil {
		t.Fatalf("requested lockout must not block yet: %v", err)
	}

	active, err := registry.Approve(ctx, lock.ID, "supervisor-3", time.Now())
	if err != nil {
		t.Fatalf("approve: %v", err)
	}
	if active.Status != model.LockoutActive {
		t.Fatalf("status = %s, want active", active.Status)
	}

	// Both covered devices are blocked and the commit never runs.
	if _, err := registry.GuardDevice(ctx, "chamber-1", model.DeviceUpstreamGate, commit, nil); !errors.Is(err, ErrDeviceLocked) {
		t.Fatalf("upstream guard error = %v, want ErrDeviceLocked", err)
	}
	if _, err := registry.GuardDevice(ctx, "chamber-1", model.DeviceFillValve, commit, nil); !errors.Is(err, ErrDeviceLocked) {
		t.Fatalf("fill valve guard error = %v, want ErrDeviceLocked", err)
	}
	// An uncovered device on the same chamber stays operable.
	if _, err := registry.GuardDevice(ctx, "chamber-1", model.DeviceDrainValve, commit, nil); err != nil {
		t.Fatalf("uncovered device blocked: %v", err)
	}
	if commits != 2 {
		t.Fatalf("commits = %d, want 2 (requested + uncovered)", commits)
	}

	// Self-approval is rejected.
	other := testLock(t, model.DeviceDrainValve)
	if err := registry.Request(ctx, other); err != nil {
		t.Fatalf("request other: %v", err)
	}
	if _, err := registry.Approve(ctx, other.ID, "tech-12", time.Now()); err == nil {
		t.Fatal("self-approval must be rejected")
	}

	// Release is refused while a covered device has a command in flight.
	atomic.StoreInt32(&pending, 1)
	if _, err := registry.Release(ctx, lock.ID, "supervisor-3", "", time.Now()); !errors.Is(err, ErrDevicesBusy) {
		t.Fatalf("release with pending commands error = %v, want ErrDevicesBusy", err)
	}
	atomic.StoreInt32(&pending, 0)

	released, err := registry.Release(ctx, lock.ID, "supervisor-3", "work complete", time.Now())
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if released.Status != model.LockoutReleased {
		t.Fatalf("status = %s, want released", released.Status)
	}
	if _, err := registry.GuardDevice(ctx, "chamber-1", model.DeviceUpstreamGate, commit, nil); err != nil {
		t.Fatalf("released lockout still blocks: %v", err)
	}
}

func TestOverlappingOpenLockoutRejected(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(nil)
	first := testLock(t, model.DeviceUpstreamGate)
	if err := registry.Request(ctx, first); err != nil {
		t.Fatalf("request first: %v", err)
	}
	if _, err := registry.Approve(ctx, first.ID, "boss", time.Now()); err != nil {
		t.Fatalf("approve first: %v", err)
	}
	second := testLock(t, model.DeviceUpstreamGate, model.DeviceFillValve)
	if err := registry.Request(ctx, second); !errors.Is(err, ErrOverlappingLockout) {
		t.Fatalf("overlap error = %v, want ErrOverlappingLockout", err)
	}
	// After release the device can be locked again.
	if _, err := registry.Release(ctx, first.ID, "boss", "", time.Now()); err != nil {
		t.Fatalf("release first: %v", err)
	}
	if err := registry.Request(ctx, second); err != nil {
		t.Fatalf("request after release: %v", err)
	}
}

// TestGuardAtomicAgainstConcurrentToggles hammers the registry with goroutines
// flipping a lockout active/released while commands are guarded. The commit
// closure re-inspects registry state *inside the same critical section*: if a
// toggle could ever interleave between the guard check and the commit, it would
// observe an active lockout and fail. Run with -race.
func TestGuardAtomicAgainstConcurrentToggles(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry(nil)
	lock := testLock(t, model.DeviceUpstreamGate)
	if err := registry.Request(ctx, lock); err != nil {
		t.Fatalf("request: %v", err)
	}
	if _, err := registry.Approve(ctx, lock.ID, "boss", time.Now()); err != nil {
		t.Fatalf("approve: %v", err)
	}

	setStatus := func(status model.LockoutStatus) {
		registry.mu.Lock()
		current := registry.locks[lock.ID]
		current.Status = status
		registry.locks[lock.ID] = current
		registry.mu.Unlock()
	}

	var violations int32
	var commits int64
	commit := func() error {
		// Same goroutine, registry mutex is held by GuardDevice: the
		// non-locking lookup must never see the lockout as active here.
		if active, ok := registry.activeForLocked("chamber-1", model.DeviceUpstreamGate); ok {
			t.Errorf("commit ran under active lockout %s: %s", active.ID, active.Reason)
			atomic.AddInt32(&violations, 1)
		}
		atomic.AddInt64(&commits, 1)
		return nil
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(release bool) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if release {
					setStatus(model.LockoutReleased)
				} else {
					setStatus(model.LockoutActive)
				}
			}
		}(i%2 == 0)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = registry.GuardDevice(context.Background(), "chamber-1", model.DeviceUpstreamGate, commit, nil)
			}
		}()
	}
	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()

	if atomic.LoadInt32(&violations) != 0 {
		t.Fatalf("observed %d guard/commit atomicity violations", violations)
	}
	if atomic.LoadInt64(&commits) == 0 {
		t.Fatal("expected some commits to complete while lockout toggled")
	}
}

func TestServiceJournalsBlockedReason(t *testing.T) {
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, time.Now)
	registry := NewRegistry(nil)
	service := NewService(registry, recorder, time.Now)
	ctx := context.Background()

	lock, err := service.Register(ctx, "chamber-1",
		[]model.DeviceKind{model.DeviceUpstreamGate}, "hydraulic ram service", "tech-9")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := service.Approve(ctx, lock.ID, "dispatcher-1"); err != nil {
		t.Fatalf("approve: %v", err)
	}

	called := false
	err = service.Guard(ctx, "chamber-1", model.DeviceUpstreamGate, "open", "test", func() error {
		called = true
		return nil
	})
	if !errors.Is(err, model.ErrDeviceUnderMaintenance) {
		t.Fatalf("guard error = %v, want maintenance sentinel", err)
	}
	if called {
		t.Fatal("blocked command must not run its issue closure")
	}

	events, err := store.Events(ctx, "chamber-1", 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	var blocked *model.Event
	for i := range events {
		if events[i].Type == model.EventCommandBlocked {
			blocked = &events[i]
		}
	}
	if blocked == nil {
		t.Fatal("blocked command was not written to the event stream")
	}
	var payload model.BlockedCommand
	if err := blocked.Decode(&payload); err != nil {
		t.Fatalf("decode blocked payload: %v", err)
	}
	if payload.Reason != "hydraulic ram service" || payload.LockoutID != lock.ID || payload.Action != "open" {
		t.Fatalf("blocked payload = %+v", payload)
	}

	// Release restores command flow without bypassing anything at this layer.
	if _, err := service.Release(ctx, lock.ID, "dispatcher-1", "done"); err != nil {
		t.Fatalf("release: %v", err)
	}
	ran := false
	if err := service.Guard(ctx, "chamber-1", model.DeviceUpstreamGate, "open", "test", func() error {
		ran = true
		return nil
	}); err != nil {
		t.Fatalf("guard after release: %v", err)
	}
	if !ran {
		t.Fatal("command must run after release")
	}
}

func TestRecoveryRestoresActiveLockout(t *testing.T) {
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, time.Now)
	registry := NewRegistry(nil)
	service := NewService(registry, recorder, time.Now)
	ctx := context.Background()

	lock, err := service.Register(ctx, "chamber-2",
		[]model.DeviceKind{model.DeviceFillValve, model.DeviceDrainValve}, "valve overhaul", "tech-1")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := service.Approve(ctx, lock.ID, "chief"); err != nil {
		t.Fatalf("approve: %v", err)
	}
	released, err := service.Register(ctx, "chamber-2",
		[]model.DeviceKind{model.DeviceUpstreamGate}, "inspection", "tech-2")
	if err != nil {
		t.Fatalf("register released-case: %v", err)
	}
	if _, err := service.Approve(ctx, released.ID, "chief"); err != nil {
		t.Fatalf("approve second: %v", err)
	}
	if _, err := service.Release(ctx, released.ID, "chief", "ok"); err != nil {
		t.Fatalf("release second: %v", err)
	}

	// Fresh registry replays the same journal.
	rebuilt := NewRegistry(nil)
	recovery := NewRecovery(journal.NewReplayer(store), rebuilt)
	restored, err := recovery.RestoreAll(ctx)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if len(restored) != 2 {
		t.Fatalf("restored %d lockouts, want 2", len(restored))
	}
	if active := rebuilt.Active("chamber-2"); len(active) != 1 || active[0].ID != lock.ID {
		t.Fatalf("active after replay = %+v, want only the valve overhaul", active)
	}
	if _, blocked := rebuilt.ActiveFor("chamber-2", model.DeviceFillValve); !blocked {
		t.Fatal("fill valve must remain blocked after replay")
	}
	if _, blocked := rebuilt.ActiveFor("chamber-2", model.DeviceUpstreamGate); blocked {
		t.Fatal("released upstream gate must not be blocked after replay")
	}
}
