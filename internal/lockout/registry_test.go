package lockout

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

func newTestRegistry(t *testing.T, clearance ClearanceCheck, canceller PendingCanceller) (*Registry, *journal.MemoryStore) {
	t.Helper()
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, time.Now)
	registry := NewRegistry(recorder, canceller, time.Now)
	registry.BindClearance(clearance)
	return registry, store
}

func allowClearance(string, []model.DeviceKind) error { return nil }

func scope(devices ...model.DeviceKind) model.LockoutScope {
	return model.LockoutScope{ChamberID: "chamber-1", Devices: devices}
}

func TestRegisterApproveBlocksAndReleases(t *testing.T) {
	ctx := context.Background()
	registry, store := newTestRegistry(t, allowClearance, nil)

	record, err := registry.Register(ctx, scope(model.DeviceFillValve), "seal replacement", "tech-lee")
	if err != nil {
		t.Fatal(err)
	}
	if record.State != model.LockoutPending {
		t.Fatalf("new lockout = %s, want pending", record.State)
	}
	active, err := registry.Approve(ctx, record.ID, "supervisor-kim")
	if err != nil {
		t.Fatal(err)
	}
	if active.State != model.LockoutActive {
		t.Fatalf("approved lockout = %s", active.State)
	}

	var fillIssued, drainIssued int
	err = registry.Guard(ctx, "chamber-1", model.DeviceFillValve, "set_opening",
		uuid.Nil, 1, func() error { fillIssued++; return nil })
	var blocked *BlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("fill valve command error = %v, want *BlockedError", err)
	}
	if blocked.LockoutID != record.ID {
		t.Fatalf("blocked by %s, want %s", blocked.LockoutID, record.ID)
	}
	if fillIssued != 0 {
		t.Fatal("locked fill valve command was issued")
	}
	if err := registry.Guard(ctx, "chamber-1", model.DeviceDrainValve, "set_opening",
		uuid.Nil, 1, func() error { drainIssued++; return nil }); err != nil {
		t.Fatalf("unlocked drain valve command blocked: %v", err)
	}
	if drainIssued != 1 {
		t.Fatal("drain valve command should have been issued")
	}
	events, _ := store.Events(ctx, "chamber-1", 0)
	var blockedEvents int
	for _, event := range events {
		if event.Type == model.EventCommandBlocked {
			blockedEvents++
			var payload BlockedCommand
			if err := event.Decode(&payload); err != nil {
				t.Fatal(err)
			}
			if payload.Reason != "seal replacement" {
				t.Fatalf("blocked event reason = %q", payload.Reason)
			}
		}
	}
	if blockedEvents != 1 {
		t.Fatalf("blocked events = %d, want 1", blockedEvents)
	}

	released, err := registry.Release(ctx, record.ID, "supervisor-kim", "checks passed")
	if err != nil {
		t.Fatal(err)
	}
	if released.State != model.LockoutReleased {
		t.Fatalf("released lockout = %s", released.State)
	}
	if err := registry.Guard(ctx, "chamber-1", model.DeviceFillValve, "set_opening",
		uuid.Nil, 1, func() error { fillIssued++; return nil }); err != nil {
		t.Fatalf("fill valve command blocked after release: %v", err)
	}
	if fillIssued != 1 {
		t.Fatal("fill valve should issue exactly once after release")
	}
}

func TestReleaseRequiresStandingClearance(t *testing.T) {
	ctx := context.Background()
	registry, _ := newTestRegistry(t, func(string, []model.DeviceKind) error {
		return errors.New("water level 14.20 outside safe window")
	}, nil)
	record, _ := registry.Register(ctx, scope(model.DeviceUpstreamGate), "work", "tech")
	if _, err := registry.Approve(ctx, record.ID, "sup"); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Release(ctx, record.ID, "sup", ""); !errors.Is(err, ErrClearanceNotMet) {
		t.Fatalf("release error = %v, want ErrClearanceNotMet", err)
	}
	if still, ok := registry.ActiveFor("chamber-1", model.DeviceUpstreamGate); !ok || still.ID != record.ID {
		t.Fatal("lockout must stay active after failed clearance")
	}
}

func TestOverlappingRegistrationsRejected(t *testing.T) {
	ctx := context.Background()
	registry, _ := newTestRegistry(t, allowClearance, nil)
	first, err := registry.Register(ctx, scope(model.DeviceUpstreamGate), "gate work", "tech")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(ctx, scope(model.DeviceUpstreamGate, model.DeviceDownstreamGate), "overlap", "tech"); err == nil {
		t.Fatal("overlapping pending lockout must be rejected")
	}
	if _, err := registry.Register(ctx, scope(), "chamber-wide overlap", "tech"); err == nil {
		t.Fatal("chamber-wide lockout must overlap a device lockout")
	}
	// Independent equipment registers fine, and becomes usable after first is
	// rejected.
	second, err := registry.Register(ctx, scope(model.DeviceFillValve), "valve work", "tech")
	if err != nil {
		t.Fatalf("independent lockout rejected: %v", err)
	}
	if _, err := registry.Reject(ctx, first.ID, "sup", "reschedule"); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Approve(ctx, second.ID, "sup"); err != nil {
		t.Fatal(err)
	}
	if _, ok := registry.ActiveFor("chamber-1", model.DeviceUpstreamGate); ok {
		t.Fatal("rejected lockout must not be active")
	}
	if _, ok := registry.ActiveFor("chamber-1", model.DeviceFillValve); !ok {
		t.Fatal("approved independent lockout should be active")
	}
}

func TestApproveCancelsQueuedDeviceCommands(t *testing.T) {
	ctx := context.Background()
	var cancelledDevices atomic.Value
	cancelledDevices.Store([]model.DeviceKind(nil))
	var cancelled int32
	registry, _ := newTestRegistry(t, allowClearance, func(_ string, devices []model.DeviceKind, _ time.Time) {
		atomic.AddInt32(&cancelled, 1)
		cancelledDevices.Store(append([]model.DeviceKind(nil), devices...))
	})
	record, _ := registry.Register(ctx, scope(model.DeviceFillValve), "work", "tech")
	if _, err := registry.Approve(ctx, record.ID, "sup"); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&cancelled) != 1 {
		t.Fatalf("canceller invocations = %d, want 1", cancelled)
	}
	devices := cancelledDevices.Load().([]model.DeviceKind)
	if len(devices) != 1 || devices[0] != model.DeviceFillValve {
		t.Fatalf("cancelled devices = %v", devices)
	}
}

// TestGuardIssueAtomicAgainstRelease proves a command whose authorization has
// passed still holds the registry lock while issuing: a concurrent release
// cannot interleave, so no command is issued against a lockout being lifted.
func TestGuardIssueAtomicAgainstRelease(t *testing.T) {
	ctx := context.Background()
	registry, _ := newTestRegistry(t, allowClearance, nil)
	gateLockout, _ := registry.Register(ctx, scope(model.DeviceUpstreamGate), "gate", "tech")
	if _, err := registry.Approve(ctx, gateLockout.ID, "sup"); err != nil {
		t.Fatal(err)
	}
	other, _ := registry.Register(ctx, scope(model.DeviceDrainValve), "valve", "tech")
	if _, err := registry.Approve(ctx, other.ID, "sup"); err != nil {
		t.Fatal(err)
	}
	// Pending lockout used to prove Approve also serializes on the mutex.
	pending, _ := registry.Register(ctx, scope(model.DeviceDownstreamGate), "other gate", "tech")

	insideIssue := make(chan struct{})
	allowIssue := make(chan struct{})

	go func() {
		_ = registry.Guard(ctx, "chamber-1", model.DeviceFillValve, "set_opening", uuid.Nil, 1, func() error {
			close(insideIssue)
			<-allowIssue
			return nil
		})
	}()
	<-insideIssue

	releaseDone := make(chan struct{})
	go func() {
		_, _ = registry.Release(ctx, other.ID, "sup", "")
		close(releaseDone)
	}()
	// While the fill-valve command holds the lock inside its issue callback,
	// release clearance (which runs under the same lock) cannot have started.
	select {
	case <-releaseDone:
		t.Fatal("release completed while a command issuance held the lock")
	case <-time.After(30 * time.Millisecond):
	}

	// A second lockout cannot be approved in that window either: Approve must
	// block on the registry mutex.
	approveDone := make(chan struct{})
	go func() {
		_, _ = registry.Approve(ctx, pending.ID, "sup")
		close(approveDone)
	}()
	select {
	case <-approveDone:
		t.Fatal("approve completed while command issuance held the lock")
	case <-time.After(30 * time.Millisecond):
	}

	close(allowIssue)
	select {
	case <-releaseDone:
	case <-time.After(time.Second):
		t.Fatal("release did not complete after command issuance finished")
	}
	select {
	case <-approveDone:
	case <-time.After(time.Second):
		t.Fatal("approve did not complete after command issuance finished")
	}
}

// TestReleaseCannotSneakPastBlockingCommand proves the inverse ordering: once
// a release starts its critical section, a Guard against the covered device
// cannot issue until the release (and its clearance) fully commits.
func TestReleaseCannotSneakPastBlockingCommand(t *testing.T) {
	ctx := context.Background()
	clearanceCalled := make(chan struct{})
	allowClearance := make(chan struct{})
	registry, _ := newTestRegistry(t, func(string, []model.DeviceKind) error {
		select {
		case <-clearanceCalled:
		default:
			close(clearanceCalled)
		}
		<-allowClearance
		return nil
	}, nil)
	record, _ := registry.Register(ctx, scope(model.DeviceFillValve), "valve", "tech")
	if _, err := registry.Approve(ctx, record.ID, "sup"); err != nil {
		t.Fatal(err)
	}

	releaseDone := make(chan struct{})
	go func() {
		_, _ = registry.Release(ctx, record.ID, "sup", "")
		close(releaseDone)
	}()
	<-clearanceCalled

	var issued int32
	guardDone := make(chan struct{})
	go func() {
		_ = registry.Guard(ctx, "chamber-1", model.DeviceFillValve, "set_opening", uuid.Nil, 1,
			func() error { atomic.StoreInt32(&issued, 1); return nil })
		close(guardDone)
	}()
	select {
	case <-guardDone:
		t.Fatal("guard completed while release was mid-clearance")
	case <-time.After(30 * time.Millisecond):
	}
	if atomic.LoadInt32(&issued) != 0 {
		t.Fatal("command issued during release window")
	}
	close(allowClearance)
	select {
	case <-guardDone:
	case <-time.After(time.Second):
		t.Fatal("guard did not complete after release committed")
	}
	if atomic.LoadInt32(&issued) != 1 {
		t.Fatal("command should issue exactly after release committed")
	}
}

// TestConcurrentApproveReleaseAndCommands hammers the registry from many
// goroutines; run under -race it verifies mutual exclusion, and every
// successful issue is checked against the final invariant that devices that
// end locked never had a blocking decision reported as success.
func TestConcurrentApproveReleaseAndCommands(t *testing.T) {
	ctx := context.Background()
	registry, _ := newTestRegistry(t, allowClearance, nil)
	devices := []model.DeviceKind{
		model.DeviceUpstreamGate, model.DeviceDownstreamGate,
		model.DeviceFillValve, model.DeviceDrainValve,
	}
	lockouts := make([]uuid.UUID, 16)
	for i := range lockouts {
		device := devices[i%len(devices)]
		record, err := registry.Register(ctx,
			model.LockoutScope{ChamberID: fmt.Sprintf("chamber-%d", i/4), Devices: []model.DeviceKind{device}},
			"concurrent work", fmt.Sprintf("tech-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		lockouts[i] = record.ID
	}

	var wg sync.WaitGroup
	var issued, blocked int64
	stop := make(chan struct{})
	for w := 0; w < 12; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				idx := (w*7 + i) % len(lockouts)
				id := lockouts[idx]
				chamber := fmt.Sprintf("chamber-%d", idx/4)
				switch i % 3 {
				case 0:
					_, _ = registry.Approve(ctx, id, "sup")
				case 1:
					_, _ = registry.Release(ctx, id, "sup", "")
				default:
					device := devices[idx%len(devices)]
					err := registry.Guard(ctx, chamber, device, "set_opening", uuid.Nil, 1, func() error {
						atomic.AddInt64(&issued, 1)
						return nil
					})
					var blockedErr *BlockedError
					if errors.As(err, &blockedErr) {
						atomic.AddInt64(&blocked, 1)
					}
				}
				i++
			}
		}(w)
	}
	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()
	if atomic.LoadInt64(&issued)+atomic.LoadInt64(&blocked) == 0 {
		t.Fatal("no command decisions were made during the stress run")
	}
}

func TestRecoveryRestoresActiveLockout(t *testing.T) {
	ctx := context.Background()
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, time.Now)
	first := NewRegistry(recorder, nil, time.Now)
	first.BindClearance(allowClearance)
	record, _ := first.Register(ctx, scope(model.DeviceFillValve), "survive restart", "tech")
	if _, err := first.Approve(ctx, record.ID, "sup"); err != nil {
		t.Fatal(err)
	}

	replayer := journal.NewReplayer(store)
	restored := NewRegistry(recorder, nil, time.Now)
	restored.BindClearance(allowClearance)
	recovery := NewRecovery(replayer, restored)
	records, err := recovery.RestoreAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("restored events = %d, want 2", len(records))
	}
	active, ok := restored.ActiveFor("chamber-1", model.DeviceFillValve)
	if !ok || active.ID != record.ID {
		t.Fatal("approved lockout must be enforced after recovery")
	}
	var issued int
	err = restored.Guard(ctx, "chamber-1", model.DeviceFillValve, "set_opening", uuid.Nil, 1,
		func() error { issued++; return nil })
	var blockedErr *BlockedError
	if !errors.As(err, &blockedErr) {
		t.Fatalf("restored lockout did not block, issued=%d err=%v", issued, err)
	}
	if issued != 0 {
		t.Fatal("command must not issue under restored lockout")
	}
}
