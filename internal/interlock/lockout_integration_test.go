package interlock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/lockout"
	"github.com/wyw14/cry-96/internal/model"
)

type rig struct {
	ctx       context.Context
	recorder  *journal.Recorder
	store     *journal.MemoryStore
	commands  *gate.Commands
	gates     *gate.Controller
	lockouts  *lockout.Registry
	coord     *Coordinator
	level     float64
	emergency bool
}

func newRig(t *testing.T) *rig {
	t.Helper()
	r := &rig{ctx: context.Background(), level: 6}
	r.store = journal.NewMemoryStore()
	r.recorder = journal.NewRecorder(r.store, time.Now)
	state := gate.NewState("chamber-1")
	r.commands = gate.NewCommands()
	r.gates = gate.NewController(state, r.commands, r.recorder, time.Now)
	r.lockouts = lockout.NewRegistry(r.recorder,
		func(chamberID string, devices []model.DeviceKind, now time.Time) {
			r.commands.CancelDevices(chamberID, devices, now)
		}, time.Now)
	coord := NewCoordinator(
		DefaultPolicy(), NewArbiter(), r.gates,
		func(string) float64 { return r.level },
		func(string) bool { return r.emergency },
		r.lockouts,
	)
	r.coord = coord
	r.lockouts.BindClearance(coord.Clearance)
	return r
}

func testCycle() model.TransitCycle {
	cycle, err := model.NewTransitCycle("chamber-1", uuid.New(), model.DirectionUpstream, 1, time.Now())
	if err != nil {
		panic(err)
	}
	return cycle
}

func activeLockout(t *testing.T, r *rig, devices ...model.DeviceKind) model.MaintenanceLockout {
	t.Helper()
	record, err := r.lockouts.Register(r.ctx,
		model.LockoutScope{ChamberID: "chamber-1", Devices: devices},
		"replace seal", "tech-lee")
	if err != nil {
		t.Fatal(err)
	}
	record, err = r.lockouts.Approve(r.ctx, record.ID, "supervisor-kim")
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestGateOpenBlockedByLockout(t *testing.T) {
	r := newRig(t)
	activeLockout(t, r, model.DeviceUpstreamGate)
	cycle := testCycle()

	if _, err := r.coord.RequestOpen(r.ctx, cycle, model.DirectionUpstream, "owner-1"); !errors.Is(err, lockout.ErrDeviceLocked) {
		t.Fatalf("open error = %v, want lockout.ErrDeviceLocked", err)
	}
	if _, reserved := r.coord.Reservation("chamber-1"); reserved {
		t.Fatal("blocked open must not reserve the chamber")
	}
	// Downstream gate is independent and still operable under standing policy.
	if _, err := r.coord.RequestOpen(r.ctx, cycle, model.DirectionDownstream, "owner-2"); err != nil {
		t.Fatalf("independent downstream gate blocked: %v", err)
	}
}

func TestGateCloseBlockedByLockout(t *testing.T) {
	r := newRig(t)
	activeLockout(t, r, model.DeviceDownstreamGate)
	if _, err := r.coord.RequestClose(r.ctx, testCycle(), model.DirectionDownstream); !errors.Is(err, lockout.ErrDeviceLocked) {
		t.Fatalf("close error = %v, want lockout.ErrDeviceLocked", err)
	}
}

// Releasing a lockout must restore normal operation but never bypass the
// standing water-level / opposite-gate / emergency interlocks.
func TestReleaseStillRequiresStandingChecks(t *testing.T) {
	r := newRig(t)
	record := activeLockout(t, r, model.DeviceUpstreamGate)

	// Unsafe level must block release itself.
	r.level = 14
	if _, err := r.lockouts.Release(r.ctx, record.ID, "sup", ""); !errors.Is(err, lockout.ErrClearanceNotMet) {
		t.Fatalf("release at unsafe level error = %v", err)
	}

	// Emergency active must also block release.
	r.level = 6
	r.emergency = true
	if _, err := r.lockouts.Release(r.ctx, record.ID, "sup", ""); !errors.Is(err, lockout.ErrClearanceNotMet) {
		t.Fatalf("release during emergency error = %v", err)
	}
	r.emergency = false

	// Open the opposite gate: release must be refused until it is sealed.
	if _, err := r.coord.RequestOpen(r.ctx, testCycle(), model.DirectionDownstream, "owner-ds"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.lockouts.Release(r.ctx, record.ID, "sup", ""); !errors.Is(err, lockout.ErrClearanceNotMet) {
		t.Fatalf("release with open opposite gate error = %v", err)
	}
	if err := r.coord.Observe("chamber-1", model.DirectionDownstream, model.GateOpen, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.lockouts.Release(r.ctx, record.ID, "sup", ""); !errors.Is(err, lockout.ErrClearanceNotMet) {
		t.Fatalf("release with opposite gate observed open error = %v", err)
	}
	if err := r.coord.Observe("chamber-1", model.DirectionDownstream, model.GateClosed, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := r.coord.Release("chamber-1", "owner-ds"); err != nil {
		t.Fatal(err)
	}

	released, err := r.lockouts.Release(r.ctx, record.ID, "sup", "field clear")
	if err != nil {
		t.Fatalf("release with all checks met: %v", err)
	}
	if released.State != model.LockoutReleased {
		t.Fatalf("state = %s", released.State)
	}

	// After release the full policy applies again: an unsafe level blocks the
	// gate open even though the maintenance lockout is gone.
	r.level = 20
	if _, err := r.coord.RequestOpen(r.ctx, testCycle(), model.DirectionUpstream, "owner-1"); err == nil {
		t.Fatal("gate opened outside safe water-level window after lockout release")
	}
}

func TestChamberWideLockoutBlocksBothGates(t *testing.T) {
	r := newRig(t)
	activeLockout(t, r) // empty device list => whole chamber
	for _, direction := range []model.Direction{model.DirectionUpstream, model.DirectionDownstream} {
		if _, err := r.coord.RequestOpen(r.ctx, testCycle(), direction, "owner"); !errors.Is(err, lockout.ErrDeviceLocked) {
			t.Fatalf("%s open error = %v", direction, err)
		}
	}
}
