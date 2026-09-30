package hydraulics

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

func valveRig(t *testing.T) (*Controller, *lockout.Registry, model.TransitCycle) {
	t.Helper()
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, time.Now)
	commands := gate.NewCommands()
	controller := NewController(commands, recorder, NewRamp(time.Millisecond, time.Now), time.Now)
	registry := lockout.NewRegistry(recorder,
		func(chamberID string, devices []model.DeviceKind, now time.Time) {
			commands.CancelDevices(chamberID, devices, now)
		}, time.Now)
	registry.BindClearance(func(string, []model.DeviceKind) error { return nil })
	controller.BindLockouts(registry)
	cycle, err := model.NewTransitCycle("chamber-1", uuid.New(), model.DirectionUpstream, 1, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return controller, registry, cycle
}

func approveLockout(t *testing.T, registry *lockout.Registry, devices ...model.DeviceKind) model.MaintenanceLockout {
	t.Helper()
	record, err := registry.Register(context.Background(),
		model.LockoutScope{ChamberID: "chamber-1", Devices: devices},
		"actuator service", "tech-lee")
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.Approve(context.Background(), record.ID, "supervisor-kim")
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestValveCommandsBlockedByLockout(t *testing.T) {
	ctx := context.Background()
	controller, registry, cycle := valveRig(t)
	approveLockout(t, registry, model.DeviceFillValve)

	if err := controller.IssueValve(ctx, cycle, 0.5); !errors.Is(err, lockout.ErrDeviceLocked) {
		t.Fatalf("fill valve error = %v, want ErrDeviceLocked", err)
	}
	// Negative target addresses the drain valve, which is not covered.
	if err := controller.IssueValve(ctx, cycle, -0.5); err != nil {
		t.Fatalf("unlocked drain valve blocked: %v", err)
	}
	// A ramp must abort at its first blocked step.
	_, err := controller.RampTo(ctx, cycle, []float64{0.4, 0.6})
	if !errors.Is(err, lockout.ErrDeviceLocked) {
		t.Fatalf("ramp error = %v, want ErrDeviceLocked", err)
	}
}

func TestValveCommandsResumeAfterRelease(t *testing.T) {
	ctx := context.Background()
	controller, registry, cycle := valveRig(t)
	record := approveLockout(t, registry, model.DeviceDrainValve)
	if err := controller.IssueValve(ctx, cycle, -0.2); !errors.Is(err, lockout.ErrDeviceLocked) {
		t.Fatalf("drain valve error = %v", err)
	}
	if _, err := registry.Release(ctx, record.ID, "sup", "checks complete"); err != nil {
		t.Fatal(err)
	}
	if err := controller.IssueValve(ctx, cycle, -0.2); err != nil {
		t.Fatalf("drain valve blocked after release: %v", err)
	}
}
