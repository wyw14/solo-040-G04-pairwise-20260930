package interlock

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/model"
)

// LockoutAuthority atomically authorizes command issuance against active
// maintenance lockouts. It is implemented by *lockout.Registry; the interface
// keeps this package decoupled from the lockout package.
type LockoutAuthority interface {
	Guard(ctx context.Context, chamberID string, device model.DeviceKind, action string,
		cycleID uuid.UUID, generation uint64, issue func() error) error
}

// Coordinator keeps policy evaluation and command reservation in one flow.
type Coordinator struct {
	policy    Policy
	arbiter   *Arbiter
	gates     *gate.Controller
	level     func(string) float64
	emergency func(string) bool
	lockouts  LockoutAuthority
}

func NewCoordinator(policy Policy, arbiter *Arbiter, gates *gate.Controller, level func(string) float64, emergency func(string) bool, lockouts LockoutAuthority) *Coordinator {
	return &Coordinator{
		policy: policy, arbiter: arbiter, gates: gates,
		level: level, emergency: emergency, lockouts: lockouts,
	}
}

func (c *Coordinator) Snapshot(chamberID string) EquipmentSnapshot {
	level := 0.0
	if c.level != nil {
		level = c.level(chamberID)
	}
	emergency := false
	if c.emergency != nil {
		emergency = c.emergency(chamberID)
	}
	return EquipmentSnapshot{
		ChamberID: chamberID, Gates: c.gates.State().Snapshot(chamberID),
		LevelM: level, Emergency: emergency,
	}
}

// RequestOpen validates the standing interlocks, then atomically reserves and
// issues the gate-open command. Policy evaluation, reservation and issuance
// run inside the lockout authority's critical section, so an approved or
// released lockout can never create a window where a command slips through.
func (c *Coordinator) RequestOpen(ctx context.Context, cycle model.TransitCycle, direction model.Direction, owner string) (model.DeviceCommand, error) {
	device := gateDevice(direction)
	var command model.DeviceCommand
	issue := func() error {
		if err := c.policy.ValidateOpen(c.Snapshot(cycle.ChamberID), direction); err != nil {
			return err
		}
		_, err := c.arbiter.Reserve(cycle.ChamberID, direction, owner, func() error {
			issued, issueErr := c.gates.Request(ctx, cycle, direction, true)
			command = issued
			return issueErr
		})
		return err
	}
	if err := c.guard(ctx, cycle, device, "open", issue); err != nil {
		return model.DeviceCommand{}, err
	}
	return command, nil
}

func (c *Coordinator) RequestClose(ctx context.Context, cycle model.TransitCycle, direction model.Direction) (model.DeviceCommand, error) {
	device := gateDevice(direction)
	var command model.DeviceCommand
	issue := func() error {
		if err := c.policy.ValidateClose(c.Snapshot(cycle.ChamberID), direction); err != nil {
			return err
		}
		issued, err := c.gates.Request(ctx, cycle, direction, false)
		command = issued
		return err
	}
	if err := c.guard(ctx, cycle, device, "close", issue); err != nil {
		return model.DeviceCommand{}, err
	}
	return command, nil
}

func (c *Coordinator) guard(ctx context.Context, cycle model.TransitCycle, device model.DeviceKind, action string, issue func() error) error {
	if c.lockouts == nil {
		return issue()
	}
	return c.lockouts.Guard(ctx, cycle.ChamberID, device, action, cycle.ID, cycle.Generation, issue)
}

// Clearance is the standing safety gate evaluated before a maintenance
// lockout may be released: no emergency, water level inside the safe window,
// and every covered gate's opposite gate sealed. Passing clearance does not
// skip the same checks on later commands — RequestOpen still runs the full
// policy.
func (c *Coordinator) Clearance(chamberID string, devices []model.DeviceKind) error {
	snapshot := c.Snapshot(chamberID)
	if snapshot.Emergency {
		return fmt.Errorf("interlock emergency active chamber=%s", chamberID)
	}
	if snapshot.LevelM < c.policy.MinimumLevel || snapshot.LevelM > c.policy.MaximumLevel {
		return fmt.Errorf("water level %.2f outside safe window", snapshot.LevelM)
	}
	directions := make([]model.Direction, 0, 2)
	if len(devices) == 0 {
		directions = append(directions, model.DirectionUpstream, model.DirectionDownstream)
	} else {
		for _, device := range devices {
			if direction, ok := gateDirection(device); ok {
				directions = append(directions, direction)
			}
		}
	}
	for _, direction := range directions {
		if opposite := snapshot.Gates[direction.Opposite()]; opposite != model.GateClosed {
			return fmt.Errorf("opposite %s gate is %s", direction.Opposite(), opposite)
		}
	}
	return nil
}

func gateDevice(direction model.Direction) model.DeviceKind {
	if direction == model.DirectionDownstream {
		return model.DeviceDownstreamGate
	}
	return model.DeviceUpstreamGate
}

func gateDirection(device model.DeviceKind) (model.Direction, bool) {
	switch device {
	case model.DeviceUpstreamGate:
		return model.DirectionUpstream, true
	case model.DeviceDownstreamGate:
		return model.DirectionDownstream, true
	default:
		return "", false
	}
}

func (c *Coordinator) Release(chamberID, owner string) error {
	if !c.arbiter.Release(chamberID, owner) {
		return fmt.Errorf("reservation owner %q does not control chamber %s", owner, chamberID)
	}
	return nil
}

func (c *Coordinator) Reservation(chamberID string) (Reservation, bool) {
	return c.arbiter.Current(chamberID)
}

func (c *Coordinator) Observe(chamberID string, direction model.Direction, position model.GatePosition, _ time.Time) error {
	return c.gates.Observe(chamberID, direction, position)
}
