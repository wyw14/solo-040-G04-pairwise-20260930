package model

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// LockoutState is the lifecycle stage of a maintenance lockout.
type LockoutState string

const (
	LockoutPending  LockoutState = "pending"  // registered, awaiting approval
	LockoutActive   LockoutState = "active"   // approved and enforced
	LockoutRejected LockoutState = "rejected" // approval denied
	LockoutReleased LockoutState = "released" // cleared after release checks
)

func (s LockoutState) Terminal() bool {
	return s == LockoutReleased || s == LockoutRejected
}

// LockoutScope selects what a maintenance lockout covers. Empty devices means
// the whole chamber (every gate and valve).
type LockoutScope struct {
	ChamberID string       `json:"chamber_id"`
	Devices   []DeviceKind `json:"devices,omitempty"`
}

func (s LockoutScope) Covers(device DeviceKind) bool {
	if s.ChamberID == "" {
		return false
	}
	if len(s.Devices) == 0 {
		return true
	}
	for _, covered := range s.Devices {
		if covered == device {
			return true
		}
	}
	return false
}

func (s LockoutScope) Overlaps(other LockoutScope) bool {
	if s.ChamberID != other.ChamberID {
		return false
	}
	if len(s.Devices) == 0 || len(other.Devices) == 0 {
		return true
	}
	for _, device := range s.Devices {
		if other.Covers(device) {
			return true
		}
	}
	return false
}

func (s LockoutScope) Clone() LockoutScope {
	s.Devices = append([]DeviceKind(nil), s.Devices...)
	return s
}

// MaintenanceLockout is the formal record of a field-maintenance isolation.
type MaintenanceLockout struct {
	ID          uuid.UUID    `json:"id"`
	Scope       LockoutScope `json:"scope"`
	State       LockoutState `json:"state"`
	Reason      string       `json:"reason"`
	RequestedBy string       `json:"requested_by"`
	ApprovedBy  string       `json:"approved_by,omitempty"`
	ReleasedBy  string       `json:"released_by,omitempty"`
	ReleaseNote string       `json:"release_note,omitempty"`
	RequestedAt time.Time    `json:"requested_at"`
	ApprovedAt  *time.Time   `json:"approved_at,omitempty"`
	ReleasedAt  *time.Time   `json:"released_at,omitempty"`
	UpdatedAt   time.Time    `json:"updated_at"`
}

// validLockoutDevices is the equipment that maintenance lockouts can cover.
var validLockoutDevices = []DeviceKind{
	DeviceUpstreamGate, DeviceDownstreamGate, DeviceFillValve, DeviceDrainValve,
}

func NewMaintenanceLockout(scope LockoutScope, reason, requestedBy string, now time.Time) (MaintenanceLockout, error) {
	if scope.ChamberID == "" {
		return MaintenanceLockout{}, errors.New("lockout requires a chamber")
	}
	if reason == "" {
		return MaintenanceLockout{}, errors.New("lockout reason is required")
	}
	if requestedBy == "" {
		return MaintenanceLockout{}, errors.New("lockout requesting operator is required")
	}
	seen := make(map[DeviceKind]struct{}, len(scope.Devices))
	for _, device := range scope.Devices {
		valid := false
		for _, allowed := range validLockoutDevices {
			if device == allowed {
				valid = true
				break
			}
		}
		if !valid {
			return MaintenanceLockout{}, fmt.Errorf("lockout device %q is not lockable equipment", device)
		}
		if _, dup := seen[device]; dup {
			return MaintenanceLockout{}, fmt.Errorf("lockout device %q listed more than once", device)
		}
		seen[device] = struct{}{}
	}
	stamp := now.UTC()
	return MaintenanceLockout{
		ID: uuid.New(), Scope: scope.Clone(), State: LockoutPending,
		Reason: reason, RequestedBy: requestedBy,
		RequestedAt: stamp, UpdatedAt: stamp,
	}, nil
}

func (l *MaintenanceLockout) Approve(operator string, now time.Time) error {
	if l.State != LockoutPending {
		return fmt.Errorf("lockout %s is %s, not pending approval", l.ID, l.State)
	}
	if operator == "" {
		return errors.New("approving operator is required")
	}
	stamp := now.UTC()
	l.State = LockoutActive
	l.ApprovedBy = operator
	l.ApprovedAt = &stamp
	l.UpdatedAt = stamp
	return nil
}

func (l *MaintenanceLockout) Reject(operator, note string, now time.Time) error {
	if l.State != LockoutPending {
		return fmt.Errorf("lockout %s is %s, not pending approval", l.ID, l.State)
	}
	if operator == "" {
		return errors.New("rejecting operator is required")
	}
	l.State = LockoutRejected
	l.ApprovedBy = operator
	l.ReleaseNote = note
	l.UpdatedAt = now.UTC()
	return nil
}

func (l *MaintenanceLockout) MarkReleased(operator, note string, now time.Time) error {
	if l.State != LockoutActive {
		return fmt.Errorf("lockout %s is %s, not active", l.ID, l.State)
	}
	if operator == "" {
		return errors.New("releasing operator is required")
	}
	stamp := now.UTC()
	l.State = LockoutReleased
	l.ReleasedBy = operator
	l.ReleaseNote = note
	l.ReleasedAt = &stamp
	l.UpdatedAt = stamp
	return nil
}

func (l MaintenanceLockout) Clone() MaintenanceLockout {
	l.Scope = l.Scope.Clone()
	return l
}
