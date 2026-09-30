package model

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// DeviceGuard atomically checks maintenance lockouts for a chamber device and
// runs issue only when the device is free. It is implemented by lockout.Service
// and injected into the command controllers, keeping the check and command
// issuance in one critical section. On rejection it records the reason.
type DeviceGuard func(ctx context.Context, chamberID string, device DeviceKind, action, source string, issue func() error) error

// ErrDeviceUnderMaintenance is the canonical rejection shared by the lockout
// registry and the command controllers, so HTTP handlers can classify a
// lockout refusal via errors.Is without cross-package coupling.
var ErrDeviceUnderMaintenance = errors.New("device is under maintenance lockout")

// LockoutStatus is the lifecycle state of a maintenance lockout.
type LockoutStatus string

const (
	LockoutRequested LockoutStatus = "requested" // 登记，待审批
	LockoutActive    LockoutStatus = "active"    // 审批通过，设备被封锁
	LockoutReleased  LockoutStatus = "released"  // 已解除（保留历史）
)

func (s LockoutStatus) Valid() bool {
	return s == LockoutRequested || s == LockoutActive || s == LockoutReleased
}

// ParseDeviceKind parses an external device identifier.
func ParseDeviceKind(value string) (DeviceKind, error) {
	switch strings.TrimSpace(value) {
	case string(DeviceUpstreamGate):
		return DeviceUpstreamGate, nil
	case string(DeviceDownstreamGate):
		return DeviceDownstreamGate, nil
	case string(DeviceFillValve):
		return DeviceFillValve, nil
	case string(DeviceDrainValve):
		return DeviceDrainValve, nil
	default:
		return "", fmt.Errorf("unknown device %q", value)
	}
}

// MaintenanceLock is registered per chamber and device set, then approved and
// eventually released. While active it blocks every related actuator command.
type MaintenanceLock struct {
	ID          uuid.UUID     `json:"id"`
	ChamberID   string        `json:"chamber_id"`
	Devices     []DeviceKind  `json:"devices"`
	Status      LockoutStatus `json:"status"`
	Reason      string        `json:"reason"`
	RequestedBy string        `json:"requested_by"`
	ApprovedBy  string        `json:"approved_by,omitempty"`
	ReleasedBy  string        `json:"released_by,omitempty"`
	RequestedAt time.Time     `json:"requested_at"`
	ApprovedAt  *time.Time    `json:"approved_at,omitempty"`
	ReleasedAt  *time.Time    `json:"released_at,omitempty"`
	ReleaseNote string        `json:"release_note,omitempty"`
}

func NewMaintenanceLock(chamberID string, devices []DeviceKind, reason, requestedBy string, now time.Time) (MaintenanceLock, error) {
	if chamberID == "" {
		return MaintenanceLock{}, errors.New("lockout requires a chamber")
	}
	if len(devices) == 0 {
		return MaintenanceLock{}, errors.New("lockout requires at least one device")
	}
	seen := make(map[DeviceKind]struct{}, len(devices))
	for _, device := range devices {
		if _, err := ParseDeviceKind(string(device)); err != nil {
			return MaintenanceLock{}, err
		}
		seen[device] = struct{}{}
	}
	if len(seen) != len(devices) {
		return MaintenanceLock{}, errors.New("lockout devices must be unique")
	}
	if strings.TrimSpace(reason) == "" {
		return MaintenanceLock{}, errors.New("lockout requires a reason")
	}
	if strings.TrimSpace(requestedBy) == "" {
		return MaintenanceLock{}, errors.New("lockout requires a requesting technician")
	}
	return MaintenanceLock{
		ID: uuid.New(), ChamberID: chamberID,
		Devices: append([]DeviceKind(nil), devices...),
		Status:  LockoutRequested, Reason: strings.TrimSpace(reason),
		RequestedBy: strings.TrimSpace(requestedBy), RequestedAt: now.UTC(),
	}, nil
}

// Covers reports whether the lockout's device set contains the given device.
func (l MaintenanceLock) Covers(device DeviceKind) bool {
	for _, candidate := range l.Devices {
		if candidate == device {
			return true
		}
	}
	return false
}

// AppliesTo reports whether this is an active lockout for the given chamber/device.
func (l MaintenanceLock) AppliesTo(chamberID string, device DeviceKind) bool {
	return l.Status == LockoutActive && l.ChamberDeviceMatch(chamberID, device)
}

func (l MaintenanceLock) ChamberDeviceMatch(chamberID string, device DeviceKind) bool {
	return l.ChamberID == chamberID && l.Covers(device)
}

func (l MaintenanceLock) Clone() MaintenanceLock {
	l.Devices = append([]DeviceKind(nil), l.Devices...)
	return l
}

// BlockedCommand is recorded when a lockout rejects an actuator command.
type BlockedCommand struct {
	ChamberID string     `json:"chamber_id"`
	Device    DeviceKind `json:"device"`
	Action    string     `json:"action"`
	LockoutID uuid.UUID  `json:"lockout_id"`
	Reason    string     `json:"reason"`
	Source    string     `json:"source,omitempty"`
	BlockedAt time.Time  `json:"blocked_at"`
}
