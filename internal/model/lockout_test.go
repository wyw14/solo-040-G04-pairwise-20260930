package model

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func fixedClock() (func() time.Time, *time.Time) {
	stamp := time.Date(2026, 9, 30, 9, 0, 0, 0, time.UTC)
	return func() time.Time { return stamp }, &stamp
}

func TestLockoutScopeOverlap(t *testing.T) {
	chamber := LockoutScope{ChamberID: "c1", Devices: []DeviceKind{DeviceUpstreamGate}}
	cases := []struct {
		name string
		a, b LockoutScope
		want bool
	}{
		{"same device", chamber, LockoutScope{ChamberID: "c1", Devices: []DeviceKind{DeviceUpstreamGate}}, true},
		{"independent devices", chamber, LockoutScope{ChamberID: "c1", Devices: []DeviceKind{DeviceFillValve}}, false},
		{"chamber-wide overlaps device", LockoutScope{ChamberID: "c1"}, chamber, true},
		{"both chamber-wide", LockoutScope{ChamberID: "c1"}, LockoutScope{ChamberID: "c1"}, true},
		{"different chambers", chamber, LockoutScope{ChamberID: "c2", Devices: []DeviceKind{DeviceUpstreamGate}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Overlaps(tc.b); got != tc.want {
				t.Fatalf("Overlaps() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestLockoutLifecycleTransitions(t *testing.T) {
	now, stamp := fixedClock()
	record, err := NewMaintenanceLockout(
		LockoutScope{ChamberID: "c1", Devices: []DeviceKind{DeviceDrainValve}},
		"inspect drain actuator", "tech-lee", now())
	if err != nil {
		t.Fatal(err)
	}
	if record.State != LockoutPending || record.ID == uuid.Nil {
		t.Fatalf("new lockout not initialized: %+v", record)
	}
	if err := record.Approve("supervisor-kim", now()); err != nil {
		t.Fatal(err)
	}
	if record.State != LockoutActive || record.ApprovedBy != "supervisor-kim" || record.ApprovedAt == nil {
		t.Fatalf("approval not recorded: %+v", record)
	}
	*stamp = stamp.Add(time.Minute)
	if err := record.MarkReleased("supervisor-kim", "checks passed", now()); err != nil {
		t.Fatal(err)
	}
	if record.State != LockoutReleased || record.ReleasedAt == nil {
		t.Fatalf("release not recorded: %+v", record)
	}
	if err := record.Approve("x", now()); err == nil {
		t.Fatal("released lockout must not be re-approvable")
	}
}

func TestNewMaintenanceLockoutValidation(t *testing.T) {
	now, _ := fixedClock()
	cases := []struct {
		name     string
		scope    LockoutScope
		reason   string
		operator string
	}{
		{"missing chamber", LockoutScope{}, "reason", "op"},
		{"missing reason", LockoutScope{ChamberID: "c1"}, "", "op"},
		{"missing operator", LockoutScope{ChamberID: "c1"}, "reason", ""},
		{"unknown device", LockoutScope{ChamberID: "c1", Devices: []DeviceKind{"pump"}}, "r", "op"},
		{"duplicate device", LockoutScope{ChamberID: "c1", Devices: []DeviceKind{DeviceFillValve, DeviceFillValve}}, "r", "op"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewMaintenanceLockout(tc.scope, tc.reason, tc.operator, now()); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
