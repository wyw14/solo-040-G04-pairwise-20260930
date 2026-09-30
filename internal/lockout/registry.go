package lockout

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// ErrDeviceLocked signals that a command targets equipment under an active
// maintenance lockout.
var ErrDeviceLocked = errors.New("device locked out for maintenance")

// ErrClearanceNotMet signals that release was refused because the standing
// water-level, opposite-gate or emergency checks did not pass.
var ErrClearanceNotMet = errors.New("maintenance lockout release clearance not met")

// BlockedError identifies the lockout that rejected a command.
type BlockedError struct {
	LockoutID uuid.UUID
	ChamberID string
	Device    model.DeviceKind
	Reason    string
}

func (e *BlockedError) Error() string {
	return fmt.Sprintf("%s %s blocked by maintenance lockout %s: %s",
		e.ChamberID, e.Device, e.LockoutID, e.Reason)
}

func (e *BlockedError) Unwrap() error { return ErrDeviceLocked }

// BlockedCommand is the payload of a command.blocked event.
type BlockedCommand struct {
	LockoutID  uuid.UUID        `json:"lockout_id"`
	ChamberID  string           `json:"chamber_id"`
	Device     model.DeviceKind `json:"device"`
	Action     string           `json:"action"`
	CycleID    uuid.UUID        `json:"cycle_id,omitempty"`
	Generation uint64           `json:"generation,omitempty"`
	Reason     string           `json:"reason"`
	BlockedAt  time.Time        `json:"blocked_at"`
}

// PendingCanceller neutralizes commands already queued for the covered
// devices when a lockout becomes active. It MUST NOT call back into the
// registry while invoked.
type PendingCanceller func(chamberID string, devices []model.DeviceKind, now time.Time)

// ClearanceCheck runs the standing release checks (water level, opposite
// gate, emergency state) before a lockout may be lifted.
type ClearanceCheck func(chamberID string, devices []model.DeviceKind) error

// Registry is the single authority for maintenance lockout state. Its mutex
// also serializes command authorization, so the "no active lockout" check and
// the downstream command issue happen as one atomic step.
type Registry struct {
	mu        sync.Mutex
	records   map[uuid.UUID]model.MaintenanceLockout
	recorder  *journal.Recorder
	cancel    PendingCanceller
	clearance ClearanceCheck
	now       func() time.Time
}

func NewRegistry(recorder *journal.Recorder, canceller PendingCanceller, now func() time.Time) *Registry {
	if now == nil {
		now = time.Now
	}
	return &Registry{
		records:  make(map[uuid.UUID]model.MaintenanceLockout),
		recorder: recorder, cancel: canceller, now: now,
	}
}

// BindClearance attaches the standing interlock checks evaluated on release.
func (r *Registry) BindClearance(check ClearanceCheck) {
	r.mu.Lock()
	r.clearance = check
	r.mu.Unlock()
}

// Register opens a lockout request. An overlapping non-terminal lockout
// (pending or active) is rejected so every covered device has one owner.
func (r *Registry) Register(ctx context.Context, scope model.LockoutScope, reason, requestedBy string) (model.MaintenanceLockout, error) {
	record, err := model.NewMaintenanceLockout(scope, reason, requestedBy, r.now())
	if err != nil {
		return model.MaintenanceLockout{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if conflict, ok := r.findOverlap(record.Scope, uuid.Nil); ok {
		return model.MaintenanceLockout{}, fmt.Errorf(
			"lockout %s (%s) already covers part of the requested equipment",
			conflict.ID, conflict.State)
	}
	r.records[record.ID] = record
	if err := r.recordLocked(ctx, model.EventLockoutRequested, record); err != nil {
		delete(r.records, record.ID)
		return model.MaintenanceLockout{}, err
	}
	return record.Clone(), nil
}

// Approve enforces a pending lockout and cancels queued commands for its
// equipment in the same critical section.
func (r *Registry) Approve(ctx context.Context, lockoutID uuid.UUID, operator string) (model.MaintenanceLockout, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, err := r.mustGet(lockoutID)
	if err != nil {
		return model.MaintenanceLockout{}, err
	}
	if err := record.Approve(operator, r.now()); err != nil {
		return model.MaintenanceLockout{}, err
	}
	if r.cancel != nil {
		r.cancel(record.Scope.ChamberID, record.Scope.Devices, r.now())
	}
	r.records[record.ID] = record
	if err := r.recordLocked(ctx, model.EventLockoutApproved, record); err != nil {
		return model.MaintenanceLockout{}, err
	}
	return record.Clone(), nil
}

// Reject denies a pending lockout request.
func (r *Registry) Reject(ctx context.Context, lockoutID uuid.UUID, operator, note string) (model.MaintenanceLockout, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, err := r.mustGet(lockoutID)
	if err != nil {
		return model.MaintenanceLockout{}, err
	}
	if err := record.Reject(operator, note, r.now()); err != nil {
		return model.MaintenanceLockout{}, err
	}
	r.records[record.ID] = record
	if err := r.recordLocked(ctx, model.EventLockoutRejected, record); err != nil {
		return model.MaintenanceLockout{}, err
	}
	return record.Clone(), nil
}

// Release lifts an active lockout only after the standing clearance checks
// (water level window, opposite gate sealed, no emergency) pass. Command
// authorization stays blocked until the state is committed, so a concurrent
// command can never observe the unlocked gap.
func (r *Registry) Release(ctx context.Context, lockoutID uuid.UUID, operator, note string) (model.MaintenanceLockout, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, err := r.mustGet(lockoutID)
	if err != nil {
		return model.MaintenanceLockout{}, err
	}
	if record.State != model.LockoutActive {
		return model.MaintenanceLockout{}, fmt.Errorf("lockout %s is %s, not active", record.ID, record.State)
	}
	if r.clearance == nil {
		return model.MaintenanceLockout{}, errors.New("release clearance checks are not configured")
	}
	if err := r.clearance(record.Scope.ChamberID, record.Scope.Devices); err != nil {
		return model.MaintenanceLockout{}, fmt.Errorf("cannot release lockout %s: %w",
			record.ID, errors.Join(ErrClearanceNotMet, err))
	}
	if err := record.MarkReleased(operator, note, r.now()); err != nil {
		return model.MaintenanceLockout{}, err
	}
	r.records[record.ID] = record
	if err := r.recordLocked(ctx, model.EventLockoutReleased, record); err != nil {
		return model.MaintenanceLockout{}, err
	}
	return record.Clone(), nil
}

// Guard runs issue only when the device is not covered by an active lockout.
// The lookup and issue share the registry mutex, so lockout approval/release
// cannot interleave with command issuance. A blocked attempt is written to the
// event stream and returned as *BlockedError.
func (r *Registry) Guard(ctx context.Context, chamberID string, device model.DeviceKind, action string, cycleID uuid.UUID, generation uint64, issue func() error) error {
	if issue == nil {
		return errors.New("lockout guard requires an issue function")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if active, ok := r.findActive(chamberID, device); ok {
		blocked := BlockedCommand{
			LockoutID: active.ID, ChamberID: chamberID, Device: device,
			Action: action, CycleID: cycleID, Generation: generation,
			Reason: active.Reason, BlockedAt: r.now().UTC(),
		}
		if err := r.recordLocked(ctx, model.EventCommandBlocked, blocked); err != nil {
			return err
		}
		return &BlockedError{
			LockoutID: active.ID, ChamberID: chamberID,
			Device: device, Reason: active.Reason,
		}
	}
	return issue()
}

// ActiveFor reports the active lockout covering a device, if any.
func (r *Registry) ActiveFor(chamberID string, device model.DeviceKind) (model.MaintenanceLockout, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.findActive(chamberID, device)
	return record.Clone(), ok
}

// Active lists every enforced lockout, optionally for one chamber.
func (r *Registry) Active(chamberID string) []model.MaintenanceLockout {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result []model.MaintenanceLockout
	for _, record := range r.records {
		if record.State != model.LockoutActive {
			continue
		}
		if chamberID != "" && record.Scope.ChamberID != chamberID {
			continue
		}
		result = append(result, record.Clone())
	}
	sortRecords(result)
	return result
}

// Get returns one lockout record.
func (r *Registry) Get(lockoutID uuid.UUID) (model.MaintenanceLockout, bool) {
	r.mu.Lock()
	record, ok := r.records[lockoutID]
	r.mu.Unlock()
	return record.Clone(), ok
}

// List returns all lockout records newest-first, optionally chamber-scoped.
func (r *Registry) List(chamberID string) []model.MaintenanceLockout {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]model.MaintenanceLockout, 0, len(r.records))
	for _, record := range r.records {
		if chamberID == "" || record.Scope.ChamberID == chamberID {
			result = append(result, record.Clone())
		}
	}
	sortRecords(result)
	return result
}

// Restore rebuilds non-terminal state from replayed events.
func (r *Registry) Restore(record model.MaintenanceLockout) {
	r.mu.Lock()
	if existing, ok := r.records[record.ID]; ok && existing.UpdatedAt.After(record.UpdatedAt) {
		r.mu.Unlock()
		return
	}
	r.records[record.ID] = record.Clone()
	r.mu.Unlock()
}

func (r *Registry) mustGet(lockoutID uuid.UUID) (model.MaintenanceLockout, error) {
	if lockoutID == uuid.Nil {
		return model.MaintenanceLockout{}, errors.New("lockout id is required")
	}
	record, ok := r.records[lockoutID]
	if !ok {
		return model.MaintenanceLockout{}, fmt.Errorf("lockout %s not found", lockoutID)
	}
	return record, nil
}

// findActive and friends run with r.mu held by the caller.
func (r *Registry) findActive(chamberID string, device model.DeviceKind) (model.MaintenanceLockout, bool) {
	for _, record := range r.records {
		if record.State == model.LockoutActive && record.Scope.Covers(device) &&
			record.Scope.ChamberID == chamberID {
			return record, true
		}
	}
	return model.MaintenanceLockout{}, false
}

func (r *Registry) findOverlap(scope model.LockoutScope, ignore uuid.UUID) (model.MaintenanceLockout, bool) {
	for _, record := range r.records {
		if record.ID == ignore || record.State.Terminal() {
			continue
		}
		if record.Scope.Overlaps(scope) {
			return record, true
		}
	}
	return model.MaintenanceLockout{}, false
}

func (r *Registry) recordLocked(ctx context.Context, kind model.EventType, payload any) error {
	chamberID := ""
	switch value := payload.(type) {
	case model.MaintenanceLockout:
		chamberID = value.Scope.ChamberID
	case BlockedCommand:
		chamberID = value.ChamberID
	}
	_, err := r.recorder.Record(ctx, chamberID, kind, uuid.Nil, 0, payload)
	return err
}

func sortRecords(records []model.MaintenanceLockout) {
	sort.Slice(records, func(i, j int) bool {
		if records[i].RequestedAt.Equal(records[j].RequestedAt) {
			return records[i].ID.String() < records[j].ID.String()
		}
		return records[i].RequestedAt.After(records[j].RequestedAt)
	})
}
