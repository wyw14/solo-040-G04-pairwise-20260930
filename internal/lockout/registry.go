package lockout

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

var (
	// ErrDeviceLocked wraps the shared model sentinel so callers can classify a
	// lockout refusal via errors.Is(err, model.ErrDeviceUnderMaintenance).
	ErrDeviceLocked = model.ErrDeviceUnderMaintenance
	// ErrLockNotFound indicates an unknown lockout identifier.
	ErrLockNotFound = errors.New("maintenance lockout not found")
	// ErrOverlappingLockout rejects a second open work order for the same device.
	ErrOverlappingLockout = errors.New("device already has an open maintenance lockout")
	// ErrInvalidTransition rejects lifecycle moves that are not allowed.
	ErrInvalidTransition = errors.New("invalid maintenance lockout transition")
	// ErrDevicesBusy refuses release while covered devices still have commands in flight.
	ErrDevicesBusy = errors.New("locked devices still have commands in flight")
)

// PendingCheck reports whether a device has non-terminal commands. It is the
// safety gate used before a lockout may be released.
type PendingCheck func(chamberID string, device model.DeviceKind) bool

// PersistFunc appends the event for a lockout transition. It is invoked while
// the registry mutex is held, so the durable event stream is linearized with
// every state change and every guarded command.
type PersistFunc func(ctx context.Context, kind model.EventType, chamberID string, payload any) error

// Registry is the authoritative store of maintenance lockouts. A single mutex
// guards registration, approval, release, event persistence and every
// device-command check, so a concurrent release can never open a window
// between the check and the command.
type Registry struct {
	mu      sync.Mutex
	locks   map[uuid.UUID]model.MaintenanceLock
	order   []uuid.UUID
	pending PendingCheck
	persist PersistFunc
}

func NewRegistry(pending PendingCheck) *Registry {
	return &Registry{locks: make(map[uuid.UUID]model.MaintenanceLock), pending: pending}
}

// SetPersist installs the journal callback used by every lifecycle transition.
func (r *Registry) SetPersist(persist PersistFunc) {
	r.mu.Lock()
	r.persist = persist
	r.mu.Unlock()
}

func (r *Registry) persistLocked(ctx context.Context, kind model.EventType, chamberID string, payload any) error {
	if r.persist == nil {
		return nil
	}
	return r.persist(ctx, kind, chamberID, payload)
}

// Request files a new (not-yet-approved) lockout. A device may only have one
// open lockout — requested or active — at a time.
func (r *Registry) Request(ctx context.Context, lock model.MaintenanceLock) error {
	if lock.ID == uuid.Nil || lock.ChamberID == "" || len(lock.Devices) == 0 {
		return errors.New("lockout requires id, chamber and devices")
	}
	if lock.Status != model.LockoutRequested {
		return errors.New("new lockout must start in requested state")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.locks[lock.ID]; exists {
		return fmt.Errorf("lockout %s already registered", lock.ID)
	}
	for _, device := range lock.Devices {
		if existing, ok := r.openForLocked(lock.ChamberID, device); ok {
			return fmt.Errorf("%w: chamber=%s device=%s lockout=%s",
				ErrOverlappingLockout, lock.ChamberID, device, existing.ID)
		}
	}
	r.locks[lock.ID] = lock.Clone()
	r.order = append(r.order, lock.ID)
	if err := r.persistLocked(ctx, model.EventLockoutRequested, lock.ChamberID, lock); err != nil {
		delete(r.locks, lock.ID)
		r.order = r.order[:len(r.order)-1]
		return err
	}
	return nil
}

// Approve moves a requested lockout into the active (blocking) state.
// Approval must be performed by someone other than the requester.
func (r *Registry) Approve(ctx context.Context, id uuid.UUID, approver string, now time.Time) (model.MaintenanceLock, error) {
	if approver == "" {
		return model.MaintenanceLock{}, errors.New("approver is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	lock, ok := r.locks[id]
	if !ok {
		return model.MaintenanceLock{}, fmt.Errorf("%w: %s", ErrLockNotFound, id)
	}
	if lock.Status != model.LockoutRequested {
		return model.MaintenanceLock{}, fmt.Errorf("%w: lockout %s is %s", ErrInvalidTransition, id, lock.Status)
	}
	if approver == lock.RequestedBy {
		return model.MaintenanceLock{}, errors.New("lockout approver must differ from the requesting technician")
	}
	for _, device := range lock.Devices {
		if existing, ok := r.openForLocked(lock.ChamberID, device); ok && existing.ID != id {
			return model.MaintenanceLock{}, fmt.Errorf("%w: chamber=%s device=%s lockout=%s",
				ErrOverlappingLockout, lock.ChamberID, device, existing.ID)
		}
	}
	previous := lock
	stamp := now.UTC()
	lock.Status = model.LockoutActive
	lock.ApprovedBy = approver
	lock.ApprovedAt = &stamp
	r.locks[id] = lock
	if err := r.persistLocked(ctx, model.EventLockoutApproved, lock.ChamberID, lock); err != nil {
		r.locks[id] = previous
		return model.MaintenanceLock{}, err
	}
	return lock.Clone(), nil
}

// Release lifts an active lockout. All covered devices must be quiescent:
// a command in flight keeps the lockout in force until it settles or cancels.
func (r *Registry) Release(ctx context.Context, id uuid.UUID, releasedBy, note string, now time.Time) (model.MaintenanceLock, error) {
	if releasedBy == "" {
		return model.MaintenanceLock{}, errors.New("releasing operator is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	lock, ok := r.locks[id]
	if !ok {
		return model.MaintenanceLock{}, fmt.Errorf("%w: %s", ErrLockNotFound, id)
	}
	if lock.Status != model.LockoutActive {
		return model.MaintenanceLock{}, fmt.Errorf("%w: lockout %s is %s", ErrInvalidTransition, id, lock.Status)
	}
	if r.pending != nil {
		for _, device := range lock.Devices {
			if r.pending(lock.ChamberID, device) {
				return model.MaintenanceLock{}, fmt.Errorf("%w: chamber=%s device=%s lockout=%s",
					ErrDevicesBusy, lock.ChamberID, device, id)
			}
		}
	}
	previous := lock
	stamp := now.UTC()
	lock.Status = model.LockoutReleased
	lock.ReleasedBy = releasedBy
	lock.ReleasedAt = &stamp
	lock.ReleaseNote = note
	r.locks[id] = lock
	if err := r.persistLocked(ctx, model.EventLockoutReleased, lock.ChamberID, lock); err != nil {
		r.locks[id] = previous
		return model.MaintenanceLock{}, err
	}
	return lock.Clone(), nil
}

// GuardDevice runs commit only when no active lockout covers the device. The
// check and commit share the registry critical section with Request/Approve/
// Release and their journal writes, so concurrent lockout changes cannot
// briefly admit a command. On rejection describeBlocked is invoked within the
// same critical section to produce the rejection event payload, which is
// persisted (under the same lock) before returning; commit is never called.
func (r *Registry) GuardDevice(
	ctx context.Context,
	chamberID string,
	device model.DeviceKind,
	commit func() error,
	describeBlocked func(model.MaintenanceLock) (model.EventType, any),
) (model.MaintenanceLock, error) {
	if commit == nil {
		return model.MaintenanceLock{}, errors.New("guard requires a commit function")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	lock, ok := r.activeForLocked(chamberID, device)
	if !ok {
		if err := commit(); err != nil {
			return model.MaintenanceLock{}, err
		}
		return model.MaintenanceLock{}, nil
	}
	if describeBlocked != nil {
		kind, payload := describeBlocked(lock)
		if err := r.persistLocked(ctx, kind, chamberID, payload); err != nil {
			return model.MaintenanceLock{}, err
		}
	}
	return lock.Clone(), fmt.Errorf("%w: chamber=%s device=%s lockout=%s reason=%q",
		ErrDeviceLocked, chamberID, device, lock.ID, lock.Reason)
}

// ActiveFor returns the active lockout covering a chamber device.
func (r *Registry) ActiveFor(chamberID string, device model.DeviceKind) (model.MaintenanceLock, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lock, ok := r.activeForLocked(chamberID, device)
	if !ok {
		return model.MaintenanceLock{}, false
	}
	return lock.Clone(), true
}

// Active returns every active lockout for a chamber (all chambers if empty).
func (r *Registry) Active(chamberID string) []model.MaintenanceLock {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result []model.MaintenanceLock
	for _, id := range r.order {
		lock := r.locks[id]
		if lock.Status == model.LockoutActive && (chamberID == "" || lock.ChamberID == chamberID) {
			result = append(result, lock.Clone())
		}
	}
	return result
}

// Get returns a single lockout by identifier.
func (r *Registry) Get(id uuid.UUID) (model.MaintenanceLock, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lock, ok := r.locks[id]
	return lock.Clone(), ok
}

// List returns all lockouts (newest first), optionally filtered by chamber.
func (r *Registry) List(chamberID string) []model.MaintenanceLock {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]model.MaintenanceLock, 0, len(r.order))
	for _, id := range r.order {
		lock := r.locks[id]
		if chamberID == "" || lock.ChamberID == chamberID {
			result = append(result, lock.Clone())
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].RequestedAt.After(result[j].RequestedAt) })
	return result
}

// Restore rebuilds registry state from replayed journal events.
func (r *Registry) Restore(lock model.MaintenanceLock) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.locks[lock.ID]; exists {
		return
	}
	r.locks[lock.ID] = lock.Clone()
	r.order = append(r.order, lock.ID)
}

func (r *Registry) activeForLocked(chamberID string, device model.DeviceKind) (model.MaintenanceLock, bool) {
	for _, id := range r.order {
		lock := r.locks[id]
		if lock.AppliesTo(chamberID, device) {
			return lock, true
		}
	}
	return model.MaintenanceLock{}, false
}

func (r *Registry) openForLocked(chamberID string, device model.DeviceKind) (model.MaintenanceLock, bool) {
	for _, id := range r.order {
		lock := r.locks[id]
		if lock.Status == model.LockoutReleased {
			continue
		}
		if lock.ChamberDeviceMatch(chamberID, device) {
			return lock, true
		}
	}
	return model.MaintenanceLock{}, false
}
