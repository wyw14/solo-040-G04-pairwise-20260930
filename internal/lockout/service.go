package lockout

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Service turns registry transitions into journaled events and provides the
// atomic guard used by the gate and valve command paths.
type Service struct {
	registry *Registry
	recorder *journal.Recorder
	now      func() time.Time
}

func NewService(registry *Registry, recorder *journal.Recorder, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	service := &Service{registry: registry, recorder: recorder, now: now}
	// Persistence runs inside the registry critical section, so lifecycle
	// events and guarded command events share one total order.
	registry.SetPersist(func(ctx context.Context, kind model.EventType, chamberID string, payload any) error {
		_, err := recorder.Record(ctx, chamberID, kind, uuid.Nil, 0, payload)
		return err
	})
	return service
}

// Registry exposes the registry for read-only queries and recovery.
func (s *Service) Registry() *Registry { return s.registry }

// Register files a maintenance lockout. It is not blocking until approved.
func (s *Service) Register(ctx context.Context, chamberID string, devices []model.DeviceKind, reason, requestedBy string) (model.MaintenanceLock, error) {
	lock, err := model.NewMaintenanceLock(chamberID, devices, reason, requestedBy, s.now())
	if err != nil {
		return model.MaintenanceLock{}, err
	}
	if err := s.registry.Request(ctx, lock); err != nil {
		return model.MaintenanceLock{}, err
	}
	return lock, nil
}

// Approve puts a registered lockout in force. Separation of duties requires
// the approver to differ from the requesting technician.
func (s *Service) Approve(ctx context.Context, id uuid.UUID, approver string) (model.MaintenanceLock, error) {
	return s.registry.Approve(ctx, id, approver, s.now())
}

// Release lifts a lockout after covered devices are quiescent. The original
// water-level, opposite-gate and emergency interlocks still apply to the next
// command; releasing a lockout never bypasses them.
func (s *Service) Release(ctx context.Context, id uuid.UUID, releasedBy, note string) (model.MaintenanceLock, error) {
	return s.registry.Release(ctx, id, releasedBy, note, s.now())
}

// Guard runs issue only when no active lockout covers the device. When blocked,
// the lockout reason is written to the event stream and ErrDeviceLocked is
// returned. The check, the rejection event and the issue are all serialized
// against approval/release under the registry mutex, so a concurrent approval
// or release can never briefly admit or reject the wrong command.
func (s *Service) Guard(ctx context.Context, chamberID string, device model.DeviceKind, action, source string, issue func() error) error {
	describeBlocked := func(lock model.MaintenanceLock) (model.EventType, any) {
		return model.EventCommandBlocked, model.BlockedCommand{
			ChamberID: chamberID, Device: device, Action: action,
			LockoutID: lock.ID, Reason: lock.Reason, Source: source,
			BlockedAt: s.now().UTC(),
		}
	}
	_, err := s.registry.GuardDevice(ctx, chamberID, device, issue, describeBlocked)
	return err
}

func (s *Service) Active(chamberID string) []model.MaintenanceLock {
	return s.registry.Active(chamberID)
}

func (s *Service) List(chamberID string) []model.MaintenanceLock {
	return s.registry.List(chamberID)
}

func (s *Service) Get(id uuid.UUID) (model.MaintenanceLock, bool) {
	return s.registry.Get(id)
}
