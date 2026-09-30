package lockout

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Recovery rebuilds lockout state from the per-chamber event journals.
type Recovery struct {
	replayer *journal.Replayer
	registry *Registry
}

func NewRecovery(replayer *journal.Replayer, registry *Registry) *Recovery {
	return &Recovery{replayer: replayer, registry: registry}
}

// RestoreAll replays every chamber partition and folds lockout events into the
// registry in journal order, so each work order reflects request→approve→release.
func (r *Recovery) RestoreAll(ctx context.Context) (map[uuid.UUID]model.MaintenanceLock, error) {
	states, err := r.replayer.ReplayAll(ctx)
	if err != nil {
		return nil, err
	}
	restored := make(map[uuid.UUID]model.MaintenanceLock)
	for chamberID, state := range states {
		for _, event := range state.Applied {
			if event.ChamberID != chamberID {
				return nil, fmt.Errorf("event partition mismatch chamber=%s event=%s", chamberID, event.ChamberID)
			}
			switch event.Type {
			case model.EventLockoutRequested, model.EventLockoutApproved, model.EventLockoutReleased:
				var lock model.MaintenanceLock
				if err := event.Decode(&lock); err != nil {
					return nil, err
				}
				if lock.ChamberID != chamberID {
					return nil, fmt.Errorf("lockout partition mismatch chamber=%s lockout=%s", chamberID, lock.ChamberID)
				}
				if !lock.Status.Valid() {
					return nil, fmt.Errorf("lockout %s replayed with invalid status %q", lock.ID, lock.Status)
				}
				restored[lock.ID] = lock
			}
		}
	}
	for _, lock := range restored {
		r.registry.Restore(lock)
	}
	return restored, nil
}
