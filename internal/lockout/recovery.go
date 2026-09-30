package lockout

import (
	"context"
	"fmt"

	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Recovery rebuilds lockout state from the journal. Approved lockouts stay
// enforced across restarts; the full register/approve/release history is
// restored for the operator pages.
type Recovery struct {
	replayer *journal.Replayer
	registry *Registry
}

func NewRecovery(replayer *journal.Replayer, registry *Registry) *Recovery {
	return &Recovery{replayer: replayer, registry: registry}
}

func (r *Recovery) RestoreAll(ctx context.Context) ([]model.MaintenanceLockout, error) {
	states, err := r.replayer.ReplayAll(ctx)
	if err != nil {
		return nil, err
	}
	restored := make([]model.MaintenanceLockout, 0)
	for chamberID, state := range states {
		for _, event := range state.Applied {
			if !isLockoutEvent(event.Type) {
				continue
			}
			if event.ChamberID != chamberID {
				return nil, fmt.Errorf("lockout event partition mismatch chamber=%s event=%s", chamberID, event.ChamberID)
			}
			var record model.MaintenanceLockout
			if err := event.Decode(&record); err != nil {
				return nil, err
			}
			if record.Scope.ChamberID != chamberID {
				return nil, fmt.Errorf("lockout record partition mismatch chamber=%s record=%s", chamberID, record.Scope.ChamberID)
			}
			r.registry.Restore(record)
			restored = append(restored, record)
		}
	}
	return restored, nil
}

func isLockoutEvent(kind model.EventType) bool {
	switch kind {
	case model.EventLockoutRequested, model.EventLockoutApproved,
		model.EventLockoutRejected, model.EventLockoutReleased:
		return true
	default:
		return false
	}
}
