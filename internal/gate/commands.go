package gate

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// Commands owns command identity, lifecycle and late-ack classification.
type Commands struct {
	mu        sync.RWMutex
	byID      map[uuid.UUID]model.DeviceCommand
	byChamber map[string][]uuid.UUID
	nextEpoch map[string]uint64
}

func NewCommands() *Commands {
	return &Commands{
		byID:      make(map[uuid.UUID]model.DeviceCommand),
		byChamber: make(map[string][]uuid.UUID),
		nextEpoch: make(map[string]uint64),
	}
}

func epochKey(chamberID string, device model.DeviceKind) string {
	return chamberID + ":" + string(device)
}

func (c *Commands) Issue(cycle model.TransitCycle, device model.DeviceKind, action string, target float64, now time.Time) (model.DeviceCommand, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := epochKey(cycle.ChamberID, device)
	c.nextEpoch[key]++
	command, err := model.NewDeviceCommand(cycle, c.nextEpoch[key], device, action, target, now)
	if err != nil {
		return model.DeviceCommand{}, err
	}
	c.byID[command.ID] = command
	c.byChamber[cycle.ChamberID] = append(c.byChamber[cycle.ChamberID], command.ID)
	return command, nil
}

func (c *Commands) Accept(commandID uuid.UUID, now time.Time) error {
	return c.transition(commandID, model.CommandAccepted, now)
}

func (c *Commands) Execute(commandID uuid.UUID, now time.Time) error {
	return c.transition(commandID, model.CommandExecuting, now)
}

func (c *Commands) transition(commandID uuid.UUID, state model.CommandState, now time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	command, ok := c.byID[commandID]
	if !ok {
		return fmt.Errorf("unknown command %s", commandID)
	}
	if err := command.SetState(state, now); err != nil {
		return err
	}
	c.byID[commandID] = command
	return nil
}

func (c *Commands) Settle(ack model.DeviceAck, now time.Time) (model.DeviceCommand, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	command, ok := c.byID[ack.CommandID]
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("unknown command %s", ack.CommandID)
	}
	if !ack.Matches(command) {
		return model.DeviceCommand{}, errors.New("ack identity does not match command generation")
	}
	if !ack.Settled {
		return model.DeviceCommand{}, errors.New("device has not settled")
	}
	if err := command.SetState(model.CommandSettled, now); err != nil {
		return model.DeviceCommand{}, err
	}
	c.byID[command.ID] = command
	return command, nil
}

func (c *Commands) CancelChamber(chamberID string, now time.Time) []model.DeviceCommand {
	c.mu.Lock()
	defer c.mu.Unlock()
	var cancelled []model.DeviceCommand
	for _, commandID := range c.byChamber[chamberID] {
		command := c.byID[commandID]
		if command.State.Terminal() {
			continue
		}
		if command.SetState(model.CommandCancelled, now) == nil {
			c.byID[commandID] = command
			cancelled = append(cancelled, command)
		}
	}
	return cancelled
}

func (c *Commands) Pending(chamberID string) []model.DeviceCommand {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var pending []model.DeviceCommand
	for _, commandID := range c.byChamber[chamberID] {
		command := c.byID[commandID]
		if !command.State.Terminal() {
			pending = append(pending, command)
		}
	}
	return pending
}

// HasPendingDevice reports whether a specific device has a non-terminal command.
// It gates maintenance-lockout release: a locked device must be quiescent first.
func (c *Commands) HasPendingDevice(chamberID string, device model.DeviceKind) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, commandID := range c.byChamber[chamberID] {
		command := c.byID[commandID]
		if command.Device == device && !command.State.Terminal() {
			return true
		}
	}
	return false
}

func (c *Commands) Get(commandID uuid.UUID) (model.DeviceCommand, bool) {
	c.mu.RLock()
	command, ok := c.byID[commandID]
	c.mu.RUnlock()
	return command, ok
}
