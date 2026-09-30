package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/lockout"
	"github.com/wyw14/cry-96/internal/model"
)

func openGateHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		direction := model.Direction(chi.URLParam(r, "direction"))
		if !direction.Valid() {
			writeError(w, http.StatusBadRequest, errors.New("invalid gate direction"))
			return
		}
		command, err := runtime.Interlocks.RequestOpen(r.Context(), mustCurrent(runtime, chamberID), direction, "http:"+requestID(r.Context()))
		if err != nil {
			writeError(w, commandFailureStatus(err), err)
			return
		}
		writeJSON(w, http.StatusAccepted, command)
	}
}

// commandFailureStatus maps maintenance lockouts to 423 Locked and leaves all
// other rejections at 409 Conflict.
func commandFailureStatus(err error) int {
	if errors.Is(err, lockout.ErrDeviceLocked) {
		return http.StatusLocked
	}
	return http.StatusConflict
}

func mustCurrent(runtime *Runtime, chamberID string) model.TransitCycle {
	cycleValue, _ := runtime.Cycles.Current(chamberID)
	return cycleValue
}

func commandAckHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		commandID, err := uuid.Parse(chi.URLParam(r, "commandID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		command, ok := runtime.GateCommands.Get(commandID)
		if !ok || command.ChamberID != chi.URLParam(r, "chamberID") {
			writeError(w, http.StatusNotFound, errors.New("command not found"))
			return
		}
		ack := model.DeviceAck{
			CommandID: command.ID, CycleID: command.CycleID, ChamberID: command.ChamberID,
			Generation: command.Generation, Epoch: command.Epoch, Device: command.Device,
			Settled: true, ObservedAt: runtime.Now().UTC(),
		}
		if err := runtime.Cycles.ApplyGateAck(r.Context(), ack); err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, ack)
	}
}

func emergencyHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		record, err := runtime.Safety.EmergencyStop(r.Context(), chi.URLParam(r, "chamberID"))
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, record)
	}
}
