package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-96/internal/lockout"
	"github.com/wyw14/cry-96/internal/model"
)

type lockoutInput struct {
	ChamberID   string   `json:"chamber_id"`
	Devices     []string `json:"devices"`
	Reason      string   `json:"reason"`
	RequestedBy string   `json:"requested_by"`
}

type lockoutApproval struct {
	ApprovedBy string `json:"approved_by"`
}

type lockoutRelease struct {
	ReleasedBy string `json:"released_by"`
	Note       string `json:"note"`
}

// writeCommandError maps a command-path failure to HTTP. A maintenance lockout
// rejection is surfaced as 423 Locked so operators can distinguish it from an
// ordinary interlock conflict.
func writeCommandError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, model.ErrDeviceUnderMaintenance):
		writeError(w, http.StatusLocked, err)
	case errors.Is(err, lockout.ErrDevicesBusy):
		writeError(w, http.StatusConflict, err)
	default:
		writeError(w, http.StatusConflict, err)
	}
}

func (runtime *Runtime) validChamber(chamberID string) bool {
	for _, known := range runtime.Chambers {
		if known == chamberID {
			return true
		}
	}
	return false
}

func registerLockoutHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input lockoutInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if !runtime.validChamber(input.ChamberID) {
			writeError(w, http.StatusBadRequest, errors.New("unknown chamber"))
			return
		}
		if len(input.Devices) == 0 {
			writeError(w, http.StatusBadRequest, errors.New("at least one device is required"))
			return
		}
		devices := make([]model.DeviceKind, 0, len(input.Devices))
		for _, raw := range input.Devices {
			device, err := model.ParseDeviceKind(raw)
			if err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			devices = append(devices, device)
		}
		lock, err := runtime.Lockouts.Register(r.Context(), input.ChamberID, devices, input.Reason, input.RequestedBy)
		if err != nil {
			writeCommandError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, lock)
	}
}

func listLockoutsHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := r.URL.Query().Get("chamber_id")
		writeJSON(w, http.StatusOK, runtime.Lockouts.List(chamberID))
	}
}

func approveLockoutHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input lockoutApproval
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		id, err := parseUUID(chi.URLParam(r, "lockoutID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		lock, err := runtime.Lockouts.Approve(r.Context(), id, input.ApprovedBy)
		if err != nil {
			writeCommandError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, lock)
	}
}

func releaseLockoutHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input lockoutRelease
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		id, err := parseUUID(chi.URLParam(r, "lockoutID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		lock, err := runtime.Lockouts.Release(r.Context(), id, input.ReleasedBy, input.Note)
		if err != nil {
			writeCommandError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, lock)
	}
}
