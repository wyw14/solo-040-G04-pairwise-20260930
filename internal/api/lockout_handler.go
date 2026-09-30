package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-96/internal/lockout"
	"github.com/wyw14/cry-96/internal/model"
)

type registerLockoutInput struct {
	// Devices scopes the lockout to specific equipment; an empty list means
	// the whole chamber (both gates and both valves).
	Devices     []model.DeviceKind `json:"devices"`
	Reason      string             `json:"reason"`
	RequestedBy string             `json:"requested_by"`
}

type lockoutDecisionInput struct {
	Operator string `json:"operator"`
	Note     string `json:"note"`
}

func registerLockoutHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		var input registerLockoutInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		scope := model.LockoutScope{ChamberID: chamberID, Devices: input.Devices}
		record, err := runtime.Lockouts.Register(r.Context(), scope, input.Reason, input.RequestedBy)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusCreated, record)
	}
}

func chamberLockoutsHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, runtime.Lockouts.List(chi.URLParam(r, "chamberID")))
	}
}

func allLockoutsHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, runtime.Lockouts.List(r.URL.Query().Get("chamber_id")))
	}
}

// lockoutActionHandler handles approve, reject and release. Release failure
// from a failed standing check returns 412 rather than 409.
func lockoutActionHandler(runtime *Runtime, action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		lockoutID, err := parseUUID(chi.URLParam(r, "lockoutID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		var input lockoutDecisionInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		record, found := runtime.Lockouts.Get(lockoutID)
		if !found || record.Scope.ChamberID != chamberID {
			writeError(w, http.StatusNotFound, errors.New("lockout not found"))
			return
		}
		var result model.MaintenanceLockout
		switch action {
		case "approve":
			result, err = runtime.Lockouts.Approve(r.Context(), lockoutID, input.Operator)
		case "reject":
			result, err = runtime.Lockouts.Reject(r.Context(), lockoutID, input.Operator, input.Note)
		case "release":
			result, err = runtime.Lockouts.Release(r.Context(), lockoutID, input.Operator, input.Note)
		default:
			writeError(w, http.StatusBadRequest, errors.New("unknown lockout action"))
			return
		}
		if err != nil {
			status := http.StatusConflict
			if errors.Is(err, lockout.ErrClearanceNotMet) {
				status = http.StatusPreconditionFailed
			}
			writeError(w, status, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	}
}
