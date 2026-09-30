package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-96/internal/model"
)

type rampInput struct {
	Targets []float64 `json:"targets"`
}

func valveRampHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input rampInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if len(input.Targets) == 0 || len(input.Targets) > 12 {
			writeError(w, http.StatusBadRequest, errors.New("targets must contain 1 to 12 steps"))
			return
		}
		chamberID := chi.URLParam(r, "chamberID")
		cycleValue, ok := runtime.Cycles.Current(chamberID)
		if !ok {
			writeError(w, http.StatusConflict, errors.New("active transit not found"))
			return
		}
		result, err := runtime.Hydraulics.RampTo(r.Context(), cycleValue, input.Targets)
		if err != nil {
			if errors.Is(err, model.ErrDeviceUnderMaintenance) {
				writeError(w, http.StatusLocked, err)
				return
			}
			writeError(w, http.StatusRequestTimeout, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	}
}
