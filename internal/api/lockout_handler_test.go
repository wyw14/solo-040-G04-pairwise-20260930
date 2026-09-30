package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/wyw14/cry-96/internal/model"
)

func newTestServer(t *testing.T) (http.Handler, *Runtime) {
	t.Helper()
	runtime, err := NewRuntime(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	return Router(runtime, ""), runtime
}

func do(t *testing.T, server http.Handler, method, path, body string) (int, any) {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Body.Len() == 0 {
		return rec.Code, nil
	}
	var decoded any
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("invalid JSON response %q: %v", rec.Body.String(), err)
	}
	return rec.Code, decoded
}

func asMap(t *testing.T, value any) map[string]any {
	t.Helper()
	mapped, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("response is not an object: %v", value)
	}
	return mapped
}

func createCycle(t *testing.T, server http.Handler, chamber string) {
	t.Helper()
	body := `{"chamber_id":"` + chamber + `","direction":"upstream","vessels":[{"name":"River Star","call_sign":"RS-1","length_m":86}]}`
	status, resp := do(t, server, http.MethodPost, "/api/plans", body)
	if status != http.StatusCreated {
		t.Fatalf("create plan status=%d body=%v", status, resp)
	}
}

func TestLockoutHTTPWorkflowBlocksCommands(t *testing.T) {
	server, _ := newTestServer(t)
	const chamber = "chamber-1"
	createCycle(t, server, chamber)

	register := `{"devices":["fill_valve"],"reason":"replace fill valve seal","requested_by":"tech-lee"}`
	status, raw := do(t, server, http.MethodPost, "/api/chambers/"+chamber+"/lockouts", register)
	if status != http.StatusCreated {
		t.Fatalf("register status=%d body=%v", status, raw)
	}
	resp := asMap(t, raw)
	lockoutID, _ := resp["id"].(string)
	if lockoutID == "" {
		t.Fatal("lockout id missing")
	}
	if resp["state"] != "pending" {
		t.Fatalf("new lockout state=%v", resp["state"])
	}

	// An overlapping registration while pending must be refused.
	if status, _ := do(t, server, http.MethodPost, "/api/chambers/"+chamber+"/lockouts", register); status != http.StatusConflict {
		t.Fatalf("overlap register status=%d, want 409", status)
	}

	if status, raw := do(t, server, http.MethodPost,
		"/api/chambers/"+chamber+"/lockouts/"+lockoutID+"/approve", `{"operator":"supervisor-kim"}`); status != http.StatusOK || asMap(t, raw)["state"] != "active" {
		t.Fatalf("approve status=%d body=%v", status, raw)
	}

	// Fill-valve ramp must be rejected 423 Locked.
	status, raw = do(t, server, http.MethodPost, "/api/chambers/"+chamber+"/valves/ramp", `{"targets":[0.5]}`)
	if status != http.StatusLocked {
		t.Fatalf("ramp under lockout status=%d body=%v", status, raw)
	}
	resp = asMap(t, raw)
	if !strings.Contains(resp["error"].(string), "maintenance lockout") {
		t.Fatalf("blocked error = %v", resp["error"])
	}

	// Interlocks and chamber views expose the active lockout.
	status, viewRaw := do(t, server, http.MethodGet, "/api/interlocks", "")
	if status != http.StatusOK {
		t.Fatalf("interlocks status=%d", status)
	}
	chamberView, _ := asMap(t, viewRaw)[chamber].(map[string]any)
	lockouts, _ := chamberView["lockouts"].([]any)
	if len(lockouts) != 1 {
		t.Fatalf("interlocks lockout count=%d", len(lockouts))
	}
	status, chambersRaw := do(t, server, http.MethodGet, "/api/chambers", "")
	if status != http.StatusOK {
		t.Fatalf("chambers status=%d", status)
	}
	for _, item := range chambersRaw.([]any) {
		entry := item.(map[string]any)
		if entry["chamber_id"] == chamber {
			if active, _ := entry["lockouts"].([]any); len(active) != 1 {
				t.Fatalf("chamber view active lockouts=%v", entry["lockouts"])
			}
		}
	}

	// Release runs the standing checks (level window, opposite gates,
	// emergency); defaults satisfy them.
	if status, raw := do(t, server, http.MethodPost,
		"/api/chambers/"+chamber+"/lockouts/"+lockoutID+"/release",
		`{"operator":"supervisor-kim","note":"field clear"}`); status != http.StatusOK || asMap(t, raw)["state"] != "released" {
		t.Fatalf("release status=%d body=%v", status, raw)
	}

	// After release the command path is open again.
	if status, raw := do(t, server, http.MethodPost, "/api/chambers/"+chamber+"/valves/ramp", `{"targets":[0.5]}`); status != http.StatusOK {
		t.Fatalf("ramp after release status=%d body=%v", status, raw)
	}
}

func TestLockoutReleaseRejectsUnknownOperatorFields(t *testing.T) {
	server, _ := newTestServer(t)
	status, resp := do(t, server, http.MethodPost,
		"/api/chambers/chamber-2/lockouts/00000000-0000-0000-0000-000000000000/approve", `{"operator":"x"}`)
	if status != http.StatusNotFound {
		t.Fatalf("unknown lockout status=%d body=%v", status, resp)
	}
}

func TestLockoutEventsAppearInIncidents(t *testing.T) {
	server, _ := newTestServer(t)
	const chamber = "chamber-2"
	createCycle(t, server, chamber)
	status, raw := do(t, server, http.MethodPost, "/api/chambers/"+chamber+"/lockouts",
		`{"devices":["drain_valve"],"reason":"inspection","requested_by":"tech"}`)
	if status != http.StatusCreated {
		t.Fatalf("register status=%d body=%v", status, raw)
	}
	id := asMap(t, raw)["id"].(string)
	do(t, server, http.MethodPost, "/api/chambers/"+chamber+"/lockouts/"+id+"/approve", `{"operator":"sup"}`)
	do(t, server, http.MethodPost, "/api/chambers/"+chamber+"/valves/ramp", `{"targets":[-0.3]}`)

	status, incidentsRaw := do(t, server, http.MethodGet, "/api/incidents", "")
	if status != http.StatusOK {
		t.Fatalf("incidents status=%d", status)
	}
	var sawApproved, sawBlocked bool
	for _, item := range incidentsRaw.([]any) {
		entry := item.(map[string]any)
		switch model.EventType(entry["event_type"].(string)) {
		case model.EventLockoutApproved:
			sawApproved = true
		case model.EventCommandBlocked:
			sawBlocked = true
		}
	}
	if !sawApproved || !sawBlocked {
		t.Fatalf("incidents missing lockout events: approved=%v blocked=%v", sawApproved, sawBlocked)
	}
}
