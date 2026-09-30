package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wyw14/cry-96/internal/model"
)

func newTestRouter(t *testing.T) (http.Handler, *Runtime) {
	t.Helper()
	runtime, err := NewRuntime(context.Background(), "")
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	return Router(runtime, ""), runtime
}

func doReq(router http.Handler, method, path string, body any) (int, map[string]any) {
	var reader *bytes.Reader
	if body != nil {
		encoded, _ := json.Marshal(body)
		reader = bytes.NewReader(encoded)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	var decoded map[string]any
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &decoded)
	}
	return rec.Code, decoded
}

func do(t *testing.T, router http.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()
	return doReq(router, method, path, body)
}

func startCycle(t *testing.T, router http.Handler, chamber, direction string) {
	t.Helper()
	status, body := do(t, router, http.MethodPost, "/api/plans", map[string]any{
		"chamber_id": chamber, "direction": direction,
		"vessels": []map[string]any{{"name": "River Star", "call_sign": "RS-1", "length_m": 80}},
	})
	if status != http.StatusCreated {
		t.Fatalf("create plan status=%d body=%v", status, body)
	}
}

func registerApproveLock(t *testing.T, router http.Handler, chamber string, devices []string) string {
	t.Helper()
	status, body := do(t, router, http.MethodPost, "/api/lockouts", map[string]any{
		"chamber_id": chamber, "devices": devices,
		"reason": "seal replacement", "requested_by": "tech-12",
	})
	if status != http.StatusCreated {
		t.Fatalf("register status=%d body=%v", status, body)
	}
	lockID, _ := body["id"].(string)
	if status, body = do(t, router, http.MethodPost, fmt.Sprintf("/api/lockouts/%s/approve", lockID),
		map[string]any{"approved_by": "dispatcher-7"}); status != http.StatusOK {
		t.Fatalf("approve status=%d body=%v", status, body)
	}
	return lockID
}

// TestLockoutBlocksGateLifecycle exercises register → approve → block → busy
// release → settle → release → command flows again through the original policy.
func TestLockoutBlocksGateLifecycle(t *testing.T) {
	router, _ := newTestRouter(t)
	const chamber = "chamber-2"
	startCycle(t, router, chamber, "upstream")

	// A gate command succeeds before the lockout.
	status, body := do(t, router, http.MethodPost,
		fmt.Sprintf("/api/chambers/%s/gates/upstream/open", chamber), nil)
	if status != http.StatusAccepted {
		t.Fatalf("open before lockout status=%d body=%v", status, body)
	}
	commandID, _ := body["id"].(string)

	lockID := registerApproveLock(t, router, chamber, []string{"upstream_gate"})

	// Same-person approval is rejected on a fresh order.
	status, body = do(t, router, http.MethodPost, "/api/lockouts", map[string]any{
		"chamber_id": chamber, "devices": []string{"drain_valve"},
		"reason": "valve packing", "requested_by": "tech-12",
	})
	if status != http.StatusCreated {
		t.Fatalf("register valve lockout status=%d body=%v", status, body)
	}
	valveLockID, _ := body["id"].(string)
	if status, _ = do(t, router, http.MethodPost,
		fmt.Sprintf("/api/lockouts/%s/approve", valveLockID),
		map[string]any{"approved_by": "tech-12"}); status != http.StatusConflict {
		t.Fatalf("self-approval status=%d, want 409", status)
	}

	// Open is rejected with 423 Locked while the order is active, reason included.
	status, body = do(t, router, http.MethodPost,
		fmt.Sprintf("/api/chambers/%s/gates/upstream/open", chamber), nil)
	if status != http.StatusLocked {
		t.Fatalf("open under lockout status=%d body=%v, want 423", status, body)
	}
	if reason, _ := body["error"].(string); !bytes.Contains([]byte(reason), []byte("seal replacement")) {
		t.Fatalf("blocked response must carry the lockout reason, got %q", reason)
	}

	// The unrelated device on the same chamber is not blocked.
	status, _ = do(t, router, http.MethodPost,
		fmt.Sprintf("/api/chambers/%s/valves/ramp", chamber),
		map[string]any{"targets": []float64{-0.3}})
	if status != http.StatusOK {
		t.Fatalf("drain ramp on unlocked device status=%d, want 200", status)
	}

	// Release refuses while the earlier gate command is still in flight.
	status, _ = do(t, router, http.MethodPost, fmt.Sprintf("/api/lockouts/%s/release", lockID),
		map[string]any{"released_by": "dispatcher-7", "note": "done"})
	if status != http.StatusConflict {
		t.Fatalf("release with pending command status=%d, want 409", status)
	}

	// Settle the command, then release succeeds.
	if status, body = do(t, router, http.MethodPost,
		fmt.Sprintf("/api/chambers/%s/commands/%s/ack", chamber, commandID), nil); status != http.StatusOK {
		t.Fatalf("ack status=%d body=%v", status, body)
	}
	if status, body = do(t, router, http.MethodPost, fmt.Sprintf("/api/lockouts/%s/release", lockID),
		map[string]any{"released_by": "dispatcher-7", "note": "seal fitted"}); status != http.StatusOK {
		t.Fatalf("release status=%d body=%v", status, body)
	}

	// Command flows again — still evaluated by the original interlock policy.
	// Upstream is now open, so opening the opposite gate is refused by the
	// original opposite-gate interlock (not by the released lockout).
	status, body = do(t, router, http.MethodPost,
		fmt.Sprintf("/api/chambers/%s/gates/downstream/open", chamber), nil)
	if status != http.StatusConflict {
		t.Fatalf("opposite gate open status=%d body=%v, want original interlock 409", status, body)
	}

	// The blocked attempt is visible in the incident/event review.
	found := false
	for _, item := range incidentSummaryList(t, router) {
		if item["event_type"] == "command.blocked" {
			found = true
		}
	}
	if !found {
		t.Fatal("command.blocked event missing from incident review")
	}
}

func incidentSummaryList(t *testing.T, router http.Handler) []map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/incidents", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	var list []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode incidents: %v", err)
	}
	return list
}

// TestLockoutBlocksValveRamp verifies valve commands are blocked per device and
// that a blocked ramp writes no command.
func TestLockoutBlocksValveRamp(t *testing.T) {
	router, _ := newTestRouter(t)
	const chamber = "chamber-1"
	startCycle(t, router, chamber, "downstream")

	lockID := registerApproveLock(t, router, chamber, []string{"fill_valve"})

	// Positive targets drive the fill valve → blocked on the first step.
	status, body := do(t, router, http.MethodPost,
		fmt.Sprintf("/api/chambers/%s/valves/ramp", chamber),
		map[string]any{"targets": []float64{0.4, 0.8}})
	if status != http.StatusLocked {
		t.Fatalf("fill ramp status=%d body=%v, want 423", status, body)
	}
	if reason, _ := body["error"].(string); !bytes.Contains([]byte(reason), []byte("seal replacement")) {
		t.Fatalf("blocked ramp must carry the reason, got %v", body)
	}

	// Release (no fill-valve command was issued, so nothing is in flight) ...
	if status, body = do(t, router, http.MethodPost, fmt.Sprintf("/api/lockouts/%s/release", lockID),
		map[string]any{"released_by": "dispatcher-7"}); status != http.StatusOK {
		t.Fatalf("release status=%d body=%v", status, body)
	}
	// ... then the ramp proceeds.
	status, _ = do(t, router, http.MethodPost,
		fmt.Sprintf("/api/chambers/%s/valves/ramp", chamber),
		map[string]any{"targets": []float64{0.4}})
	if status != http.StatusOK {
		t.Fatalf("ramp after release status=%d, want 200", status)
	}
}

// TestConcurrentOpenAndLockoutToggles hammers the HTTP surface with lockouts
// being filed/approved/released while gate-open requests race. Correctness is
// proven from the durable, totally-ordered chamber event stream: no
// command.queued for the locked device may land between a lockout's approved
// and released events. Run with -race for memory safety.
func TestConcurrentOpenAndLockoutToggles(t *testing.T) {
	router, runtime := newTestRouter(t)
	const chamber = "chamber-1"
	startCycle(t, router, chamber, "upstream")

	openPath := fmt.Sprintf("/api/chambers/%s/gates/upstream/open", chamber)

	var accepted, blocked, unexpected int32
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Toggler: each cycle files a brand-new lockout, approves it, drains any
	// in-flight commands and releases it — alternating the window between
	// locked and free.
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			code, body := doReq(router, http.MethodPost, "/api/lockouts", map[string]any{
				"chamber_id": chamber, "devices": []string{"upstream_gate"},
				"reason": fmt.Sprintf("cyclic inspection %d", n), "requested_by": "tech-1",
			})
			if code != http.StatusCreated {
				continue // an order from a prior iteration is still open
			}
			lockID, _ := body["id"].(string)
			if code, _ = doReq(router, http.MethodPost,
				fmt.Sprintf("/api/lockouts/%s/approve", lockID),
				map[string]any{"approved_by": "boss-1"}); code != http.StatusOK {
				continue
			}
			// Hold the lockout briefly, then drain and release.
			for j := 0; j < 20; j++ {
				runtime.GateCommands.CancelChamber(chamber, runtime.Now())
			}
			doReq(router, http.MethodPost, fmt.Sprintf("/api/lockouts/%s/release", lockID),
				map[string]any{"released_by": "boss-1"})
			n++
		}
	}()

	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				switch code, _ := doReq(router, http.MethodPost, openPath, nil); code {
				case http.StatusAccepted:
					atomic.AddInt32(&accepted, 1)
				case http.StatusLocked:
					atomic.AddInt32(&blocked, 1)
				case http.StatusConflict:
					// ordinary interlock/reservation contention — allowed
				default:
					atomic.AddInt32(&unexpected, 1)
				}
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	if unexpected != 0 {
		t.Fatalf("unexpected HTTP status count=%d", unexpected)
	}
	if atomic.LoadInt32(&accepted) == 0 || atomic.LoadInt32(&blocked) == 0 {
		t.Fatalf("expected both accepted and blocked outcomes, got accepted=%d blocked=%d",
			atomic.LoadInt32(&accepted), atomic.LoadInt32(&blocked))
	}

	// Linearizability check against the totally-ordered chamber journal:
	// a queued upstream-gate command must never fall inside an active interval.
	events, err := runtime.Store.Events(context.Background(), chamber, 0)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	approved := make(map[string]uint64) // lockout id -> approval sequence
	released := make(map[string]uint64) // lockout id -> release sequence
	type queued struct {
		seq uint64
		dev string
	}
	var queuedCommands []queued
	for _, event := range events {
		switch event.Type {
		case model.EventLockoutApproved:
			var lock model.MaintenanceLock
			if err := event.Decode(&lock); err != nil {
				t.Fatalf("decode approved: %v", err)
			}
			approved[lock.ID.String()] = event.Sequence
		case model.EventLockoutReleased:
			var lock model.MaintenanceLock
			if err := event.Decode(&lock); err != nil {
				t.Fatalf("decode released: %v", err)
			}
			released[lock.ID.String()] = event.Sequence
		case model.EventCommandQueued:
			var command model.DeviceCommand
			if err := event.Decode(&command); err != nil {
				t.Fatalf("decode queued: %v", err)
			}
			queuedCommands = append(queuedCommands, queued{event.Sequence, string(command.Device)})
		}
	}
	const end = ^uint64(0)
	for _, q := range queuedCommands {
		if q.dev != string(model.DeviceUpstreamGate) {
			continue
		}
		for id, start := range approved {
			finish, isReleased := released[id]
			if !isReleased {
				finish = end // lockout still active at stream end
			}
			if q.seq > start && (!isReleased || q.seq < finish) {
				t.Fatalf("linearization violation: command.queued seq=%d for %s inside active lockout %s [%d,%v)",
					q.seq, q.dev, id, start, map[bool]uint64{true: finish}[isReleased])
			}
		}
	}
}
