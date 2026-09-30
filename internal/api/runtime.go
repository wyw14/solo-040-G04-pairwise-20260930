package api

import (
	"context"
	"time"

	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/cycle"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/interlock"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/lease"
	"github.com/wyw14/cry-96/internal/lockout"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/monitor"
	"github.com/wyw14/cry-96/internal/safety"
	"github.com/wyw14/cry-96/internal/schedule"
	"github.com/wyw14/cry-96/internal/signal"
)

// Runtime contains the long-lived component graph used by HTTP handlers.
type Runtime struct {
	Store           journal.Store
	Recorder        *journal.Recorder
	GateState       *gate.State
	GateCommands    *gate.Commands
	Gates           *gate.Controller
	SignalStore     *signal.Store
	SignalPublisher *signal.Publisher
	Signals         *signal.Service
	AdmissionQueue  *admission.Queue
	Admission       *admission.Service
	Planner         *schedule.Planner
	Schedule        *schedule.Service
	Hydraulics      *hydraulics.Controller
	Interlocks      *interlock.Coordinator
	Cycles          *cycle.Coordinator
	CycleService    *cycle.Service
	Leases          *lease.Service
	LeaseSweeper    *lease.Sweeper
	Safety          *safety.Service
	Lockouts        *lockout.Registry
	LockoutRecovery *lockout.Recovery
	Snapshot        *monitor.Snapshot
	Incidents       *monitor.IncidentReview
	Health          *monitor.HealthRegistry
	Recovery        *cycle.Recovery
	Chambers        []string
	Now             func() time.Time
}

func NewRuntime(ctx context.Context, dataDirectory string) (*Runtime, error) {
	now := time.Now
	store, err := journal.NewRuntimeStore(dataDirectory)
	if err != nil {
		return nil, err
	}
	recorder := journal.NewRecorder(store, now)
	chambers := []string{"chamber-1", "chamber-2"}
	gateState := gate.NewState(chambers...)
	gateCommands := gate.NewCommands()
	gateController := gate.NewController(gateState, gateCommands, recorder, now)
	signalStore := signal.NewStore()
	signalPublisher := signal.NewPublisher(signalStore, recorder, now)
	signalService := signal.NewService(signalStore)
	admissionQueue := admission.NewQueue()
	admissionController := admission.NewController(admissionQueue, gateState, signalStore, now)
	admissionService := admission.NewService(admissionController, recorder, now)
	leaseRegistry := lease.NewRegistry()
	leaseService := lease.NewService(leaseRegistry, admissionController, recorder, now)
	leaseSweeper := lease.NewSweeper(leaseRegistry, now)
	planner := schedule.NewPlanner(admissionService, recorder, now)
	scheduleService := schedule.NewService(planner, signalPublisher)
	ramp := hydraulics.NewRamp(25*time.Millisecond, now)
	hydraulicsController := hydraulics.NewController(gateCommands, recorder, ramp, now)
	barrier := safety.NewBarrier()
	// The maintenance-lockout authority is created before the interlock and
	// hydraulics coordinators so every gate/valve command is authorized
	// against it. Approving a lockout cancels queued commands for its scope.
	lockoutRegistry := lockout.NewRegistry(recorder, func(chamberID string, devices []model.DeviceKind, at time.Time) {
		gateCommands.CancelDevices(chamberID, devices, at)
	}, now)
	hydraulicsController.BindLockouts(lockoutRegistry)
	var safetyCoordinator *safety.Coordinator
	levelByChamber := func(chamberID string) float64 {
		samples := hydraulicsController.WindowSamples(chamberID)
		if len(samples) == 0 {
			return 0
		}
		return samples[len(samples)-1].Meters
	}
	emergencyActive := func(chamberID string) bool {
		return safetyCoordinator != nil && safetyCoordinator.Active(chamberID)
	}
	interlockCoordinator := interlock.NewCoordinator(
		interlock.DefaultPolicy(), interlock.NewArbiter(), gateController,
		levelByChamber, emergencyActive, lockoutRegistry,
	)
	// Releasing a lockout still requires the standing level/opposite-gate/
	// emergency checks; binding them keeps the registry free of safety deps.
	lockoutRegistry.BindClearance(interlockCoordinator.Clearance)
	cycleCoordinator := cycle.NewCoordinator(gateController, hydraulicsController, recorder, now)
	cycleService := cycle.NewService(cycleCoordinator, admissionService)
	safetyCoordinator = safety.NewCoordinator(gateCommands, barrier, recorder, now)
	safetyService := safety.NewService(safetyCoordinator, cycleCoordinator)
	signalService.BindTransitLookup(cycleCoordinator)
	replayer := journal.NewReplayer(store)
	recovery := cycle.NewRecovery(replayer, cycleCoordinator)
	if _, err := recovery.RestoreAll(ctx); err != nil {
		return nil, err
	}
	lockoutRecovery := lockout.NewRecovery(replayer, lockoutRegistry)
	if _, err := lockoutRecovery.RestoreAll(ctx); err != nil {
		return nil, err
	}
	health := monitor.NewHealthRegistry()
	for _, chamberID := range chambers {
		for _, device := range []model.DeviceKind{
			model.DeviceUpstreamGate, model.DeviceDownstreamGate,
			model.DeviceFillValve, model.DeviceDrainValve,
		} {
			health.Observe(chamberID, device, true, "telemetry current", now())
		}
	}
	return &Runtime{
		Store: store, Recorder: recorder, GateState: gateState,
		GateCommands: gateCommands, Gates: gateController,
		SignalStore: signalStore, SignalPublisher: signalPublisher, Signals: signalService,
		AdmissionQueue: admissionController.Queue(), Admission: admissionService,
		Planner: planner, Schedule: scheduleService, Hydraulics: hydraulicsController,
		Interlocks: interlockCoordinator, Cycles: cycleCoordinator, CycleService: cycleService,
		Leases: leaseService, LeaseSweeper: leaseSweeper, Safety: safetyService,
		Lockouts: lockoutRegistry, LockoutRecovery: lockoutRecovery,
		Snapshot:  monitor.NewSnapshot(cycleCoordinator, gateState, admissionService, signalService, safetyService, lockoutRegistry, now),
		Incidents: monitor.NewIncidentReview(store), Health: health, Recovery: recovery,
		Chambers: chambers, Now: now,
	}, nil
}
