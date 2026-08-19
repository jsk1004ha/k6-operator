package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/grafana/k6-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestClaimSetupExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		plz         bool
		setupStatus *metav1.ConditionStatus
		setupReason string
		wantState   setupExecutionState
		wantStatus  metav1.ConditionStatus
		wantReason  string
	}{
		{
			name:        "claims an unexecuted PLZ setup",
			plz:         true,
			setupStatus: conditionStatus(metav1.ConditionFalse),
			wantState:   setupExecutionClaimed,
			wantStatus:  metav1.ConditionUnknown,
			wantReason:  setupReasonClaimed,
		},
		{
			name:        "recognizes a completed PLZ setup",
			plz:         true,
			setupStatus: conditionStatus(metav1.ConditionTrue),
			wantState:   setupExecutionCompleted,
			wantStatus:  metav1.ConditionTrue,
			wantReason:  v1alpha1.SetupExecuted + string(metav1.ConditionTrue),
		},
		{
			name:        "waits for a fresh claimed PLZ setup",
			plz:         true,
			setupStatus: conditionStatus(metav1.ConditionUnknown),
			setupReason: setupReasonClaimed,
			wantState:   setupExecutionInProgress,
			wantStatus:  metav1.ConditionUnknown,
			wantReason:  setupReasonClaimed,
		},
		{
			name:       "claims a PLZ setup when upgrading a resource without the condition",
			plz:        true,
			wantState:  setupExecutionClaimed,
			wantStatus: metav1.ConditionUnknown,
			wantReason: setupReasonClaimed,
		},
		{
			name:        "does not claim setup for a non-PLZ test run",
			setupStatus: conditionStatus(metav1.ConditionFalse),
			wantState:   setupExecutionNotRequired,
			wantStatus:  metav1.ConditionFalse,
			wantReason:  v1alpha1.SetupExecuted + string(metav1.ConditionFalse),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			testRun := setupConditionTestRun(tt.plz, tt.setupStatus)
			if tt.setupReason != "" {
				setSetupTestCondition(t, testRun, *tt.setupStatus, tt.setupReason, time.Now().Add(-time.Minute))
			}
			r, objectKey := setupConditionTestReconciler(t, testRun)
			current := getSetupConditionTestRun(t, ctx, r, objectKey)

			state, err := claimSetupExecution(ctx, current, r, logr.Discard())
			if err != nil {
				t.Fatalf("claimSetupExecution() error = %v", err)
			}
			if state != tt.wantState {
				t.Errorf("claimSetupExecution() state = %v, want %v", state, tt.wantState)
			}

			persisted := getSetupConditionTestRun(t, ctx, r, objectKey)
			condition := meta.FindStatusCondition(persisted.Status.Conditions, v1alpha1.SetupExecuted)
			if condition == nil {
				t.Fatal("SetupExecuted condition was not persisted")
			}
			if condition.Status != tt.wantStatus {
				t.Errorf("persisted SetupExecuted = %v, want %v", condition.Status, tt.wantStatus)
			}
			if condition.Reason != tt.wantReason {
				t.Errorf("persisted reason = %q, want %q", condition.Reason, tt.wantReason)
			}

			if tt.wantState == setupExecutionClaimed {
				stateAgain, err := claimSetupExecution(ctx, persisted, r, logr.Discard())
				if err != nil {
					t.Fatalf("second claimSetupExecution() error = %v", err)
				}
				if stateAgain != setupExecutionInProgress {
					t.Errorf("second claim state = %v, want %v", stateAgain, setupExecutionInProgress)
				}
			}
		})
	}
}

func TestClaimSetupExecutionUsesOptimisticLock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, objectKey := setupConditionTestReconciler(
		t,
		setupConditionTestRun(true, conditionStatus(metav1.ConditionFalse)),
	)
	first := getSetupConditionTestRun(t, ctx, r, objectKey)
	stale := first.DeepCopy()

	state, err := claimSetupExecution(ctx, first, r, logr.Discard())
	if err != nil {
		t.Fatalf("first claimSetupExecution() error = %v", err)
	}
	if state != setupExecutionClaimed {
		t.Fatalf("first claim state = %v, want %v", state, setupExecutionClaimed)
	}

	state, err = claimSetupExecution(ctx, stale, r, logr.Discard())
	if state == setupExecutionClaimed {
		t.Error("stale reconciler unexpectedly claimed setup execution")
	}
	if err == nil {
		t.Fatal("stale reconciler did not receive an optimistic-lock error")
	}
	if !apierrors.IsConflict(err) {
		t.Errorf("stale reconciler error = %v, want a conflict", err)
	}
}

func TestResetSetupExecutionAllowsRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, objectKey := setupConditionTestReconciler(
		t,
		setupConditionTestRun(true, conditionStatus(metav1.ConditionFalse)),
	)
	current := getSetupConditionTestRun(t, ctx, r, objectKey)

	state, err := claimSetupExecution(ctx, current, r, logr.Discard())
	if err != nil {
		t.Fatalf("claimSetupExecution() error = %v", err)
	}
	if state != setupExecutionClaimed {
		t.Fatalf("setup execution state = %v, want %v", state, setupExecutionClaimed)
	}

	if err := markSetupExecutionPrepared(ctx, current, r, logr.Discard()); err != nil {
		t.Fatalf("markSetupExecutionPrepared() error = %v", err)
	}

	if err := markSetupExecutionRetryableFailure(
		ctx,
		current,
		r,
		logr.Discard(),
		errors.New("temporary network error"),
	); err != nil {
		t.Fatalf("markSetupExecutionRetryableFailure() error = %v", err)
	}

	if err := resetSetupExecution(ctx, current, r, logr.Discard()); err != nil {
		t.Fatalf("resetSetupExecution() error = %v", err)
	}

	persisted := getSetupConditionTestRun(t, ctx, r, objectKey)
	if !v1alpha1.IsFalse(persisted, v1alpha1.SetupExecuted) {
		t.Error("SetupExecuted was not reset after a retryable setup error")
	}

	state, err = claimSetupExecution(ctx, persisted, r, logr.Discard())
	if err != nil {
		t.Fatalf("retry claimSetupExecution() error = %v", err)
	}
	if state != setupExecutionClaimed {
		t.Errorf("retry claim state = %v, want %v", state, setupExecutionClaimed)
	}
}

func TestCompleteSetupExecutionAllowsStarter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, objectKey := setupConditionTestReconciler(
		t,
		setupConditionTestRun(true, conditionStatus(metav1.ConditionFalse)),
	)
	current := getSetupConditionTestRun(t, ctx, r, objectKey)

	state, err := claimSetupExecution(ctx, current, r, logr.Discard())
	if err != nil {
		t.Fatalf("claimSetupExecution() error = %v", err)
	}
	if state != setupExecutionClaimed {
		t.Fatalf("setup execution state = %v, want %v", state, setupExecutionClaimed)
	}

	if err := markSetupExecutionPrepared(ctx, current, r, logr.Discard()); err != nil {
		t.Fatalf("markSetupExecutionPrepared() error = %v", err)
	}

	if err := markSetupExecutionSucceeded(ctx, current, r, logr.Discard()); err != nil {
		t.Fatalf("markSetupExecutionSucceeded() error = %v", err)
	}

	if err := completeSetupExecution(ctx, current, r, logr.Discard()); err != nil {
		t.Fatalf("completeSetupExecution() error = %v", err)
	}

	persisted := getSetupConditionTestRun(t, ctx, r, objectKey)
	if !v1alpha1.IsTrue(persisted, v1alpha1.SetupExecuted) {
		t.Error("SetupExecuted was not marked complete after setup succeeded")
	}

	state, err = claimSetupExecution(ctx, persisted, r, logr.Discard())
	if err != nil {
		t.Fatalf("completed claimSetupExecution() error = %v", err)
	}
	if state != setupExecutionCompleted {
		t.Errorf("completed setup state = %v, want %v", state, setupExecutionCompleted)
	}
	if !setupExecutionAllowsStarter(state) {
		t.Error("completed setup did not allow starter creation")
	}
}

func TestClaimHolderCannotOverwriteTerminalFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, objectKey := setupConditionTestReconciler(
		t,
		setupConditionTestRun(true, conditionStatus(metav1.ConditionFalse)),
	)
	claimHolder := getSetupConditionTestRun(t, ctx, r, objectKey)

	state, err := claimSetupExecution(ctx, claimHolder, r, logr.Discard())
	if err != nil {
		t.Fatalf("claimSetupExecution() error = %v", err)
	}
	if state != setupExecutionClaimed {
		t.Fatalf("setup execution state = %v, want %v", state, setupExecutionClaimed)
	}
	if err := markSetupExecutionPrepared(ctx, claimHolder, r, logr.Discard()); err != nil {
		t.Fatalf("markSetupExecutionPrepared() error = %v", err)
	}
	staleClaimHolder := claimHolder.DeepCopy()

	timeoutReconciler := getSetupConditionTestRun(t, ctx, r, objectKey)
	if err := markSetupExecutionFailed(
		ctx,
		timeoutReconciler,
		r,
		logr.Discard(),
		setupReasonPrepared,
		"setup execution claim timed out",
	); err != nil {
		t.Fatalf("markSetupExecutionFailed() error = %v", err)
	}

	err = markSetupExecutionSucceeded(ctx, staleClaimHolder, r, logr.Discard())
	if !errors.Is(err, errSetupExecutionConditionChanged) {
		t.Fatalf("markSetupExecutionSucceeded() error = %v, want state-change rejection", err)
	}

	persisted := getSetupConditionTestRun(t, ctx, r, objectKey)
	condition := meta.FindStatusCondition(persisted.Status.Conditions, v1alpha1.SetupExecuted)
	if condition == nil {
		t.Fatal("SetupExecuted condition is missing")
	}
	if condition.Status != metav1.ConditionUnknown {
		t.Errorf("status = %v, want %v", condition.Status, metav1.ConditionUnknown)
	}
	if condition.Reason != setupReasonFailed {
		t.Errorf("reason = %q, want %q", condition.Reason, setupReasonFailed)
	}

	state, err = claimSetupExecution(ctx, persisted, r, logr.Discard())
	if err != nil {
		t.Fatalf("claimSetupExecution() after terminal failure error = %v", err)
	}
	if state != setupExecutionFailed {
		t.Errorf("state = %v, want %v", state, setupExecutionFailed)
	}
	if setupExecutionAllowsStarter(state) {
		t.Error("terminal setup failure unexpectedly allowed starter creation")
	}
}

func TestClaimSetupExecutionRecoversPersistedPhases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		reason     string
		message    string
		wantState  setupExecutionState
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{
			name:       "finalizes a previously successful setup",
			reason:     setupReasonSucceeded,
			wantState:  setupExecutionCompleted,
			wantStatus: metav1.ConditionTrue,
			wantReason: "SetupExecutedTrue",
		},
		{
			name:       "releases a persisted retryable failure",
			reason:     setupReasonRetryableFailure,
			wantState:  setupExecutionInProgress,
			wantStatus: metav1.ConditionFalse,
			wantReason: "SetupExecutedFalse",
		},
		{
			name:       "retains a non-retryable failure",
			reason:     setupReasonFailed,
			message:    "unsafe to replay",
			wantState:  setupExecutionFailed,
			wantStatus: metav1.ConditionUnknown,
			wantReason: setupReasonFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			testRun := setupConditionTestRun(true, conditionStatus(metav1.ConditionUnknown))
			setSetupTestCondition(t, testRun, metav1.ConditionUnknown, tt.reason, time.Now().Add(-time.Minute))
			condition := meta.FindStatusCondition(testRun.Status.Conditions, v1alpha1.SetupExecuted)
			condition.Message = tt.message
			r, objectKey := setupConditionTestReconciler(t, testRun)
			current := getSetupConditionTestRun(t, ctx, r, objectKey)

			state, err := claimSetupExecution(ctx, current, r, logr.Discard())
			if err != nil {
				t.Fatalf("claimSetupExecution() error = %v", err)
			}
			if state != tt.wantState {
				t.Errorf("state = %v, want %v", state, tt.wantState)
			}

			persisted := getSetupConditionTestRun(t, ctx, r, objectKey)
			persistedCondition := meta.FindStatusCondition(persisted.Status.Conditions, v1alpha1.SetupExecuted)
			if persistedCondition.Status != tt.wantStatus {
				t.Errorf("status = %v, want %v", persistedCondition.Status, tt.wantStatus)
			}
			if persistedCondition.Reason != tt.wantReason {
				t.Errorf("reason = %q, want %q", persistedCondition.Reason, tt.wantReason)
			}
		})
	}
}

func TestClaimSetupExecutionReleasesOrphanedUnpreparedClaim(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testRun := setupConditionTestRun(true, conditionStatus(metav1.ConditionUnknown))
	setSetupTestCondition(
		t,
		testRun,
		metav1.ConditionUnknown,
		setupReasonClaimed,
		time.Now().Add(-setupExecutionClaimTimeout-time.Minute),
	)
	r, objectKey := setupConditionTestReconciler(t, testRun)
	current := getSetupConditionTestRun(t, ctx, r, objectKey)

	state, err := claimSetupExecution(ctx, current, r, logr.Discard())
	if err != nil {
		t.Fatalf("claimSetupExecution() error = %v", err)
	}
	if state != setupExecutionInProgress {
		t.Fatalf("state = %v, want %v", state, setupExecutionInProgress)
	}

	persisted := getSetupConditionTestRun(t, ctx, r, objectKey)
	if !v1alpha1.IsFalse(persisted, v1alpha1.SetupExecuted) {
		t.Error("orphaned pre-execution claim was not released")
	}
}

type fakeSetupDataClient struct {
	dataByHostname map[string]json.RawMessage
	runData        json.RawMessage
	runErr         error
}

func newFakeSetupDataClient() *fakeSetupDataClient {
	return &fakeSetupDataClient{dataByHostname: make(map[string]json.RawMessage)}
}

func (f *fakeSetupDataClient) RunSetup(_ context.Context, hostname string) (json.RawMessage, error) {
	if f.runErr != nil {
		return nil, f.runErr
	}
	f.dataByHostname[hostname] = append(json.RawMessage(nil), f.runData...)
	return append(json.RawMessage(nil), f.runData...), nil
}

func (f *fakeSetupDataClient) GetSetupData(
	_ context.Context,
	hostname string,
) (json.RawMessage, error) {
	return append(json.RawMessage(nil), f.dataByHostname[hostname]...), nil
}

func (f *fakeSetupDataClient) SetSetupData(
	_ context.Context,
	hostnames []string,
	data json.RawMessage,
) error {
	for _, hostname := range hostnames {
		f.dataByHostname[hostname] = append(json.RawMessage(nil), data...)
	}
	return nil
}

func TestPreparedSetupRecoversAfterClaimHolderExit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupData json.RawMessage
	}{
		{name: "object setup data", setupData: json.RawMessage(`{"value":1}`)},
		{name: "undefined setup data", setupData: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			r, objectKey := setupConditionTestReconciler(
				t,
				setupConditionTestRun(true, conditionStatus(metav1.ConditionFalse)),
			)
			claimHolder := getSetupConditionTestRun(t, ctx, r, objectKey)
			state, err := claimSetupExecution(ctx, claimHolder, r, logr.Discard())
			if err != nil {
				t.Fatalf("claimSetupExecution() error = %v", err)
			}
			if state != setupExecutionClaimed {
				t.Fatalf("state = %v, want %v", state, setupExecutionClaimed)
			}

			hostnames := []string{"runner-0", "runner-1"}
			setupClient := newFakeSetupDataClient()
			claimID := setupExecutionClaimID(claimHolder)
			if err := prepareSetupRunner(ctx, hostnames, claimID, setupClient); err != nil {
				t.Fatalf("prepareSetupRunner() error = %v", err)
			}
			if err := markSetupExecutionPrepared(ctx, claimHolder, r, logr.Discard()); err != nil {
				t.Fatalf("markSetupExecutionPrepared() error = %v", err)
			}

			// Simulate setup returning successfully and the claim holder exiting
			// before it can persist SetupExecutionSucceeded.
			setupClient.dataByHostname[hostnames[0]] = append(json.RawMessage(nil), tt.setupData...)

			reconciled := getSetupConditionTestRun(t, ctx, r, objectKey)
			state, err = reconcileSetupExecution(
				ctx,
				logr.Discard(),
				reconciled,
				r,
				hostnames,
				setupClient,
			)
			if err != nil {
				t.Fatalf("reconcileSetupExecution() error = %v", err)
			}
			if state != setupExecutionCompleted {
				t.Fatalf("state = %v, want %v", state, setupExecutionCompleted)
			}

			persisted := getSetupConditionTestRun(t, ctx, r, objectKey)
			if !v1alpha1.IsTrue(persisted, v1alpha1.SetupExecuted) {
				t.Error("recovered setup was not marked complete")
			}
			for _, hostname := range hostnames {
				if string(setupClient.dataByHostname[hostname]) != string(tt.setupData) {
					t.Errorf(
						"setup data for %s = %q, want %q",
						hostname,
						setupClient.dataByHostname[hostname],
						tt.setupData,
					)
				}
			}
		})
	}
}

func TestSetupExecutionAllowsStarter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		state setupExecutionState
		want  bool
	}{
		{state: setupExecutionNotRequired, want: true},
		{state: setupExecutionClaimed, want: false},
		{state: setupExecutionPrepared, want: false},
		{state: setupExecutionInProgress, want: false},
		{state: setupExecutionCompleted, want: true},
		{state: setupExecutionFailed, want: false},
	}

	for _, tt := range tests {
		if got := setupExecutionAllowsStarter(tt.state); got != tt.want {
			t.Errorf("setupExecutionAllowsStarter(%v) = %v, want %v", tt.state, got, tt.want)
		}
	}
}

func setupConditionTestRun(
	plz bool,
	setupStatus *metav1.ConditionStatus,
) *v1alpha1.TestRun {
	oldTransitionTime := metav1.NewTime(time.Now().Add(-time.Minute))
	conditions := []metav1.Condition{
		{
			Type:               v1alpha1.CloudPLZTestRun,
			Status:             metav1.ConditionFalse,
			LastTransitionTime: oldTransitionTime,
			Reason:             "CloudPLZTestRunFalse",
		},
	}
	if plz {
		conditions[0].Status = metav1.ConditionTrue
		conditions[0].Reason = "CloudPLZTestRunTrue"
	}
	if setupStatus != nil {
		conditions = append(conditions, metav1.Condition{
			Type:               v1alpha1.SetupExecuted,
			Status:             *setupStatus,
			LastTransitionTime: oldTransitionTime,
			Reason:             v1alpha1.SetupExecuted + string(*setupStatus),
		})
	}

	return &v1alpha1.TestRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "default",
		},
		Status: v1alpha1.TestRunStatus{
			Stage:      "created",
			Conditions: conditions,
		},
	}
}

func setSetupTestCondition(
	t *testing.T,
	k6 *v1alpha1.TestRun,
	status metav1.ConditionStatus,
	reason string,
	transition time.Time,
) {
	t.Helper()
	condition := meta.FindStatusCondition(k6.Status.Conditions, v1alpha1.SetupExecuted)
	if condition == nil {
		t.Fatal("SetupExecuted condition is missing")
	}
	condition.Status = status
	condition.Reason = reason
	condition.LastTransitionTime = metav1.NewTime(transition)
}

func setupConditionTestReconciler(
	t *testing.T,
	k6 *v1alpha1.TestRun,
) (*TestRunReconciler, types.NamespacedName) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding TestRun to scheme: %v", err)
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.TestRun{}).
		WithObjects(k6).
		Build()

	return &TestRunReconciler{Client: client, Scheme: scheme}, k6.NamespacedName()
}

func getSetupConditionTestRun(
	t *testing.T,
	ctx context.Context,
	r *TestRunReconciler,
	key types.NamespacedName,
) *v1alpha1.TestRun {
	t.Helper()

	k6 := &v1alpha1.TestRun{}
	if err := r.Get(ctx, key, k6); err != nil {
		t.Fatalf("fetching persisted TestRun: %v", err)
	}
	return k6
}

func conditionStatus(status metav1.ConditionStatus) *metav1.ConditionStatus {
	return &status
}
