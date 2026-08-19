from __future__ import annotations

import re
from pathlib import Path


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one exact match, found {count}\n--- needle ---\n{old}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def replace_regex(path: Path, pattern: str, replacement: str) -> None:
    text = path.read_text(encoding="utf-8")
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.DOTALL)
    if count != 1:
        raise SystemExit(f"{path}: expected one regex match, found {count}\n--- pattern ---\n{pattern}")
    path.write_text(updated, encoding="utf-8")


# Add a readable setup-data probe to the k6 API client.
k6client = Path("pkg/testrun/k6client.go")
replace_once(
    k6client,
    "func SetSetupData(ctx context.Context, hostnames []string, data json.RawMessage) (err error) {",
    '''func GetSetupData(ctx context.Context, hostname string) (json.RawMessage, error) {
\tc, err := k6Client.New(fmt.Sprintf("%v:6565", hostname), k6Client.WithHTTPClient(&http.Client{
\t\tTimeout: 0,
\t}))
\tif err != nil {
\t\treturn nil, err
\t}

\tvar response types.SetupData
\tif err := c.CallAPI(ctx, "GET", &url.URL{Path: "/v1/setup"}, nil, &response); err != nil {
\t\treturn nil, err
\t}

\tif response.Data.Attributes.Data != nil {
\t\tvar decoded any
\t\tif err := json.Unmarshal(response.Data.Attributes.Data, &decoded); err != nil {
\t\t\treturn nil, err
\t\t}
\t}

\treturn response.Data.Attributes.Data, nil
}

func SetSetupData(ctx context.Context, hostnames []string, data json.RawMessage) (err error) {''',
)

# Replace the direct controller-side setup call with a marker-backed protocol.
common = Path("internal/controller/common.go")
replace_regex(
    common,
    r'''// runSetup returns an outcome of HTTP calls, as well as\n// a retry bool showing whether operation should be retried\n// despite the error\.\n// \(for example, if there was a networking glitch\)\.\nfunc runSetup\(.*?\n}\n\nfunc runTeardown''',
    r'''type setupDataClient interface {
\tRunSetup(context.Context, string) (json.RawMessage, error)
\tGetSetupData(context.Context, string) (json.RawMessage, error)
\tSetSetupData(context.Context, []string, json.RawMessage) error
}

type k6SetupDataClient struct{}

func (k6SetupDataClient) RunSetup(ctx context.Context, hostname string) (json.RawMessage, error) {
\treturn testrun.RunSetup(ctx, hostname)
}

func (k6SetupDataClient) GetSetupData(ctx context.Context, hostname string) (json.RawMessage, error) {
\treturn testrun.GetSetupData(ctx, hostname)
}

func (k6SetupDataClient) SetSetupData(
\tctx context.Context,
\thostnames []string,
\tdata json.RawMessage,
) error {
\treturn testrun.SetSetupData(ctx, hostnames, data)
}

const setupMarkerField = "__k6_operator_setup_claim"

type setupRunOutcome int

const (
\tsetupRunSucceeded setupRunOutcome = iota
\tsetupRunRetryable
\tsetupRunFailed
\tsetupRunPending
)

func setupMarkerData(claimID string) json.RawMessage {
\tdata, err := json.Marshal(map[string]string{setupMarkerField: claimID})
\tif err != nil {
\t\tpanic(err)
\t}
\treturn data
}

func isSetupMarkerData(data json.RawMessage, claimID string) bool {
\tvar payload map[string]json.RawMessage
\tif err := json.Unmarshal(data, &payload); err != nil || len(payload) != 1 {
\t\treturn false
\t}

\trawClaimID, ok := payload[setupMarkerField]
\tif !ok {
\t\treturn false
\t}

\tvar storedClaimID string
\treturn json.Unmarshal(rawClaimID, &storedClaimID) == nil && storedClaimID == claimID
}

func prepareSetupRunner(
\tctx context.Context,
\thostnames []string,
\tclaimID string,
\tsetupClient setupDataClient,
) error {
\tif len(hostnames) == 0 {
\t\treturn errors.New("no k6 Service is available to prepare setup")
\t}

\tmarker := setupMarkerData(claimID)
\tif err := setupClient.SetSetupData(ctx, hostnames[:1], marker); err != nil {
\t\treturn fmt.Errorf("storing setup execution marker: %w", err)
\t}

\tstored, err := setupClient.GetSetupData(ctx, hostnames[0])
\tif err != nil {
\t\treturn fmt.Errorf("verifying setup execution marker: %w", err)
\t}
\tif !isSetupMarkerData(stored, claimID) {
\t\treturn errors.New("setup execution marker was not retained by the first runner")
\t}
\treturn nil
}

func recoverSetupData(
\tctx context.Context,
\thostnames []string,
\tclaimID string,
\tsetupClient setupDataClient,
) (bool, error) {
\tif len(hostnames) == 0 {
\t\treturn false, errors.New("no k6 Service is available to recover setup")
\t}

\tsetupData, err := setupClient.GetSetupData(ctx, hostnames[0])
\tif err != nil {
\t\treturn false, fmt.Errorf("reading setup recovery data: %w", err)
\t}
\tif isSetupMarkerData(setupData, claimID) {
\t\treturn false, nil
\t}

\tif err := setupClient.SetSetupData(ctx, hostnames, setupData); err != nil {
\t\treturn false, fmt.Errorf("redistributing recovered setup data: %w", err)
\t}
\treturn true, nil
}

// runSetup executes a setup that has already been fenced by a persisted
// Prepared condition and a claim-specific marker on the first runner.
func runSetup(
\tctx context.Context,
\thostnames []string,
\tclaimID string,
\tsetupClient setupDataClient,
\tlog logr.Logger,
) (setupRunOutcome, error) {
\tlog.Info("Invoking setup() on the first runner")

\tsetupData, err := setupClient.RunSetup(ctx, hostnames[0])
\tif err != nil {
\t\t// A lost HTTP response can happen after setup completed. The first
\t\t// runner's marker is overwritten only by a successful setup, so probe
\t\t// it before deciding whether the operation is safe to retry.
\t\trecovered, recoveryErr := recoverSetupData(ctx, hostnames, claimID, setupClient)
\t\tif recoveryErr != nil {
\t\t\treturn setupRunPending, errors.Join(err, recoveryErr)
\t\t}
\t\tif recovered {
\t\t\treturn setupRunSucceeded, nil
\t\t}

\t\tif strings.Contains(err.Error(), "Error executing") {
\t\t\treturn setupRunFailed, err
\t\t}
\t\treturn setupRunRetryable, err
\t}

\t// POST leaves the marker unchanged when the script has no setup export.
\t// Clearing it preserves the original undefined setup-data semantics.
\tif isSetupMarkerData(setupData, claimID) {
\t\tsetupData = nil
\t}

\tlog.Info("Sending setup data to the runners")
\tif err := setupClient.SetSetupData(ctx, hostnames, setupData); err != nil {
\t\t// The first runner remains the durable source for a later reconcile.
\t\treturn setupRunPending, err
\t}

\treturn setupRunSucceeded, nil
}

func runTeardown''',
)

# Extend the setup state machine with a durable Prepared phase and claim IDs.
start = Path("internal/controller/k6_start.go")
replace_once(start, '"net/http"\n\t"time"', '"net/http"\n\t"strings"\n\t"time"')
replace_once(
    start,
    'metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"\n\t"k8s.io/client-go/util/retry"',
    'metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"\n\t"k8s.io/apimachinery/pkg/util/uuid"\n\t"k8s.io/client-go/util/retry"',
)
replace_once(
    start,
    '''\tsetupExecutionClaimed
\tsetupExecutionInProgress''',
    '''\tsetupExecutionClaimed
\tsetupExecutionPrepared
\tsetupExecutionInProgress''',
)
replace_once(
    start,
    '''\tsetupReasonClaimed          = "SetupExecutionClaimed"
\tsetupReasonSucceeded''',
    '''\tsetupReasonClaimed          = "SetupExecutionClaimed"
\tsetupReasonPrepared         = "SetupExecutionPrepared"
\tsetupReasonSucceeded''',
)
replace_once(
    start,
    '''var errSetupExecutionConditionChanged = errors.New("setup execution condition changed")''',
    '''const setupClaimMessagePrefix = "claim-id="

var errSetupExecutionConditionChanged = errors.New("setup execution condition changed")''',
)

replace_regex(
    start,
    r'''\t// setup\n.*?\n\t// starter''',
    r'''\t// setup

\tsetupState, err := reconcileSetupExecution(ctx, log, k6, r, hostnames, k6SetupDataClient{})
\tif err != nil {
\t\treturn ctrl.Result{}, err
\t}
\tif setupState == setupExecutionFailed {
\t\treturn failSetupExecution(
\t\t\tctx,
\t\t\tk6,
\t\t\tr,
\t\t\tcloudClient,
\t\t\tlog,
\t\t\tsetupExecutionFailureMessage(k6),
\t\t)
\t}
\tif !setupExecutionAllowsStarter(setupState) {
\t\treturn res, nil
\t}

\t// starter''',
)

reconcile_code = r'''func reconcileSetupExecution(
\tctx context.Context,
\tlog logr.Logger,
\tk6 *v1alpha1.TestRun,
\tr *TestRunReconciler,
\thostnames []string,
\tsetupClient setupDataClient,
) (setupExecutionState, error) {
\tsetupState, err := claimSetupExecution(ctx, k6, r, log)
\tif err != nil {
\t\treturn setupExecutionInProgress, err
\t}

\tswitch setupState {
\tcase setupExecutionClaimed:
\t\tclaimID := setupExecutionClaimID(k6)
\t\tif claimID == "" {
\t\t\treturn setupExecutionInProgress, errors.New("setup execution claim is missing its ID")
\t\t}

\t\tif err := prepareSetupRunner(ctx, hostnames, claimID, setupClient); err != nil {
\t\t\treleaseErr := releaseSetupExecutionClaim(ctx, k6, r, log)
\t\t\tif releaseErr != nil {
\t\t\t\treturn setupExecutionInProgress, errors.Join(err, releaseErr)
\t\t\t}
\t\t\treturn setupExecutionInProgress, err
\t\t}
\t\tif err := markSetupExecutionPrepared(ctx, k6, r, log); err != nil {
\t\t\treturn setupExecutionInProgress, err
\t\t}

\t\toutcome, setupErr := runSetup(ctx, hostnames, claimID, setupClient, log)
\t\tswitch outcome {
\t\tcase setupRunSucceeded:
\t\t\treturn finishSetupExecution(ctx, k6, r, log)
\t\tcase setupRunRetryable:
\t\t\tif phaseErr := markSetupExecutionRetryableFailure(ctx, k6, r, log, setupErr); phaseErr != nil {
\t\t\t\treturn setupExecutionInProgress, errors.Join(setupErr, phaseErr)
\t\t\t}
\t\t\tif resetErr := resetSetupExecution(ctx, k6, r, log); resetErr != nil {
\t\t\t\treturn setupExecutionInProgress, errors.Join(setupErr, resetErr)
\t\t\t}
\t\t\treturn setupExecutionInProgress, setupErr
\t\tcase setupRunFailed:
\t\t\tdetail := fmt.Sprintf("setup function failed: %v", setupErr)
\t\t\tif phaseErr := markSetupExecutionFailed(
\t\t\t\tctx,
\t\t\t\tk6,
\t\t\t\tr,
\t\t\t\tlog,
\t\t\t\tsetupReasonPrepared,
\t\t\t\tdetail,
\t\t\t); phaseErr != nil {
\t\t\t\treturn setupExecutionInProgress, errors.Join(setupErr, phaseErr)
\t\t\t}
\t\t\treturn setupExecutionFailed, nil
\t\tcase setupRunPending:
\t\t\treturn setupExecutionPrepared, setupErr
\t\tdefault:
\t\t\treturn setupExecutionInProgress, errors.New("unknown setup execution outcome")
\t\t}

\tcase setupExecutionPrepared:
\t\tclaimID := setupExecutionClaimID(k6)
\t\trecovered, recoverErr := recoverSetupData(ctx, hostnames, claimID, setupClient)
\t\tif recoverErr != nil {
\t\t\treturn setupExecutionInProgress, recoverErr
\t\t}
\t\tif recovered {
\t\t\treturn finishSetupExecution(ctx, k6, r, log)
\t\t}

\t\tcondition := meta.FindStatusCondition(k6.Status.Conditions, v1alpha1.SetupExecuted)
\t\tif condition != nil && time.Since(condition.LastTransitionTime.Time) >= setupExecutionClaimTimeout {
\t\t\tdetail := fmt.Sprintf(
\t\t\t\t"prepared setup did not produce a completion marker within %s; replay is unsafe",
\t\t\t\tsetupExecutionClaimTimeout,
\t\t\t)
\t\t\tif err := markSetupExecutionFailed(
\t\t\t\tctx,
\t\t\t\tk6,
\t\t\t\tr,
\t\t\t\tlog,
\t\t\t\tsetupReasonPrepared,
\t\t\t\tdetail,
\t\t\t); err != nil {
\t\t\t\treturn setupExecutionInProgress, err
\t\t\t}
\t\t\treturn setupExecutionFailed, nil
\t\t}
\t\treturn setupExecutionInProgress, nil

\tdefault:
\t\treturn setupState, nil
\t}
}

func finishSetupExecution(
\tctx context.Context,
\tk6 *v1alpha1.TestRun,
\tr *TestRunReconciler,
\tlog logr.Logger,
) (setupExecutionState, error) {
\tif err := markSetupExecutionSucceeded(ctx, k6, r, log); err != nil {
\t\treturn setupExecutionInProgress, err
\t}
\tif err := completeSetupExecution(ctx, k6, r, log); err != nil {
\t\treturn setupExecutionInProgress, err
\t}
\treturn setupExecutionCompleted, nil
}

'''
replace_once(start, "func claimSetupExecution(\n", reconcile_code + "func claimSetupExecution(\n")

replace_regex(
    start,
    r'''func claimSetupExecution\(.*?\n}\n\nfunc markSetupExecutionSucceeded''',
    r'''func claimSetupExecution(
\tctx context.Context,
\tk6 *v1alpha1.TestRun,
\tr *TestRunReconciler,
\tlog logr.Logger,
) (setupExecutionState, error) {
\tif !v1alpha1.IsTrue(k6, v1alpha1.CloudPLZTestRun) {
\t\treturn setupExecutionNotRequired, nil
\t}

\tif condition := meta.FindStatusCondition(k6.Status.Conditions, v1alpha1.SetupExecuted); condition != nil {
\t\tswitch condition.Status {
\t\tcase metav1.ConditionTrue:
\t\t\treturn setupExecutionCompleted, nil
\t\tcase metav1.ConditionUnknown:
\t\t\tswitch condition.Reason {
\t\t\tcase setupReasonSucceeded:
\t\t\t\tif err := completeSetupExecution(ctx, k6, r, log); err != nil {
\t\t\t\t\treturn setupExecutionInProgress, err
\t\t\t\t}
\t\t\t\treturn setupExecutionCompleted, nil
\t\t\tcase setupReasonRetryableFailure:
\t\t\t\tif err := resetSetupExecution(ctx, k6, r, log); err != nil {
\t\t\t\t\treturn setupExecutionInProgress, err
\t\t\t\t}
\t\t\t\treturn setupExecutionInProgress, nil
\t\t\tcase setupReasonFailed:
\t\t\t\treturn setupExecutionFailed, nil
\t\t\tcase setupReasonPrepared:
\t\t\t\treturn setupExecutionPrepared, nil
\t\t\tcase setupReasonClaimed:
\t\t\t\tif time.Since(condition.LastTransitionTime.Time) < setupExecutionClaimTimeout {
\t\t\t\t\treturn setupExecutionInProgress, nil
\t\t\t\t}
\t\t\t\t// A setup is never invoked before the Prepared phase is durable,
\t\t\t\t// so an orphaned Claim can be released without replay risk.
\t\t\t\tif err := releaseSetupExecutionClaim(ctx, k6, r, log); err != nil {
\t\t\t\t\treturn setupExecutionInProgress, err
\t\t\t\t}
\t\t\t\treturn setupExecutionInProgress, nil
\t\t\tdefault:
\t\t\t\tif time.Since(condition.LastTransitionTime.Time) < setupExecutionClaimTimeout {
\t\t\t\t\treturn setupExecutionInProgress, nil
\t\t\t\t}
\t\t\t\tdetail := fmt.Sprintf(
\t\t\t\t\t"setup execution state %q was not completed within %s; replay is unsafe",
\t\t\t\t\tcondition.Reason,
\t\t\t\t\tsetupExecutionClaimTimeout,
\t\t\t\t)
\t\t\t\tif err := markSetupExecutionFailed(
\t\t\t\t\tctx,
\t\t\t\t\tk6,
\t\t\t\t\tr,
\t\t\t\t\tlog,
\t\t\t\t\tcondition.Reason,
\t\t\t\t\tdetail,
\t\t\t\t); err != nil {
\t\t\t\t\treturn setupExecutionInProgress, err
\t\t\t\t}
\t\t\t\treturn setupExecutionFailed, nil
\t\t\t}
\t\t}
\t}

\tclaimID := string(uuid.NewUUID())
\tbase := k6.DeepCopy()
\tsetSetupExecutionCondition(
\t\tk6,
\t\tmetav1.ConditionUnknown,
\t\tsetupReasonClaimed,
\t\tclaimID,
\t\t"setup execution is claimed",
\t)
\tif err := r.Status().Patch(
\t\tctx,
\t\tk6,
\t\tclient.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}),
\t); err != nil {
\t\tlog.Error(err, "Failed to persist setup execution claim")
\t\treturn setupExecutionInProgress, err
\t}

\treturn setupExecutionClaimed, nil
}

func markSetupExecutionSucceeded''',
)

replace_regex(
    start,
    r'''func markSetupExecutionSucceeded\(.*?\n}\n\nfunc setSetupExecutionCondition''',
    r'''func markSetupExecutionPrepared(
\tctx context.Context,
\tk6 *v1alpha1.TestRun,
\tr *TestRunReconciler,
\tlog logr.Logger,
) error {
\treturn persistSetupExecutionCondition(
\t\tctx,
\t\tk6,
\t\tr,
\t\tlog,
\t\tsetupReasonClaimed,
\t\tmetav1.ConditionUnknown,
\t\tsetupReasonPrepared,
\t\t"setup execution marker is durable; setup may start",
\t)
}

func markSetupExecutionSucceeded(
\tctx context.Context,
\tk6 *v1alpha1.TestRun,
\tr *TestRunReconciler,
\tlog logr.Logger,
) error {
\treturn persistSetupExecutionCondition(
\t\tctx,
\t\tk6,
\t\tr,
\t\tlog,
\t\tsetupReasonPrepared,
\t\tmetav1.ConditionUnknown,
\t\tsetupReasonSucceeded,
\t\t"setup completed; persisting the terminal condition",
\t)
}

func markSetupExecutionRetryableFailure(
\tctx context.Context,
\tk6 *v1alpha1.TestRun,
\tr *TestRunReconciler,
\tlog logr.Logger,
\tsetupErr error,
) error {
\treturn persistSetupExecutionCondition(
\t\tctx,
\t\tk6,
\t\tr,
\t\tlog,
\t\tsetupReasonPrepared,
\t\tmetav1.ConditionUnknown,
\t\tsetupReasonRetryableFailure,
\t\tfmt.Sprintf("retryable setup error: %v", setupErr),
\t)
}

func markSetupExecutionFailed(
\tctx context.Context,
\tk6 *v1alpha1.TestRun,
\tr *TestRunReconciler,
\tlog logr.Logger,
\texpectedReason string,
\tdetail string,
) error {
\treturn persistSetupExecutionCondition(
\t\tctx,
\t\tk6,
\t\tr,
\t\tlog,
\t\texpectedReason,
\t\tmetav1.ConditionUnknown,
\t\tsetupReasonFailed,
\t\tdetail,
\t)
}

func completeSetupExecution(
\tctx context.Context,
\tk6 *v1alpha1.TestRun,
\tr *TestRunReconciler,
\tlog logr.Logger,
) error {
\treturn persistSetupExecutionCondition(
\t\tctx,
\t\tk6,
\t\tr,
\t\tlog,
\t\tsetupReasonSucceeded,
\t\tmetav1.ConditionTrue,
\t\t"SetupExecutedTrue",
\t\t"setup completed successfully",
\t)
}

func resetSetupExecution(
\tctx context.Context,
\tk6 *v1alpha1.TestRun,
\tr *TestRunReconciler,
\tlog logr.Logger,
) error {
\treturn persistSetupExecutionCondition(
\t\tctx,
\t\tk6,
\t\tr,
\t\tlog,
\t\tsetupReasonRetryableFailure,
\t\tmetav1.ConditionFalse,
\t\t"SetupExecutedFalse",
\t\t"setup may be retried",
\t)
}

func releaseSetupExecutionClaim(
\tctx context.Context,
\tk6 *v1alpha1.TestRun,
\tr *TestRunReconciler,
\tlog logr.Logger,
) error {
\treturn persistSetupExecutionCondition(
\t\tctx,
\t\tk6,
\t\tr,
\t\tlog,
\t\tsetupReasonClaimed,
\t\tmetav1.ConditionFalse,
\t\t"SetupExecutedFalse",
\t\t"setup claim was released before execution began",
\t)
}

func persistSetupExecutionCondition(
\tctx context.Context,
\tk6 *v1alpha1.TestRun,
\tr *TestRunReconciler,
\tlog logr.Logger,
\texpectedReason string,
\tstatus metav1.ConditionStatus,
\treason string,
\tdetail string,
) error {
\tkey := k6.NamespacedName()
\texpectedClaimID := setupExecutionClaimID(k6)
\terr := retry.OnError(
\t\tretry.DefaultBackoff,
\t\tfunc(err error) bool { return !errors.Is(err, errSetupExecutionConditionChanged) },
\t\tfunc() error {
\t\t\tcurrent := &v1alpha1.TestRun{}
\t\t\tif err := r.Get(ctx, key, current); err != nil {
\t\t\t\treturn err
\t\t\t}

\t\t\tcondition := meta.FindStatusCondition(current.Status.Conditions, v1alpha1.SetupExecuted)
\t\t\tif condition == nil ||
\t\t\t\tcondition.Status != metav1.ConditionUnknown ||
\t\t\t\tcondition.Reason != expectedReason ||
\t\t\t\tsetupExecutionClaimIDFromCondition(condition) != expectedClaimID {
\t\t\t\tcurrent.DeepCopyInto(k6)
\t\t\t\treturn errSetupExecutionConditionChanged
\t\t\t}

\t\t\tbase := current.DeepCopy()
\t\t\tsetSetupExecutionCondition(current, status, reason, expectedClaimID, detail)
\t\t\tif err := r.Status().Patch(
\t\t\t\tctx,
\t\t\t\tcurrent,
\t\t\t\tclient.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}),
\t\t\t); err != nil {
\t\t\t\treturn err
\t\t\t}

\t\t\tcurrent.DeepCopyInto(k6)
\t\t\treturn nil
\t\t},
\t)
\tif err != nil {
\t\tlog.Error(err, "Failed to update setup execution condition", "status", status, "reason", reason)
\t\treturn fmt.Errorf("updating setup execution condition to %s/%s: %w", status, reason, err)
\t}
\treturn nil
}

func setSetupExecutionCondition''',
)

replace_regex(
    start,
    r'''func setSetupExecutionCondition\(.*?\n}\n\nfunc setupExecutionFailureMessage''',
    r'''func setSetupExecutionCondition(
\tk6 *v1alpha1.TestRun,
\tstatus metav1.ConditionStatus,
\treason string,
\tclaimID string,
\tdetail string,
) {
\tmeta.SetStatusCondition(&k6.GetStatus().Conditions, metav1.Condition{
\t\tType:               v1alpha1.SetupExecuted,
\t\tStatus:             status,
\t\tObservedGeneration: k6.Generation,
\t\tLastTransitionTime: metav1.Now(),
\t\tReason:             reason,
\t\tMessage:            setupExecutionConditionMessage(claimID, detail),
\t})
}

func setupExecutionConditionMessage(claimID string, detail string) string {
\tif claimID == "" {
\t\treturn detail
\t}
\tif detail == "" {
\t\treturn setupClaimMessagePrefix + claimID
\t}
\treturn setupClaimMessagePrefix + claimID + "; " + detail
}

func setupExecutionClaimID(k6 *v1alpha1.TestRun) string {
\treturn setupExecutionClaimIDFromCondition(
\t\tmeta.FindStatusCondition(k6.Status.Conditions, v1alpha1.SetupExecuted),
\t)
}

func setupExecutionClaimIDFromCondition(condition *metav1.Condition) string {
\tif condition == nil {
\t\treturn ""
\t}
\tremainder, ok := strings.CutPrefix(condition.Message, setupClaimMessagePrefix)
\tif !ok {
\t\treturn ""
\t}
\tclaimID, _, _ := strings.Cut(remainder, ";")
\treturn strings.TrimSpace(claimID)
}

func setupExecutionConditionDetail(condition *metav1.Condition) string {
\tif condition == nil {
\t\treturn ""
\t}
\tif _, detail, ok := strings.Cut(condition.Message, "; "); ok {
\t\treturn detail
\t}
\tif strings.HasPrefix(condition.Message, setupClaimMessagePrefix) {
\t\treturn ""
\t}
\treturn condition.Message
}

func setupExecutionFailureMessage''',
)

replace_once(
    start,
    '''\tcondition := meta.FindStatusCondition(k6.Status.Conditions, v1alpha1.SetupExecuted)
\tif condition != nil && condition.Message != "" {
\t\treturn condition.Message
\t}''',
    '''\tcondition := meta.FindStatusCondition(k6.Status.Conditions, v1alpha1.SetupExecuted)
\tif detail := setupExecutionConditionDetail(condition); detail != "" {
\t\treturn detail
\t}''',
)

# Update existing tests for the Prepared phase and add crash-recovery coverage.
test = Path("internal/controller/k6_start_test.go")
replace_once(test, '"context"\n\t"errors"', '"context"\n\t"encoding/json"\n\t"errors"')

prepare_transition = '''
\tif err := markSetupExecutionPrepared(ctx, current, r, logr.Discard()); err != nil {
\t\tt.Fatalf("markSetupExecutionPrepared() error = %v", err)
\t}
'''
replace_once(
    test,
    '''\tif state != setupExecutionClaimed {
\t\tt.Fatalf("setup execution state = %v, want %v", state, setupExecutionClaimed)
\t}

\tif err := markSetupExecutionRetryableFailure(''',
    '''\tif state != setupExecutionClaimed {
\t\tt.Fatalf("setup execution state = %v, want %v", state, setupExecutionClaimed)
\t}
''' + prepare_transition + '''
\tif err := markSetupExecutionRetryableFailure(''',
)
replace_once(
    test,
    '''\tif state != setupExecutionClaimed {
\t\tt.Fatalf("setup execution state = %v, want %v", state, setupExecutionClaimed)
\t}

\tif err := markSetupExecutionSucceeded(ctx, current, r, logr.Discard()); err != nil {''',
    '''\tif state != setupExecutionClaimed {
\t\tt.Fatalf("setup execution state = %v, want %v", state, setupExecutionClaimed)
\t}
''' + prepare_transition + '''
\tif err := markSetupExecutionSucceeded(ctx, current, r, logr.Discard()); err != nil {''',
)
replace_once(
    test,
    '''\tif state != setupExecutionClaimed {
\t\tt.Fatalf("setup execution state = %v, want %v", state, setupExecutionClaimed)
\t}
\tstaleClaimHolder := claimHolder.DeepCopy()

\ttimeoutReconciler''',
    '''\tif state != setupExecutionClaimed {
\t\tt.Fatalf("setup execution state = %v, want %v", state, setupExecutionClaimed)
\t}
\tif err := markSetupExecutionPrepared(ctx, claimHolder, r, logr.Discard()); err != nil {
\t\tt.Fatalf("markSetupExecutionPrepared() error = %v", err)
\t}
\tstaleClaimHolder := claimHolder.DeepCopy()

\ttimeoutReconciler''',
)
replace_once(test, '\t\tsetupReasonClaimed,\n\t\t"setup execution claim timed out",', '\t\tsetupReasonPrepared,\n\t\t"setup execution claim timed out",')

replace_regex(
    test,
    r'''func TestClaimSetupExecutionExpiresOrphanedClaim\(t \*testing\.T\) \{.*?\n}\n\nfunc TestSetupExecutionAllowsStarter''',
    r'''func TestClaimSetupExecutionReleasesOrphanedUnpreparedClaim(t *testing.T) {
\tt.Parallel()

\tctx := context.Background()
\ttestRun := setupConditionTestRun(true, conditionStatus(metav1.ConditionUnknown))
\tsetSetupTestCondition(
\t\tt,
\t\ttestRun,
\t\tmetav1.ConditionUnknown,
\t\tsetupReasonClaimed,
\t\ttime.Now().Add(-setupExecutionClaimTimeout-time.Minute),
\t)
\tr, objectKey := setupConditionTestReconciler(t, testRun)
\tcurrent := getSetupConditionTestRun(t, ctx, r, objectKey)

\tstate, err := claimSetupExecution(ctx, current, r, logr.Discard())
\tif err != nil {
\t\tt.Fatalf("claimSetupExecution() error = %v", err)
\t}
\tif state != setupExecutionInProgress {
\t\tt.Fatalf("state = %v, want %v", state, setupExecutionInProgress)
\t}

\tpersisted := getSetupConditionTestRun(t, ctx, r, objectKey)
\tif !v1alpha1.IsFalse(persisted, v1alpha1.SetupExecuted) {
\t\tt.Error("orphaned pre-execution claim was not released")
\t}
}

type fakeSetupDataClient struct {
\tdataByHostname map[string]json.RawMessage
\trunData        json.RawMessage
\trunErr         error
}

func newFakeSetupDataClient() *fakeSetupDataClient {
\treturn &fakeSetupDataClient{dataByHostname: make(map[string]json.RawMessage)}
}

func (f *fakeSetupDataClient) RunSetup(_ context.Context, hostname string) (json.RawMessage, error) {
\tif f.runErr != nil {
\t\treturn nil, f.runErr
\t}
\tf.dataByHostname[hostname] = append(json.RawMessage(nil), f.runData...)
\treturn append(json.RawMessage(nil), f.runData...), nil
}

func (f *fakeSetupDataClient) GetSetupData(
\t_ context.Context,
\thostname string,
) (json.RawMessage, error) {
\treturn append(json.RawMessage(nil), f.dataByHostname[hostname]...), nil
}

func (f *fakeSetupDataClient) SetSetupData(
\t_ context.Context,
\thostnames []string,
\tdata json.RawMessage,
) error {
\tfor _, hostname := range hostnames {
\t\tf.dataByHostname[hostname] = append(json.RawMessage(nil), data...)
\t}
\treturn nil
}

func TestPreparedSetupRecoversAfterClaimHolderExit(t *testing.T) {
\tt.Parallel()

\ttests := []struct {
\t\tname      string
\t\tsetupData json.RawMessage
\t}{
\t\t{name: "object setup data", setupData: json.RawMessage(`{"value":1}`)},
\t\t{name: "undefined setup data", setupData: nil},
\t}

\tfor _, tt := range tests {
\t\tt.Run(tt.name, func(t *testing.T) {
\t\t\tt.Parallel()

\t\t\tctx := context.Background()
\t\t\tr, objectKey := setupConditionTestReconciler(
\t\t\t\tt,
\t\t\t\tsetupConditionTestRun(true, conditionStatus(metav1.ConditionFalse)),
\t\t\t)
\t\t\tclaimHolder := getSetupConditionTestRun(t, ctx, r, objectKey)
\t\t\tstate, err := claimSetupExecution(ctx, claimHolder, r, logr.Discard())
\t\t\tif err != nil {
\t\t\t\tt.Fatalf("claimSetupExecution() error = %v", err)
\t\t\t}
\t\t\tif state != setupExecutionClaimed {
\t\t\t\tt.Fatalf("state = %v, want %v", state, setupExecutionClaimed)
\t\t\t}

\t\t\thostnames := []string{"runner-0", "runner-1"}
\t\t\tsetupClient := newFakeSetupDataClient()
\t\t\tclaimID := setupExecutionClaimID(claimHolder)
\t\t\tif err := prepareSetupRunner(ctx, hostnames, claimID, setupClient); err != nil {
\t\t\t\tt.Fatalf("prepareSetupRunner() error = %v", err)
\t\t\t}
\t\t\tif err := markSetupExecutionPrepared(ctx, claimHolder, r, logr.Discard()); err != nil {
\t\t\t\tt.Fatalf("markSetupExecutionPrepared() error = %v", err)
\t\t\t}

\t\t\t// Simulate setup returning successfully and the claim holder exiting
\t\t\t// before it can persist SetupExecutionSucceeded.
\t\t\tsetupClient.dataByHostname[hostnames[0]] = append(json.RawMessage(nil), tt.setupData...)

\t\t\treconciled := getSetupConditionTestRun(t, ctx, r, objectKey)
\t\t\tstate, err = reconcileSetupExecution(
\t\t\t\tctx,
\t\t\t\tlogr.Discard(),
\t\t\t\treconciled,
\t\t\t\tr,
\t\t\t\thostnames,
\t\t\t\tsetupClient,
\t\t\t)
\t\t\tif err != nil {
\t\t\t\tt.Fatalf("reconcileSetupExecution() error = %v", err)
\t\t\t}
\t\t\tif state != setupExecutionCompleted {
\t\t\t\tt.Fatalf("state = %v, want %v", state, setupExecutionCompleted)
\t\t\t}

\t\t\tpersisted := getSetupConditionTestRun(t, ctx, r, objectKey)
\t\t\tif !v1alpha1.IsTrue(persisted, v1alpha1.SetupExecuted) {
\t\t\t\tt.Error("recovered setup was not marked complete")
\t\t\t}
\t\t\tfor _, hostname := range hostnames {
\t\t\t\tif string(setupClient.dataByHostname[hostname]) != string(tt.setupData) {
\t\t\t\t\tt.Errorf(
\t\t\t\t\t\t"setup data for %s = %q, want %q",
\t\t\t\t\t\thostname,
\t\t\t\t\t\tsetupClient.dataByHostname[hostname],
\t\t\t\t\t\tt.setupData,
\t\t\t\t\t)
\t\t\t\t}
\t\t\t}
\t\t})
\t}
}

func TestSetupExecutionAllowsStarter''',
)
replace_once(
    test,
    '''\t\t{state: setupExecutionClaimed, want: false},
\t\t{state: setupExecutionInProgress, want: false},''',
    '''\t\t{state: setupExecutionClaimed, want: false},
\t\t{state: setupExecutionPrepared, want: false},
\t\t{state: setupExecutionInProgress, want: false},''',
)
