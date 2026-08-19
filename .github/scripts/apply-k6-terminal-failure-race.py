from pathlib import Path


def replace_once(path: Path, old: str, new: str, label: str) -> None:
    text = path.read_text(encoding="utf-8")
    if old in text:
        count = text.count(old)
        if count != 1:
            raise SystemExit(f"{path}: expected one {label} anchor, found {count}")
        path.write_text(text.replace(old, new, 1), encoding="utf-8")
        return
    if new not in text:
        raise SystemExit(f"{path}: neither original nor patched {label} form was found")


source = Path("internal/controller/k6_start.go")

replace_once(
    source,
    '''const (
\tsetupExecutionClaimTimeout = 30 * time.Minute

\tsetupReasonClaimed          = "SetupExecutionClaimed"
\tsetupReasonSucceeded        = "SetupExecutionSucceeded"
\tsetupReasonRetryableFailure = "SetupExecutionRetryableFailure"
\tsetupReasonFailed           = "SetupExecutionFailed"
)
''',
    '''const (
\tsetupExecutionClaimTimeout = 30 * time.Minute

\tsetupReasonClaimed          = "SetupExecutionClaimed"
\tsetupReasonSucceeded        = "SetupExecutionSucceeded"
\tsetupReasonRetryableFailure = "SetupExecutionRetryableFailure"
\tsetupReasonFailed           = "SetupExecutionFailed"
)

var errSetupExecutionConditionChanged = errors.New("setup execution condition changed")
''',
    "transition sentinel",
)

replace_once(
    source,
    'if phaseErr := markSetupExecutionFailed(ctx, k6, r, log, detail); phaseErr != nil {',
    'if phaseErr := markSetupExecutionFailed(ctx, k6, r, log, setupReasonClaimed, detail); phaseErr != nil {',
    "claim-holder failure transition",
)

replace_once(
    source,
    'if err := markSetupExecutionFailed(ctx, k6, r, log, detail); err != nil {',
    'if err := markSetupExecutionFailed(ctx, k6, r, log, condition.Reason, detail); err != nil {',
    "timeout failure transition",
)

replace_once(
    source,
    '''\treturn persistSetupExecutionCondition(
\t\tctx,
\t\tk6,
\t\tr,
\t\tlog,
\t\tmetav1.ConditionUnknown,
\t\tsetupReasonSucceeded,
\t\t"setup completed; persisting the terminal condition",
\t)
''',
    '''\treturn persistSetupExecutionCondition(
\t\tctx,
\t\tk6,
\t\tr,
\t\tlog,
\t\tsetupReasonClaimed,
\t\tmetav1.ConditionUnknown,
\t\tsetupReasonSucceeded,
\t\t"setup completed; persisting the terminal condition",
\t)
''',
    "success phase transition",
)

replace_once(
    source,
    '''\treturn persistSetupExecutionCondition(
\t\tctx,
\t\tk6,
\t\tr,
\t\tlog,
\t\tmetav1.ConditionUnknown,
\t\tsetupReasonRetryableFailure,
\t\tfmt.Sprintf("retryable setup error: %v", setupErr),
\t)
''',
    '''\treturn persistSetupExecutionCondition(
\t\tctx,
\t\tk6,
\t\tr,
\t\tlog,
\t\tsetupReasonClaimed,
\t\tmetav1.ConditionUnknown,
\t\tsetupReasonRetryableFailure,
\t\tfmt.Sprintf("retryable setup error: %v", setupErr),
\t)
''',
    "retryable phase transition",
)

replace_once(
    source,
    '''func markSetupExecutionFailed(
\tctx context.Context,
\tk6 *v1alpha1.TestRun,
\tr *TestRunReconciler,
\tlog logr.Logger,
\tdetail string,
) error {
\treturn persistSetupExecutionCondition(
\t\tctx,
\t\tk6,
\t\tr,
\t\tlog,
\t\tmetav1.ConditionUnknown,
\t\tsetupReasonFailed,
\t\tdetail,
\t)
}
''',
    '''func markSetupExecutionFailed(
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
''',
    "failure phase helper",
)

replace_once(
    source,
    '''\treturn persistSetupExecutionCondition(
\t\tctx,
\t\tk6,
\t\tr,
\t\tlog,
\t\tmetav1.ConditionTrue,
\t\t"SetupExecutedTrue",
\t\t"setup completed successfully",
\t)
''',
    '''\treturn persistSetupExecutionCondition(
\t\tctx,
\t\tk6,
\t\tr,
\t\tlog,
\t\tsetupReasonSucceeded,
\t\tmetav1.ConditionTrue,
\t\t"SetupExecutedTrue",
\t\t"setup completed successfully",
\t)
''',
    "completion transition",
)

replace_once(
    source,
    '''\treturn persistSetupExecutionCondition(
\t\tctx,
\t\tk6,
\t\tr,
\t\tlog,
\t\tmetav1.ConditionFalse,
\t\t"SetupExecutedFalse",
\t\t"setup may be retried",
\t)
''',
    '''\treturn persistSetupExecutionCondition(
\t\tctx,
\t\tk6,
\t\tr,
\t\tlog,
\t\tsetupReasonRetryableFailure,
\t\tmetav1.ConditionFalse,
\t\t"SetupExecutedFalse",
\t\t"setup may be retried",
\t)
''',
    "retry reset transition",
)

replace_once(
    source,
    '''func persistSetupExecutionCondition(
\tctx context.Context,
\tk6 *v1alpha1.TestRun,
\tr *TestRunReconciler,
\tlog logr.Logger,
\tstatus metav1.ConditionStatus,
\treason string,
\tmessage string,
) error {
''',
    '''func persistSetupExecutionCondition(
\tctx context.Context,
\tk6 *v1alpha1.TestRun,
\tr *TestRunReconciler,
\tlog logr.Logger,
\texpectedReason string,
\tstatus metav1.ConditionStatus,
\treason string,
\tmessage string,
) error {
''',
    "persist helper signature",
)

replace_once(
    source,
    'err := retry.OnError(retry.DefaultBackoff, func(error) bool { return true }, func() error {',
    'err := retry.OnError(retry.DefaultBackoff, func(err error) bool { return !errors.Is(err, errSetupExecutionConditionChanged) }, func() error {',
    "retry predicate",
)

replace_once(
    source,
    '''\t\tif err := r.Get(ctx, key, current); err != nil {
\t\t\treturn err
\t\t}

\t\tbase := current.DeepCopy()
''',
    '''\t\tif err := r.Get(ctx, key, current); err != nil {
\t\t\treturn err
\t\t}

\t\tcondition := meta.FindStatusCondition(current.Status.Conditions, v1alpha1.SetupExecuted)
\t\tif condition == nil ||
\t\t\tcondition.Status != metav1.ConditionUnknown ||
\t\t\tcondition.Reason != expectedReason {
\t\t\tcurrent.DeepCopyInto(k6)
\t\t\treturn errSetupExecutionConditionChanged
\t\t}

\t\tbase := current.DeepCopy()
''',
    "compare-and-set precondition",
)


tests = Path("internal/controller/k6_start_test.go")

replace_once(
    tests,
    '''import (
\t"context"
''',
    '''import (
\t"context"
\t"errors"
''',
    "test errors import",
)

replace_once(
    tests,
    '''\tif err := resetSetupExecution(ctx, current, r, logr.Discard()); err != nil {
\t\tt.Fatalf("resetSetupExecution() error = %v", err)
\t}
''',
    '''\tif err := markSetupExecutionRetryableFailure(
\t\tctx,
\t\tcurrent,
\t\tr,
\t\tlogr.Discard(),
\t\terrors.New("temporary network error"),
\t); err != nil {
\t\tt.Fatalf("markSetupExecutionRetryableFailure() error = %v", err)
\t}

\tif err := resetSetupExecution(ctx, current, r, logr.Discard()); err != nil {
\t\tt.Fatalf("resetSetupExecution() error = %v", err)
\t}
''',
    "retry setup test phase",
)

replace_once(
    tests,
    '''\tif err := completeSetupExecution(ctx, current, r, logr.Discard()); err != nil {
\t\tt.Fatalf("completeSetupExecution() error = %v", err)
\t}
''',
    '''\tif err := markSetupExecutionSucceeded(ctx, current, r, logr.Discard()); err != nil {
\t\tt.Fatalf("markSetupExecutionSucceeded() error = %v", err)
\t}

\tif err := completeSetupExecution(ctx, current, r, logr.Discard()); err != nil {
\t\tt.Fatalf("completeSetupExecution() error = %v", err)
\t}
''',
    "successful setup test phase",
)

replace_once(
    tests,
    '''func TestClaimSetupExecutionRecoversPersistedPhases(t *testing.T) {
''',
    '''func TestClaimHolderCannotOverwriteTerminalFailure(t *testing.T) {
\tt.Parallel()

\tctx := context.Background()
\tr, objectKey := setupConditionTestReconciler(
\t\tt,
\t\tsetupConditionTestRun(true, conditionStatus(metav1.ConditionFalse)),
\t)
\tclaimHolder := getSetupConditionTestRun(t, ctx, r, objectKey)

\tstate, err := claimSetupExecution(ctx, claimHolder, r, logr.Discard())
\tif err != nil {
\t\tt.Fatalf("claimSetupExecution() error = %v", err)
\t}
\tif state != setupExecutionClaimed {
\t\tt.Fatalf("setup execution state = %v, want %v", state, setupExecutionClaimed)
\t}
\tstaleClaimHolder := claimHolder.DeepCopy()

\ttimeoutReconciler := getSetupConditionTestRun(t, ctx, r, objectKey)
\tif err := markSetupExecutionFailed(
\t\tctx,
\t\ttimeoutReconciler,
\t\tr,
\t\tlogr.Discard(),
\t\tsetupReasonClaimed,
\t\t"setup execution claim timed out",
\t); err != nil {
\t\tt.Fatalf("markSetupExecutionFailed() error = %v", err)
\t}

\terr = markSetupExecutionSucceeded(ctx, staleClaimHolder, r, logr.Discard())
\tif !errors.Is(err, errSetupExecutionConditionChanged) {
\t\tt.Fatalf("markSetupExecutionSucceeded() error = %v, want state-change rejection", err)
\t}

\tpersisted := getSetupConditionTestRun(t, ctx, r, objectKey)
\tcondition := meta.FindStatusCondition(persisted.Status.Conditions, v1alpha1.SetupExecuted)
\tif condition == nil {
\t\tt.Fatal("SetupExecuted condition is missing")
\t}
\tif condition.Status != metav1.ConditionUnknown {
\t\tt.Errorf("status = %v, want %v", condition.Status, metav1.ConditionUnknown)
\t}
\tif condition.Reason != setupReasonFailed {
\t\tt.Errorf("reason = %q, want %q", condition.Reason, setupReasonFailed)
\t}

\tstate, err = claimSetupExecution(ctx, persisted, r, logr.Discard())
\tif err != nil {
\t\tt.Fatalf("claimSetupExecution() after terminal failure error = %v", err)
\t}
\tif state != setupExecutionFailed {
\t\tt.Errorf("state = %v, want %v", state, setupExecutionFailed)
\t}
\tif setupExecutionAllowsStarter(state) {
\t\tt.Error("terminal setup failure unexpectedly allowed starter creation")
\t}
}

func TestClaimSetupExecutionRecoversPersistedPhases(t *testing.T) {
''',
    "terminal failure race regression test",
)
