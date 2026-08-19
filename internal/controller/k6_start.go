package controllers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/grafana/k6-operator/api/v1alpha1"
	"github.com/grafana/k6-operator/pkg/cloud"
	"github.com/grafana/k6-operator/pkg/resources/jobs"
	"go.k6.io/k6/v2/cloudapi"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type setupExecutionState int

const (
	setupExecutionNotRequired setupExecutionState = iota
	setupExecutionClaimed
	setupExecutionPrepared
	setupExecutionInProgress
	setupExecutionCompleted
	setupExecutionFailed
)

const (
	setupExecutionClaimTimeout = 30 * time.Minute

	setupReasonClaimed          = "SetupExecutionClaimed"
	setupReasonPrepared         = "SetupExecutionPrepared"
	setupReasonSucceeded        = "SetupExecutionSucceeded"
	setupReasonRetryableFailure = "SetupExecutionRetryableFailure"
	setupReasonFailed           = "SetupExecutionFailed"
)

const setupClaimMessagePrefix = "claim-id="

var errSetupExecutionConditionChanged = errors.New("setup execution condition changed")

func setupExecutionAllowsStarter(state setupExecutionState) bool {
	return state == setupExecutionNotRequired || state == setupExecutionCompleted
}

func isServiceReady(log logr.Logger, service *v1.Service) bool {
	resp, err := http.Get(fmt.Sprintf("http://%v:6565/v1/status", service.Spec.ClusterIP))

	if err != nil {
		log.Error(err, fmt.Sprintf("failed to get status from %v", service.Name))
		return false
	}
	defer resp.Body.Close() //nolint:errcheck

	return resp.StatusCode < 400
}

// StartJobs in the Ready phase using a curl container
func StartJobs(ctx context.Context, log logr.Logger, k6 *v1alpha1.TestRun, r *TestRunReconciler, cloudClient *cloudapi.Client) (res ctrl.Result, err error) {
	// It may take some time to get Services up, so check in frequently
	res = ctrl.Result{RequeueAfter: time.Second}

	if len(k6.GetStatus().TestRunID) > 0 {
		log = log.WithValues("testRunId", k6.GetStatus().TestRunID)
	}

	log.Info("Waiting for pods to get ready")

	opts := k6.ListOptions()

	pl := &v1.PodList{}
	if err = r.List(ctx, pl, opts); err != nil {
		log.Error(err, "Could not list pods")
		return res, nil
	}

	var count int
	for _, pod := range pl.Items {
		if pod.Status.Phase != "Running" {
			continue
		}
		count++
	}

	log.Info(fmt.Sprintf("%d/%d runner pods ready", count, k6.GetSpec().Parallelism))

	if count != int(k6.GetSpec().Parallelism) {
		if t, ok := v1alpha1.LastUpdate(k6, v1alpha1.TestRunRunning); !ok {
			// this should never happen
			return res, errors.New("cannot find condition TestRunRunning")
		} else if time.Since(t).Minutes() > 5 {
			msg := fmt.Sprintf(errMessageTooLong, "runner pods", "runner jobs and pods")
			log.Info(msg)

			if v1alpha1.IsTrue(k6, v1alpha1.CloudTestRun) {
				events := cloud.ErrorEvent(cloud.K6OperatorStartError).
					WithDetail(msg).
					WithAbort()
				cloud.SendTestRunEvents(cloudClient, k6.TestRunID(), log, events)
			}
		}

		return res, nil
	}

	// services

	log.Info("Waiting for services to get ready")

	hostnames, err := r.hostnames(ctx, log, true, opts)
	log.Info(fmt.Sprintf("err: %v, hostnames: %v", err, hostnames))
	if err != nil {
		return ctrl.Result{}, err
	}

	log.Info(fmt.Sprintf("%d/%d services ready", len(hostnames), k6.GetSpec().Parallelism))

	// setup

	setupState, err := reconcileSetupExecution(ctx, log, k6, r, hostnames, k6SetupDataClient{})
	if err != nil {
		return ctrl.Result{}, err
	}
	if setupState == setupExecutionFailed {
		return failSetupExecution(
			ctx,
			k6,
			r,
			cloudClient,
			log,
			setupExecutionFailureMessage(k6),
		)
	}
	if !setupExecutionAllowsStarter(setupState) {
		return res, nil
	}

	// starter

	starter := jobs.NewStarterJob(k6, hostnames)

	if err = ctrl.SetControllerReference(k6, starter, r.Scheme); err != nil {
		log.Error(err, "Failed to set controller reference for the start job")
	}

	created, err := createJobIfNotExists(ctx, r.Client, starter)
	if err != nil {
		log.Error(err, "Failed to launch k6 test starter")
		return res, nil
	}

	if created {
		log.Info("Created starter job")
	} else {
		log.Info("Starter job already exists")
	}

	log.Info("Changing stage of TestRun status to started")
	k6.GetStatus().Stage = "started"
	v1alpha1.UpdateCondition(k6, v1alpha1.TestRunRunning, metav1.ConditionTrue)

	if updateHappened, err := r.UpdateStatus(ctx, k6, log); err != nil {
		return ctrl.Result{}, err
	} else if updateHappened {
		return ctrl.Result{Requeue: true}, nil
	}
	return ctrl.Result{}, nil
}

func reconcileSetupExecution(
	ctx context.Context,
	log logr.Logger,
	k6 *v1alpha1.TestRun,
	r *TestRunReconciler,
	hostnames []string,
	setupClient setupDataClient,
) (setupExecutionState, error) {
	setupState, err := claimSetupExecution(ctx, k6, r, log)
	if err != nil {
		return setupExecutionInProgress, err
	}

	switch setupState {
	case setupExecutionClaimed:
		claimID := setupExecutionClaimID(k6)
		if claimID == "" {
			return setupExecutionInProgress, errors.New("setup execution claim is missing its ID")
		}

		if err := prepareSetupRunner(ctx, hostnames, claimID, setupClient); err != nil {
			releaseErr := releaseSetupExecutionClaim(ctx, k6, r, log)
			if releaseErr != nil {
				return setupExecutionInProgress, errors.Join(err, releaseErr)
			}
			return setupExecutionInProgress, err
		}
		if err := markSetupExecutionPrepared(ctx, k6, r, log); err != nil {
			return setupExecutionInProgress, err
		}

		outcome, setupErr := runSetup(ctx, hostnames, claimID, setupClient, log)
		switch outcome {
		case setupRunSucceeded:
			return finishSetupExecution(ctx, k6, r, log)
		case setupRunRetryable:
			if phaseErr := markSetupExecutionRetryableFailure(ctx, k6, r, log, setupErr); phaseErr != nil {
				return setupExecutionInProgress, errors.Join(setupErr, phaseErr)
			}
			if resetErr := resetSetupExecution(ctx, k6, r, log); resetErr != nil {
				return setupExecutionInProgress, errors.Join(setupErr, resetErr)
			}
			return setupExecutionInProgress, setupErr
		case setupRunFailed:
			detail := fmt.Sprintf("setup function failed: %v", setupErr)
			if phaseErr := markSetupExecutionFailed(
				ctx,
				k6,
				r,
				log,
				setupReasonPrepared,
				detail,
			); phaseErr != nil {
				return setupExecutionInProgress, errors.Join(setupErr, phaseErr)
			}
			return setupExecutionFailed, nil
		case setupRunPending:
			return setupExecutionPrepared, setupErr
		default:
			return setupExecutionInProgress, errors.New("unknown setup execution outcome")
		}

	case setupExecutionPrepared:
		claimID := setupExecutionClaimID(k6)
		recovered, recoverErr := recoverSetupData(ctx, hostnames, claimID, setupClient)
		if recoverErr != nil {
			return setupExecutionInProgress, recoverErr
		}
		if recovered {
			return finishSetupExecution(ctx, k6, r, log)
		}

		condition := meta.FindStatusCondition(k6.Status.Conditions, v1alpha1.SetupExecuted)
		if condition != nil && time.Since(condition.LastTransitionTime.Time) >= setupExecutionClaimTimeout {
			detail := fmt.Sprintf(
				"prepared setup did not produce a completion marker within %s; replay is unsafe",
				setupExecutionClaimTimeout,
			)
			if err := markSetupExecutionFailed(
				ctx,
				k6,
				r,
				log,
				setupReasonPrepared,
				detail,
			); err != nil {
				return setupExecutionInProgress, err
			}
			return setupExecutionFailed, nil
		}
		return setupExecutionInProgress, nil

	default:
		return setupState, nil
	}
}

func finishSetupExecution(
	ctx context.Context,
	k6 *v1alpha1.TestRun,
	r *TestRunReconciler,
	log logr.Logger,
) (setupExecutionState, error) {
	if err := markSetupExecutionSucceeded(ctx, k6, r, log); err != nil {
		return setupExecutionInProgress, err
	}
	if err := completeSetupExecution(ctx, k6, r, log); err != nil {
		return setupExecutionInProgress, err
	}
	return setupExecutionCompleted, nil
}

func claimSetupExecution(
	ctx context.Context,
	k6 *v1alpha1.TestRun,
	r *TestRunReconciler,
	log logr.Logger,
) (setupExecutionState, error) {
	if !v1alpha1.IsTrue(k6, v1alpha1.CloudPLZTestRun) {
		return setupExecutionNotRequired, nil
	}

	if condition := meta.FindStatusCondition(k6.Status.Conditions, v1alpha1.SetupExecuted); condition != nil {
		switch condition.Status {
		case metav1.ConditionTrue:
			return setupExecutionCompleted, nil
		case metav1.ConditionUnknown:
			switch condition.Reason {
			case setupReasonSucceeded:
				if err := completeSetupExecution(ctx, k6, r, log); err != nil {
					return setupExecutionInProgress, err
				}
				return setupExecutionCompleted, nil
			case setupReasonRetryableFailure:
				if err := resetSetupExecution(ctx, k6, r, log); err != nil {
					return setupExecutionInProgress, err
				}
				return setupExecutionInProgress, nil
			case setupReasonFailed:
				return setupExecutionFailed, nil
			case setupReasonPrepared:
				return setupExecutionPrepared, nil
			case setupReasonClaimed:
				if time.Since(condition.LastTransitionTime.Time) < setupExecutionClaimTimeout {
					return setupExecutionInProgress, nil
				}
				// A setup is never invoked before the Prepared phase is durable,
				// so an orphaned Claim can be released without replay risk.
				if err := releaseSetupExecutionClaim(ctx, k6, r, log); err != nil {
					return setupExecutionInProgress, err
				}
				return setupExecutionInProgress, nil
			default:
				if time.Since(condition.LastTransitionTime.Time) < setupExecutionClaimTimeout {
					return setupExecutionInProgress, nil
				}
				detail := fmt.Sprintf(
					"setup execution state %q was not completed within %s; replay is unsafe",
					condition.Reason,
					setupExecutionClaimTimeout,
				)
				if err := markSetupExecutionFailed(
					ctx,
					k6,
					r,
					log,
					condition.Reason,
					detail,
				); err != nil {
					return setupExecutionInProgress, err
				}
				return setupExecutionFailed, nil
			}
		}
	}

	claimID := string(uuid.NewUUID())
	base := k6.DeepCopy()
	setSetupExecutionCondition(
		k6,
		metav1.ConditionUnknown,
		setupReasonClaimed,
		claimID,
		"setup execution is claimed",
	)
	if err := r.Status().Patch(
		ctx,
		k6,
		client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}),
	); err != nil {
		log.Error(err, "Failed to persist setup execution claim")
		return setupExecutionInProgress, err
	}

	return setupExecutionClaimed, nil
}

func markSetupExecutionPrepared(
	ctx context.Context,
	k6 *v1alpha1.TestRun,
	r *TestRunReconciler,
	log logr.Logger,
) error {
	return persistSetupExecutionCondition(
		ctx,
		k6,
		r,
		log,
		setupReasonClaimed,
		metav1.ConditionUnknown,
		setupReasonPrepared,
		"setup execution marker is durable; setup may start",
	)
}

func markSetupExecutionSucceeded(
	ctx context.Context,
	k6 *v1alpha1.TestRun,
	r *TestRunReconciler,
	log logr.Logger,
) error {
	return persistSetupExecutionCondition(
		ctx,
		k6,
		r,
		log,
		setupReasonPrepared,
		metav1.ConditionUnknown,
		setupReasonSucceeded,
		"setup completed; persisting the terminal condition",
	)
}

func markSetupExecutionRetryableFailure(
	ctx context.Context,
	k6 *v1alpha1.TestRun,
	r *TestRunReconciler,
	log logr.Logger,
	setupErr error,
) error {
	return persistSetupExecutionCondition(
		ctx,
		k6,
		r,
		log,
		setupReasonPrepared,
		metav1.ConditionUnknown,
		setupReasonRetryableFailure,
		fmt.Sprintf("retryable setup error: %v", setupErr),
	)
}

func markSetupExecutionFailed(
	ctx context.Context,
	k6 *v1alpha1.TestRun,
	r *TestRunReconciler,
	log logr.Logger,
	expectedReason string,
	detail string,
) error {
	return persistSetupExecutionCondition(
		ctx,
		k6,
		r,
		log,
		expectedReason,
		metav1.ConditionUnknown,
		setupReasonFailed,
		detail,
	)
}

func completeSetupExecution(
	ctx context.Context,
	k6 *v1alpha1.TestRun,
	r *TestRunReconciler,
	log logr.Logger,
) error {
	return persistSetupExecutionCondition(
		ctx,
		k6,
		r,
		log,
		setupReasonSucceeded,
		metav1.ConditionTrue,
		"SetupExecutedTrue",
		"setup completed successfully",
	)
}

func resetSetupExecution(
	ctx context.Context,
	k6 *v1alpha1.TestRun,
	r *TestRunReconciler,
	log logr.Logger,
) error {
	return persistSetupExecutionCondition(
		ctx,
		k6,
		r,
		log,
		setupReasonRetryableFailure,
		metav1.ConditionFalse,
		"SetupExecutedFalse",
		"setup may be retried",
	)
}

func releaseSetupExecutionClaim(
	ctx context.Context,
	k6 *v1alpha1.TestRun,
	r *TestRunReconciler,
	log logr.Logger,
) error {
	return persistSetupExecutionCondition(
		ctx,
		k6,
		r,
		log,
		setupReasonClaimed,
		metav1.ConditionFalse,
		"SetupExecutedFalse",
		"setup claim was released before execution began",
	)
}

func persistSetupExecutionCondition(
	ctx context.Context,
	k6 *v1alpha1.TestRun,
	r *TestRunReconciler,
	log logr.Logger,
	expectedReason string,
	status metav1.ConditionStatus,
	reason string,
	detail string,
) error {
	key := k6.NamespacedName()
	expectedClaimID := setupExecutionClaimID(k6)
	err := retry.OnError(
		retry.DefaultBackoff,
		func(err error) bool { return !errors.Is(err, errSetupExecutionConditionChanged) },
		func() error {
			current := &v1alpha1.TestRun{}
			if err := r.Get(ctx, key, current); err != nil {
				return err
			}

			condition := meta.FindStatusCondition(current.Status.Conditions, v1alpha1.SetupExecuted)
			if condition == nil ||
				condition.Status != metav1.ConditionUnknown ||
				condition.Reason != expectedReason ||
				setupExecutionClaimIDFromCondition(condition) != expectedClaimID {
				current.DeepCopyInto(k6)
				return errSetupExecutionConditionChanged
			}

			base := current.DeepCopy()
			setSetupExecutionCondition(current, status, reason, expectedClaimID, detail)
			if err := r.Status().Patch(
				ctx,
				current,
				client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}),
			); err != nil {
				return err
			}

			current.DeepCopyInto(k6)
			return nil
		},
	)
	if err != nil {
		log.Error(err, "Failed to update setup execution condition", "status", status, "reason", reason)
		return fmt.Errorf("updating setup execution condition to %s/%s: %w", status, reason, err)
	}
	return nil
}

func setSetupExecutionCondition(
	k6 *v1alpha1.TestRun,
	status metav1.ConditionStatus,
	reason string,
	claimID string,
	detail string,
) {
	meta.SetStatusCondition(&k6.GetStatus().Conditions, metav1.Condition{
		Type:               v1alpha1.SetupExecuted,
		Status:             status,
		ObservedGeneration: k6.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            setupExecutionConditionMessage(claimID, detail),
	})
}

func setupExecutionConditionMessage(claimID string, detail string) string {
	if claimID == "" {
		return detail
	}
	if detail == "" {
		return setupClaimMessagePrefix + claimID
	}
	return setupClaimMessagePrefix + claimID + "; " + detail
}

func setupExecutionClaimID(k6 *v1alpha1.TestRun) string {
	return setupExecutionClaimIDFromCondition(
		meta.FindStatusCondition(k6.Status.Conditions, v1alpha1.SetupExecuted),
	)
}

func setupExecutionClaimIDFromCondition(condition *metav1.Condition) string {
	if condition == nil {
		return ""
	}
	remainder, ok := strings.CutPrefix(condition.Message, setupClaimMessagePrefix)
	if !ok {
		return ""
	}
	claimID, _, _ := strings.Cut(remainder, ";")
	return strings.TrimSpace(claimID)
}

func setupExecutionConditionDetail(condition *metav1.Condition) string {
	if condition == nil {
		return ""
	}
	if _, detail, ok := strings.Cut(condition.Message, "; "); ok {
		return detail
	}
	if strings.HasPrefix(condition.Message, setupClaimMessagePrefix) {
		return ""
	}
	return condition.Message
}

func setupExecutionFailureMessage(k6 *v1alpha1.TestRun) string {
	condition := meta.FindStatusCondition(k6.Status.Conditions, v1alpha1.SetupExecuted)
	if detail := setupExecutionConditionDetail(condition); detail != "" {
		return detail
	}
	return "setup execution failed and cannot be replayed safely"
}

func failSetupExecution(
	ctx context.Context,
	k6 *v1alpha1.TestRun,
	r *TestRunReconciler,
	cloudClient *cloudapi.Client,
	log logr.Logger,
	detail string,
) (ctrl.Result, error) {
	log.Error(errors.New(detail), "Setup function failed, requesting abort.")
	events := cloud.ErrorEvent(cloud.SetupError).
		WithDetail(detail).
		WithAbort()
	cloud.SendTestRunEvents(cloudClient, k6.TestRunID(), log, events)

	k6.GetStatus().Stage = "error"
	if _, err := r.UpdateStatus(ctx, k6, log); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}
