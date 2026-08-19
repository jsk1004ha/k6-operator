package controllers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-logr/logr"
	"github.com/grafana/k6-operator/api/v1alpha1"
	"github.com/grafana/k6-operator/pkg/cloud"
	"github.com/grafana/k6-operator/pkg/resources/jobs"
	"go.k6.io/k6/v2/cloudapi"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type setupExecutionState int

const (
	setupExecutionNotRequired setupExecutionState = iota
	setupExecutionClaimed
	setupExecutionInProgress
	setupExecutionCompleted
	setupExecutionFailed
)

const (
	setupExecutionClaimTimeout = 30 * time.Minute

	setupReasonClaimed          = "SetupExecutionClaimed"
	setupReasonSucceeded        = "SetupExecutionSucceeded"
	setupReasonRetryableFailure = "SetupExecutionRetryableFailure"
	setupReasonFailed           = "SetupExecutionFailed"
)

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

	setupState, err := claimSetupExecution(ctx, k6, r, log)
	if err != nil {
		return ctrl.Result{}, err
	}

	if setupState == setupExecutionClaimed {
		if setupErr, retrySetup := runSetup(ctx, hostnames, log); setupErr != nil {
			if retrySetup {
				if phaseErr := markSetupExecutionRetryableFailure(ctx, k6, r, log, setupErr); phaseErr != nil {
					return ctrl.Result{}, errors.Join(setupErr, phaseErr)
				}
				if resetErr := resetSetupExecution(ctx, k6, r, log); resetErr != nil {
					return ctrl.Result{}, errors.Join(setupErr, resetErr)
				}
				return ctrl.Result{}, setupErr
			}

			detail := fmt.Sprintf("setup function failed: %v", setupErr)
			if phaseErr := markSetupExecutionFailed(ctx, k6, r, log, setupReasonClaimed, detail); phaseErr != nil {
				return ctrl.Result{}, errors.Join(setupErr, phaseErr)
			}
			return failSetupExecution(ctx, k6, r, cloudClient, log, detail)
		}

		if err := markSetupExecutionSucceeded(ctx, k6, r, log); err != nil {
			return ctrl.Result{}, err
		}
		if err := completeSetupExecution(ctx, k6, r, log); err != nil {
			return ctrl.Result{}, err
		}
		setupState = setupExecutionCompleted
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

	// A second reconciliation may observe the claim while setup() is still
	// running. It must not run setup again or create the starter.
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
			default:
				if time.Since(condition.LastTransitionTime.Time) < setupExecutionClaimTimeout {
					return setupExecutionInProgress, nil
				}

				detail := fmt.Sprintf(
					"setup execution claim was not completed within %s; replay is unsafe",
					setupExecutionClaimTimeout,
				)
				if err := markSetupExecutionFailed(ctx, k6, r, log, condition.Reason, detail); err != nil {
					return setupExecutionInProgress, err
				}
				return setupExecutionFailed, nil
			}
		}
	}

	base := k6.DeepCopy()
	setSetupExecutionCondition(k6, metav1.ConditionUnknown, setupReasonClaimed, "setup execution is claimed")
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
		setupReasonClaimed,
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
		setupReasonClaimed,
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

func persistSetupExecutionCondition(
	ctx context.Context,
	k6 *v1alpha1.TestRun,
	r *TestRunReconciler,
	log logr.Logger,
	expectedReason string,
	status metav1.ConditionStatus,
	reason string,
	message string,
) error {
	key := k6.NamespacedName()
	err := retry.OnError(retry.DefaultBackoff, func(err error) bool { return !errors.Is(err, errSetupExecutionConditionChanged) }, func() error {
		current := &v1alpha1.TestRun{}
		if err := r.Get(ctx, key, current); err != nil {
			return err
		}

		condition := meta.FindStatusCondition(current.Status.Conditions, v1alpha1.SetupExecuted)
		if condition == nil ||
			condition.Status != metav1.ConditionUnknown ||
			condition.Reason != expectedReason {
			current.DeepCopyInto(k6)
			return errSetupExecutionConditionChanged
		}

		base := current.DeepCopy()
		setSetupExecutionCondition(current, status, reason, message)
		if err := r.Status().Patch(
			ctx,
			current,
			client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}),
		); err != nil {
			return err
		}

		current.DeepCopyInto(k6)
		return nil
	})
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
	message string,
) {
	meta.SetStatusCondition(&k6.GetStatus().Conditions, metav1.Condition{
		Type:               v1alpha1.SetupExecuted,
		Status:             status,
		ObservedGeneration: k6.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	})
}

func setupExecutionFailureMessage(k6 *v1alpha1.TestRun) string {
	condition := meta.FindStatusCondition(k6.Status.Conditions, v1alpha1.SetupExecuted)
	if condition != nil && condition.Message != "" {
		return condition.Message
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
