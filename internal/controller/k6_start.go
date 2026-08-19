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
)

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
		} else {
			// let's try this approach
			if time.Since(t).Minutes() > 5 {
				msg := fmt.Sprintf(errMessageTooLong, "runner pods", "runner jobs and pods")
				log.Info(msg)

				if v1alpha1.IsTrue(k6, v1alpha1.CloudTestRun) {
					events := cloud.ErrorEvent(cloud.K6OperatorStartError).
						WithDetail(msg).
						WithAbort()
					cloud.SendTestRunEvents(cloudClient, k6.TestRunID(), log, events)
				}
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
		if err, retrySetup := runSetup(ctx, hostnames, log); err != nil {
			if retrySetup {
				if resetErr := resetSetupExecution(ctx, k6, r, log); resetErr != nil {
					return ctrl.Result{}, errors.Join(err, resetErr)
				}
				return ctrl.Result{}, err
			}

			log.Error(err, "Setup function failed, requesting abort.")
			events := cloud.ErrorEvent(cloud.SetupError).
				WithDetail(fmt.Sprintf("setup function failed: %v", err)).
				WithAbort()
			cloud.SendTestRunEvents(cloudClient, k6.TestRunID(), log, events)

			return ctrl.Result{Requeue: false}, nil
		}

		if err := completeSetupExecution(ctx, k6, r, log); err != nil {
			return ctrl.Result{}, err
		}
		setupState = setupExecutionCompleted
	}

	// A second reconciliation may observe the claim while setup() is still
	// running (or after a non-retryable failure). It must not start the test.
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
			return setupExecutionInProgress, nil
		}
	}

	base := k6.DeepCopy()
	v1alpha1.UpdateCondition(k6, v1alpha1.SetupExecuted, metav1.ConditionUnknown)
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

func completeSetupExecution(
	ctx context.Context,
	k6 *v1alpha1.TestRun,
	r *TestRunReconciler,
	log logr.Logger,
) error {
	return setSetupExecutionCondition(ctx, k6, r, log, metav1.ConditionTrue)
}

func resetSetupExecution(
	ctx context.Context,
	k6 *v1alpha1.TestRun,
	r *TestRunReconciler,
	log logr.Logger,
) error {
	return setSetupExecutionCondition(ctx, k6, r, log, metav1.ConditionFalse)
}

func setSetupExecutionCondition(
	ctx context.Context,
	k6 *v1alpha1.TestRun,
	r *TestRunReconciler,
	log logr.Logger,
	status metav1.ConditionStatus,
) error {
	key := k6.NamespacedName()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &v1alpha1.TestRun{}
		if err := r.Get(ctx, key, current); err != nil {
			return err
		}

		base := current.DeepCopy()
		v1alpha1.UpdateCondition(current, v1alpha1.SetupExecuted, status)
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
		log.Error(err, "Failed to update setup execution condition", "status", status)
		return fmt.Errorf("updating setup execution condition to %s: %w", status, err)
	}
	return nil
}
