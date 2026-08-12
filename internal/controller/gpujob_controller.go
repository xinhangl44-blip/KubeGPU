/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	schedulerv1alpha1 "lawson.com/gpu-crd/api/v1alpha1"
)

const (
	// These constants MUST match the corresponding constants in
	// pkg/plugins/gang/gang.go and pkg/plugins/vramfit/plugin.go.
	labelJobName   = "gpu-job-name"
	labelMinMember = "gpu-job-min-member"
	// vramResourceName must match vramfit.VRAMResourceName.
	vramResourceName = "custom.com/vram"
	schedulerName = "my-gpu-scheduler"
)

type GPUJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=scheduler.lawson.com,resources=gpujobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=scheduler.lawson.com,resources=gpujobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=scheduler.lawson.com,resources=gpujobs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete

func (r *GPUJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	gpuJob := &schedulerv1alpha1.GPUJob{}
	if err := r.Get(ctx, req.NamespacedName, gpuJob); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	podList := &corev1.PodList{}
	labels := map[string]string{labelJobName: gpuJob.Name}
	if err := r.List(ctx, podList, client.InNamespace(req.Namespace), client.MatchingLabels(labels)); err != nil {
		return ctrl.Result{}, err
	}
	currentPods := len(podList.Items)
	desiredPods := int(gpuJob.Spec.GPUCount)
	if currentPods < desiredPods {
		logger.Info("Creating pods for GPUJob", "current", currentPods, "desired", desiredPods)
		for i := 0; i < desiredPods; i++ {
			pod, err := r.buildPodForJob(gpuJob, i)
			if err != nil {
				logger.Error(err, "Failed to build Pod spec", "index", i)
				return ctrl.Result{}, fmt.Errorf("failed to build pod %d: %w", i, err)
			}
			existingPod := &corev1.Pod{}
			err = r.Get(ctx, client.ObjectKey{Namespace: gpuJob.Namespace, Name: pod.Name}, existingPod)
			if err == nil {
				continue
			}
			if !errors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			if err := controllerutil.SetControllerReference(gpuJob, pod, r.Scheme); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.Create(ctx, pod); err != nil {
				if errors.IsAlreadyExists(err) {
					continue
				}
				logger.Error(err, "Failed to create Pod", "index", i)
				return ctrl.Result{}, fmt.Errorf("failed to create pod %d: %w", i, err)
			}
		}
	}
	return ctrl.Result{}, nil
}

// buildPodForJob constructs the Pod spec for one member of a GPUJob's gang.
func (r *GPUJobReconciler) buildPodForJob(gpuJob *schedulerv1alpha1.GPUJob, index int) (*corev1.Pod, error) {
	vramQty, err := resource.ParseQuantity(gpuJob.Spec.VRAMPerGPU)
	if err != nil {
		return nil, fmt.Errorf("invalid spec.vramPerGPU %q: %w", gpuJob.Spec.VRAMPerGPU, err)
	}
	gangSize := gpuJob.Spec.GangSize
	if gangSize <= 0 {
		gangSize = gpuJob.Spec.GPUCount
	}
	priorityClassName := priorityClassForValue(gpuJob.Spec.Priority)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-pod-%d", gpuJob.Name, index),
			Namespace: gpuJob.Namespace,
			Labels: map[string]string{
				labelJobName:   gpuJob.Name,
				labelMinMember: strconv.Itoa(int(gangSize)),
			},
		},
		Spec: corev1.PodSpec{
			SchedulerName:     gpuJob.Spec.SchedulerName,
			RestartPolicy:     corev1.RestartPolicyNever,
			PriorityClassName: priorityClassName,
			Containers: []corev1.Container{
				{
					Name:    "cuda-training",
					Image:   "busybox",
					Command: []string{"sleep", "3600"},
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceName(vramResourceName): vramQty,
						},
						Requests: corev1.ResourceList{
							corev1.ResourceName(vramResourceName): vramQty,
						},
					},
				},
			},
		},
	}
	return pod, nil
}

func priorityClassForValue(priority int32) string {
	switch {
	case priority >= 100:
		return "gpujob-priority-high"
	case priority >= 50:
		return "gpujob-priority-medium"
	case priority > 0:
		return "gpujob-priority-low"
	default:
		return ""
	}
}

func (r *GPUJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&schedulerv1alpha1.GPUJob{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}
