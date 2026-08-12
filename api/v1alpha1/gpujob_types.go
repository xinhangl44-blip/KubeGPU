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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:validation:XValidation:rule="self.gangSize <= self.gpuCount",message="spec.gangSize cannot be greater than spec.gpuCount"
type GPUJobSpec struct {
	// +kubebuilder:validation:Minimum=1
	GPUCount int32 `json:"gpuCount"`

	VRAMPerGPU string `json:"vramPerGPU"`

	Priority int32 `json:"priority"`

	// +kubebuilder:validation:Minimum=1
	GangSize int32 `json:"gangSize"`

	// SchedulerName specifies the name of the scheduler that should handle this job.
	// If set to "default-scheduler", the native K8s scheduler will take over.
	// If set to "my-gpu-scheduler", your custom scheduler will manage it.
	// +optional
	SchedulerName string `json:"schedulerName,omitempty"`
}

type GPUJobStatus struct {
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// +optional
	Phase string `json:"phase,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

type GPUJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GPUJobSpec   `json:"spec,omitempty"`
	Status GPUJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type GPUJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GPUJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&GPUJob{}, &GPUJobList{})
}
