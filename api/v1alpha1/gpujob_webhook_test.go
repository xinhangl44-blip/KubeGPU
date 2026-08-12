package v1alpha1

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("GPUJob Webhook Validation Test", func() {
	Context("When creating a GPUJob", func() {
		It("should ALLOW creation when gangSize <= gpuCount", func() {
			job := &GPUJob{
				Spec: GPUJobSpec{
					GPUCount:   4,
					GangSize:   2,
					VRAMPerGPU: "16Gi",
				},
			}
			_, err := job.ValidateCreate()
			Expect(err).NotTo(HaveOccurred()) // 预期不报错，放行
		})

		It("should DENY creation when gangSize > gpuCount", func() {
			job := &GPUJob{
				Spec: GPUJobSpec{
					GPUCount:   4,
					GangSize:   8, // 故意写大，触发拦截
					VRAMPerGPU: "16Gi",
				},
			}
			_, err := job.ValidateCreate()
			Expect(err).To(HaveOccurred()) // 预期必须报错
			Expect(err.Error()).To(ContainSubstring("cannot be greater than spec.gpuCount"))
		})
	})
})
