package irq

import "vectorized/pkg/utils"

func GetAllIRQs(deviceIRQs map[string][]int) []int {
	irqsSet := map[int]bool{}
	for _, irqs := range deviceIRQs {
		for _, irq := range irqs {
			irqsSet[irq] = true
		}
	}
	return utils.GetIntKeys(irqsSet)
}
