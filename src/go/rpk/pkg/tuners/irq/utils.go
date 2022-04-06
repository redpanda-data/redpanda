// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package irq

import "github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"

func GetAllIRQs(deviceIRQs map[string][]int) []int {
	irqsSet := map[int]bool{}
	for _, irqs := range deviceIRQs {
		for _, irq := range irqs {
			irqsSet[irq] = true
		}
	}
	return utils.GetIntKeys(irqsSet)
}
