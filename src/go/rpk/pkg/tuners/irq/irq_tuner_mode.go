// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package irq

/*
Modes are ordered from the one that cuts the biggest number of CPUs
from the compute CPUs' set to the one that takes the smallest ('mq' doesn't
cut any CPU from the compute set).
This fact is used when we calculate the 'common quotient' mode out of a
given set of modes (e.g. default modes of different Tuners) - this would
be the smallest among the given modes.

Modes description:
sq - set all IRQs of a given NIC to CPU0 and configure RPS

	to spreads NAPIs' handling between other CPUs.

sq_split - divide all IRQs of a given NIC between CPU0 and its HT siblings and configure RPS

	to spreads NAPIs' handling between other CPUs.

mq - distribute NIC's IRQs among all CPUs instead of binding

	them all to CPU0. In this mode RPS is always enabled to
	spreads NAPIs' handling between all CPUs.

If there isn't any mode given script will use a default mode:
  - If number of physical CPU cores per Rx HW queue is greater than 4 - use the 'sq-split' mode.
  - Otherwise, if number of hyperthreads per Rx HW queue is greater than 4 - use the 'sq' mode.
  - Otherwise use the 'mq' mode.
*/
type Mode string

const (
	SqSplit Mode = "sq-split"
	Sq      Mode = "sq"
	Mq      Mode = "mq"
	Default Mode = "def"
)

func ModeFromString(modeString string) Mode {
	if modeString == "mq" {
		return Mq
	} else if modeString == "sq" {
		return Sq
	} else if modeString == "sq-split" {
		return SqSplit
	}

	return Default
}
