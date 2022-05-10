// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package irq

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

var procFileLines = []string{
	"       CPU0            CPU1       CPU2    CPU3                              ",
	"1:     184233          0          0       7985   IO-APIC   1-edge      i8042",
	"5:          0          0          0          0   IO-APIC   5-edge      parport0",
	"8:          1          0          0          0   IO-APIC   8-edge      rtc0",
}

var awsMiniProcFile = `
CPU0       
  0:         36   IO-APIC   2-edge      timer
  1:          9  xen-pirq   1-ioapic-edge  i8042
  4:       4341  xen-pirq   4-ioapic-edge  ttyS0
  8:          2  xen-pirq   8-ioapic-edge  rtc0
  9:          0  xen-pirq   9-ioapic-level  acpi
 12:          3  xen-pirq  12-ioapic-edge  i8042
 14:          0   IO-APIC  14-edge      ata_piix
 15:          0   IO-APIC  15-edge      ata_piix
 48:     773698  xen-percpu    -virq      timer0
 49:          0  xen-percpu    -ipi       resched0
 50:          0  xen-percpu    -ipi       callfunc0
 51:          0  xen-percpu    -virq      debug0
 52:          0  xen-percpu    -ipi       callfuncsingle0
 53:          0  xen-percpu    -ipi       spinlock0
 54:        266   xen-dyn    -event     xenbus
 55:     146580   xen-dyn    -event     blkif
 56:     464982   xen-dyn    -event     eth0
NMI:          0   Non-maskable interrupts
LOC:          0   Local timer interrupts
SPU:          0   Spurious interrupts
PMI:          0   Performance monitoring interrupts
IWI:          0   IRQ work interrupts
RTR:          0   APIC ICR read retries
RES:          0   Rescheduling interrupts
CAL:          0   Function call interrupts
TLB:          0   TLB shootdowns
TRM:          0   Thermal event interrupts
THR:          0   Threshold APIC interrupts
DFR:          0   Deferred Error APIC interrupts
MCE:          0   Machine check exceptions
MCP:       1671   Machine check polls
HYP:    1384378   Hypervisor callback interrupts
ERR:          0
MIS:          0
PIN:          0   Posted-interrupt notification event
NPI:          0   Nested posted-interrupt event
PIW:          0   Posted-interrupt wakeup event
`

func TestProcFile_GetIRQProcFileLinesMap(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	_ = utils.WriteFileLines(fs, procFileLines, "/proc/interrupts")
	procFile := NewProcFile(fs)
	// when
	procFileLinesMap, err := procFile.GetIRQProcFileLinesMap()
	// then
	require.NoError(t, err)
	require.Equal(t, len(procFileLinesMap), 3)
	require.Equal(t, procFileLinesMap[1], procFileLines[1])
	require.Equal(t, procFileLinesMap[5], procFileLines[2])
	require.Equal(t, procFileLinesMap[8], procFileLines[3])
}

func TestProcFile_GetIRQProcFileLinesMap_AWS(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	_ = afero.WriteFile(fs, "/proc/interrupts", []byte(awsMiniProcFile), 0o644)
	procFile := NewProcFile(fs)
	// when
	procFileLinesMap, err := procFile.GetIRQProcFileLinesMap()
	// then
	require.NoError(t, err)
	require.Len(t, procFileLinesMap, 17)
}
