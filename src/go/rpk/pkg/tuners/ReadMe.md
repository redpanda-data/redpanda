# Redpanda Keeper Performance Tuners

This package contains all performance tuners that are used by rpk to adjust Linux to achieve the best performance while using Redpanda.

## Supported Tuners

The following tuners are supported

### Disk IRQs

The disk IRQs tuner binds all disk IRQs to requested set of CPUs. This tuner uses `hwloc` library to compute CPU masks. Prevent IRQ Balance from moving tuned devices IRQs. CPU set that is used by the tuner can be limited by CPU mask parameter. If mask parameter is provided then only those CPUs that are masked will be considered as available. Mask covering all host CPUs is used as a default.

IRQs are distributed according to the following rules:

- Distribute NVMe disks IRQs equally among all available CPUs.
- Distribute non-NVMe disks IRQs equally among designated CPUs or among all available CPUs in the `mq` mode.

#### Modes description

 `sq` - set all IRQs of a given disk to CPU0

 `sq_split` - divide all IRQs of a given disk between CPU0 and its HT siblings

 `mq` - distribute disk IRQs among all available CPUs instead of binding them all to CPU0

  If there isn't any mode given script will use a default mode:
     - If there are no non-NVMe disks the `mq` mode will be used
     - For non-NVMe default mode will be set to `mq` if number HT siblings is less or equal to 4 otherwise if number of cores is less or equal to 4 the `sq` mode will be used in any other conditions the `sq-split` will be the default

### Disks Scheduler

The disk scheduler tuner set the block device I/O scheduler in order to minimize the overhead of kernel I/O scheduler. Preferred scheduler is of `none` type - it istruct kernel not to use I/O scheduler and use `blk-mq` module instead (there will be one queue per CPU), if the one is not supported `noop` will be used.

### Network Tuner

The Network IRQs tuner binds all NIC IRQs to requested set of CPUs. This tuner uses `hwloc` library to compute CPU masks. Prevent IRQ Balance from moving tuned devices IRQs. CPU set that is used by the tuner can be limited by CPU mask parameter. If mask parameter is provided then only those CPUs that are masked will be considered as available. Mask covering all host CPUs is used as a default.

As a result some of the CPUs may be destined to only handle the IRQs and taken out of the CPU set that should be used to run the seastar application ("compute CPU set"). 

#### Modes description

 `sq` - set all IRQs of a given NIC to CPU0 and configure RPS
      to spreads NAPIs' handling between other CPUs - should be used as default for low end NICs

 `sq_split` - divide all IRQs of a given NIC between CPU0 and its HT siblings and configure RPS
      to spreads NAPIs' handling between other CPUs

 `mq` - distribute NIC's IRQs among all CPUs instead of binding
      them all to CPU0. In this mode RPS is always enabled to
      spreads NAPIs' handling between all CPUs - should be used as default for server workloads and high throughput NICs

 If there isn't any mode given script will use a default mode:
    - If number of physical CPU cores per Rx HW queue is greater than 4 - use the 'sq-split' mode.
    - Otherwise, if number of hyperthreads per Rx HW queue is greater than 4 - use the 'sq' mode.
    - Otherwise use the 'mq' mode.