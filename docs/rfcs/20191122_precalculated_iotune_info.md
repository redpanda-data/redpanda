- Feature Name: Precompiled iotune info
- Status: draft
- Start Date: 2019-11-22
- Authors: David Castillo <david@vectorized.io>
- Issue: #200

# Executive Summary

One of the tools rpk relies on for tuning the machine where redpanda will run is
[iotune](https://github.com/scylladb/seastar/tree/master/apps/iotune). It runs
IO benchmarks to find the optimal read/write IOPS and IO bandwidth values, which
are used to configure the Seastar IO scheduler. To get the best results, iotune
should be run for at least a couple of minutes, up to ~45 minutes. This means
that even if the redpanda installation, tuner execution and startup can take as
few as 1 minute, the iotune step can make the whole process seem like an
eternity for our users.

However, it is expected that many of Vectorized's clients will run redpanda in
any of the most popular cloud vendors' (AWS, Azure, GCP) standard VM types. We
can run iotune on the VM/ elastic storage volume types that we project will be
the most used for redpanda, and encode the obtained data into rpk to decrease
further the time it takes for an user to have redpanda up and running.

## What is being proposed

a. To include precompiled iotune results from all the recommended major cloud
vendor/ VM/ storage type combinations, and including them in rpk.

b. Additionally, to collect VM metrics such as CPU type, CPU Features, disk
type, RAID setup, network settings, memory settings, NUMA socket settings,
hyperthread settings, kernel version settings, operative system settings.

## Why (short reason)

a. To reduce the time it takes to set up redpanda and run it, and to make the best
first impression redpanda can as a product.

b. To be able to map the resulting iotune values to not only a VM type, but more
generally to OS and hardware features and settings.

## How (short plan)

1. Creating a Python script to iterate over a major cloud vendor/ VM type/
storage device type matrix, leveraging our
[Terraform configuration](https://github.com/redpanda-data/v/tree/master/infra)
to deploy test nodes, overriding its default behavior, which is to run the rpk
tuners and then redpanda, with a script to
2. Running iotune on the deployed VMs,
  2.1. Collecting the VM OS settings and hardware features described above
3. Uploading each result to s3 for analysis and aggregation.
4. Aggregating the collected data to embed it into rpk as a vendor to
machine-type to storage-device-type matrix, which will enable the user to skip
iotune execution or the related configuration, and start redpanda right away.

Supported cloud vendors:
- AWS
- GCP
- Azure

Recommended VM types by vendor:

**AWS**
> See https://aws.amazon.com/ec2/instance-types/ for detailed info on instance
> type specifications
> EBS-only instance types aren't included because networked IO is throttled
- Compute optimized
  - c5d: large, xlarge, 2xlarge, 4xlarge, 9xlarge, 12xlarge, 18xlarge,
    24xlarge, metal
- Storage optimized
  - i3: large, xlarge, 2xlarge, 4xlarge, 8xlarge, 16xlarge, metal
  - i3en: large, xlarge, 2xlarge, 3xlarge, 6xlarge, 12xlarge, 24xlarge, metal

**GCP**
> See https://cloud.google.com/compute/docs/machine-types for more info on GCP
> machine types
- General purpose
  - n1: standard, highcpu (in their 2, 4, 8, 16, 32, 64 and 96 vCPUs variants)
  - n2: standard, highcpu (2, 4, 8, 16, 32, 48, 64, 80 vCPUs variants)
- Compute optimized
  - c2: standard (4, 8, 16, 30, 60 vCPUs variants)

All will be tested with Local SSDs (SCSI and NVMe). Details on GCP block
storage types at
https://cloud.google.com/compute/docs/disks/performance.

**Azure**
> See the virtual machine specs at
> https://docs.microsoft.com/en-us/azure/virtual-machines/linux/sizes
- General purpose
  - Dsv3: Standard\_D{2, 4, 8, 16, 32, 48, 64}s\_v3
  - Dasv4: Standard\_D{2, 4, 8, 16, 32, 48, 64}as\_v4
  - Dv3: Standard\_D{2, 4, 8, 16, 32, 48, 64}\_v3
  - Dav4: Standard\_D{2, 4, 8, 16, 32, 48, 64}2a\_v4
  - DSv2: Standard\_DS{2, 3, 4, 5}\_v2
  - Dv2: Standard\_D{2, 3, 4, 5}\_v2
  - Av2: Standard\_A{2, 4, 8}\_v2, Standard\_A{2, 4, 8}m\_v2
- Compute optimized
  - Fsv2: Standard_\F{2, 4, 8, 16, 32, 48, 64, 72}s\_v2
- Storage optimized
  - Lsv2: Standard_\L{8, 16, 32, 48, 64, 80}s\_v2
  
All will be tested with
[Standard, Premium and Ultra SSDs](https://docs.microsoft.com/en-us/azure/virtual-machines/windows/disks-types),
in all their sizes.

## Impact

This will reduce the amount it takes to get started with redpanda, leading to
a better user experience.

# Motivation
## Why are we doing this?

The time available for a product to create a good first impression on the user
(what we refer to as time-to-wow, or T2W) is very little. We hypothesize that
this is specially true for software experts, which have probably heard that some
new tool is "the next silver bullet" one-too-many times.

Because of this, we have a goal of keeping the T2W at 60 seconds or less (T2W
budget).
Having a precompiled data set for the optimal settings for the recommended VM/
storage setups will allow us to reduce the time it takes for a user to be
convinced that redpanda and Vectorized will deliver what we promise.
Essentially, we're turning something that takes 45 minutes into millisecond
scale.

## What use cases does it support?

Ease of installation, ensuring the best performance.

## What is the expected outcome?

A redpanda node, working with near-optimal settings within our T2W budget.

# Guide-level explanation
## How do we teach this?

`rpk` comes with a command called `iotune` which finds the optimal configuration
for redpanda's IO scheduler, with the current storage device and processor. To
get the most precise results, iotune should usually run for approximately 30
minutes - or 1800 seconds (i.e. `rpk iotune --duration 1800`).

However, rpk ships with a matrix of optimal configurations for the
recommended VM/ storage device type setups on AWS, GCP and Azure. Thanks to
this, when running redpanda on the recommended setups, you can just fire it up
with no additional steps.

> It is still recommended to execute iotune on custom setups. There's no need
> to run it every time, however: you can run it once for each setup that you
> will use to run a redpanda node, keep the results, and reuse them from there.

Upon statup, rpk will try to detect the current cloud and instance type via the
different vendors' metadata APIs, setting the correct iotune properties if the
detected setup is a supported one.

If access to the metadata API isn't allowed from the instance, you can also hint
the desired setup by passing the `--well-known-io` flag to `rpk start` with the
cloud vendor, VM type and storage type surrounded by quotes and separated by
colons (`:`):

```sh
rpk start --well-known-io 'aws:l3.xlarge:io1'
```

It can also be specified in the redpanda YAML configuration file, under the
`rpk` object:

```yaml
rpk:
  well_known_io: 'gcp:c2-standard-16:nvme'
```

> If `well-known-io` is specified in the config file, and as a flag, the value
> passed with the flag will take precedence.

In the case where a certain cloud vendor, machine type or storage type isn't
found, or if the metadata isn't available and no hint is given, `rpk` will
print an error pointing out the issue and **continue** using the default values.

# Reference-level explanation

## Interaction with other features

The cloud vendor and VM type detection will be on by default, and on unsupported
VMs or VMs where access to the vendor's metadata API isn't allowed, the vendor
and VM can be hinted `rpk` by passing a new flag, `--well-known-io`. This aligns
with the UI built so far.

The only forseen conflict is with the `--io-properties` flag in the start command,
since they both serve as a way of specifying the source for Seastar's IO
scheduler configuration. If both flags are passed, `rpk` will print a message
prompting the user to pick only one and stop.

Hopefully, `--well-known-io` will make easier for a large percentage of our
users to run redpanda. However, there will be some users which will want to run
redpanda in their own infrastructure or on non-recommended setups. Because of
that, the `iotune` command is still useful and won't be replaced or made
obsolete by this.

## Telemetry & Observability

Described in the sections below.

## Corner cases dissected by example.

As explained in **Interaction with other features**, if the user also passes the
`--io-properties` flag along with `--well-known-io`, or when setting a value for
`rpk.well_known_io` in `redpanda.yaml`, this is considered a conflict that the
user is expected to resolve.

## Detailed design 

### What needs to change to get there

The implementation of this feature will be divided in 3 steps.

1. Use the
  [available iotune result data](https://github.com/scylladb/scylla/blob/master/dist/common/scripts/scylla_io_setup)
  for AWS i3 instances, collected by the ScyllaDB team, and embed it into rpk.
  This could happen even before this RFC is approved, in order to have
  something ready for the scheduled 2019-12-15 release.
2. Deployment and data collection and aggregation, and extension of the initial
  data included in rpk during step 1 with the new data.

### How it works

#### Step 1

To achieve step 1, the `--well-known-io` flag will be added to the `start` command,
following the same patterns used by all flags in `rpk`. This flag's effect will
be to look up the configuration for the given vendor, VM, and storage type - in
an in-memory matrix, and to propagate the settings to redpanda/ seastar via its
[`--io-properties` flag](https://github.com/scylladb/seastar/blob/master/src/core/reactor.cc#L3225).

#### Step 2

This step is the bulk of the feature's development. It will be divided into 3
phases too:

a. Collecting data for AWS setups
b. Collecting data for GCP setups
c. Collecting data for Azure setups

This part is divided to provide incremental updates, and because it is expected
that the Terraform module for each vendor will be different from the other ones.
The order is determined to provide the updates as fast as possible; there's
already a module to deploy on AWS, so the AWS data will be collected first.
Some potential clients use GCP too, so it will come second, and finally, Azure
data will be collected.

When each phase is completed, the results will be integrated into rpk building
upon step 1's progress.

In order to deploy each vendor's module with its different VM-storage type
combinations, a python script will be created to iterate over the vendor/ vm/ 
storage matrix described in the **How (Short Plan)** section. It will leverage
Terraform's action's `--state-out` and `state` flags to be able to use the same
module, but manage multiple simultaneous deployments.

The state file names will have the format
`<vendor>-<vm type>-<storage type>.tfstate`, thus making deployments easily
traceable to their state files.

Each phase will consist of:
- Deploying all the specified VM/ storage type combinations, and choosing for
  either RHEL 8 and Ubuntu 18.04 randomly
- Running iotune
- Saving the iotune data in an S3 bucket
- Repeating this process a couple times to be able to average the results
- Aggregating the results
- Adding the new data to `rpk`

This will be done using Terraform's `remote-exec` provisioner, which allows
executing commands on the deployed machine, and a bash script that it will
provision to each deployed VM, encoding the process above as a bash script.

The structure of the data in S3 will be
```
root-bucket
|- iotune-<date> 
   |- <vendor>
   |   |- <vm type name>
   |      |- <uuid>
   |         |- <storage type name>
   |            |- io-properties.yaml
   |            |- vm-data.txt
   |- <vendor b>
...
```

## Drawbacks

The deployment of hundreds of VMs, many of which belong to the highest tiers,
will no doubt be super expensive. However, it's something that doesn't need to
be done so frequently (only when new VM and storage types are launched), and the
improvement this feature poses in terms of UX is great, reducing the time it
takes to start redpanda for the first time from ~30 minutes to <1 minute, all
while it ensures that the user is running redpanda with the best IO config.

## Rationale and Alternatives

As mentioned before, the feature is fairly easy to implement, but the challenge
is in the deployment and the data collection. However, it will be a proof of how
redpanda will go the last mile for its customers, and that we're always one step
ahead.

Additionally, it will provide us with a very broad picure of the approximate
performance baseline across clouds and storage types.

### Alternatives considered

- Not including the precompiled iotune data
  Keeping `rpk` as is, and deferring the optimal IO settings to run redpanda to 
  the user would certainly be cheaper in terms of effort, as well as
  financially, but its likely that our users will use either AWS, GCP or Azure,
  so important that we make it as easy for them to run redpanda on the cloud.

## Unresolved questions
- What other data would be valuable to collect, besides the dataset described
  above?
