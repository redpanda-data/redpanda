- Feature Name: Redpanda modes and feature flags
- Status: in-progress
- Start Date: 2019-10-23
- Authors: David Castillo
- Issue: [#226](https://github.com/redpanda-data/v/issues/226), [#227](https://github.com/redpanda-data/v/issue7/227)


# Summary

Sometimes, settings, checks and tuners that are desirable for a production environment
aren't necessary for someone who's just evaluating redpanda on their personal laptop
or on a small machine. These settings can even crash the developer's computer
(see [#226](https://github.com/redpanda-data/v/issues/226)), which is not a good first
impression.
To avoid these issues, we should have feature flags to be able to control some
redpanda settings (described below) in a more fine-grained scope, while also making
it easy for the user to run redpanda without having to go over each option.
In a nutshell, we should have a simple way to allow developers to disable production
settings when they're running redpanda in their personal computers or in an evaluation
environment.

## What is being proposed
We introduce the concept of feature flags and execution modes to redpanda. We want to
allow the user to enable and apply the tuners and optional redpanda settings that they wish.

At the moment of writing this proposal, these are:

**Tuners**:
Net
- IRQ balance banning
- IRQ affinity
- Receive Packet Steering (RPS)
- Receive Flow Steering (RFS)
- NTuple tuning
- XPS queues setup
- RFS table setup
- Listen backlog size tuning
- SYN backlog size tuning
Disk
- IRQ balance banning
- IRQ affinity
- No-op scheduling
- NoMerges
CPU
- HT tuner disabling
- CPU governor setup
- Power saving modes disabling (requires to setup GRUB and reboot machine**

**Runtime features**
- Memory locking
- Real time scheduling

**Optional configuration files**
- Custom coredump pattern & coredump processing script.

Currently, redpanda's DEB and RPM packages also ship with a custom coredump processing
script, enabled through a sysctl
[.conf file](https://www.freedesktop.org/software/systemd/man/sysctl.d.html#), which is
automatically installed with the package.
While this script is useful if the user wants to save a coredump for later analysis, it
puts a very high load on IO for common development environments, freezing or crashing
them due to the size of the core dump, which is usually over 50GB. Because of this, the
installation of this script should be a choice and not enabled by default, so a feature
flag should be created for it too. In general, all optional system configuration files
should have a feature flag to allow rpk or redpanda to install them if the user chooses
to. The use case described above is the main reason for this proposal.

Additionally, we introduce "development modes", which are a way to easily enable or
disable groups of features at once. Specifically, we propose the implementation of
2 main modes:

- **development**, which removes the custom coredump pattern configuration file, and the
  processing script it enables; an disables tuners that might reduce the overall perceived
  performance of a development environment, which usually has limited resources shared
  among many heterogeneous processes.
- **production**, which enables all the tuners, as well as some seastar features such
  as memory locking and real-time scheduling.

## Why (short reason)

To allow the developers working on, evaluating, or locally developing software
that depends on redpanda, to disable settings and features that might make the
experience of using redpanda bad or confusing.

## How (short plan)

- Adding an `rpk` section to the `redpanda.yaml` config file, where each tuner can be
  enabled (set to `true`) or disabled (set to `false`).
- Exposing the relevant seastar configurations, such as memory locking and
  real time scheduling, in the `redpanda.yaml` configuration file under the `rpk`
  section.

These configuration fields would serve as feature flags for redpanda's tuners and
settings, as well as seastar's.

To save the users the unnecessary effort of disabling or enabling these settings
themselves, a new rpk command is proposed, called `mode`. When executed (i.e. `rpk
mode development`), this command will set the relevant fields to `false`, so that the
user can get panda up and running in their environment, without affecting other processes.

## Impact

This would greatly improve the developer and user experience, so it's really
important for the first release.

# Motivation

Currently some redpanda and seastar settings, although meant to make redpanda's
performance as good as possible in a production setting, make other processes halt
or slow down, and could even crash the developer's machine. For example, the
redpanda deb and rpm packages install a custom systemd coredump script that saves all
coredumps in `/var/lib/redpanda/coredump`. If redpanda crashes, the core dump data
can take more than 50 GB, which can bring their computer to a halt. Because the
redpanda systemd service is enabled by default and is set to restart after crashing,
if the user should reboot their computer this process could go on and on filling the
user's storage.

Likewise, seastar's real time scheduling may decrease the performance of other processes
usually present in developers' machines, such as IDEs, music streaming apps, and even
shells.

Because of this, we should allow users and developers to disable these settings in a
simple way.

# Guide-level explanation

To achieve the best performance possible, redpanda relies on interrupt coalescing, interrupt
distribution, no-op IO scheduling, CPU frequency scaling (performance governor), NIC
multi-queue setup, memory locking and real-time scheduling. However, enabling these features
isn't always the best for a personal computer running multiple processes, each of which are
always competing for memory, CPU and IO.

Because of this, redpanda enables them by default, but allows them to be disabled according to
your needs. To do it easily, you can toggle on redpanda's Developer Mode, which disables these
production settings so that you can still use it in your laptop.

Enabling Developer Mode is as simple as running

```sh
# You may need to run this command as root.
rpk mode developer
```

For runtime settings (that is, settings that are only effective while redpanda is running),
it will conveniently disable any which may affect other processes' performance, by setting the
corresponding fields in the `redpanda.yaml` configuration file to `false`.

For persistent session or system configurations, it will also undo any changes redpanda might
have made in your machine (such as enabling real-time scheduling or disabling certain interrupt
requests for some CPUs).

Likewise, if you'd like to re-enable the default, production settings, just run

```sh
# You may need to run this command as root.
rpk mode production
```

If you'd like to review everything `rpk mode` will do, you can also run it interactively with
the --interactive flag:

```sh
# You may need to run this command as root.
rpk mode developer --interactive
```

Some redpanda tuners change GRUB configurations, thus requiring a restart for them to take
place. You may choose to disable those by adding `--with-reboot-options false` (or enable
them, passing `true`):

```sh
# You may need to run this command as root.
rpk mode production --with-reboot-options false
```

These commands are idempotent, so they're safe to run multiple times.

For runtime settings, you can also review and edit `redpanda.yaml` and enable or disable
any of them. However, if you run `rpk mode` again, it might overwrite any changes you
made in the affected fields.

If you switch modes or change any setting, you have to restart redpanda for the changes
to take place.

# Reference-level explanation

## Detailed design

The user-facing changes, as mentioned in the previous section, include:

1. Adding a new command (`mode`) to rpk:

  The `mode` command should disable the redpanda tuners and features - by setting their
  respective fields in the `rpk` section in `redpanda.yaml`-, as well disabling OS features
  and settings - by running any commands and editing any system files necessary -, that
  might not play well with the multitude of processes usually active in developer machines.
  
  Editing `redpanda.yaml` should be done by loading the current contents, changing the
  necessary values only, and writing the file again, so as not to unnecessarily overwrite
  any unrelated config fields, such as a port or an IP address.
  
  For running processes the current `Proc` interface available in rpk can be used.

2. Adding an `rpk` section in the config file.
  
  The new section

  ```yaml
  ---
   rpk:
     network:
       ban_irq_balancing:    true | false
       irq_affinity:         true | false
       rps:                  true | false
       rfs:                  true | false
       rfs_table:            true | false
       ntuple:               true | false
       xps_queues:           true | false
       listen_backlog_size:  true | false
       syn_backlog_size:     true | false
     
     memory:
       memory_locking:       true | false

     cpu:
       real_time_scheduling: true | false
       disable_ht_tuner:     true | false
       cpu_governor:         true | false
       disable_power_saving: true | false
 
     disk:
       ban_irq_balance:      true | false
       irq_affinity:         true | false
       noop_scheduling:      true | false
       nomerges:
       
     coredump:
       install_custom:       true | false
  ---
  redpanda:
    # current redpanda settings 
   
  ```
  
  should be included in the `Config` struct in rpk, with the appropriate tags for the
  yaml parser, like so:
  
  ```go
  type Rpk struct {
      Network       `yaml:"network"`
      Memory        `yaml:"memory"`
      CPU           `yaml:"cpu"`
      Disk          `yaml:"disk"`
      Coredump      `yaml:"coredump"`
  }
  
  type Config struct {
	  Rpk       `yaml:"rpk"`
      Redpanda  `yaml:"redpanda"`
	  // current fields...
  }
  ``` 
  
  The values can then be queried by any part of the rpk code to enable or disable features:
  
  ```go
  if config.Rpk.realTimeScheduling {
      err = os.NewProc().RunWithSystemLdPath("sysctl", "-w", "kernel.sched_rt_runtime_us=-1")
  }
  ```
  
  Some of the fields in the new config section which correspond to runtime settings will
  not translate to specific commands, but will be passed to redpanda/ seastar as flags.

## Drawbacks

Arguably, this adds complexity to redpanda/ rpk. If a way is found to run redpanda safely
on a development environment, that doesn't require exposing a choice to the user, it
should be preferred over this one. For example, detecting the available resources and
deciding whether to activate or deactivate a feature or tuner.

On a related note, some users **will** skip the docs and run redpanda/ rpk right
away, so this doesn't take away the underlying risks mentioned above.
Likewise, it also opens a way to run redpanda in a way that limits its performance.
For example, a user could install redpanda on a production machine and run
`rpk mode developer`, forgetting to change it back, leading to inferior performance.

## Rationale and Alternatives

Having fine-grained configuration could greatly simplify setting up redpanda in a developer
machine.
Also, having development and production modes makes it super easy for a user to run redpanda
without having to familiarize themselves with the different OS settings redpanda depends on,
and then judging whether they should enable or disable it, or researching which command they
should run to turn them on or off.

Additionally, its implementation is trivial:
- Adding new commands is pretty straightforward with cobra (the cli lib used in rpk)
- New config fields need to be added to the yaml file, and loaded into the `Config`
  struct, which is done with go's field tags and the go-yaml library.
- The most involved part is identifying which checks, tuners and seastar flags should be
  disabled with the development mode, and how to undo production mode changes made to the
  machine if the user should decide that they want to run in development mode after all
  (see the unresolved questions section).

### Alternatives considered

1. Having 2 separate **services**, one which doesn't allow further configuration, and a
  [template service](https://fedoramagazine.org/systemd-template-unit-files/) that allows
  the user to override the default (production) config passing flags to the `ExecStart`
  command (e.g. `systemctl start redpanda@--development-mode.service`).
  This alternative, although it makes it straightforward to start redpanda in development
  mode with systemd, requires us to have 2 different service files, which can be
  [inconvenient for some users](https://github.com/redpanda-data/v/issues/212#issuecomment-544196619),
  and doesn't allow for installing/ uninstalling files (like the default coredump script),
  which `rpk mode` does.
  
2. Having 2 different **packages**, one for dev mode, which doesn't install the systemd
  coredump script, and a production one that runs redpanda with `--lock-memory true` and the
  coredump script. This alternative could be very confusing for users, and adds a lot of
  overhead to our current packaging and installation process, since we'd have to maintain 2
  packages with minimal - albeit fundamental - differences.
  
3. Detecting which features it makes sense to activate, and which tuners rpk should run,
  based on the environment's resources at the moment of execution. This would be a 100%
  hands-free experience for the user, but it's a more involved solution that could take
  more than the time we have before redpanda's first release deadline (1 week at the time
  of writing). It should however be re-evaluated in the near future since it could greatly
  improve the user experience.

4. Having only one new field, `developer_mode`, and a new flag `--developer-mode` for rpk's
  `tune`, `start`, and `check` commands. This alternative was discarded in favor of more
  fine-grained control over which features and tuners should be toggled on or off.

## Unresolved questions

Should redpanda store the current machine's relevant state before switching to developer mode, so it
can reliably roll the changes back?
