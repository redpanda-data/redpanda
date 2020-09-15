# Engineer onboarding

## Bootstrap

- [ ] Create gsuite account
- [ ] Add to GitHub organization
  - [ ] Change your email for vectorizedio organization to point to vectorized
- [ ] [Setup google cloud account](https://console.cloud.google.com/)
  - [ ] Get added to the `redpandaci` project
  - [ ] Get added to the `vectorizedio` project
- [ ] [Setup AWS cloud account](https://vectorizedio.signin.aws.amazon.com/console)
- [ ] Laptop (strong suggest: fedora:32)
- [ ] [Setup clubhouse.io](https://clubhouse.io)
- [ ] [Join our slack](https://join.slack.com/t/vectorizedio/signup)

## Day 1

- [ ] Read the [Company Identity](./identity.md)
- [ ] Welcome e-mail
   * Primary focus area
   * Contact info
- [ ] Invite to chat rooms
- [ ] Invite to stand-up
- [ ] Team introductions
- [ ] Schedule 1-1 meetings with all team members and recurring 1-1 with manager
- [ ] Fork [`v`](https://github.com/vectorizedio/v)
   * Compile `v` @ master (see the [readme](../README.md) for instructions)
- [ ] Access to [`v-dev`](https://groups.google.com/a/vectorized.io/forum/#!forum/v-dev) and archives
- [ ] [Development process](https://github.com/vectorizedio/v/blob/master/CONTRIBUTING.md)
- [ ] Deploy a cluster as if you were a customer
- [ ] Scan our Request For Comment process - [RFC](./rfcs/README.md)
- [ ] Scan our [dev design document folder](https://drive.google.com/drive/folders/1eXsW_ldmziIuLHtk5riB02peCQyDbtYP
)
- [ ] See our pre-recorded [pandachat discussions](https://drive.google.com/drive/u/0/folders/1GvuNZzPn7FmXB5OOLM1NA5Du5SQ63cw4)

Be sure to change your notification settings in GitHub for the Vectorized
organization so that Vectorized-related GitHub e-mails are delivered to your
Vectorized e-mail address.

1. Register `<you>@vectorized.io` with GitHub
   * See _add email address_ at https://github.com/settings/emails
2. Visit https://github.com/settings/notifications
3. Scroll down to the section _Custom routing_
4. Update the e-mail address for the `vectorizedio` organization by selecting
   your `<you>@vectorized.io` e-mail address.
5. Follow [this guide](https://help.github.com/en/articles/choosing-the-delivery-method-for-your-notifications)
   to choose the notification delivery method of your preference.

## Background reading

> All of the Redpanda (and other) design documents related to Vectorized software
> can be found in the shared folder:
> https://drive.google.com/drive/folders/1eXsW_ldmziIuLHtk5riB02peCQyDbtYP

Quick introduction to logs and Kafka.

1. https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying
2. https://kafka.apache.org/documentation/#introduction
3. https://kafka.apache.org/documentation/#uses
4. (stretch goal) https://kafka.apache.org/documentation/#design

Vectorized is building a _Kafka API_-compatible system (Redpanda) to address
limitations in the Kafka implementation related to performance, manageability,
and safety.

1. Performance
   * Issues surrounding the JVM and GC
   * Outdated server design for modern hardware

2. Safety
   * https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum

3. Manageability
   * https://kafka.apache.org/documentation/#operations
   * Running a system doesn't need to be this complex

Some of the technologies Vectorized is using :

1. [Seastar](http://seastar.io/), a High performance server-side application framework(they have an amazing [tutorial](https://github.com/scylladb/seastar/blob/master/doc/tutorial.md))
2. [Raft](https://raft.github.io/) an algorithm for solving the problem of distributed consensus. Here are some references to learn more about it:
   * http://thesecretlivesofdata.com/raft/
   * https://eli.thegreenplace.net/2020/implementing-raft-part-0-introduction/

## Laptop Setup

### Thinkpad p53

1. Go into BIOS and make these changes:
  * Display: discrete graphics
  * Secure Boot: disabled
  
2. Install fedora 32
  * Select "Custom" disk partitioning options, click automatically create partitions then change the / and /home partitions to be XFS.
  
3. Installing nvidia Drivers from rpmfusion
  * Run the following command:
    ```
    sudo -- sh -c ' \
    dnf install https://download1.rpmfusion.org/free/fedora/rpmfusion-free-release-$(rpm -E %fedora).noarch.rpm https://download1.rpmfusion.org/nonfree/fedora/rpmfusion-nonfree-release-$(rpm -E %fedora).noarch.rpm && \
    dnf update -y && \
    dnf install -y akmod-nvidia'
    ```
  * Then reboot to enable the nvidia driver.

4. After reboot your touchpad might stop working.
  * To fix this parameter should be added to the kernel command line:
    ```
    sudo grubby --update-kernel=ALL --args="psmouse.elantech_smbus=0"
    ```

