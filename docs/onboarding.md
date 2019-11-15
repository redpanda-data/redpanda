# Engineer onboarding

## Bootstrap

- [ ] Create gsuite account
- [ ] Add to GitHub organization
- [ ] Setup cloud account(s)
- [ ] Laptop (suggest: fedora workstation)

## Day 1

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
- [ ] [Starter issues on GitHub](https://github.com/vectorizedio/v/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
   * Review the different tags' meaning in the [tag guide](github_tags.md).
- [ ] [Development process](https://github.com/vectorizedio/v/blob/master/CONTRIBUTING.md)

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

Quick introduction to logs and Kafka.

1. https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying
2. https://kafka.apache.org/documentation/#introduction
3. https://kafka.apache.org/documentation/#uses
4. (stretch goal) https://kafka.apache.org/documentation/#design

Vectorized is building a _Kafka API_-compatible system (Red Panda) to address
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

Some of the technologies Vectorized is using do this:

1. [Seastar](http://seastar.io/) (and the amazing [Seastar tutorial](https://github.com/scylladb/seastar/blob/master/doc/tutorial.md))
2. [Raft consensus](https://raft.github.io/)

All of the Red Panda (and other) design documents related to Vectorized software
can be found in the shared folder:

https://drive.google.com/drive/folders/1eXsW_ldmziIuLHtk5riB02peCQyDbtYP
