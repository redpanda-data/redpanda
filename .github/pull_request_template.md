## Cover letter

Describe in plain language the motivation (bug, feature, etc.) behind the change in this PR and how the included commits address it.

<!-- Use the GitHub keyword `Fixes` to link to bug(s) this PR will fix. -->
Fixes #ISSUE-NUMBER, Fixes #ISSUE-NUMBER, ...

## Backport Required

<!-- Specify which branches this should be backported to, e.g.: -->
- [ ] not a bug fix
- [ ] issue does not exist in previous branches
- [ ] papercut/not impactful enough to backport
- [ ] v22.3.x
- [ ] v22.2.x
- [ ] v22.1.x

## UX changes

Describe in plain language how this PR affects an end-user. What topic flags, configuration flags, command line flags, deprecation policies etc are added/changed.

<!-- don't ship user breaking changes. Ping PMs for help with user visible changes  -->

## Release notes
<!--

If this PR does not need to be included in the release notes, then
simply have a bullet point for `none` directly under the `Release notes`
section, e.g.

* none

Otherwise, add one or more of the following sections. A section must have
at least 1 bullet point. You can add multiple sections with multiple
bullet points if this PR represents multiple release note items. See
the CONTRIBUTING.md guidelines for more details.

### Features

* Short description of the feature. Explain how to configure the new feature if applicable.

### Improvements

* Short description of how this PR improves redpanda.

-->
