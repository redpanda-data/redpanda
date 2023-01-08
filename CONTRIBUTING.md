# Contributing to Redpanda

Welcome! This guide is intended to help developers that are new to the community
navigate the process of making contributions to the Redpanda project. Please be
sure to also read our [code of conduct](CODE_OF_CONDUCT.md). We work hard to
make this a welcoming community for all, and we're excited that you are here!

The basics:

* Use the [Issue Tracker](https://github.com/redpanda-data/redpanda/issues/) to
  report bugs, crashes, performance issues, etc... Please include as much detail
  as possible.
  
* [Discussions](https://github.com/redpanda-data/redpanda/discussions) is a great
  venue to ask questions, start a design discussion, or post an RFC.
  
* We ask that developers sign our [contributor license
  agreement](https://github.com/redpanda-data/redpanda/tree/dev/licenses). The
  process of signing the CLA is automated, and you'll be prompted with instructions
  the first time you submit a pull request to the project.

# Submitting changes

We use a standard GitHub fork + pull request model for merging and reviewing
changes. New pull requests should be made against the upstream `dev` branch.
After a pull request is opened it will be reviewed, and merged after
passing continuous integration tests and being accepted by a project or
sub-system maintainer.

## Review process

In addition to the content of the changes being made, reviews will consider
three additional properties:

1. Formatting of code and documentation
2. Composition of commit history
3. Content of commit messages

We care a great deal about the hygiene of the patches that are merged into the
project, and we've designed our review process and standards to reflect that.
However, we're always trying to improve, and welcome any suggestions and
feedback.

## Formatting

Located in the root of the Redpanda project are style property files for various
languages used in the project (e.g.
[clang-format](https://github.com/redpanda-data/redpanda/blob/dev/.clang-format)
for C/C++).  Our continuous integration checks enforce these styles by verifying
changes made in each pull request.  All mainstream editors and IDEs should
provide support (either natively or through a plugin) for integrating with code
formatting tools that can consume the project property files. Configuring
your editor to do this is highly recommended.

Styling rules are enforced at the granularity of the pull request, rather than on
each commit. This is important because occasionally a small change may cause a large
reformatting, which makes reviewing changes difficult. In this case it would be
appropriate to split the code change and formatting into separate commits. This is
usually unnecessary.

## Commit history

The ordering and structure of commits in a pull request is very important. This
is because the way in which a large change is broken down into individual
commits has a significant impact on the review process and later on the ability
to efficiently find certain types of regressions. A good rule of thumb is that
a set of changes is in good shape when the following properties hold:

1. Each commit introduces a single logical change
2. Each commit represents a tree that builds without errors

Combined, these two properties tend to produce a history with changes that are
easier to review (smaller, focused commits), and ensure that tools like `git
bisect` are able to be applied to the tree.

Here is an example of commits in a hypothetical pull request:

```
b9f49db32 credits: opening sequence
b3032f211 scene: morty and summer enter the garage
60a49a8b9 scene: dog robots build space laser
a34b3eaa0 scene: morty turns into a glass jar
187547195 credit: closing sequence
```

After receiving feedback, it may be the case that the following additional
changes are made:

```
b9f49db32 credits: opening sequence
b3032f211 scene: morty and summer enter the garage
60a49a8b9 scene: dog robots build space laser
a34b3eaa0 scene: morty turns into a glass jar
187547195 credit: closing sequence

// extra commits
20929ab22 add extra joke to garage scene
330fff211 add a new scene in living room
b0329aaa1 morty glass jar should also levitate
```

Before merging we expect that the extra commits be folded back into any previous
commits that they modify, resulting in concise commit history that doesn't
reflect the intermediate changes made based on feedback. In this example, the
following is a reasonable final set of commits:

```
cbff210db Merge pull request #123 from rick/morty
b9f49db32 credits: opening sequence
b3032f211 scene: morty and summer enter the garage
60a49a8b9 scene: dog robots build space laser
a34b3eaa0 scene: morty turns into a glass jar
330fff211 scene: summer and snuffles watching tv
187547195 credit: closing sequence
```

It is common to force push to the pull request branch with the modified history.
In this case it can be useful to use the `--keep-base` option to `git rebase`
which will ensure a minimal change set for reviewers.  However, depending on the
nature of pull request and feedback it may be useful to push extra commits and
clean up the history as a last step.

## Commit messages

In addition to code formatting and commit history discussed above, the commit
messages themselves (both the format and content being communicated) are also
important.  First let's take a look at formatting.

It may be surprising that we consider commit message formatting to be important
enough to have its own section in the contributors guide. There are a couple
reasons for this.  First, in our experience we have found that adopting a
standard for issues related to style and formatting helps reduce cognitive load
when consuming content like code or technical writing. But from a practical
standpoint, many developers use terminal based editors with restrictions on
line width which is the primary dimension to the formatting guidelines of commit
messages.

Here is a sample commit message and below we'll discuss formatting guidelines in
the context of this message:

```
server: display redpanda logo in log when starting

When redpanda starts it logs lots of important messages like the active
configuration and details about the system resources. However, it fails
to do anything fun. This patch resolves this shortcoming by displaying
the following redpanda ascii art logo when the system starts.
                                                                                                                      
       ___           ___                         ___         ___           ___                         ___         
      /\  \         /\__\         _____         /\  \       /\  \         /\  \         _____         /\  \        
     /::\  \       /:/ _/_       /::\  \       /::\  \     /::\  \        \:\  \       /::\  \       /::\  \       
    /:/\:\__\     /:/ /\__\     /:/\:\  \     /:/\:\__\   /:/\:\  \        \:\  \     /:/\:\  \     /:/\:\  \      
   /:/ /:/  /    /:/ /:/ _/_   /:/  \:\__\   /:/ /:/  /  /:/ /::\  \   _____\:\  \   /:/  \:\__\   /:/ /::\  \     
  /:/_/:/__/___ /:/_/:/ /\__\ /:/__/ \:|__| /:/_/:/  /  /:/_/:/\:\__\ /::::::::\__\ /:/__/ \:|__| /:/_/:/\:\__\    
  \:\/:::::/  / \:\/:/ /:/  / \:\  \ /:/  / \:\/:/  /   \:\/:/  \/__/ \:\~~\~~\/__/ \:\  \ /:/  / \:\/:/  \/__/    
   \::/~~/~~~~   \::/_/:/  /   \:\  /:/  /   \::/__/     \::/__/       \:\  \        \:\  /:/  /   \::/__/         
    \:\~~\        \:\/:/  /     \:\/:/  /     \:\  \      \:\  \        \:\  \        \:\/:/  /     \:\  \         
     \:\__\        \::/  /       \::/  /       \:\__\      \:\__\        \:\__\        \::/  /       \:\__\        
      \/__/         \/__/         \/__/         \/__/       \/__/         \/__/         \/__/         \/__/        
         

The logo is printed to standard out on start-up and when logging to a
normal log file. It is not displayed when logging to journald as the
formatting is not particularly nice.

The ascii art string literal is annotated with a compiler directive to
indicate that this string is rarely used and can be stored in a location
optimized for such data.
```

The format of the subject is `area[/detail]: short description` and the entire
line should be no more than 50 characters in length. The `prefix:` is helpful
for readers to quickly identify the relevant subsystem. We recommend taking
a look at recent Git history for examples of common `prefix:` choices.

There is no restriction on the length of the body, but lines should
wrap at 72 characters. The exception to this rule is text that should be
formatted verbatim like log messages, stack traces, and redpanda ascii art.

### Content

While there is no algorithm for writing the perfect commit message, we've found
that working towards the following three goals is often a positive signal that
a commit message is in good shape:

1. Explain why the change is being made (the before, and after)
2. Describe important / non-obvious technical details
3. Require no extra context to understand

Good technical communication takes a lot of practice, and what makes sense to
one person isn't always optimal for another reader. So our suggestion is to
err on the side of more detail, and not stress about making it perfect. Read
on below if you are curious for more details about these bullet points.

### Explanation and advice

When composing a commit message it is important to consider that the content
serves two distinct goals: optimizing for reviewer time and precision, and
providing a historical narrative of changes. In the short term a commit message
will be the first thing a reviewer consumes, and can have a dramatic impact on
the review process, even for small changes which might require a lot of context
to understand.

An important property of a commit message is that it is understandable when read
in the future by developers that did not participate in the original design and
review process. In complex systems like Redpanda it is not uncommon to review
historical changes to sub-systems while debugging an issue or investigating
regressions. But stumbling upon what looks like a critical change with no
explanation can make this process slow and arduous. Since the chance of any one
commit being the bottleneck in such a process are tiny, the only robust solution
is to treat each commit as being likely involved in such a process.

A good rule of thumb is one paragraph for describing why the change is being
made. This should generally stand independent of the physical change being made,
and describe the state of the system being transitioned _away from_, why that
state is no longer desirable, and how the new state addresses those shortcomings.

It is much harder to decide what technical details are important or non-obvious.
But, if you happened to think one aspect of the changes were interesting or
challenging, reviewers and future debuggers will probably have the same--but
magnified--reaction.

Finally, the commit message should stand on its own. This means that it should
largely not depend on or refer to other commits in a series (e.g. "in the
previous commit..." or "and in a later commit this will be implemented"), nor
should it refer to out-of-band conversations (e.g. "in last week's meeting").
Refer to high-level context in the pull request body.

## Pull request body

Creating a new pull request (PR) will start with section headers in the body. Fill out the sections with information to help guide the PR reviewers in understanding the code changes. The body will also be parsed by scripts to generation release notes, so be sure to adhere to the structure as documented.

### Contents of the top section

Describe, in plain language, the motivation behind the change (bug fix, feature, improvement) in this PR and how the included commits address it, e.g.

```
Some users complain the `abort` button is hard to find. Talked with design
team and they suggested increasing the size of the button instead of
changing the color or moving its position. So, increased width of button
to match the width of other buttons on the page.
```

If any of the git commits in the PR address a bug, link the issue that the PR will address using the `Fixes` [keyword](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue#linking-a-pull-request-to-an-issue-using-a-keyword), e.g.

```
Fixes #123
```

If the PR is a backport, link to the backport with `Backport of PR` as well as the issue, e.g.

```
Backport of PR #789
Fixes #567
```

### Contents of `Backports Required` section

Check at least one of the checkboxes if the PR is not a backport. If the PR is a backport, then do not check any of the check boxes. The `none` checkboxes and the checkboxes for backport branches are mutually exclusive. Checking the checkbox for a release branch means the PR will need backport after merge.

```
## Backports Required

- [ ] none - not a bug fix
- [ ] none - issue does not exist in previous branches
- [ ] none - papercut/not impactful enough to backport
- [x] v22.3.x
- [x] v22.2.x
- [x] v22.1.x
```

If no backport is needed, check the checkbox for one of `none`: 

```
## Backports Required

- [x] none - not a bug fix
- [ ] none - issue does not exist in previous branches
- [ ] none - papercut/not impactful enough to backport
- [ ] v22.3.x
- [ ] v22.2.x
- [ ] v22.1.x
```

### Contents of `UX Changes` section

Describe, in plain language, how this PR affects an end-user. Explain topic flags, configuration flags, command line flags, deprecation policies, etc. that are added or modified. Don't ship user breaking changes. Ask the @redpanda-data/product team if you need help with user visible changes.

Example:

```
## UX Changes

* Additional configuration section for coffee strength in `config.json` with `strength` key.
* Getting started documentation needs update in separate PR
```

### Contents of `Release Notes` section

The contents of this section is used to generate the release notes published on the [releases](https://github.com/redpanda-data/redpanda/releases) page.

If the PR is created towards `dev` branch, this section MUST be filled out with either sub-sections or the `none` bullet point.

#### Adding a `Bug fixes` sub-section

If the PR is a bug fix targeting `dev` branch, add a **Bug Fixes** sub-section with a bullet point explaining the bug that is fixed, e.g.

```
## Release Notes

### Bug Fixes

* Fix crash if log line is over 255 characters.
```

Do not include a link to the issue that is fixed in the bullet point. This will be automatically derived for the release notes based on the `Fixes` keyword in the top section.

If the PR is a bug fix targeting a release branch, e.g. `v22.2.x`, as a backport PR, then this section is optional because the release notes of the original PR towards `dev` branch will be used. Adding a release notes section in a backport PR will override the original PR's release notes section.

If the PR is a bug fix targeting a release branch as a unique fix, i.e. not a backport PR, then add a **Bug Fixes** sub-section with a bullet point explaining the bug that is fixed.

#### Adding a `Features` sub-section

If the PR introduces new functionality, include it under a **Features** sub-section. Be sure to explain how to configure the feature if applicable, e.g.

```
## Release Notes

### Features

* Add option to configure strength of coffee. Set in `config.json` file with `strength` key. Valid values: `light`, `medium` (default), `strong`.
```

#### Adding an `Improvements` sub-section

Improvements make existing functionality perform more efficient without user intervention or change in functional behavior, e.g.

* speed of operation
* memory utilization
* disk utilization

If the PR includes improvements, add a bullet point under an **Improvements** sub-section, e.g.:

```
## Release Notes

### Improvements

* Eliminate 10 ms of delay in outputting debug log messages.
* Use 50% less memory for each debug log message.
```

#### Omitting release notes

A release notes entry is not needed if the PR only contains modifications that do not change the functionality of the product, e.g.

* bug fixes in tests that do not require a corresponding change to the core codebase
* typo fixes in documentation
* code formatting changes

Indicate this by adding a bullet point with `none`, e.g.

```
## Release Notes

* none
```

### Example PR Body

A collection of fictitious PR bodies that have a valid, expected format.

#### Example Feature PR Body

An example of a PR to `dev` branch that adds a new feature for the next release and references the tracking issue that will be closed when the PR is merged:

```
Closes #999456

Some users complain the `abort` button is hard to find. Talked with design
team and they suggested increasing the size of the button instead of
changing the color or moving its position. So, increased width of button
to match the width of other buttons on the page.

## Backports Required

- [x] none - not a bug fix
- [ ] none - issue does not exist in previous branches
- [ ] none - papercut/not impactful enough to backport
- [ ] v22.3.x
- [ ] v22.2.x
- [ ] v22.1.x

## UX Changes

* Additional configuration section for coffee strength in `config.json` with `strength` key.
* Getting started documentation needs update in separate PR

## Release Notes

### Features

* Add option to configure strength of coffee. Set in `config.json` file with `strength` key. Valid values: `light`, `medium` (default), `strong`.
```

#### Example Bug Fix PR Body

An example of a PR to `dev` branch where a bug fix also happens to add an improvement:

```
Fixes #999123

## Backports Required

- [ ] none - not a bug fix
- [ ] none - issue does not exist in previous branches
- [ ] none - papercut/not impactful enough to backport
- [x] v22.3.x
- [x] v22.2.x
- [x] v22.1.x

## UX Changes

## Release Notes

### Bug Fixes

* Fix crash if log line is over 255 characters.

### Improvements

* Eliminate 10 ms of delay in outputting debug log messages.
```

#### Example Backport PR Body

An example of a PR to a release branch of a simple backport:

```
Backport of PR #999234
Fixes #999123
```

#### Example Backport PR Body with Release Notes override

An example of a PR to a release branch with override of the release notes.

```
Backport of PR #999234
Fixes #999123

Cherry-pick of original PR had conflicts. The changes to eliminate the
crash was resolvable, but was unable to take the changes to eliminate
10 ms delay.

## Release Notes

### Bug Fixes

* Fix crash if log line is over 255 characters.
```
