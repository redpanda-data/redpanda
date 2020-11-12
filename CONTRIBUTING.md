# Contributing to Redpanda

Please submit pull requests on GitHub against the `dev` branch and reference any
relevant issues using the `fixes: #NN` magic tag.

## Pull request guidelines

Take care to separate your changes into commits that each address a single,
logical issue. Many reviewers examine each commit in isolation, so a commit
should be understandable on its own within the patch series.

We generally do not squash pull requests into a single commit, nor do we push
separate commits onto a pull request branch to address feedback. Rather, we aim
to fold changes back into the relevant commits to keep the history as concise as
possible.

Our CI system enforces coding style (see style configuration files in the root
of the tree) for each pull request. We prefer that each commit be styled
correctly, but we also strive to make reviewing as easy as possible. Therefore,
if a small change in a commit results in a large amount of reformatting, it is
acceptable to include a minimal patch plus an additional patch that applies only
the reformatting.
