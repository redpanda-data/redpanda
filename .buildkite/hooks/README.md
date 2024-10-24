Hooks in this folder are expected to come from the vtools repo at the 
time the CI pipeline runs, as these are repository-level hooks. The 
ones that cannot be shared and need to be copy-pasted (from the vtools 
repo into this folder) are the `post-checkout` and `pre-command` 
hooks. See [here][bk-hooks] for more on how hooks work.

[bk-hooks]: https://buildkite.com/docs/agent/v3/hooks#job-lifecycle-hooks
