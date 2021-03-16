---
title: Redpanda v21.3.x release notes
order: 0
---

# Redpanda v21.3.x release notes

(Add upgrade process)

## [release v21.3.4](https://github.com/vectorizedio/redpanda/releases/tag/v21.3.4)

- Fix: Fixed a configuration binding bug in rpk

## [release v21.3.3](https://github.com/vectorizedio/redpanda/releases/tag/v21.3.3)

- Fix: fixed configuration invariant checking

## [release v21.3.2](https://github.com/vectorizedio/redpanda/releases/tag/v21.3.2)

- Fix: Makes startup errors fatal when changing node-id

## [release v21.3.1](https://github.com/vectorizedio/redpanda/releases/tag/v21.3.1)

This is a stability and bugfix release.
There are major infrastructure patches that are not yet public or documented like idempotent producers.
They will be release on the last release of March.
This release includes 246 commits (290 files changed)

What's new in this release?

- HTTP API is almost ready for public consumption.
- Idempotent producers are finished. Will be released with transaction support at the end of the month.
- SASL authentication with SCRAM support was added.
- S3 archival support is added to the tree with a release ready for public consumption in a couple of weeks.
- Kubernetes operator infrasture has landed in this release.