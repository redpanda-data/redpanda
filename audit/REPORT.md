# Cloud topics correctness audit — findings for review

Any client can disable a cloud-topic partition for good by producing one control batch: the broker
throws on it, retries the same record for ever, and replays the poison on every restart. `DeleteRecords`
followed by a timestamp lookup makes the broker throw and drop the connection. Timestamp lookups
otherwise skip records without saying so. One read-replica topic stops L0 garbage collection across an
entire cluster, and object storage then grows without bound for every cloud topic on it.

We audited all 406 classes in `src/v/cloud_topics` at `4a4ddbc338`, raised 73 candidate defects,
confirmed 42 after adversarial review, and tested 26. Four are proven on a real cluster. Severity is
our judgement except where the "Proven by" column says otherwise.

## Defects, ranked

| # | Defect | Site | Proven by | Consequence |
|---|---|---|---|---|
| 1 | Timestamp seek returns the index entry *at* the target, not the last one before it | `level_one/common/object.cc:271` | unit; wrong on 14/47 probes | ListOffsets-by-timestamp skips up to one indexing interval (4 MiB default) of records. Unrecoverable — reads are forward-only. Also answered −1 for a timestamp that has records. |
| 2 | A client control batch becomes a placeholder with its record key stripped; the STM applicator then retries for ever without advancing | `level_zero/stm/placeholder.cc:32` | unit + caller chain | Partition stops applying. Durably committed, so every restart replays it. The produce handler does not reject `isControl`, so any client can send it. |
| 3 | Timestamp lookup after `DeleteRecords` builds a local-log reader at a Kafka offset below the local log's start | `frontend/frontend.cc:572`, `:509` | **6-node cluster**; 60 throws per run, and a no-trim control passes | Broker throws `std::runtime_error (Reader cannot read before start of the log 1000 < 7518)` and drops the connection. ListOffsets-by-timestamp does not answer at all. Deterministic. |
| 4 | The GC epoch snapshot filters on `is_cloud_topic()`, which tests `storage_mode` only and so admits a **read-replica** cloud topic. Read replicas have no `ctp_stm` and no epoch by design, and the join is fail-closed | `level_zero/gc/level_zero_gc.cc:471` | **6-node cluster**; 37 join failures naming the topic, all 3 brokers | Every L0 GC round is rejected for as long as the read replica exists — one broker completed no successful round at all. Object storage grows without bound for every cloud topic in the cluster. Needs `readreplica` **and** `storage.mode=cloud`, a combination `storage_mode_properties.h` permits. |
| 5 | Placeholder derives `last_offset_delta` from `record_count` instead of copying it | `level_zero/stm/placeholder.cc:17` | unit | Delivers a batch declaring a shorter offset span than it contains, with valid CRCs. Redpanda's own compaction produces such batches; cluster linking routes them here. |
| 6 | `DeleteRecords` to the high watermark stops reconciliation while the partition is idle | `reconciler/reconciliation_source.cc:179` | **6-node cluster**, both storage modes | Local log pinned, L1 retention blocked, cluster-wide L0 GC held. Clears on the next produce. Logs nothing above debug. |
| 7 | Deleting a cloud topic never removes its topic manifest | `metastore/topic_purger.cc:83` | **6-node cluster + bucket** | Keys are revision-scoped, so each creation of a name leaves one object behind for ever. A read replica of a recreated name resolves against the stale manifest. |
| 8 | The read path recomputes both CRCs instead of verifying the produce-time CRC stored in the object | `level_zero/reader/materialized_extent.cc:107` | unit | Corrupt L0 bytes are re-stamped valid and delivered as good records. Nothing on this path checks integrity. |
| 9 | The producer queue orders nothing for non-idempotent producers, at any cap value including 1 | `level_zero/common/producer_queue.cc:138` | unit, 5/5 | Records from one `no_producer_id` producer reach the log out of send order. |
| 10 | `remove_topics` reads an error as "no extents", tombstones the partition's metadata row anyway, and returns success | `lsm/state_update.cc:1486` | unit, fault injection | Destroys the only pointer to the surviving extent rows, orphaning them and their objects for ever. Three units found this independently. |
| 11 | A zero-record batch reaches a `vassert` with no guard before it | `level_zero/stm/placeholder.cc:19` | unit + caller chain | Broker aborts, in every build. |
| 12 | `flush()` stops at the first unhealthy metastore partition | `replicated_metastore.cc:1128` | unit | Higher-numbered partitions never advance `max_persisted_seqno`, the L1 GC gate, so healthy partitions stop collecting. |
| 13 | The reconciler's batch-cache fast path returns before it records the placeholder read floor | `level_zero/frontend_reader/level_zero_reader.cc:136` | unit, PARTIAL | Harmless in cloud mode — the floor is read only in the `tiered_cloud` branch and a low floor is conservative. After a `cloud → tiered_cloud` flip the gap can span the whole pre-flip log, which is the likely shape for a low-throughput topic. |

Contract C21 (timestamp → offset) is the weakest area: #1, #3, and #3's second half are three defects in
one lookup path. Their fixes interact — see below.

## What is proven, and what is not

The **mechanism** claims are reliable. Site claims were correct 14 times out of 14 under test, two
defects were found independently by two units each, and #10 by three.

The **severity** claims are weaker. Every correction during validation landed there. Two of two
"permanent" claims that we tested self-healed. Read the Consequence column as our best current
reading, not as measured.

Sixteen of the 42 confirmed defects have no test. #2 and #11 cannot be driven by a stock client —
both need a deliberately malformed batch — so they stay at unit level.

**One open question, not a filed defect.** Creating a read replica with `readreplica` alone and no
`storage.mode` yields `read_replica=true` with the default storage mode, even when the only manifest for
that name is a cloud-topic manifest. So a tiered-storage read replica is created against cloud-topics
metadata, silently. It may be unable to serve reads. Unexamined.

## Three fixes that break each other

- **C21 is one change, not three.** #1 moves the seek earlier. #3's clamp must be applied in offset
  space or it undoes #1 — and the throw shows a Kafka offset reaching a log-offset reader, so the clamp
  and the offset-space mixing are the same code path. The remaining fallback would *mask* #1. Land #1
  first.
- **#10 has an order that matters.** Fix the dropped `has_more` flag first, then error propagation, and
  add the `can_apply` size guard **last**. Added first, that guard turns a batch that currently commits
  into a topic delete that fails for ever.
- **The two GC defects pull opposite ways.** Fixing the cross-replica `max` that discards the reader
  holdback triggers the fail-closed stall. One design, not two patches. #13 is subsumed by fixing the
  `tiered_cloud` read floor; the reverse is not true.

## Three tests and a doc comment defend a bug

Worse than no test, and they cluster in the best-covered code. Four of seven; the rest are in
`BUGS.md`.

1. `L1ObjectsIndex.TimestampSearch` (`object_test.cc:157`) passes while asserting #1's unsafe seek
   positions.
2. `object.h:186-199` — the doc comment *specifies* the unsafe rule. Fix it with the code, or someone
   will restore the bug from the contract.
3. `L1Objects.TimestampSearch` (`object_test.cc:334`) is vacuous: `indexing_interval=1` indexes every
   batch, and line 404 kills the second half of the test.
4. `DbDomainManagerTest.TestRemoveTopicsBatchedExtentRemoval:1766` passes while the 1000-extent bound
   is violated. Its own sibling test asserts that bound.

## Next steps

1. Fix #1 with its doc comment and test together. One line of code, highest impact.
2. Fix #3. It is deterministic, cluster-proven, and breaks a Kafka API after an ordinary admin call.
3. Decide #2's severity. It is remotely triggerable and survives restarts. A guard on
   `frontend::replicate` matching the one `replicate_at_offset` already has looks sufficient.
4. Confirm #4 on a two-cluster read-replica setup. Largest claimed blast radius still unobserved.
5. Build a `cloud → tiered_cloud` mode-flip fixture. Four defects cannot be checked without it,
   including #13's real severity.

Failing reproducers for #1, #2, #3, #4, #5, #6, #7, #8, #9, #10, #11 and #12 are committed in this
worktree. Run cluster tests with `./tools/dt run <symbol>`.

## Appendices

- `BUGS.md` — validated defects, evidence, and all five fix interactions.
- `map/CONTRACTS.md` — 60 invariants with owners and client-visible symptoms.
- `map/RESOLUTIONS.md` — corrections to the above; it wins where they disagree.
- `findings/` — per-module findings, including the 31 rejected and why.
- `validation/` — test evidence and the audit's own error analysis.
- `NOTEBOOK.md` — coverage checklist and per-wave rejection rates.
