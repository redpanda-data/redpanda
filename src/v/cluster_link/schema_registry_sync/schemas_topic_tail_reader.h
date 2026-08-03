/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "cluster_link/schema_registry_sync/tail_reader.h"
#include "model/fundamental.h"

#include <expected>
#include <memory>

namespace kafka::client {
class cluster;
class direct_consumer;
} // namespace kafka::client

namespace cluster_link::schema_registry_sync {

/// \brief A `tail_reader` that consumes the source's `_schemas` topic over the
/// Kafka API.
///
/// Cheaper and lower-latency than polling the source's HTTP API: consuming the
/// topic reports registrations, deletions and mode/config changes in one pass,
/// and costs a buffered fetch when nothing changed.
///
/// Only the record *keys* are read. Each names a target, which the caller then
/// re-reads from the source Schema Registry, so the record values -- and any
/// vendor differences in them -- never matter, and a replayed record is a
/// no-op.
///
/// A source that does not expose `_schemas` (Confluent Cloud, Redpanda
/// Serverless) simply never arms. That is not an error: the link keeps
/// replicating on its full-sync interval.
class schemas_topic_tail_reader final : public tail_reader {
public:
    /// Least time between attempts to resume a stopped tail; the attempt lands
    /// on the next poll at or after it expires.
    static constexpr auto default_rearm_backoff = std::chrono::minutes{1};
    /// Paces the Kafka fetcher's long-poll when the source topic is idle: the
    /// direct consumer's 100ms default costs about ten empty fetches a second
    /// per link, while anything longer is added to tail latency, since a change
    /// written during a parked fetch is not seen until it expires.
    static constexpr auto default_idle_fetch_max_wait = std::chrono::seconds{1};

    explicit schemas_topic_tail_reader(
      kafka::client::cluster& source,
      ss::lowres_clock::duration rearm_backoff = default_rearm_backoff,
      std::chrono::milliseconds idle_fetch_max_wait
      = default_idle_fetch_max_wait);
    schemas_topic_tail_reader(const schemas_topic_tail_reader&) = delete;
    schemas_topic_tail_reader&
    operator=(const schemas_topic_tail_reader&) = delete;
    schemas_topic_tail_reader(schemas_topic_tail_reader&&) = delete;
    schemas_topic_tail_reader& operator=(schemas_topic_tail_reader&&) = delete;
    // Out-of-line so the header need not see direct_consumer's definition.
    ~schemas_topic_tail_reader() override;

    ss::future<tail_availability> arm(ss::abort_source&) override;

    /// Disarms rather than failing: a poll that hits trouble reports whatever
    /// it decoded before it, because those records are already consumed and
    /// dropping them would strand their changes, and because a dead tail is an
    /// availability change rather than a sync error.
    ///
    /// Also where a stopped tail resumes, so a link that relies on tailing
    /// recovers without waiting for a full sync to re-arm it.
    ss::future<source_result<tail_batch>> poll(ss::abort_source&) override;

    ss::future<> rewind() override;

    /// Aborts a parked idle long-poll via the fetcher's abort source, so
    /// teardown does not wait it out.
    ss::future<> stop() override;

private:
    /// Releases the consumer, so later polls report nothing until a subsequent
    /// `arm` builds a new one. Does nothing when tailing was not live.
    ss::future<> release_consumer();

    /// Stops tailing, reporting `reason` at info as the cause. Silent when
    /// tailing was not live, so a caller whose message matters either way logs
    /// it itself.
    ss::future<> disarm(ss::sstring reason);

    /// The source's current high watermark for the topic, i.e. the position a
    /// fresh consumer must start from. The error carries why the source would
    /// not report it within a few attempts, so tailing that stays off is
    /// diagnosable -- a missing Kafka READ ACL on the topic otherwise looks
    /// like a source that does not expose it at all.
    ///
    /// Resolved here rather than left to the consumer's `latest` reset policy:
    /// that resolves lazily on the consumer's first background fetch, so the
    /// position would land wherever the log happened to be by then, silently
    /// skipping anything written in between.
    ss::future<std::expected<kafka::offset, ss::sstring>>
    source_high_watermark(ss::abort_source&);

    /// One attempt at `source_high_watermark`.
    ss::future<std::expected<kafka::offset, ss::sstring>>
    list_source_high_watermark(ss::abort_source&);

    /// Brings a consumer up reading from `offset` and, on success, makes it
    /// this reader's. Reports why it could not rather than throwing, so a
    /// caller can treat a failed start as tailing being unavailable.
    ss::future<tail_availability> start_consumer_at(kafka::offset offset);

    /// Folds what the consumer has buffered into `batch`, up to one poll's byte
    /// budget. `batch` is the caller's so that what was decoded before a
    /// failure survives one.
    ss::future<> drain(ss::abort_source&, tail_batch& batch);

    kafka::client::cluster* _source;
    /// The consumer's presence is the armed state: it holds the tail position,
    /// so releasing it is what disarms tailing.
    std::unique_ptr<kafka::client::direct_consumer> _consumer;
    /// Where a re-arm resumes: one past the last offset `drain` passed over.
    /// Survives `disarm`, so a dead tail can resume without a full sync. Unset
    /// until the first successful arm, which is what keeps a source that cannot
    /// be tailed at all from being retried on every poll.
    std::optional<kafka::offset> _next_offset;
    /// Where the last `poll` began reading, i.e. where `rewind` puts it back.
    std::optional<kafka::offset> _poll_start;
    ss::lowres_clock::duration _rearm_backoff;
    std::chrono::milliseconds _idle_fetch_max_wait;
    /// Earliest time `poll` may try to resume.
    ss::lowres_clock::time_point _next_arm_attempt{};
    /// Set once by `stop` and never cleared, so an in-flight arm or poll cannot
    /// resurrect the reader during teardown.
    bool _stopped{false};
};

class schemas_topic_tail_reader_factory final : public tail_reader_factory {
public:
    std::unique_ptr<tail_reader> create(link* link) override;
};

} // namespace cluster_link::schema_registry_sync
