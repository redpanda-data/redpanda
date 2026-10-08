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

#include "cluster_link/schema_registry_sync/schemas_topic_tail_reader.h"

#include "base/units.h"
#include "base/vassert.h"
#include "cluster_link/link.h"
#include "cluster_link/logger.h"
#include "cluster_link/utils.h"
#include "container/chunked_vector.h"
#include "json/chunked_input_stream.h"
#include "json/document.h"
#include "kafka/client/cluster.h"
#include "kafka/client/direct_consumer/direct_consumer.h"
#include "kafka/protocol/errors.h"
#include "model/batch_compression.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "pandaproxy/schema_registry/storage.h"
#include "ssx/future-util.h"
#include "ssx/sformat.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <rapidjson/error/en.h>

#include <ranges>

namespace cluster_link::schema_registry_sync {

namespace {

// The source topic to tail. Its name is fixed in Redpanda and is Confluent's
// `kafkastore.topic` default; a source that renamed it reads as not exposing it
// at all, and the link falls back to full syncs alone.
const ::model::topic& schemas_topic() {
    return ::model::schema_registry_internal_tp.topic;
}

// Schema Registry's log is totally ordered, so its store topic has exactly one
// partition: Redpanda fixes it, and Confluent documents `kafkastore.topic` as
// single-partition and validates it at startup.
::model::partition_id schemas_partition() {
    return ::model::schema_registry_internal_tp.partition;
}

// Bounds one poll's decoding work. The source produces one small record per
// Schema Registry write, so this is only reached while draining a backlog.
constexpr size_t max_poll_bytes = 1_MiB;

// A fetch that finds nothing buffered must return promptly, but `fetch_next`
// only reads its queue inside a `while (now < deadline)` loop, so a zero
// timeout would never read at all.
constexpr auto poll_timeout = std::chrono::milliseconds{1};

// Rate limit for the warnings about records this reader cannot use, applied per
// warning site. Set to the default full-sync interval, the period over which
// such a skipped change is outstanding, so each pending miss is reported about
// once.
constexpr auto ignored_record_log_rate = std::chrono::minutes{5};

kafka::client::direct_consumer::configuration
consumer_config(std::chrono::milliseconds idle_fetch_max_wait) {
    return kafka::client::direct_consumer::configuration{
      .max_fetch_size = static_cast<int32_t>(max_poll_bytes),
      .partition_max_bytes = static_cast<int32_t>(max_poll_bytes),
      // Start at the topic's end: everything already written is covered by the
      // full sync that arms this reader.
      .reset_policy = kafka::client::offset_reset_policy::latest,
      .max_wait_time = idle_fetch_max_wait,
      // One poll's worth, rather than the default 10MiB. Fetched records carry
      // their values -- schema bodies, up to megabytes each -- which this
      // reader never reads, so anything buffered past what a poll decodes is
      // held for a tail interval and then dropped. An oversized response is
      // admitted while the queue is empty, so the cap cannot stall a fetch.
      .max_buffered_bytes = max_poll_bytes,
    };
}

// Bounds the retry of the arming offset lookup. Short: arming is on the full
// sync's critical path, and a failure only postpones tailing to the next one.
constexpr int list_offsets_attempts = 3;
constexpr auto list_offsets_backoff = std::chrono::milliseconds{100};

// Reports that this source cannot be tailed, and why. Info: arming runs once
// per full sync, not once per tail tick.
void log_unavailable(std::string_view reason) {
    vlog(cllog.info, "Schema Registry topic tailing unavailable: {}", reason);
}

// Stops a consumer, logging rather than propagating: every caller is already
// reporting why it is being dropped, and a failed stop must not mask that.
ss::future<> stop_consumer_logged(
  kafka::client::direct_consumer& consumer, std::string_view what) {
    auto stopped = co_await ss::coroutine::as_future(consumer.stop());
    if (stopped.failed()) {
        auto ex = stopped.get_exception();
        vlog(cllog.warn, "Error stopping {}: {}", what, ex);
    }
}

// Starts the consumer reading from `offset`. Both steps in one coroutine so a
// caller can wrap them in a single `as_future`.
ss::future<>
start_at(kafka::client::direct_consumer& consumer, kafka::offset offset) {
    co_await consumer.start();
    chunked_vector<kafka::client::partition_assignment> assignments;
    assignments.push_back(
      kafka::client::partition_assignment{
        .partition_id = schemas_partition(), .next_offset = offset});
    chunked_vector<kafka::client::topic_assignment> topics;
    topics.push_back(
      kafka::client::topic_assignment{
        .topic = schemas_topic(), .partitions = std::move(assignments)});
    co_await consumer.assign_partitions(std::move(topics));
}

// A latest-timestamp ListOffsets for the single `_schemas` partition.
kafka::list_offsets_request high_watermark_request(kafka::leader_epoch epoch) {
    kafka::list_offset_topic topic_req;
    topic_req.name = schemas_topic();
    topic_req.partitions.push_back(
      kafka::list_offset_partition{
        .partition_index = schemas_partition(),
        // Rejects the answer of a leader our metadata is behind on, rather than
        // pinning the tail to an offset from a superseded log.
        .current_leader_epoch = epoch,
        .timestamp = kafka::list_offsets_request::latest_timestamp});
    kafka::list_offsets_request req;
    // A normal consumer, not a replica. Brokers answer a replica with the log
    // end offset and ignore the isolation level, so the default would pin the
    // tail past the high watermark the consumer can actually read.
    req.data.replica_id = ::model::node_id{-1};
    // read_uncommitted, matching the consumer that reads from this position.
    req.data.isolation_level = static_cast<int8_t>(
      ::model::isolation_level::read_uncommitted);
    req.data.topics.push_back(std::move(topic_req));
    return req;
}

// The oldest ListOffsets this reader will use. Below v4 the wire silently drops
// fields the request above depends on -- current_leader_epoch (v4+) and
// isolation_level (v2+) -- and a v0 reply carries its offsets in
// `old_style_offsets`, leaving the field read below at its -1 default, which
// would pin the tail at an invalid position. v4 is Kafka 2.1, so a source that
// cannot meet it is not one we expect to tail; it still replicates by full
// sync.
constexpr auto min_list_offsets_version = kafka::api_version{4};

// The high watermark a ListOffsets reply reports, or why it did not.
std::expected<kafka::offset, ss::sstring>
high_watermark_from(const kafka::list_offsets_response& resp) {
    const auto& resp_topics = resp.data.topics;
    if (resp_topics.size() != 1 || resp_topics.front().partitions.size() != 1) {
        return std::unexpected(
          ssx::sformat(
            "{} answered for {} topics, expected one topic with one partition",
            kafka::list_offsets_api::name,
            resp_topics.size()));
    }
    const auto& partition = resp_topics.front().partitions.front();
    if (partition.error_code != kafka::error_code::none) {
        return std::unexpected(
          ssx::sformat(
            "{} failed for partition {}: {}",
            kafka::list_offsets_api::name,
            partition.partition_index,
            kafka::error_code_to_str(partition.error_code)));
    }
    auto offset = ::model::offset_cast(partition.offset);
    if (offset < kafka::offset{0}) {
        return std::unexpected(
          ssx::sformat(
            "{} reported no offset for partition {}",
            kafka::list_offsets_api::name,
            partition.partition_index));
    }
    return offset;
}

// Maps one source `_schemas` record key to the refresh it implies. Only the key
// is read: it names the target to re-read from the source, so the record's
// value never matters.
//
// The key is read permissively -- peek `keytype` and `subject`, ignore every
// other field -- rather than through the store's strict per-keytype handlers:
// vendors write keytypes Redpanda does not model (e.g. Confluent's
// CLEAR_SUBJECT), and those keys still name a subject worth refreshing. Keys
// are small and bounded, so DOM parsing is appropriate here; the no-DOM rule
// covers schema bodies, which can be megabytes.
void record_refresh(::model::offset offset, iobuf key, tail_batch& batch) {
    // Every skip below leaves a source change unnoticed until the next full
    // sync, so they warn rather than pass quietly.
    static thread_local ss::logger::rate_limit rate{ignored_record_log_rate};

    ::json::Document doc;
    ::json::chunked_input_stream stream{std::move(key)};
    if (doc.ParseStream(stream).HasParseError() || !doc.IsObject()) {
        // A key this cannot read says nothing about the rest of the batch.
        vloglr(
          cllog,
          ss::log_level::warn,
          rate,
          "Ignoring unusable source _schemas key at offset {}: {}",
          offset,
          doc.HasParseError() ? rapidjson::GetParseError_En(doc.GetParseError())
                              : "not a JSON object");
        return;
    }
    // Top-level string members only, so a nested object carrying a field of the
    // same name cannot be mistaken for the key's own.
    auto member = [&doc](const char* name) -> std::optional<std::string_view> {
        auto it = doc.FindMember(name);
        if (it == doc.MemberEnd() || !it->value.IsString()) {
            return std::nullopt;
        }
        return std::string_view{
          it->value.GetString(), it->value.GetStringLength()};
    };

    const auto subject_str = member("subject");
    // Subjects in `_schemas` keys are context-qualified (":.ctx:sub") by the
    // source's convention, so parse them as qualified explicitly, as the HTTP
    // source reader does. Reading this node's qualified-subjects setting here
    // would flatten a source's qualified key into a default-context literal
    // whenever the destination has the setting disabled.
    auto subject = [&subject_str] {
        return ppsr::context_subject::from_string(
          *subject_str, ppsr::qualified_subjects_enabled::yes);
    };

    const auto keytype_str = member("keytype").value_or("");
    auto keytype = ppsr::from_string_view<ppsr::topic_key_type>(keytype_str);
    if (!keytype.has_value()) {
        if (subject_str.has_value()) {
            // A keytype this version does not model, naming a subject: refresh
            // it. Refreshing is idempotent and a subject the source does not
            // hold lists as absent, so over-matching is harmless, while
            // skipping would defer a real change to the next full sync.
            batch.subjects.insert(subject());
        } else {
            vloglr(
              cllog,
              ss::log_level::warn,
              rate,
              "Ignoring source _schemas record at offset {} with unknown "
              "keytype {}",
              offset,
              keytype_str);
        }
        return;
    }
    switch (*keytype) {
    case ppsr::topic_key_type::schema:
    case ppsr::topic_key_type::delete_subject:
        if (!subject_str.has_value()) {
            // Malformed: these keytypes always name a subject.
            vloglr(
              cllog,
              ss::log_level::warn,
              rate,
              "Ignoring source _schemas {} record at offset {} with no subject",
              keytype_str,
              offset);
            return;
        }
        batch.subjects.insert(subject());
        return;
    case ppsr::topic_key_type::config:
    case ppsr::topic_key_type::mode:
        // A null subject is the default context's target (the pre-context
        // serialization of `PUT /config`), not the registry-wide one, which
        // travels as an explicit ":.__GLOBAL:" subject.
        batch.mode_configs.insert(
          subject_str.has_value()
            ? subject()
            : ppsr::context_subject{ppsr::default_context, ppsr::subject{""}});
        return;
    case ppsr::topic_key_type::context: {
        // The context's own subjects each produce their own records, so this
        // only reports that the source's set of contexts may have changed.
        const auto ctx_str = member("context");
        if (!ctx_str.has_value()) {
            vloglr(
              cllog,
              ss::log_level::warn,
              rate,
              "Ignoring source _schemas CONTEXT record at offset {} with no "
              "context",
              offset);
            return;
        }
        batch.contexts.insert(ppsr::context{*ctx_str});
        return;
    }
    case ppsr::topic_key_type::noop:
        // Written on leader election by Confluent's kafkastore; names nothing.
        return;
    }
}

// Folds one fetched partition's batches into the refreshes their keys imply.
ss::future<>
decode_batches(chunked_vector<::model::record_batch> data, tail_batch& batch) {
    static thread_local ss::logger::rate_limit rate{ignored_record_log_rate};

    for (auto& raw : data) {
        const auto last = raw.last_offset();
        if (raw.header().attrs.is_control()) {
            continue;
        }
        try {
            auto records = raw.compressed()
                             ? co_await ::model::decompress_batch(raw)
                             : std::move(raw);
            const auto base = records.base_offset();
            records.for_each_record([&batch, base](::model::record rec) {
                record_refresh(
                  base + ::model::offset_delta{rec.offset_delta()},
                  rec.release_key(),
                  batch);
            });
        } catch (...) {
            auto ex = std::current_exception();
            if (ssx::is_shutdown_exception(ex)) {
                throw;
            }
            // A batch the source wrote inconsistently costs its own records,
            // not the tail: the rest of this poll still decodes.
            vloglr(
              cllog,
              ss::log_level::warn,
              rate,
              "Skipping undecodable source _schemas batch ending at offset {}: "
              "{}",
              last,
              ex);
        }
        // Decoding a backlog is pure CPU work; yield between batches so a large
        // one cannot stall the reactor.
        co_await ss::coroutine::maybe_yield();
    }
}

} // namespace

schemas_topic_tail_reader::schemas_topic_tail_reader(
  kafka::client::cluster& source,
  ss::lowres_clock::duration rearm_backoff,
  std::chrono::milliseconds idle_fetch_max_wait)
  : _source(&source)
  , _rearm_backoff(rearm_backoff)
  , _idle_fetch_max_wait(idle_fetch_max_wait) {}

schemas_topic_tail_reader::~schemas_topic_tail_reader() = default;

ss::future<> schemas_topic_tail_reader::release_consumer() {
    auto consumer = std::exchange(_consumer, nullptr);
    if (!consumer) {
        co_return;
    }
    co_await stop_consumer_logged(*consumer, "source _schemas consumer");
}

ss::future<> schemas_topic_tail_reader::disarm(ss::sstring reason) {
    if (!_consumer) {
        co_return;
    }
    vlog(cllog.info, "Schema Registry topic tailing stopped: {}", reason);
    co_await release_consumer();
}

ss::future<std::expected<kafka::offset, ss::sstring>>
schemas_topic_tail_reader::source_high_watermark(ss::abort_source& as) {
    // The topic is in the metadata cache by now, so a failure here is a stale
    // cached leader or a leadership move in flight -- worth another attempt
    // after refreshing metadata. Every failure is retried rather than
    // classified: a code this misses would silently stop tailing from ever
    // arming, while retrying a source that keeps refusing costs two requests.
    ss::sstring reason;
    for (const auto attempt : std::views::iota(0, list_offsets_attempts)) {
        if (attempt > 0) {
            co_await ss::sleep_abortable(list_offsets_backoff, as);
            auto refreshed = co_await ss::coroutine::as_future(
              _source->request_metadata_update(
                chunked_vector<::model::topic>{schemas_topic()}));
            if (refreshed.failed()) {
                auto ex = refreshed.get_exception();
                if (ssx::is_shutdown_exception(ex)) {
                    std::rethrow_exception(ex);
                }
                reason = ssx::sformat("source metadata request failed: {}", ex);
                continue;
            }
        }
        auto result = co_await list_source_high_watermark(as);
        if (result.has_value()) {
            co_return result;
        }
        reason = std::move(result.error());
    }
    co_return std::unexpected(std::move(reason));
}

ss::future<std::expected<kafka::offset, ss::sstring>>
schemas_topic_tail_reader::list_source_high_watermark(ss::abort_source& as) {
    // Leader lookup and version negotiation mirror kafka::client::client's own
    // list_offsets path, which cannot be reused here: that client owns its
    // cluster connection, and this reader shares the link's.
    auto& topics = _source->get_topics();
    const auto tp = ::model::topic_partition_view{
      schemas_topic(), schemas_partition()};
    auto leader = topics.leader(tp);
    if (!leader.has_value()) {
        // Metadata has not caught up with a new or moved leader yet.
        co_return std::unexpected(
          ssx::sformat(
            "source metadata reports no leader for partition {}",
            tp.partition));
    }

    auto req = high_watermark_request(
      topics.leader_epoch(tp).value_or(kafka::invalid_leader_epoch));

    // Negotiated against the leader we dispatch to, not the whole cluster: this
    // reads one partition, and a cluster-wide negotiation would refuse whenever
    // any broker fails to report, however healthy that leader is.
    auto version = co_await negotiate_api_version<kafka::list_offsets_api>(
      *_source, as, *leader, min_list_offsets_version);
    if (!version.has_value()) {
        co_return std::unexpected(std::move(version.error()));
    }
    auto resp = co_await ss::coroutine::as_future(
      _source->dispatch_to(*leader, std::move(req), *version));
    if (resp.failed()) {
        auto ex = resp.get_exception();
        if (ssx::is_shutdown_exception(ex)) {
            std::rethrow_exception(ex);
        }
        co_return std::unexpected(
          ssx::sformat(
            "{} request failed: {}", kafka::list_offsets_api::name, ex));
    }
    co_return high_watermark_from(resp.get());
}

ss::future<tail_availability>
schemas_topic_tail_reader::arm(ss::abort_source& as) {
    if (_stopped) {
        co_return tail_availability::unavailable;
    }
    as.check();
    if (_consumer) {
        // Already positioned: keep the position, so changes written since the
        // last poll are still replayed. Nothing below needs re-checking -- the
        // consumer tracks metadata itself, and re-deriving availability here
        // would let one transient failure discard a live position.
        co_return tail_availability::available;
    }

    // Ask for this topic specifically, so arming does not pull a large source's
    // whole topic list on every full sync.
    auto refreshed = co_await ss::coroutine::as_future(
      _source->request_metadata_update(
        chunked_vector<::model::topic>{schemas_topic()}));
    if (refreshed.failed()) {
        auto ex = refreshed.get_exception();
        if (ssx::is_shutdown_exception(ex)) {
            std::rethrow_exception(ex);
        }
        log_unavailable(ssx::sformat("source metadata request failed: {}", ex));
        co_return tail_availability::unavailable;
    }

    // Before the consumer exists, so the position is the log end as of *now*
    // rather than wherever its first background fetch happens to land.
    auto high_watermark = co_await source_high_watermark(as);
    if (!high_watermark.has_value()) {
        log_unavailable(
          ssx::sformat(
            "could not read the end of source {}: {}",
            schemas_topic(),
            high_watermark.error()));
        co_return tail_availability::unavailable;
    }

    auto armed = co_await start_consumer_at(*high_watermark);
    if (armed == tail_availability::available) {
        vlog(
          cllog.info,
          "Schema Registry topic tailing is configured to start at the end of "
          "{} (offset {})",
          schemas_topic(),
          *high_watermark);
    }
    co_return armed;
}

ss::future<tail_availability>
schemas_topic_tail_reader::start_consumer_at(kafka::offset offset) {
    auto consumer = std::make_unique<kafka::client::direct_consumer>(
      *_source, consumer_config(_idle_fetch_max_wait));
    auto started = co_await ss::coroutine::as_future(
      start_at(*consumer, offset));

    if (started.failed()) {
        auto ex = started.get_exception();
        co_await stop_consumer_logged(
          *consumer, "partially started source _schemas consumer");
        if (ssx::is_shutdown_exception(ex)) {
            std::rethrow_exception(ex);
        }
        log_unavailable(
          ssx::sformat("could not consume {}: {}", schemas_topic(), ex));
        co_return tail_availability::unavailable;
    }

    if (_stopped) {
        // stop() ran while this arm was suspended (the task stops its readers
        // while run_impl is live). Committing the consumer now would leave its
        // background fetchers running on the link's Kafka connection past the
        // reader's stop.
        co_await stop_consumer_logged(
          *consumer, "source _schemas consumer armed during reader stop");
        co_return tail_availability::unavailable;
    }
    // Replacing a live consumer would drop the tail position and destroy a
    // consumer whose fetchers are still running; the early return above is what
    // prevents it.
    vassert(
      !_consumer, "Arming {} tailing while already armed", schemas_topic());
    _consumer = std::move(consumer);
    _next_offset = offset;
    co_return tail_availability::available;
}

ss::future<>
schemas_topic_tail_reader::drain(ss::abort_source& as, tail_batch& batch) {
    size_t remaining = max_poll_bytes;
    // stop() can run while this drain is suspended (the task stops its readers
    // while run_impl is live) and it releases the consumer, so re-check both on
    // every resumption.
    while (remaining > 0 && !_stopped && _consumer) {
        as.check();
        auto fetched = co_await _consumer->fetch_next(poll_timeout);
        if (fetched.has_error()) {
            // Only errors the fetcher judged non-retriable reach here, so the
            // consumer would keep failing.
            co_await disarm(
              ssx::sformat(
                "fetching from source {} failed: {}",
                schemas_topic(),
                kafka::error_code_to_str(fetched.error())));
            co_return;
        }
        if (fetched.value().empty()) {
            // Nothing buffered: the common case, and the loop's exit.
            break;
        }
        for (auto& topic : fetched.value()) {
            for (auto& partition : topic.partitions) {
                if (partition.error != kafka::error_code::none) {
                    co_await disarm(
                      ssx::sformat(
                        "fetching from source {}/{} failed: {}",
                        topic.topic,
                        partition.partition_id,
                        kafka::error_code_to_str(partition.error)));
                    co_return;
                }
                remaining -= std::min(remaining, partition.size_bytes);
                // Batches arrive in offset order, so the last one bounds what
                // this poll has seen -- including any the decode below skips,
                // which resuming onto would only skip again.
                if (!partition.data.empty()) {
                    _next_offset = ::model::offset_cast(
                      ::model::next_offset(
                        partition.data.back().last_offset()));
                }
                co_await decode_batches(std::move(partition.data), batch);
            }
        }
    }
    batch.truncated = (remaining == 0);
}

ss::future<source_result<tail_batch>>
schemas_topic_tail_reader::poll(ss::abort_source& as) {
    // Resume a stopped tail from this reader's own progress, rather than
    // waiting for a full sync that a link relying on tailing may never get.
    if (
      !_stopped && !_consumer && _next_offset.has_value()
      && ss::lowres_clock::now() >= _next_arm_attempt) {
        _next_arm_attempt = ss::lowres_clock::now() + _rearm_backoff;
        if (
          co_await start_consumer_at(*_next_offset)
          == tail_availability::available) {
            vlog(
              cllog.info,
              "Schema Registry topic tailing resumed on {} (offset {})",
              schemas_topic(),
              *_next_offset);
        }
    }

    _poll_start = _next_offset;
    tail_batch batch;
    auto drained = co_await ss::coroutine::as_future(drain(as, batch));
    if (drained.failed()) {
        auto ex = drained.get_exception();
        if (ssx::is_shutdown_exception(ex)) {
            std::rethrow_exception(ex);
        }
        co_await disarm(
          ssx::sformat("reading source {} failed: {}", schemas_topic(), ex));
    }
    co_return std::move(batch);
}

ss::future<> schemas_topic_tail_reader::rewind() {
    if (!_poll_start.has_value()) {
        co_return;
    }
    _next_offset = _poll_start;
    vlog(
      cllog.info,
      "Schema Registry topic tailing rewound to offset {} on {} to replay a "
      "batch the caller could not apply",
      *_poll_start,
      schemas_topic());
    // Releasing the consumer is what makes the next poll re-read from there.
    // Leaves the re-arm backoff alone: a reader that was armed retries at once,
    // while one that had just resumed stays paced, which is the right split.
    co_await release_consumer();
}

bool schemas_topic_tail_reader::armed() const { return _consumer != nullptr; }

ss::future<> schemas_topic_tail_reader::stop() {
    _stopped = true;
    co_await disarm("reader stopped");
}

std::unique_ptr<tail_reader>
schemas_topic_tail_reader_factory::create(link* link) {
    return std::make_unique<schemas_topic_tail_reader>(
      link->get_cluster_connection());
}

} // namespace cluster_link::schema_registry_sync
