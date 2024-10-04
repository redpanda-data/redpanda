// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/produce.h"

#include "base/likely.h"
#include "base/vlog.h"
#include "bytes/iobuf.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "config/configuration.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/kafka_batch_adapter.h"
#include "kafka/server/replicated_partition.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "pandaproxy/schema_registry/validation.h"
#include "raft/errc.h"
#include "ssx/future-util.h"
#include "utils/remote.h"
#include "utils/to_string.h"

#include <seastar/core/execution_stage.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <boost/container_hash/extensions.hpp>
#include <fmt/ostream.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <ranges>
#include <string_view>

namespace kafka {

static constexpr auto despam_interval = std::chrono::minutes(5);

static void fill_response_with_errors(
  produce_request::topic_cit topics_begin,
  produce_request::topic_cit topics_end,
  error_code error,
  produce_response& response) {
    size_t cnt = std::distance(topics_begin, topics_end);
    response.data.responses.reserve(response.data.responses.size() + cnt);
    for (const auto& topic : std::views::counted(topics_begin, cnt)) {
        produce_response::topic& t = response.data.responses.emplace_back();
        t.name = topic.name;

        t.partitions.reserve(topic.partitions.size());
        for (const auto& partition : topic.partitions) {
            t.partitions.push_back(produce_response::partition{
              .partition_index = partition.partition_index,
              .error_code = error});
        }
    }
}

produce_response produce_request::make_error_response(error_code error) const {
    produce_response response;
    fill_response_with_errors(
      data.topics.cbegin(), data.topics.cend(), error, response);
    return response;
}

produce_response produce_request::make_full_disk_response() const {
    auto resp = make_error_response(error_code::broker_not_available);
    // TODO set a field in response to signal to quota manager to throttle the
    // client
    return resp;
}

struct topic_produce_stages {
    ss::future<> dispatched;
    ss::future<produce_response::topic> produced;
};

partition_produce_stages make_ready_stage(produce_response::partition p) {
    return partition_produce_stages{
      .dispatched = ss::now(),
      .produced = ss::make_ready_future<produce_response::partition>(
        std::move(p)),
    };
}

static raft::replicate_options
acks_to_replicate_options(int16_t acks, std::chrono::milliseconds timeout) {
    switch (acks) {
    case -1:
        return {raft::consistency_level::quorum_ack, timeout};
    case 0:
        return {raft::consistency_level::no_ack, timeout};
    case 1:
        return {raft::consistency_level::leader_ack, timeout};
    default:
        throw std::invalid_argument("Not supported ack level");
    };
}

static inline model::record_batch_reader
reader_from_lcore_batch(model::record_batch&& batch) {
    /*
     * The remainder of work for this partition is handled on its home
     * core. The foreign memory record batch reader requires that once the
     * reader is sent to the foreign core that it has exclusive access to the
     * data in reader. That is true here and is generally trivial with readers
     * that hold a copy of their data in memory.
     */
    return model::make_foreign_memory_record_batch_reader(std::move(batch));
}

static error_code map_produce_error_code(std::error_code ec) {
    if (ec.category() == raft::error_category()) {
        switch (static_cast<raft::errc>(ec.value())) {
        case raft::errc::not_leader:
        case raft::errc::replicated_entry_truncated:
            return error_code::not_leader_for_partition;
        // map shutting down error code to timeout since replication result may
        // be not determined, it may succeed or be aborted earlier and abandoned
        case raft::errc::shutting_down:
            return error_code::request_timed_out;
        case raft::errc::invalid_input_records:
            return error_code::invalid_record;
        default:
            return error_code::request_timed_out;
        }
    }

    if (ec.category() == cluster::error_category()) {
        switch (static_cast<cluster::errc>(ec.value())) {
        case cluster::errc::not_leader:
            return error_code::not_leader_for_partition;
        case cluster::errc::topic_not_exists:
        case cluster::errc::partition_not_exists:
            return error_code::unknown_topic_or_partition;
        case cluster::errc::invalid_producer_epoch:
            return error_code::invalid_producer_epoch;
        case cluster::errc::sequence_out_of_order:
            return error_code::out_of_order_sequence_number;
        case cluster::errc::invalid_request:
            return error_code::invalid_request;
        case cluster::errc::producer_ids_vcluster_limit_exceeded:
            return error_code::policy_violation;
        case cluster::errc::generic_tx_error:
            return error_code::unknown_server_error;
        default:
            return error_code::request_timed_out;
        }
    }

    if (ec.category() == kafka::error_category()) {
        return static_cast<error_code>(ec.value());
    }

    return error_code::request_timed_out;
}

/*
 * Caller is expected to catch errors that may be thrown while the kafka
 * batch is being deserialized (see reader_from_kafka_batch).
 */
static partition_produce_stages partition_append(
  model::partition_id id,
  ss::lw_shared_ptr<replicated_partition> partition,
  model::batch_identity bid,
  model::record_batch_reader reader,
  int16_t acks,
  int32_t num_records,
  int64_t num_bytes,
  std::chrono::milliseconds timeout_ms) {
    auto stages = partition->replicate(
      bid, std::move(reader), acks_to_replicate_options(acks, timeout_ms));
    return partition_produce_stages{
      .dispatched = std::move(stages.request_enqueued),
      .produced = stages.replicate_finished.then_wrapped(
        [partition, id, num_records = num_records, num_bytes](
          ss::future<result<raft::replicate_result>> f) {
            produce_response::partition p{.partition_index = id};
            try {
                auto r = f.get();
                if (r.has_value()) {
                    // have to subtract num_of_records - 1 as base_offset
                    // is inclusive
                    p.base_offset = model::offset(
                      r.value().last_offset - (num_records - 1));
                    p.error_code = error_code::none;
                    partition->probe().add_records_produced(num_records);
                    partition->probe().add_bytes_produced(num_bytes);
                } else {
                    p.error_code = map_produce_error_code(r.error());
                }
            } catch (...) {
                p.error_code = error_code::request_timed_out;
            }
            return p;
        }),
    };
}

ss::future<produce_response::partition> finalize_request_with_error_code(
  error_code ec,
  std::unique_ptr<ss::promise<>> dispatch,
  model::ntp ntp,
  ss::shard_id source_shard) {
    // submit back to promise source shard
    ssx::background = ss::smp::submit_to(
      source_shard, [dispatch = std::move(dispatch)]() mutable {
          dispatch->set_value();
          dispatch.reset();
      });
    return ss::make_ready_future<produce_response::partition>(
      produce_response::partition{
        .partition_index = ntp.tp.partition, .error_code = ec});
}

/**
 * @brief Validate the timestamps of the batch, they have to be within a window
 * to the broker's time. returns the new timestamp to set as max_timestamp to
 * the batch, if present
 */
static auto validate_batch_timestamps(
  const model::ntp& ntp,
  const model::record_batch_header& header,
  model::timestamp_type timestamp_type,
  net::server_probe& probe) -> std::optional<model::timestamp> {
    // we compute in std::chrono::timepoints, we print in model::timestamps
    auto broker_time = model::timestamp::now();
    auto broker_timepoint = model::duration_since_epoch(broker_time);

    // alert if first_timestamp is too far in the past
    // default value for threshold basically disables this check, so it's
    // wrapped in a if check
    if (auto max_before = config::shard_local_cfg()
                            .log_message_timestamp_alert_before_ms.value();
        unlikely(max_before)) {
        auto first_timepoint = model::duration_since_epoch(
          header.first_timestamp);
        if (
          broker_timepoint > first_timepoint
          && std::chrono::duration_cast<std::chrono::milliseconds>(
               broker_timepoint - first_timepoint)
               > max_before.value()) {
            // generate an alert
            thread_local static ss::logger::rate_limit rate(despam_interval);
            klog.log(
              ss::log_level::warn,
              rate,
              "produce request timestamp for {} was before the alert "
              "threshold, broker time: {}, timestamp: {}",
              ntp,
              broker_time,
              header.first_timestamp);
            // it's expected that to debug this, the observer will need to check
            // the logs for the npt that triggered the alert
            probe.produce_bad_create_time();
        }
    }

    // alert if max_timestamp is too far in the future
    if (timestamp_type == model::timestamp_type::create_time) {
        auto max_after = config::shard_local_cfg()
                           .log_message_timestamp_alert_after_ms.value();
        auto max_timepoint = model::duration_since_epoch(header.max_timestamp);
        if (
          broker_timepoint < max_timepoint
          && std::chrono::duration_cast<std::chrono::milliseconds>(
               max_timepoint - broker_timepoint)
               > max_after) {
            // same as above, generate an alert
            thread_local static ss::logger::rate_limit rate(despam_interval);
            klog.log(
              ss::log_level::warn,
              rate,
              "produce request timestamp for {} was past the alert threshold, "
              "broker time: {}, timestamp: {}",
              ntp,
              broker_time,
              header.max_timestamp);
            probe.produce_bad_create_time();
        }
    }

    /*
     * For append time setting we have to recompute
     * the CRC.
     */
    if (timestamp_type == model::timestamp_type::append_time) {
        return broker_time;
    } else {
        return std::nullopt;
    }
}

/**
 * \brief handle writing to a single topic partition.
 */
static partition_produce_stages produce_topic_partition(
  produce_ctx& octx,
  produce_request::topic& topic,
  produce_request::partition& part) {
    auto ntp = model::ntp(
      model::kafka_namespace, topic.name, part.partition_index);

    /*
     * A single produce request may contain record batches for many
     * different partitions that are managed different cores.
     */
    auto shard = octx.rctx.shards().shard_for(ntp);

    if (!shard) {
        return make_ready_stage(produce_response::partition{
          .partition_index = ntp.tp.partition,
          .error_code = error_code::not_leader_for_partition});
    }

    // steal the batch from the adapter
    auto batch = std::move(part.records->adapter.batch.value());

    auto topic_cfg = octx.rctx.metadata_cache().get_topic_cfg(
      model::topic_namespace_view(model::kafka_namespace, topic.name));

    if (!topic_cfg) {
        return make_ready_stage(produce_response::partition{
          .partition_index = ntp.tp.partition,
          .error_code = error_code::unknown_topic_or_partition});
    }

    const auto timestamp_type = topic_cfg->properties.timestamp_type.value_or(
      octx.rctx.metadata_cache().get_default_timestamp_type());
    const auto batch_max_bytes = topic_cfg->properties.batch_max_bytes.value_or(
      octx.rctx.metadata_cache().get_default_batch_max_bytes());

    // validate the batch timestamps by checking skew against broker time
    if (
      auto new_timestamp = validate_batch_timestamps(
        ntp, batch.header(), timestamp_type, octx.rctx.server_probe())) {
        batch.set_max_timestamp(
          model::timestamp_type::append_time, new_timestamp.value());
    }

    const auto& hdr = batch.header();
    auto bid = model::batch_identity::from(hdr);
    auto batch_size = batch.size_bytes();
    auto num_records = batch.record_count();
    auto reader = reader_from_lcore_batch(std::move(batch));
    auto validator
      = pandaproxy::schema_registry::maybe_make_schema_id_validator(
        octx.rctx.schema_registry(), topic.name, topic_cfg->properties);
    auto start = std::chrono::steady_clock::now();

    auto dispatch = std::make_unique<ss::promise<>>();
    auto dispatch_f = dispatch->get_future();
    auto m = octx.rctx.probe().auto_produce_measurement();
    auto timeout = octx.request.data.timeout_ms;
    if (timeout < 0ms) {
        static constexpr std::chrono::milliseconds max_timeout{
          std::numeric_limits<int32_t>::max()};
        // negative timeout translates to no timeout
        timeout = max_timeout;
    }
    auto f
      = octx.rctx.partition_manager()
          .invoke_on(
            *shard,
            octx.ssg,
            [reader = std::move(reader),
             validator = std::move(validator),
             ntp = std::move(ntp),
             dispatch = std::move(dispatch),
             num_records,
             batch_size,
             bid,
             acks = octx.request.data.acks,
             batch_max_bytes,
             timeout,
             source_shard = ss::this_shard_id()](
              cluster::partition_manager& mgr) mutable {
                auto partition = mgr.get(ntp);
                if (!partition) {
                    return finalize_request_with_error_code(
                      error_code::not_leader_for_partition,
                      std::move(dispatch),
                      ntp,
                      source_shard);
                }
                if (unlikely(
                      static_cast<uint32_t>(batch_size) > batch_max_bytes)) {
                    return finalize_request_with_error_code(
                      error_code::message_too_large,
                      std::move(dispatch),
                      ntp,
                      source_shard);
                }
                if (unlikely(!partition->is_leader())) {
                    return finalize_request_with_error_code(
                      error_code::not_leader_for_partition,
                      std::move(dispatch),
                      ntp,
                      source_shard);
                }

                auto probe = std::addressof(partition->probe());
                return pandaproxy::schema_registry::maybe_validate_schema_id(
                         std::move(validator), std::move(reader), probe)
                  .then([ntp{std::move(ntp)},
                         partition{std::move(partition)},
                         dispatch = std::move(dispatch),
                         bid,
                         acks,
                         source_shard,
                         num_records,
                         batch_size,
                         timeout](auto reader) mutable {
                      if (reader.has_error()) {
                          return finalize_request_with_error_code(
                            reader.assume_error(),
                            std::move(dispatch),
                            ntp,
                            source_shard);
                      }
                      auto stages = partition_append(
                        ntp.tp.partition,
                        ss::make_lw_shared<replicated_partition>(
                          std::move(partition)),
                        bid,
                        std::move(reader).assume_value(),
                        acks,
                        num_records,
                        batch_size,
                        timeout);
                      return stages.dispatched
                        .then_wrapped(
                          [source_shard, dispatch = std::move(dispatch)](
                            ss::future<> f) mutable {
                              if (f.failed()) {
                                  (void)ss::smp::submit_to(
                                    source_shard,
                                    [dispatch = std::move(dispatch),
                                     e = f.get_exception()]() mutable {
                                        dispatch->set_exception(e);
                                        dispatch.reset();
                                    });
                                  return;
                              }
                              (void)ss::smp::submit_to(
                                source_shard,
                                [dispatch = std::move(dispatch)]() mutable {
                                    dispatch->set_value();
                                    dispatch.reset();
                                });
                          })
                        .then([f = std::move(stages.produced)]() mutable {
                            return std::move(f);
                        });
                  });
            })
          .then([&octx, start, m = std::move(m)](
                  produce_response::partition p) {
              if (p.error_code == error_code::none) {
                  auto dur = std::chrono::steady_clock::now() - start;
                  octx.rctx.connection()->server().update_produce_latency(dur);
              } else {
                  m->cancel();
              }
              return p;
          });
    return partition_produce_stages{
      .dispatched = std::move(dispatch_f),
      .produced = std::move(f),
    };
}

namespace testing {
partition_produce_stages produce_single_partition(
  produce_ctx& octx,
  produce_request::topic& topic,
  produce_request::partition& part) {
    return produce_topic_partition(octx, topic, part);
}
} // namespace testing

/**
 * \brief Dispatch and collect topic partition produce responses
 */
static topic_produce_stages
produce_topic(produce_ctx& octx, produce_request::topic& topic) {
    std::vector<ss::future<produce_response::partition>> partitions_produced;
    std::vector<ss::future<>> partitions_dispatched;
    partitions_produced.reserve(topic.partitions.size());
    partitions_dispatched.reserve(topic.partitions.size());

    const auto* disabled_set
      = octx.rctx.metadata_cache().get_topic_disabled_set(
        model::topic_namespace_view{model::kafka_namespace, topic.name});

    for (auto& part : topic.partitions) {
        auto push_error_response = [&](error_code errc) {
            partitions_dispatched.push_back(ss::now());
            partitions_produced.push_back(
              ss::make_ready_future<produce_response::partition>(
                produce_response::partition{
                  .partition_index = part.partition_index,
                  .error_code = errc}));
        };

        const bool is_transform_logs_topic
          = topic.name == model::transform_log_internal_topic;

        const auto& kafka_noproduce_topics
          = config::shard_local_cfg().kafka_noproduce_topics();

        const bool is_noproduce_topic = is_transform_logs_topic
                                        || std::find(
                                             kafka_noproduce_topics.begin(),
                                             kafka_noproduce_topics.end(),
                                             topic.name)
                                             != kafka_noproduce_topics.end();

        const bool audit_produce_restricted
          = !octx.rctx.authorized_auditor()
            && topic.name == model::kafka_audit_logging_topic();

        // Need to make an exception here in case the audit log topic is in the
        // noproduce topics list
        const bool is_audit_produce
          = octx.rctx.authorized_auditor()
            && topic.name == model::kafka_audit_logging_topic();
        if (
          (is_noproduce_topic || audit_produce_restricted)
          && !is_audit_produce) {
            push_error_response(error_code::topic_authorization_failed);
            continue;
        }

        if (!octx.rctx.metadata_cache().contains(
              model::topic_namespace_view(model::kafka_namespace, topic.name),
              part.partition_index)) {
            push_error_response(error_code::unknown_topic_or_partition);
            continue;
        }

        if (unlikely(
              disabled_set
              && disabled_set->is_disabled(part.partition_index))) {
            push_error_response(error_code::replica_not_available);
            continue;
        }

        // the record data on the wire was null value
        if (unlikely(!part.records)) {
            push_error_response(error_code::invalid_record);
            continue;
        }

        // an error occurred handling legacy messages (magic 0 or 1)
        if (unlikely(part.records->adapter.legacy_error)) {
            push_error_response(error_code::invalid_record);
            continue;
        }

        if (unlikely(!part.records->adapter.valid_crc)) {
            push_error_response(error_code::corrupt_message);
            continue;
        }

        // produce version >= 3 (enforced for all produce requests)
        // requires exactly one record batch per request and it must use
        // the v2 format.
        //
        // NOTE: for produce version 0 and 1 the adapter transparently converts
        // the batch into an v2 batch and sets the v2_format flag. conversion
        // also produces a single record batch by accumulating legacy messages.
        if (unlikely(
              !part.records->adapter.v2_format
              || !part.records->adapter.batch)) {
            push_error_response(error_code::invalid_record);
            continue;
        }

        auto pr = produce_topic_partition(octx, topic, part);
        partitions_produced.push_back(std::move(pr.produced));
        partitions_dispatched.push_back(std::move(pr.dispatched));
    }

    // collect partition responses and build the topic response
    return topic_produce_stages{
      .dispatched = ss::when_all_succeed(
        partitions_dispatched.begin(), partitions_dispatched.end()),
      .produced
      = ss::when_all_succeed(
          partitions_produced.begin(), partitions_produced.end())
          .then([name = std::move(topic.name)](
                  std::vector<produce_response::partition> parts) mutable {
              return produce_response::topic{
                .name = std::move(name),
                .partitions = std::move(parts),
              };
          }),
    };
}

/**
 * \brief Dispatch and collect topic produce responses
 */
static std::vector<topic_produce_stages> produce_topics(produce_ctx& octx) {
    std::vector<topic_produce_stages> topics;
    topics.reserve(octx.request.data.topics.size());

    for (auto& topic : octx.request.data.topics) {
        topics.push_back(produce_topic(octx, topic));
    }

    return topics;
}

template<>
process_result_stages
produce_handler::handle(request_context ctx, ss::smp_service_group ssg) {
    produce_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    if (unlikely(ctx.recovery_mode_enabled())) {
        return process_result_stages::single_stage(ctx.respond(
          request.make_error_response(error_code::policy_violation)));
    }

    if (ctx.metadata_cache().should_reject_writes()) {
        thread_local static ss::logger::rate_limit rate(despam_interval);
        klog.log(
          ss::log_level::warn,
          rate,
          "[{}:{}] rejecting produce request: no disk space; bytes free less "
          "than configurable threshold",
          ctx.connection()->client_host(),
          ctx.connection()->client_port());

        return process_result_stages::single_stage(
          ctx.respond(request.make_full_disk_response()));
    }

    // Account for special internal topic bytes for usage
    produce_response resp;
    for (const auto& topic : request.data.topics) {
        const bool bytes_to_exclude = std::find(
                                        usage_excluded_topics.cbegin(),
                                        usage_excluded_topics.cend(),
                                        topic.name())
                                      != usage_excluded_topics.cend();
        if (bytes_to_exclude) {
            for (const auto& part : topic.partitions) {
                if (part.records) {
                    const auto& records = part.records;
                    if (records->adapter.batch) {
                        resp.internal_topic_bytes
                          += records->adapter.batch->size_bytes();
                    }
                }
            }
        }
    }

    // determine if the request has transactional / idempotent batches
    for (auto& topic : request.data.topics) {
        for (auto& part : topic.partitions) {
            if (part.records) {
                if (part.records->adapter.batch) {
                    const auto& hdr = part.records->adapter.batch->header();
                    request.has_transactional = request.has_transactional
                                                || hdr.attrs.is_transactional();
                    request.has_idempotent = request.has_idempotent
                                             || hdr.producer_id >= 0;
                }
            }
        }
    }

    if (request.has_transactional) {
        if (!ctx.are_transactions_enabled()) {
            return process_result_stages::single_stage(
              ctx.respond(request.make_error_response(
                error_code::transactional_id_authorization_failed)));
        }

        if (
          !request.data.transactional_id
          || !ctx.authorized(
            security::acl_operation::write,
            transactional_id(*request.data.transactional_id))) {
            auto ec = error_code::transactional_id_authorization_failed;

            if (!ctx.audit()) [[unlikely]] {
                ec = error_code::broker_not_available;
            }
            return process_result_stages::single_stage(
              ctx.respond(request.make_error_response(ec)));
        }
        // <kafka>Note that authorization to a transactionalId implies
        // ProducerId authorization</kafka>

    } else if (request.has_idempotent) {
        if (!ctx.is_idempotence_enabled()) {
            return process_result_stages::single_stage(
              ctx.respond(request.make_error_response(
                error_code::cluster_authorization_failed)));
        }

    } else if (request.data.acks < -1 || request.data.acks > 1) {
        // from kafka source: "if required.acks is outside accepted
        // range, something is wrong with the client Just return an
        // error and don't handle the request at all"
        klog.error(
          "unsupported acks {} see "
          "https://docs.confluent.io/current/installation/"
          "configuration/"
          "producer-configs.html",
          request.data.acks);
        return process_result_stages::single_stage(ctx.respond(
          request.make_error_response(error_code::invalid_required_acks)));
    }

    // Must now validate if we are authorized, we will remove items from the
    // request that are not authorized and create response entries for those.
    // Once authz is checked, then attempt audit
    auto unauthorized_it = std::partition(
      request.data.topics.begin(),
      request.data.topics.end(),
      [&ctx](const topic_produce_data& t) {
          return ctx.authorized(security::acl_operation::write, t.name);
      });
    if (!ctx.audit()) {
        return process_result_stages::single_stage(ctx.respond(
          request.make_error_response(error_code::broker_not_available)));
    }
    fill_response_with_errors(
      unauthorized_it,
      request.data.topics.cend(),
      error_code::topic_authorization_failed,
      resp);
    request.data.topics.erase_to_end(unauthorized_it);

    // Make sure to not write into migrated-from topics in their critical stages
    auto migrated_it = std::partition(
      request.data.topics.begin(),
      request.data.topics.end(),
      [&ctx](const topic_produce_data& t) {
          return !ctx.metadata_cache().should_reject_writes(
            model::topic_namespace_view(model::kafka_namespace, t.name));
      });
    fill_response_with_errors(
      migrated_it,
      request.data.topics.cend(),
      error_code::invalid_topic_exception,
      resp);
    request.data.topics.erase_to_end(migrated_it);

    ss::promise<> dispatched_promise;
    auto dispatched_f = dispatched_promise.get_future();
    auto produced_f = ss::do_with(
      produce_ctx(std::move(ctx), std::move(request), std::move(resp), ssg),
      [dispatched_promise = std::move(dispatched_promise)](
        produce_ctx& octx) mutable {
          // dispatch produce requests for each topic
          auto stages = produce_topics(octx);
          std::vector<ss::future<>> dispatched;
          std::vector<ss::future<produce_response::topic>> produced;
          dispatched.reserve(stages.size());
          produced.reserve(stages.size());

          for (auto& s : stages) {
              dispatched.push_back(std::move(s.dispatched));
              produced.push_back(std::move(s.produced));
          }
          return seastar::when_all_succeed(dispatched.begin(), dispatched.end())
            .then_wrapped([&octx,
                           dispatched_promise = std::move(dispatched_promise),
                           produced = std::move(produced)](
                            ss::future<> f) mutable {
                try {
                    f.get();
                    dispatched_promise.set_value();
                    // collect topic responses
                    return when_all_succeed(produced.begin(), produced.end())
                      .then(
                        [&octx](std::vector<produce_response::topic> topics) {
                            std::move(
                              topics.begin(),
                              topics.end(),
                              std::back_inserter(octx.response.data.responses));
                        })
                      .then([&octx] {
                          // send response immediately
                          if (octx.request.data.acks != 0) {
                              return octx.rctx.respond(
                                std::move(octx.response));
                          }

                          // acks = 0 is handled separately. first, check for
                          // errors
                          bool has_error = false;
                          for (const auto& topic :
                               octx.response.data.responses) {
                              for (const auto& p : topic.partitions) {
                                  if (p.error_code != error_code::none) {
                                      has_error = true;
                                      break;
                                  }
                              }
                          }

                          // in the absence of errors, acks = 0 results in the
                          // response being dropped, as the client does not
                          // expect a response. here we mark the response as
                          // noop, but let it flow back so that it can be
                          // accounted for in quota and stats tracking. it is
                          // dropped later during processing.
                          if (!has_error) {
                              return octx.rctx.respond(std::move(octx.response))
                                .then([](response_ptr resp) {
                                    resp->mark_noop();
                                    return resp;
                                });
                          }

                          // errors in a response from an acks=0 produce request
                          // result in the connection being dropped to signal an
                          // issue to the client
                          return ss::make_exception_future<response_ptr>(
                            std::runtime_error(fmt::format(
                              "Closing connection due to error in produce "
                              "response: {}",
                              octx.response)));
                      });
                } catch (...) {
                    /*
                     * if the first stage failed then we cannot resolve the
                     * current future (do_with holding octx) immediately,
                     * otherwise octx will be destroyed and all of the second
                     * stage futures (which have a reference to octx) will be
                     * backgrounded. logging about the second stage return value
                     * is handled in connection_context handler.
                     */
                    dispatched_promise.set_exception(std::current_exception());
                    return when_all_succeed(produced.begin(), produced.end())
                      .discard_result()
                      .then([] {
                          return ss::make_exception_future<response_ptr>(
                            std::runtime_error("First stage produce failed but "
                                               "second stage succeeded."));
                      })
                      .handle_exception([](std::exception_ptr e) {
                          return ss::make_exception_future<response_ptr>(e);
                      });
                }
            });
      });

    return process_result_stages(
      std::move(dispatched_f), std::move(produced_f));
}

} // namespace kafka
