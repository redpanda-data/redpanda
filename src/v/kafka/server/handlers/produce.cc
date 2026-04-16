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
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "config/configuration.h"
#include "kafka/data/partition_proxy.h"
#include "kafka/data/replicated_partition.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/kafka_batch_adapter.h"
#include "kafka/server/handlers/produce_validation.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "pandaproxy/schema_registry/validation.h"
#include "raft/errc.h"
#include "ssx/future-util.h"

#include <seastar/core/execution_stage.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <boost/container_hash/extensions.hpp>
#include <fmt/ostream.h>

#include <chrono>
#include <exception>
#include <expected>
#include <functional>

namespace kafka {
namespace {
static constexpr auto despam_interval = std::chrono::minutes(5);

void fill_response_with_errors(
  produce_request::topic_cit topics_begin,
  produce_request::topic_cit topics_end,
  produce_response& response,
  error_code error,
  const std::optional<ss::sstring>& error_msg = std::nullopt) {
    size_t cnt = std::distance(topics_begin, topics_end);
    response.data.responses.reserve(response.data.responses.size() + cnt);
    for (const auto& topic : std::views::counted(topics_begin, cnt)) {
        produce_response::topic& t = response.data.responses.emplace_back();
        t.name = topic.name;

        t.partitions.reserve(topic.partitions.size());
        for (const auto& partition : topic.partitions) {
            t.partitions.push_back(
              produce_response::partition{
                .partition_index = partition.partition_index,
                .error_code = error,
                .error_message = error_msg});
        }
    }
}

struct topic_produce_stages {
    ss::future<> dispatched;
    ss::future<produce_response::topic> produced;
};

raft::replicate_options
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

error_code map_produce_error_code(std::error_code ec) {
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
partition_produce_stages partition_append(
  model::partition_id id,
  partition_proxy partition,
  model::batch_identity bid,
  std::unique_ptr<model::record_batch> batch,
  int16_t acks,
  int32_t num_records,
  int64_t num_bytes,
  std::chrono::milliseconds timeout_ms) {
    // https://github.com/redpanda-data/redpanda/blob/dev/src/v/kafka/protocol/schemata/produce_response.json
    // If CreateTime is used for the topic, the timestamp will be -1. If
    // LogAppendTime is used for the topic, the timestamp will be the broker
    // local time when the messages are appended.
    auto log_append_time_ms = batch->header().attrs.timestamp_type()
                                  == model::timestamp_type::create_time
                                ? model::timestamp::missing()
                                : batch->header().max_timestamp;
    auto stages = partition.replicate(
      bid, std::move(*batch), acks_to_replicate_options(acks, timeout_ms));
    return partition_produce_stages{
      .dispatched = std::move(stages.request_enqueued),
      .produced = stages.replicate_finished.then_wrapped(
        [partition = std::move(partition),
         id,
         num_records = num_records,
         num_bytes,
         log_append_time_ms = log_append_time_ms](
          ss::future<result<raft::replicate_result>> f) mutable {
            produce_response::partition p{.partition_index = id};
            try {
                auto r = f.get();
                if (r.has_value()) {
                    // have to subtract num_of_records - 1 as base_offset
                    // is inclusive
                    p.base_offset = model::offset(
                      r.value().last_offset - (num_records - 1));
                    p.log_append_time_ms = log_append_time_ms;
                    p.error_code = error_code::none;
                    partition.probe().add_records_produced(num_records);
                    partition.probe().add_bytes_produced(num_bytes);
                    partition.probe().add_batches_produced(1);
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

produce_response::partition finalize_request_with_error_code(
  error_code ec,
  std::unique_ptr<ss::promise<>> dispatch,
  const model::ntp& ntp,
  ss::shard_id source_shard,
  std::optional<ss::sstring> err_msg = std::nullopt) {
    // submit back to promise source shard
    ssx::background = ss::smp::submit_to(
      source_shard, [dispatch = std::move(dispatch)]() mutable {
          dispatch->set_value();
          dispatch.reset();
      });
    return produce_response::partition{
      .partition_index = ntp.tp.partition,
      .error_code = ec,
      .error_message = std::move(err_msg)};
}

struct ntp_produce_request {
    model::ntp ntp;
    std::unique_ptr<model::record_batch> batch;
    std::optional<pandaproxy::schema_registry::schema_id_validator>
      schema_id_validator;

    size_t batch_max_bytes;
    model::timestamp_type timestamp_type;
    std::chrono::milliseconds message_timestamp_before_max_ms;
    std::chrono::milliseconds message_timestamp_after_max_ms;
};

ss::future<produce_response::partition> do_produce_topic_partition(
  produce_ctx& octx,
  ntp_produce_request req,
  std::unique_ptr<ss::promise<>> dispatched) {
    auto start = std::chrono::steady_clock::now();
    auto validate_batch_res = co_await validate_batch(
      {.batch = *req.batch,
       .timestamp_type = req.timestamp_type,
       .message_timestamp_before_max_ms = req.message_timestamp_before_max_ms,
       .message_timestamp_after_max_ms = req.message_timestamp_after_max_ms,
       .probe = octx.rctx.probe(),
       .ntp = req.ntp,
       .client_id = octx.rctx.header().client_id});

    if (validate_batch_res.has_value()) {
        co_return finalize_request_with_error_code(
          validate_batch_res->err,
          std::move(dispatched),
          req.ntp,
          ss::this_shard_id(),
          std::move(validate_batch_res->msg));
    }

    auto batch_size = req.batch->size_bytes();
    if (static_cast<uint32_t>(batch_size) > req.batch_max_bytes) {
        auto msg = ssx::sformat(
          "batch size {} exceeds max {}", batch_size, req.batch_max_bytes);
        thread_local static ss::logger::rate_limit rate(1s);
        vloglr(klog, ss::log_level::warn, rate, "{}", msg);
        co_return finalize_request_with_error_code(
          error_code::message_too_large,
          std::move(dispatched),
          req.ntp,
          ss::this_shard_id(),
          std::move(msg));
    }

    if (auto& validator = req.schema_id_validator) {
        auto ec = co_await (*validator)(*req.batch);
        if (ec != error_code::none) {
            // TODO: It's a bit much to post this to the partition probe for
            // this metric. We should probably move the metric.
            auto shard = octx.rctx.shards().shard_for(req.ntp);
            if (shard) {
                co_await octx.rctx.partition_manager().invoke_on(
                  *shard,
                  [](cluster::partition_manager& pm, const model::ntp& ntp) {
                      if (auto p = pm.get(ntp)) {
                          p->probe().add_schema_id_validation_failed();
                      }
                      return ss::now();
                  },
                  req.ntp);
            }
            co_return finalize_request_with_error_code(
              ec, std::move(dispatched), req.ntp, ss::this_shard_id());
        }
    }

    // A single produce request may contain record batches for many
    // different partitions that are managed different cores.
    auto shard = octx.rctx.shards().shard_for(req.ntp);
    if (!shard) {
        co_return finalize_request_with_error_code(
          error_code::not_leader_for_partition,
          std::move(dispatched),
          req.ntp,
          ss::this_shard_id());
    }

    auto m = octx.rctx.probe().auto_produce_measurement();
    octx.rctx.probe().record_batch(
      batch_size, req.batch->header().attrs.compression());
    octx.rctx.connection()->attributes().produce_bytes.record(batch_size);
    octx.rctx.connection()->attributes().produce_batch_count.record(1);

    auto timeout = octx.request.data.timeout_ms;
    if (timeout < 0ms) {
        static constexpr std::chrono::milliseconds max_timeout{
          std::numeric_limits<int32_t>::max()};
        // negative timeout translates to no timeout
        timeout = max_timeout;
    }

    auto p = co_await octx.rctx.partition_manager().invoke_on(
      *shard,
      octx.ssg,
      [batch = std::move(req.batch),
       ntp = std::move(req.ntp),
       dispatch = std::move(dispatched),
       acks = octx.request.data.acks,
       timeout,
       source_shard = ss::this_shard_id()](
        cluster::partition_manager& mgr) mutable {
          auto partition = kafka::make_partition_proxy(ntp, mgr);
          if (!partition || !partition->is_leader()) {
              return ss::as_ready_future(finalize_request_with_error_code(
                error_code::not_leader_for_partition,
                std::move(dispatch),
                ntp,
                source_shard));
          }

          auto bid = model::batch_identity::from(batch->header());
          auto num_records = batch->record_count();
          auto batch_size = batch->size_bytes();
          auto stages = partition_append(
            ntp.tp.partition,
            std::move(*partition),
            bid,
            std::move(batch),
            acks,
            num_records,
            batch_size,
            timeout);
          return stages.dispatched
            .then_wrapped([source_shard, dispatch = std::move(dispatch)](
                            ss::future<> f) mutable {
                if (f.failed()) {
                    ssx::background = ss::smp::submit_to(
                      source_shard,
                      [dispatch = std::move(dispatch),
                       e = f.get_exception()]() mutable {
                          dispatch->set_exception(e);
                          dispatch.reset();
                      });
                    return;
                }
                ssx::background = ss::smp::submit_to(
                  source_shard, [dispatch = std::move(dispatch)]() mutable {
                      dispatch->set_value();
                      dispatch.reset();
                  });
            })
            .then([f = std::move(stages.produced)]() mutable {
                return std::move(f);
            });
      });
    if (p.error_code == error_code::none) {
        auto dur = std::chrono::steady_clock::now() - start;
        octx.rctx.connection()->server().update_produce_latency(dur);
    } else {
        m->cancel();
    }
    co_return p;
}

struct topic_configuration_context {
    size_t batch_max_bytes;
    model::timestamp_type timestamp_type;
    std::chrono::milliseconds message_timestamp_before_max_ms;
    std::chrono::milliseconds message_timestamp_after_max_ms;
    const cluster::topic_properties* properties;
};

/**
 * \brief handle writing to a single topic partition.
 */
partition_produce_stages produce_topic_partition(
  produce_ctx& octx,
  produce_request::topic& topic,
  produce_request::partition& part,
  const topic_configuration_context& cfg_ctx) {
    auto ntp = model::ntp(
      model::kafka_namespace, topic.name, part.partition_index);
    auto validator
      = pandaproxy::schema_registry::maybe_make_schema_id_validator(
        octx.rctx.schema_registry(), topic.name, *cfg_ctx.properties);
    // steal the batch from the adapter
    auto batch = std::make_unique<model::record_batch>(
      std::move(part.records->adapter.batch.value()));
    auto dispatch = std::make_unique<ss::promise<>>();
    auto dispatch_f = dispatch->get_future();
    auto f = do_produce_topic_partition(
      octx,
      ntp_produce_request{
        .ntp = std::move(ntp),
        .batch = std::move(batch),
        .schema_id_validator = std::move(validator),
        .batch_max_bytes = cfg_ctx.batch_max_bytes,
        .timestamp_type = cfg_ctx.timestamp_type,
        .message_timestamp_before_max_ms
        = cfg_ctx.message_timestamp_before_max_ms,
        .message_timestamp_after_max_ms
        = cfg_ctx.message_timestamp_after_max_ms,
      },
      std::move(dispatch));
    return partition_produce_stages{
      .dispatched = std::move(dispatch_f),
      .produced = std::move(f),
    };
}

/**
 * Fill topic partition produce response with errors
 */
topic_produce_stages
topic_produce_error(const produce_request::topic& topic, error_code error) {
    std::vector<produce_response::partition> partitions_produced;
    partitions_produced.reserve(topic.partitions.size());

    for (const auto& topic_partition : topic.partitions) {
        partitions_produced.push_back(
          produce_response::partition{
            .partition_index = topic_partition.partition_index,
            .error_code = error});
    }

    return topic_produce_stages{
      .dispatched = ss::now(),
      .produced = ss::make_ready_future<produce_response::topic>(
        produce_response::topic{
          .name = topic.name, .partitions = std::move(partitions_produced)}),
    };
}

/**
 * \brief Dispatch and collect topic partition produce responses
 */
topic_produce_stages
produce_topic(produce_ctx& octx, produce_request::topic& topic) {
    const auto* disabled_set
      = octx.rctx.metadata_cache().get_topic_disabled_set(
        model::topic_namespace_view{model::kafka_namespace, topic.name});

    const bool is_transform_logs_topic = topic.name
                                         == model::transform_log_internal_topic;

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
    const bool is_audit_produce = octx.rctx.authorized_auditor()
                                  && topic.name
                                       == model::kafka_audit_logging_topic();
    if ((is_noproduce_topic || audit_produce_restricted) && !is_audit_produce) {
        return topic_produce_error(
          topic, error_code::topic_authorization_failed);
    }
    const auto& topic_md = octx.rctx.metadata_cache().get_topic_metadata_ref(
      model::topic_namespace_view{model::kafka_namespace, topic.name});

    if (!topic_md) {
        return topic_produce_error(
          topic, error_code::unknown_topic_or_partition);
    }
    const auto& topic_cfg = topic_md->get().get_configuration();
    topic_configuration_context cfg_ctx{
      .batch_max_bytes = topic_cfg.properties.batch_max_bytes.value_or(
        octx.rctx.metadata_cache().get_default_batch_max_bytes()),
      .timestamp_type = topic_cfg.properties.timestamp_type.value_or(
        octx.rctx.metadata_cache().get_default_timestamp_type()),
      .message_timestamp_before_max_ms
      = topic_cfg.properties.message_timestamp_before_max_ms.value_or(
        octx.rctx.metadata_cache()
          .get_default_message_timestamp_before_max_ms()),
      .message_timestamp_after_max_ms
      = topic_cfg.properties.message_timestamp_after_max_ms.value_or(
        octx.rctx.metadata_cache()
          .get_default_message_timestamp_after_max_ms()),
      .properties = &topic_cfg.properties,
    };

    std::vector<ss::future<produce_response::partition>> partitions_produced;
    std::vector<ss::future<>> partitions_dispatched;
    partitions_produced.reserve(topic.partitions.size());
    partitions_dispatched.reserve(topic.partitions.size());
    for (auto& part : topic.partitions) {
        auto push_error_response = [&](error_code errc) {
            partitions_dispatched.push_back(ss::now());
            partitions_produced.push_back(
              ss::make_ready_future<produce_response::partition>(
                produce_response::partition{
                  .partition_index = part.partition_index,
                  .error_code = errc}));
        };

        if (
          unlikely(
            disabled_set && disabled_set->is_disabled(part.partition_index))) {
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
        if (
          unlikely(
            !part.records->adapter.v2_format || !part.records->adapter.batch)) {
            push_error_response(error_code::invalid_record);
            continue;
        }
        auto pr = produce_topic_partition(octx, topic, part, cfg_ctx);
        partitions_produced.push_back(std::move(pr.produced));
        partitions_dispatched.push_back(std::move(pr.dispatched));
    }
    auto is_iceberg_enabled = topic_cfg.properties.iceberg_mode
                              != model::iceberg_mode::disabled;
    // collect partition responses and build the topic response
    return topic_produce_stages{
      .dispatched = ss::when_all_succeed(
        partitions_dispatched.begin(), partitions_dispatched.end()),
      .produced
      = ss::when_all_succeed(
          partitions_produced.begin(), partitions_produced.end())
          .then([&octx, is_iceberg_enabled](
                  std::vector<produce_response::partition> parts) {
              // if topic is iceberg enabled update iceberg throttle manager.
              if (is_iceberg_enabled) {
                  octx.rctx.server().local().mark_datalake_producer(
                    octx.rctx.header().client_id);
              }
              return ssx::now(std::move(parts));
          })
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
std::vector<topic_produce_stages> produce_topics(produce_ctx& octx) {
    std::vector<topic_produce_stages> topics;
    topics.reserve(octx.request.data.topics.size());

    for (auto& topic : octx.request.data.topics) {
        topics.push_back(produce_topic(octx, topic));
    }

    return topics;
}

} // namespace

produce_response produce_request::make_error_response(
  error_code error, const std::optional<ss::sstring>& error_msg) const {
    produce_response response;
    fill_response_with_errors(
      data.topics.cbegin(), data.topics.cend(), response, error, error_msg);
    return response;
}

produce_response
produce_request::make_full_disk_response(api_version version) const {
    // Version 4 is the same as version 3, but the requester must be prepared to
    // handle a KAFKA_STORAGE_ERROR.
    auto errc = version >= api_version(4) ? error_code::kafka_storage_error
                                          : error_code::broker_not_available;
    auto resp = make_error_response(
      errc, "no disk space; bytes free less than configurable threshold");
    // TODO set a field in response to signal to quota manager to throttle the
    // client
    return resp;
}
namespace testing {
partition_produce_stages produce_single_partition(
  produce_ctx& octx,
  produce_request::topic& topic,
  produce_request::partition& part) {
    const auto& topic_md = octx.rctx.metadata_cache().get_topic_metadata_ref(
      model::topic_namespace_view{model::kafka_namespace, topic.name});
    const auto& topic_cfg = topic_md->get().get_configuration();
    topic_configuration_context cfg_ctx{
      .batch_max_bytes = topic_cfg.properties.batch_max_bytes.value_or(
        octx.rctx.metadata_cache().get_default_batch_max_bytes()),
      .timestamp_type = topic_cfg.properties.timestamp_type.value_or(
        octx.rctx.metadata_cache().get_default_timestamp_type()),
      .message_timestamp_before_max_ms
      = topic_cfg.properties.message_timestamp_before_max_ms.value_or(
        octx.rctx.metadata_cache()
          .get_default_message_timestamp_before_max_ms()),
      .message_timestamp_after_max_ms
      = topic_cfg.properties.message_timestamp_after_max_ms.value_or(
        octx.rctx.metadata_cache()
          .get_default_message_timestamp_after_max_ms()),
      .properties = &topic_cfg.properties,
    };
    return produce_topic_partition(octx, topic, part, cfg_ctx);
}
} // namespace testing
template<>
process_result_stages
produce_handler::handle(request_context ctx, ss::smp_service_group ssg) {
    produce_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    ctx.connection()->attributes().last_transactional_id.update(
      request.data.transactional_id);

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
          ctx.respond(request.make_full_disk_response(ctx.header().version)));
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
      resp,
      error_code::topic_authorization_failed);
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
      resp,
      error_code::invalid_topic_exception);
    request.data.topics.erase_to_end(migrated_it);

    auto linked_topics_it = std::partition(
      request.data.topics.begin(),
      request.data.topics.end(),
      [&ctx](const topic_produce_data& t) {
          return ctx.is_topic_mutable(t.name);
      });
    fill_response_with_errors(
      linked_topics_it,
      request.data.topics.cend(),
      resp,
      error_code::policy_violation);
    request.data.topics.erase_to_end(linked_topics_it);

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

                          // acks = 0 is handled separately. first, check
                          // for errors
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

                          // in the absence of errors, acks = 0 results in
                          // the response being dropped, as the client
                          // does not expect a response. here we mark the
                          // response as noop, but let it flow back so
                          // that it can be accounted for in quota and
                          // stats tracking. it is dropped later during
                          // processing.
                          if (!has_error) {
                              return octx.rctx.respond(std::move(octx.response))
                                .then([](response_ptr resp) {
                                    resp->mark_noop();
                                    return resp;
                                });
                          }

                          // errors in a response from an acks=0 produce
                          // request result in the connection being
                          // dropped to signal an issue to the client
                          return ss::make_exception_future<response_ptr>(
                            std::runtime_error(
                              fmt::format(
                                "Closing connection due to error in produce "
                                "response: {}",
                                octx.response)));
                      });
                } catch (...) {
                    /*
                     * if the first stage failed then we cannot resolve
                     * the current future (do_with holding octx)
                     * immediately, otherwise octx will be destroyed and
                     * all of the second stage futures (which have a
                     * reference to octx) will be backgrounded. logging
                     * about the second stage return value is handled in
                     * connection_context handler.
                     */
                    dispatched_promise.set_exception(std::current_exception());
                    return when_all_succeed(produced.begin(), produced.end())
                      .discard_result()
                      .then([] {
                          return ss::make_exception_future<response_ptr>(
                            std::runtime_error(
                              "First stage produce failed but "
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

std::optional<ss::scheduling_group>
produce_scheduling_group_provider(const connection_context& conn_ctx) {
    return conn_ctx.server().produce_scheduling_group();
}

} // namespace kafka
