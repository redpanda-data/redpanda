/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/reconciler/reconciliation_source.h"

#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/frontend/frontend.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "cloud_topics/log_reader_config.h"
#include "cloud_topics/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition.h"
#include "cluster/topic_properties.h"
#include "config/configuration.h"
#include "kafka/utils/txn_reader.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "storage/log.h"
#include "storage/types.h"

#include <chrono>
#include <expected>
#include <utility>

using namespace std::chrono_literals;

namespace cloud_topics::reconciler {

namespace {

// True when the topic config implies the reconciler should publish a
// non-null allowed_local_start_offset (i.e. tiered_cloud mode with a
// non-compact cleanup policy and an engaged local retention limit).
// Used by both compute_local_retention_target and
// is_local_retention_shape_in_sync to avoid duplicating the predicate.
bool tiered_cloud_with_local_limit(const cluster::topic_properties& props) {
    const auto mode = props.storage_mode;
    if (mode != model::redpanda_storage_mode::tiered_cloud) {
        return false;
    }
    const bool compact
      = props.cleanup_policy_bitflags.has_value()
        && ((*props.cleanup_policy_bitflags & model::cleanup_policy_bitflags::compaction) == model::cleanup_policy_bitflags::compaction);
    if (compact) {
        return false;
    }
    const bool has_local_limit
      = (!props.retention_local_target_bytes.is_disabled()
         && props.retention_local_target_bytes.has_optional_value())
        || (!props.retention_local_target_ms.is_disabled()
            && props.retention_local_target_ms.has_optional_value());
    return has_local_limit;
}

class aborted_transaction_tracker_impl
  : public kafka::aborted_transaction_tracker {
public:
    aborted_transaction_tracker_impl(
      ss::lw_shared_ptr<cloud_topics::frontend> fe,
      ss::lw_shared_ptr<const storage::offset_translator_state> translator)
      : _fe(std::move(fe))
      , _translator(std::move(translator)) {}

    ss::future<std::vector<model::tx_range>>
    compute_aborted_transactions(model::offset base, model::offset max) final {
        return _fe->aborted_transactions(
          model::offset_cast(base), model::offset_cast(max), _translator);
    }

private:
    ss::lw_shared_ptr<cloud_topics::frontend> _fe;
    ss::lw_shared_ptr<const storage::offset_translator_state> _translator;
};

class l0_source : public source {
public:
    l0_source(
      model::ntp ntp,
      model::topic_id_partition tidp,
      data_plane_api* dp_api,
      ss::lw_shared_ptr<cluster::partition> partition)
      : source(std::move(ntp), tidp)
      , _fe(ss::make_lw_shared<frontend>(partition, dp_api))
      , _partition(std::move(partition)) {}

    bool has_pending_data() override {
        auto lro = last_reconciled_offset();
        auto lso = _fe->last_stable_offset();
        if (!lso.has_value()) {
            // LSO is invalid.
            return false;
        }
        return lso.value() > kafka::next_offset(lro);
    }

    int64_t pending_offset_lag() override {
        auto lro = last_reconciled_offset();
        auto lso = _fe->last_stable_offset();
        if (!lso.has_value()) {
            return 0;
        }
        auto next = kafka::next_offset(lro);
        if (lso.value() <= next) {
            return 0;
        }
        return (lso.value() - next)();
    }

    kafka::offset last_reconciled_offset() override {
        ctp_stm_api api(_partition->raft()->stm_manager()->get<ctp_stm>());
        return api.get_last_reconciled_offset();
    }

    ss::future<std::expected<void, errc>> set_last_reconciled_offset(
      kafka::offset offset, ss::abort_source& as) override {
        ctp_stm_api api(_partition->raft()->stm_manager()->get<ctp_stm>());
        auto res = co_await api.advance_reconciled_offset(
          offset, model::no_timeout, as);
        if (!res.has_value()) {
            switch (res.error()) {
            case ctp_stm_api_errc::timeout:
                co_return std::unexpected(errc::timeout);
            case ctp_stm_api_errc::not_leader:
                co_return std::unexpected(errc::not_leader);
            case ctp_stm_api_errc::shutdown:
                co_return std::unexpected(errc::shutdown);
            case ctp_stm_api_errc::failure:
                co_return std::unexpected(errc::failure);
            }
        }
        co_return std::expected<void, errc>();
    }

    ss::future<model::record_batch_reader>
    make_reader(reader_config input_cfg) override {
        auto effective_start = co_await _fe->sync_effective_start(
          model::no_timeout, *input_cfg.as);
        if (!effective_start.has_value()) {
            vlog(
              cd_log.warn,
              "Error querying partition start offset ({}): {}",
              _fe->ntp(),
              effective_start.error());
            co_return model::make_empty_record_batch_reader();
        }

        auto maybe_lso = _fe->last_stable_offset();
        if (!maybe_lso.has_value()) {
            vlog(
              cd_log.warn,
              "Error querying partition LSO ({}): {}",
              _fe->ntp(),
              maybe_lso.error());
            co_return model::make_empty_record_batch_reader();
        }

        cloud_topic_log_reader_config cfg(
          /*start_offset=*/std::max(
            effective_start.value(), input_cfg.start_offset),
          /*max_offset=*/kafka::prev_offset(maybe_lso.value()),
          /*min_bytes=*/1,
          /*max_bytes=*/input_cfg.max_bytes,
          /*type_filter=*/std::nullopt,
          /*time=*/std::nullopt,
          /*as=*/*input_cfg.as);
        cfg.allow_mat_failure = allow_materialization_failure(
          config::shard_local_cfg()
            .cloud_topics_allow_materialization_failure());
        if (cfg.max_offset < cfg.start_offset) {
            co_return model::make_empty_record_batch_reader();
        }
        auto reader = co_await _fe->make_reader(cfg);

        // It's important the `aborted_transaction_tracker_impl` takes a shared
        // so we don't have to worry about the lifetimes of the reader and
        // source.
        auto tracker = std::make_unique<aborted_transaction_tracker_impl>(
          _fe, std::move(reader.ot_state));

        // Wrap the reader with some readahead to hide the latency of
        // downloading a bit.
        co_return model::make_readahead_record_batch_reader(
          model::make_record_batch_reader<kafka::read_committed_reader>(
            std::move(tracker), std::move(reader.reader)));
    }

    ss::future<std::optional<std::optional<kafka::offset>>>
    compute_local_retention_target(
      const cluster::topic_properties& props) override {
        if (!_partition || !_partition->is_leader()) {
            co_return std::nullopt;
        }
        const auto mode = props.storage_mode;
        const bool compact
          = props.cleanup_policy_bitflags.has_value()
            && ((*props.cleanup_policy_bitflags & model::cleanup_policy_bitflags::compaction) == model::cleanup_policy_bitflags::compaction);

        if (mode != model::redpanda_storage_mode::tiered_cloud || compact) {
            co_return std::optional<kafka::offset>(std::nullopt);
        }

        auto log = _partition->log();
        if (!log) {
            co_return std::nullopt;
        }

        // Mirror disk_log_impl::apply_overrides(default_cfg). Build the
        // effective retention cfg the way tiered storage would for this
        // partition.
        auto max_bytes = config::shard_local_cfg().retention_bytes();
        auto default_ret_ms = config::shard_local_cfg().log_retention_ms();
        auto upper_ts = default_ret_ms.has_value()
                          ? model::to_timestamp(
                              model::timestamp_clock::now() - *default_ret_ms)
                          : model::timestamp::min();

        if (props.retention_bytes.is_disabled()) {
            max_bytes = std::nullopt;
        } else if (props.retention_bytes.has_optional_value()) {
            max_bytes = props.retention_bytes.value();
        }
        if (props.retention_duration.is_disabled()) {
            upper_ts = model::timestamp::min();
        } else if (props.retention_duration.has_optional_value()) {
            upper_ts = model::to_timestamp(
              model::timestamp_clock::now() - props.retention_duration.value());
        }

        const bool strict_local
          = config::shard_local_cfg().retention_local_strict()
            && config::shard_local_cfg().retention_local_strict_override();
        if (strict_local) {
            auto local_bytes_tri = props.retention_local_target_bytes;
            auto local_ms_tri = props.retention_local_target_ms;
            if (
              !local_bytes_tri.is_disabled()
              && !local_bytes_tri.has_optional_value()) {
                local_bytes_tri = tristate<size_t>(
                  config::shard_local_cfg()
                    .retention_local_target_bytes_default());
            }
            if (!local_ms_tri.is_engaged()) {
                local_ms_tri = tristate<std::chrono::milliseconds>(
                  config::shard_local_cfg()
                    .retention_local_target_ms_default());
            }
            if (local_bytes_tri.has_optional_value()) {
                if (max_bytes.has_value()) {
                    max_bytes = std::min(
                      local_bytes_tri.value(), max_bytes.value());
                } else {
                    max_bytes = local_bytes_tri.value();
                }
            }
            if (local_ms_tri.has_optional_value()) {
                upper_ts = std::max(
                  model::to_timestamp(
                    model::timestamp_clock::now() - local_ms_tri.value()),
                  upper_ts);
            }
        }

        storage::gc_config cfg(upper_ts, max_bytes);

        ctp_stm_api api(_partition->raft()->stm_manager()->get<ctp_stm>());
        auto lro = api.get_last_reconciled_offset();
        auto retention_log_offset = log->retention_offset(cfg);

        std::optional<kafka::offset> target;
        if (retention_log_offset.has_value()) {
            auto kafka_off = model::offset_cast(
              log->from_log_offset(*retention_log_offset));
            target = std::min(kafka_off, lro);
        }
        co_return target;
    }

    bool is_local_retention_shape_in_sync(
      const cluster::topic_properties& props) const override {
        if (!_partition || !_partition->is_leader()) {
            return true;
        }
        auto cached = local_retention_last_published();
        if (!cached.has_value()) {
            return false;
        }
        if (tiered_cloud_with_local_limit(props)) {
            return cached->has_value();
        }
        return !cached->has_value();
    }

    size_t local_retention_segment_size_bytes(
      std::optional<size_t> topic_override) const override {
        return topic_override.value_or(
          config::shard_local_cfg().log_segment_size());
    }

    ss::future<std::expected<void, errc>> publish_local_retention_target(
      std::optional<kafka::offset> value, ss::abort_source& as) override {
        ctp_stm_api api(_partition->raft()->stm_manager()->get<ctp_stm>());
        auto deadline = model::timeout_clock::now() + 5s;
        auto res = co_await api.set_allowed_local_start_offset(
          value, deadline, as);
        if (!res.has_value()) {
            switch (res.error()) {
            case ctp_stm_api_errc::timeout:
                co_return std::unexpected(errc::timeout);
            case ctp_stm_api_errc::not_leader:
                co_return std::unexpected(errc::not_leader);
            case ctp_stm_api_errc::shutdown:
                co_return std::unexpected(errc::shutdown);
            case ctp_stm_api_errc::failure:
                co_return std::unexpected(errc::failure);
            }
        }
        co_return std::expected<void, errc>();
    }

private:
    ss::lw_shared_ptr<frontend> _fe;
    ss::lw_shared_ptr<cluster::partition> _partition;
};

} // namespace

ss::shared_ptr<source> make_source(
  model::ntp ntp,
  model::topic_id_partition tidp,
  data_plane_api* dp_api,
  ss::lw_shared_ptr<cluster::partition> p) {
    return ss::make_shared<l0_source>(
      std::move(ntp), tidp, dp_api, std::move(p));
}

} // namespace cloud_topics::reconciler

auto fmt::formatter<cloud_topics::reconciler::source::errc>::format(
  const cloud_topics::reconciler::source::errc& err,
  fmt::format_context& ctx) const -> decltype(ctx.out()) {
    std::string_view name = "unknown";
    switch (err) {
    case cloud_topics::reconciler::source::errc::timeout:
        name = "timeout";
        break;
    case cloud_topics::reconciler::source::errc::not_leader:
        name = "not_leader";
        break;
    case cloud_topics::reconciler::source::errc::shutdown:
        name = "shutdown";
        break;
    case cloud_topics::reconciler::source::errc::failure:
        name = "failure";
        break;
    }
    return fmt::format_to(
      ctx.out(), "cloud_topics::reconciler::source::errc::{}", name);
}
