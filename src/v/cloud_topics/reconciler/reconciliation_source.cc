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
#include "cluster/partition.h"
#include "config/configuration.h"
#include "kafka/utils/txn_reader.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "storage/segment.h"
#include "storage/segment_set.h"
#include "utils/retry_chain_node.h"

#include <expected>
#include <utility>

namespace cloud_topics::reconciler {

namespace {

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

    std::optional<std::chrono::milliseconds>
    compaction_lag_remaining() override {
        if (!has_pending_data()) {
            return std::nullopt;
        }

        const auto& cfg = _partition->get_ntp_config();

        if (!cfg.is_remotely_compacted()) {
            return std::nullopt;
        }

        auto max_lag = cfg.max_compaction_lag_ms();

        std::optional<model::timestamp> earliest_ts;
        try {
            auto lro = last_reconciled_offset();
            auto ot_state = _partition->get_offset_translator_state();
            auto next_kafka = kafka::next_offset(lro);
            auto log_offset = ot_state->to_log_offset(
              kafka::offset_cast(next_kafka));
            const auto& segs = _partition->log()->segments();
            auto it = segs.lower_bound(log_offset);

            for (; it != segs.end(); ++it) {
                const auto& seg = *it;
                auto ts = seg->index().base_timestamp();
                if (!earliest_ts.has_value() || ts < earliest_ts.value()) {
                    earliest_ts = ts;
                }
            }
        } catch (...) {
            // fallthrough
        }

        if (!earliest_ts.has_value()) {
            return std::nullopt;
        }

        const auto now = to_time_point(model::timestamp::now());
        const auto age = now - to_time_point(earliest_ts.value());
        return std::chrono::duration_cast<std::chrono::milliseconds>(
          max_lag - age);
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
