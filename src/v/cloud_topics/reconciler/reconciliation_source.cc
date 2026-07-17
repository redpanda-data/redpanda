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

#include "cloud_io/admission_control_types.h"
#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/frontend/frontend.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "cloud_topics/log_reader_config.h"
#include "cloud_topics/logger.h"
#include "cluster/partition.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "kafka/utils/txn_reader.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"

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

    l0_source(const l0_source&) = delete;
    l0_source& operator=(const l0_source&) = delete;
    l0_source(l0_source&&) = delete;
    l0_source& operator=(l0_source&&) = delete;

    ~l0_source() override { discard_placeholder_observation(); }

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
        // The floor may only cover placeholder-backed data that this commit
        // makes readable from L1: clamp the reader's observation to the
        // committed LRO (the metadata pass may have scanned ahead of it).
        // Both the observation and the LRO are inclusive offsets while the
        // floor is the exclusive lower bound for local reads (reads below it
        // go to L1), so convert with next_offset: the last placeholder-backed
        // offset itself must be readable from L1 once its L0 object is
        // garbage-collected. A scan that saw no placeholders resolves the
        // observation with offset::min(), which must not advance the floor.
        // The feature gate is checked at the propagation point as well as
        // at observation time: the set_min_allowed_local_threshold stm
        // command cannot be applied by pre-v26.2 replicas, so it must never
        // be replicated before the cluster is fully upgraded.
        std::optional<kafka::offset> min_allowed_local_threshold;
        if (
          auto hwm = harvest_placeholder_observation();
          hwm.has_value() && *hwm >= kafka::offset{0}
          && _partition->feature_table().local().is_active(
            features::feature::tiered_cloud_topics)) {
            min_allowed_local_threshold = std::min(
              kafka::next_offset(offset), kafka::next_offset(*hwm));
        }
        ctp_stm_api api(_partition->raft()->stm_manager()->get<ctp_stm>());
        auto res = co_await api.advance_reconciled_offset(
          offset, model::no_timeout, as, min_allowed_local_threshold);
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
          /*group=*/cloud_io::group_id::default_group,
          /*start_offset=*/
          std::max(effective_start.value(), input_cfg.start_offset),
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
        discard_placeholder_observation();
        if (
          _partition->feature_table().local().is_active(
            features::feature::tiered_cloud_topics)) {
            // Ask the reader to report the placeholder-backed high watermark
            // of the range it scans; the commit path advances the local-read
            // floor (min_allowed_local_threshold) over it, so that data
            // reconciled from placeholders is read from L1 rather than from
            // local placeholders whose L0 objects may be garbage-collected.
            // Gated on the feature flag because the floor command cannot be
            // applied by pre-v26.2 replicas.
            auto p = ss::make_lw_shared<ss::promise<kafka::offset>>();
            _max_placeholder_offset = p->get_future();
            cfg.max_placeholder_offset = std::move(p);
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

private:
    // Collect the placeholder high watermark reported by the most recent
    // reader. The reader resolves the promise on destruction, which in the
    // reconciler's flow happens strictly before the LRO commit; an
    // unresolved observation is simply skipped (the next reconciliation
    // covers the same prefix again).
    std::optional<kafka::offset> harvest_placeholder_observation() {
        if (
          !_max_placeholder_offset.has_value()
          || !_max_placeholder_offset->available()) {
            return std::nullopt;
        }
        auto fut = std::move(*_max_placeholder_offset);
        _max_placeholder_offset.reset();
        if (fut.failed()) {
            fut.ignore_ready_future();
            return std::nullopt;
        }
        auto hwm = fut.get();
        if (hwm == kafka::offset::min()) {
            // The scanned range contained no placeholders.
            return std::nullopt;
        }
        return hwm;
    }

    void discard_placeholder_observation() {
        if (_max_placeholder_offset.has_value()) {
            if (_max_placeholder_offset->available()) {
                _max_placeholder_offset->ignore_ready_future();
            }
            _max_placeholder_offset.reset();
        }
    }

    ss::lw_shared_ptr<frontend> _fe;
    ss::lw_shared_ptr<cluster::partition> _partition;
    std::optional<ss::future<kafka::offset>> _max_placeholder_offset;
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
