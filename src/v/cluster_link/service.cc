/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/service.h"

#include "cluster/cluster_link/frontend.h"
#include "cluster/controller.h"
#include "cluster/controller_stm.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/id_allocator_frontend.h"
#include "cluster/members_table.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster_link/deps.h"
#include "cluster_link/group_mirroring_task.h"
#include "cluster_link/link.h"
#include "cluster_link/link_probe.h"
#include "cluster_link/logger.h"
#include "cluster_link/manager.h"
#include "cluster_link/model/types.h"
#include "cluster_link/replication/deps.h"
#include "cluster_link/replication/mux_remote_consumer.h"
#include "cluster_link/replication/types.h"
#include "cluster_link/security_migrator.h"
#include "cluster_link/shadow_linking_rpc_service.h"
#include "cluster_link/source_topic_syncer.h"
#include "config/node_config.h"
#include "kafka/client/direct_consumer/direct_consumer.h"
#include "kafka/data/make_exact_offset_replicator.h"
#include "kafka/data/partition_proxy.h"
#include "kafka/server/group_router.h"
#include "kafka/server/snc_quota_manager.h"

#include <seastar/coroutine/switch_to.hh>

namespace {
/**
 * @brief Reduces the results of shard reports into a single response
 */
struct shard_report_reducer {
    using result_t = ::cluster_link::rpc::shadow_topic_report_response;
    void operator()(result_t shard_result) {
        if (!result) {
            result = std::move(shard_result);
            return;
        }
        if (result->err_code != ::cluster_link::errc::success) {
            // once we have an error, we just keep it
            return;
        }
        if (shard_result.err_code != ::cluster_link::errc::success) {
            result->err_code = shard_result.err_code;
            // no need to populate further results
            return;
        }
        // capture the minimum revision seen across the shards. Usually all
        // shards should have the same revision, so this is a conservative
        // check.
        result->link_update_revision = std::min(
          result->link_update_revision, shard_result.link_update_revision);
        for (auto& leader : shard_result.leaders) {
            result->leaders.push_back(std::move(leader));
        }
        return;
    }

    std::optional<result_t> get() && {
        if (!result) {
            return std::nullopt;
        }
        if (result->err_code != ::cluster_link::errc::success) {
            result->leaders.clear();
            result->link_update_revision = {};
        }
        return std::move(result);
    }
    std::optional<result_t> result;
};

struct shard_link_report_reducer {
    using result_t = ::cluster_link::rpc::shadow_link_status_report_response;
    void operator()(result_t shard_result) {
        if (!result) {
            result = std::move(shard_result);
            return;
        }
        if (
          shard_result.err_code != ::cluster_link::errc::success
          && result->err_code == ::cluster_link::errc::success) {
            // Keep the first error we see
            result->err_code = shard_result.err_code;
        }
        for (auto& [topic, response] : shard_result.topic_responses) {
            auto& existing = result->topic_responses[topic];
            existing.status = response.status;
            for (auto& [pid, report] : response.partition_reports) {
                existing.partition_reports.emplace(pid, std::move(report));
            }
        }
        for (auto& [task_name, reports] : shard_result.task_status_reports) {
            auto& existing = result->task_status_reports[task_name];
            for (auto& report : reports) {
                existing.push_back(std::move(report));
            }
        }
    }
    std::optional<result_t> get() && {
        if (!result) {
            return std::nullopt;
        }
        return std::move(result);
    }
    std::optional<result_t> result;
};
} // namespace

namespace cluster_link {

using ::cluster::cluster_link::frontend;
using kafka::data::rpc::partition_leader_cache;
using kafka::data::rpc::partition_manager;
using kafka::data::rpc::topic_creator;
using kafka::data::rpc::topic_metadata_cache;
using config_provider = replication::link_configuration_provider;

class link_registry_adapter : public link_registry {
public:
    explicit link_registry_adapter(frontend* plf, service* svc)
      : _plf(plf)
      , _svc(svc) {}

    ss::future<::cluster::cluster_link::errc> upsert_link(
      model::metadata md, ::model::timeout_clock::time_point timeout) override {
        return _plf->upsert_cluster_link(std::move(md), timeout);
    }

    ss::future<::cluster::cluster_link::errc> delete_link(
      model::name_t name,
      bool force,
      ::model::timeout_clock::time_point timeout) override {
        return _plf->remove_cluster_link(std::move(name), force, timeout);
    }

    model::metadata_ptr find_link_by_id(model::id_t id) const override {
        return _plf->find_link_by_id(id);
    }

    model::metadata_ptr
    find_link_by_name(const model::name_t& name) const override {
        return _plf->find_link_by_name(name);
    }

    std::optional<model::id_t>
    find_link_id_by_name(const model::name_t& name) const final {
        return _plf->find_link_id_by_name(name);
    }

    chunked_vector<model::id_t> get_all_link_ids() const override {
        return _plf->get_all_link_ids();
    }

    std::optional<::model::revision_id>
    get_last_update_revision(const model::id_t& id) const override {
        return _plf->get_last_update_revision(id);
    }

    ss::future<::cluster::cluster_link::errc> add_mirror_topic(
      model::id_t id,
      model::add_mirror_topic_cmd cmd,
      ::model::timeout_clock::time_point timeout) override {
        return _plf->add_mirror_topic(id, std::move(cmd), timeout);
    }

    ss::future<::cluster::cluster_link::errc> update_mirror_topic_state(
      model::id_t id,
      model::update_mirror_topic_status_cmd cmd,
      ::model::timeout_clock::time_point timeout) override {
        return _plf->update_mirror_topic_status(id, std::move(cmd), timeout);
    }

    ss::future<::cluster::cluster_link::errc> update_mirror_topic_properties(
      model::id_t id,
      model::update_mirror_topic_properties_cmd cmd,
      ::model::timeout_clock::time_point timeout) override {
        return _plf->update_mirror_topic_properties(
          id, std::move(cmd), timeout);
    }

    std::optional<chunked_hash_map<
      ::model::topic,
      ::cluster_link::model::mirror_topic_metadata>>
    get_mirror_topics_for_link(model::id_t id) const override {
        return _plf->get_mirror_topics_for_link(id);
    }

    ss::future<::cluster::cluster_link::errc> update_cluster_link_configuration(
      model::id_t id,
      model::update_cluster_link_configuration_cmd cmd,
      ::model::timeout_clock::time_point timeout) override {
        return _plf->update_cluster_link_configuration(
          id, std::move(cmd), timeout);
    }

    ss::future<model::report_result_t> shadow_topic_report(
      const model::id_t& id, const ::model::topic& topic) override {
        return _svc->shadow_topic_report(id, topic);
    }

    ss::future<::cluster::cluster_link::errc> failover_link_topics(
      model::id_t id, ::model::timeout_clock::duration timeout) override {
        return _plf->failover_link_topics(id, timeout);
    }

    ss::future<::cluster::cluster_link::errc> delete_shadow_topic(
      model::id_t id,
      model::delete_mirror_topic_cmd cmd,
      ::model::timeout_clock::time_point timeout) final {
        return _plf->delete_mirror_topic(id, std::move(cmd), timeout);
    }

private:
    frontend* _plf;
    service* _svc;
};
namespace {
replication::mux_remote_consumer::configuration
make_remote_consumer_configuration(const model::connection_config& conn_cfg) {
    const size_t max_buffered_bytes = 2 * conn_cfg.get_fetch_max_bytes();
    kafka::client::direct_consumer::configuration dc_configuration;
    const auto max_wait_time = std::chrono::milliseconds(
      conn_cfg.get_fetch_wait_max_ms());

    dc_configuration.min_bytes = conn_cfg.get_fetch_min_bytes();
    dc_configuration.max_fetch_size = conn_cfg.get_fetch_max_bytes();
    dc_configuration.isolation_level = ::model::isolation_level::read_committed;
    dc_configuration.max_buffered_bytes = max_buffered_bytes;
    // We are not interested in limiting the number of buffered fetches as
    // we already set bytes limit
    dc_configuration.max_buffered_elements = std::numeric_limits<size_t>::max();
    dc_configuration.with_sessions = kafka::client::fetch_sessions_enabled::yes;

    dc_configuration.max_wait_time = max_wait_time;
    dc_configuration.partition_max_bytes
      = conn_cfg.get_fetch_partition_max_bytes();

    const size_t partition_max_buffered
      = 2 * conn_cfg.get_fetch_partition_max_bytes();
    return replication::mux_remote_consumer::configuration{
      .client_id = conn_cfg.client_id,
      .direct_consumer_configuration = dc_configuration,
      .partition_max_buffered = partition_max_buffered,
      .fetch_max_wait = max_wait_time,
    };
}

} // namespace

class remote_partition_source : public replication::data_source {
public:
    explicit remote_partition_source(
      ::model::topic_partition tp, replication::mux_remote_consumer& consumer)
      : _tp(std::move(tp))
      , _consumer(consumer) {}

    ss::future<> start(kafka::offset offset) final {
        vlog(cllog.trace, "[{}] Starting remote partition source", _tp);
        auto result = _consumer.add(_tp, offset);
        if (!result.has_value()) [[unlikely]] {
            // this is usually indicative of a bug in the manager where
            // a previous source is not deregistered, bubble it up.
            auto err = result.error();
            vlog(
              cllog.error,
              "[{}] Failed to add remote partition source: {}",
              _tp,
              err);
            return ss::make_exception_future<>(err);
        }
        return ss::now();
    }

    ss::future<> stop() noexcept final {
        vlog(cllog.trace, "[{}] Stopping remote partition source", _tp);
        auto f = _gate.close();
        co_await _consumer.remove(_tp);
        co_await std::move(f);
    }

    ss::future<> reset(kafka::offset offset) final {
        _gate.check();
        auto result = _consumer.reset(_tp, offset);
        if (!result.has_value()) [[unlikely]] {
            auto err = result.error();
            vlog(
              cllog.error,
              "[{}] Failed to reset remote partition source: {}",
              _tp,
              err);
            return ss::make_exception_future<>(err);
        }
        return ss::now();
    }

    ss::future<replication::fetch_data> fetch_next(ss::abort_source& as) final {
        auto holder = _gate.hold();
        auto result = co_await _consumer.fetch(_tp, as);
        if (!result.has_value()) [[unlikely]] {
            auto err = result.error();
            vlog(
              cllog.error,
              "[{}] Failed to fetch from remote partition source: {}",
              _tp,
              result.error());
            throw std::runtime_error(
              fmt::format(
                "[{}] Failed to fetch from remote partition source: {}",
                _tp,
                err));
        }
        auto [batches, units] = std::move(*result);
        co_return replication::fetch_data{
          .batches = std::move(batches), .units = std::move(units)};
    }

    std::optional<data_source::source_partition_offsets_report> get_offsets() {
        auto offsets = _consumer.get_source_offsets(_tp);
        if (!offsets.has_value()) {
            return std::nullopt;
        }
        return data_source::source_partition_offsets_report{
          .source_start_offset = offsets->log_start_offset,
          .source_hwm = offsets->high_watermark,
          .source_lso = offsets->last_stable_offset,
          .update_time = offsets->last_offset_update_timestamp,
        };
    }

private:
    ::model::topic_partition _tp;
    replication::mux_remote_consumer& _consumer;
    ss::gate _gate;
};

class remote_data_source_factory : public replication::data_source_factory {
public:
    explicit remote_data_source_factory(
      model::id_t link_id,
      manager* manager,
      std::unique_ptr<replication::mux_remote_consumer> consumer)
      : _link_id(link_id)
      , _manager(manager)
      , _consumer(std::move(consumer)) {}

    ss::future<> start() final {
        _notification_id = _manager->register_link_config_changes_callback(
          [this](model::id_t link_id, model::metadata_ptr md) {
              // Ignore updates for other links
              if (link_id != _link_id) {
                  return;
              }
              _consumer->update_configuration(
                make_remote_consumer_configuration(md->connection));
          });
        return _consumer->start();
    }

    ss::future<> stop() noexcept final {
        _manager->unregister_link_config_changes_callback(_notification_id);
        return _consumer->stop();
    }

    std::unique_ptr<replication::data_source>
    make_source(const ::model::ntp& ntp) final {
        return make_default_data_source(ntp.tp, *_consumer);
    }

private:
    model::id_t _link_id;
    manager* _manager;
    manager::notification_id _notification_id;
    std::unique_ptr<replication::mux_remote_consumer> _consumer;
};

/*
 * Sink for writing partition data to the partition leader on the local shard.
 */
class local_partition_sink : public replication::data_sink {
public:
    static constexpr auto sync_timeout = 10s;
    explicit local_partition_sink(
      ss::lw_shared_ptr<cluster::partition> partition,
      const cluster::metadata_cache& md_cache,
      cluster::id_allocator_frontend& id_alloc)
      : _partition(partition)
      , _metadata_cache{md_cache}
      , _id_allocator_frontend(id_alloc)
      , _replicator(kafka::make_exact_offset_replicator(partition)) {
        vassert(
          _replicator,
          "exact_offset_replicator not available for partition {}",
          _partition->ntp());
    }
    ss::future<> start() final { return initialize(); }

    ss::future<> reset() final { return initialize(); }

    ss::future<> stop() noexcept final {
        vlog(
          cllog.trace, "[{}] Stopping local partition sink", _partition->ntp());
        co_await _gate.close();
    }

    kafka::offset last_replicated_offset() const final {
        vassert(_last_replicated_offset, "Sink has not been started");
        return _last_replicated_offset.value();
    }

    raft::replicate_stages replicate(
      chunked_vector<::model::record_batch> batches,
      ::model::timeout_clock::duration timeout,
      ss::abort_source& as) final {
        _gate.check();
        vassert(_last_replicated_offset, "Sink has not been started");
        vassert(
          !batches.empty(),
          "Cannot replicate empty batch vector {}",
          _partition->ntp());

        if (_metadata_cache.should_reject_writes()) [[unlikely]] {
            throw std::runtime_error{fmt::format(
              "Replication rejected on {}. no disk space; free bytes less than "
              "configurable threshold",
              _partition->ntp())};
        }
        if (config::node().recovery_mode_enabled()) [[unlikely]] {
            throw std::runtime_error{fmt::format(
              "Replication rejected on {}. Redpanda is in recovery mode",
              _partition->ntp())};
        }

        chunked_vector<kafka::offset> expected_offsets;
        expected_offsets.reserve(batches.size());
        for (const auto& batch : batches) {
            _highest_seen_pid = std::max(
              _highest_seen_pid,
              ::model::producer_id{batch.header().producer_id});
            expected_offsets.push_back(
              ::model::offset_cast(batch.base_offset()));
        }
        auto new_last_replicated_begin = ::model::offset_cast(
          batches.front().base_offset());
        auto new_last_replicated_end = ::model::offset_cast(
          batches.back().last_offset());

        auto monotonic = new_last_replicated_begin > _last_replicated_offset
                         && new_last_replicated_end > _last_replicated_offset;
        if (!monotonic) {
            vlog(
              cllog.error,
              "[{}] Replicating offsets must be monotonically increasing last "
              "replicated: {}, attempting to replicate: [{}, {}]",
              _partition->ntp(),
              _last_replicated_offset,
              new_last_replicated_begin,
              new_last_replicated_end);
            throw replication::monotonicity_violation_exception{fmt::format(
              "[{}] Replicating offsets must be monotonically increasing last "
              "replicated: {}, attempting to replicate: [{}, {}]",
              _partition->ntp(),
              _last_replicated_offset,
              new_last_replicated_begin,
              new_last_replicated_end)};
        }
        vlog(
          cllog.trace,
          "[{}] Replicating batches in range [{} - {}], last_replicated: {}, "
          "new_last_replicated: {}",
          _partition->ntp(),
          batches.front().header(),
          batches.back().header(),
          _last_replicated_offset,
          new_last_replicated_end);
        auto stages = _replicator->replicate(
          std::move(batches),
          std::move(expected_offsets),
          _last_replicated_offset,
          timeout,
          as);
        _last_replicated_offset = new_last_replicated_end;
        return stages;
    }

    void notify_replicator_failure(::model::term_id term) final {
        if (_gate.is_closed()) {
            return;
        }
        // If the replicator failed to start _and_ the partition is still the
        // leader in the same term we are effectively stuck without a
        // replicator. Here we step down to ensure a new leader comes up and a
        // replicator start is triggered again on the new leader.
        if (_partition->term() == term) {
            ssx::spawn_with_gate(_gate, [this, term] {
                vlog(
                  cllog.warn,
                  "[{}] Stepping down partition due to replicator failure in "
                  "term "
                  "{}",
                  _partition->ntp(),
                  term);
                return _partition->raft()->step_down(
                  "replicator_start_failure");
            });
        }
    }

    kafka::offset high_watermark() const final {
        _gate.check();
        return ::model::offset_cast(
          _partition->log()->from_log_offset(_partition->high_watermark()));
    }

    bool can_prefix_truncate() const final {
        _gate.check();
        return _partition->get_ntp_config().is_locally_collectable();
    }

    ss::future<kafka::error_code> prefix_truncate(
      kafka::offset truncation_offset,
      ss::lowres_clock::time_point deadline) final {
        auto h = _gate.hold();
        auto timeout
          = std::chrono::duration_cast<::model::timeout_clock::duration>(
            deadline - ss::lowres_clock::now());
        auto err = co_await _replicator->ensure_truncatable(
          truncation_offset, timeout);
        if (err) {
            vlog(
              cllog.warn,
              "[{}] Failed to ensure truncatable offset {}: {}, will be "
              "retried later",
              _partition->ntp(),
              truncation_offset,
              err.message());
            // a blanket error to trigger a retry later
            co_return kafka::error_code::offset_out_of_range;
        }
        co_return co_await kafka::make_partition_proxy(_partition)
          .prefix_truncate(kafka::offset_cast(truncation_offset), deadline);
    }

    kafka::offset start_offset() final {
        _gate.check();
        return ::model::offset_cast(
          kafka::make_partition_proxy(_partition).start_offset());
    }

    ss::future<> maybe_sync_pid() final {
        // each time we sync the producer ID, advanced it by some fixed amount
        // greater than one to minimize the number of reset queries.
        constexpr ::model::producer_id::type pid_sync_increment{1000};
        constexpr auto timeout = 10s;
        auto h = _gate.hold();
        if (
          _highest_seen_pid
          < _last_synced_pid.value_or(::model::producer_id{0})) {
            co_return;
        }
        auto next_pid = _highest_seen_pid + pid_sync_increment;
        vlog(cllog.debug, "Setting next producer ID to {}", next_pid);
        auto res_f = co_await ss::coroutine::as_future(
          _id_allocator_frontend.reset_next_id(next_pid, timeout));

        if (res_f.failed()) {
            auto eptr = res_f.get_exception();
            vlog(
              cllog.warn,
              "Failed to set next producer ID to {}: {}",
              next_pid,
              eptr);
            co_return;
        }

        if (auto ec = res_f.get().ec; ec != cluster::errc::success) {
            vlog(
              cllog.warn,
              "Failed to set next producer ID to {}: {}",
              next_pid,
              ec);
            co_return;
        }
        _last_synced_pid = next_pid;
    }

private:
    ss::future<> initialize() {
        auto holder = _gate.hold();
        auto sync_offset = co_await _replicator->get_last_offset(sync_timeout);
        if (sync_offset.has_error()) {
            throw std::runtime_error(
              fmt::format(
                "Failed to sync write_at_offset_stm for partition {}: {}",
                _partition->ntp(),
                sync_offset.error().message()));
        }
        vlog(
          cllog.trace,
          "[{}] Reset-ing local partition sink at offset {}",
          _partition->ntp(),
          sync_offset.value());
        _last_replicated_offset = sync_offset.value();
    }

    ss::gate _gate;
    ss::lw_shared_ptr<cluster::partition> _partition;
    const cluster::metadata_cache& _metadata_cache;
    cluster::id_allocator_frontend& _id_allocator_frontend;
    std::unique_ptr<kafka::exact_offset_replicator> _replicator;
    // set in start();
    std::optional<kafka::offset> _last_replicated_offset;
    ::model::producer_id _highest_seen_pid{::model::no_producer_id};
    std::optional<::model::producer_id> _last_synced_pid;
};

class local_partition_data_sink_factory
  : public replication::data_sink_factory {
public:
    explicit local_partition_data_sink_factory(
      ss::sharded<cluster::partition_manager>& pm,
      ss::sharded<cluster::metadata_cache>& md_cache,
      ss::sharded<cluster::id_allocator_frontend>& id_alloc)
      : _partition_manager(pm)
      , _metadata_cache{md_cache}
      , _id_allocator_frontend(id_alloc) {}

    std::unique_ptr<replication::data_sink>
    make_sink(const ::model::ntp& ntp) final {
        auto partition = _partition_manager.local().get(ntp);
        if (!partition) {
            throw std::runtime_error(
              fmt::format("Partition not found: {} on this shard", ntp));
        }
        return make_default_data_sink(
          std::move(partition),
          _metadata_cache.local(),
          _id_allocator_frontend.local());
    }

private:
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    ss::sharded<cluster::id_allocator_frontend>& _id_allocator_frontend;
};

std::unique_ptr<replication::data_source> make_default_data_source(
  const ::model::topic_partition& tp,
  replication::mux_remote_consumer& consumer) {
    return std::make_unique<remote_partition_source>(tp, consumer);
}

std::unique_ptr<replication::data_sink> make_default_data_sink(
  ss::lw_shared_ptr<cluster::partition> partition,
  const cluster::metadata_cache& md_cache,
  cluster::id_allocator_frontend& id_allocator) {
    return std::make_unique<local_partition_sink>(
      std::move(partition), md_cache, id_allocator);
}

class default_link_config_provider
  : public replication::link_configuration_provider {
public:
    default_link_config_provider(
      model::id_t link_id,
      cluster_link::manager* mgr,
      kafka::client::cluster& cluster)
      : _link_id(link_id)
      , _mgr(mgr)
      , _cluster(cluster) {}
    ss::future<kafka::offset>
    start_offset(const ::model::ntp& ntp, ss::abort_source& as) override {
        constexpr auto max_timeout = 2min;
        constexpr auto backoff = 200ms;
        retry_chain_node rcn(as, max_timeout, backoff);
        while (true) {
            auto permit = rcn.retry();
            if (!permit.is_allowed) {
                break;
            }
            auto ts_opt = get_start_offset_timestamp(ntp);
            if (!ts_opt) {
                // not a retryable error, the topic is not being mirrored
                throw std::runtime_error(
                  fmt::format("Topic {} not being mirrored", ntp));
            }
            try {
                auto offset = co_await fetch_offset_for_timestamp(
                  rcn, ntp.tp, *ts_opt);
                if (offset) {
                    co_return offset.value();
                }
                vlog(
                  cllog.warn,
                  "Failed to fetch offset for timestamp {} for {}, attempt: {}",
                  *ts_opt,
                  ntp,
                  rcn.retry_count());
            } catch (...) {
                auto ex = std::current_exception();
                auto log_level = ssx::is_shutdown_exception(ex)
                                   ? ss::log_level::debug
                                   : ss::log_level::warn;
                vlogl(
                  cllog,
                  log_level,
                  "Exception fetching offset for timestamp {} for {}, "
                  "attempt: {}, error: {}",
                  *ts_opt,
                  ntp,
                  rcn.retry_count(),
                  ex);
            }
            co_await ss::sleep_abortable(permit.delay, *permit.abort_source);
        }
        throw std::runtime_error(
          fmt::format(
            "Failed to get starting offset for {}, retries exhausted", ntp));
    }

private:
    /// Dispatch a ListOffsets request for the given timestamp and return the
    /// offset from the response. Returns std::nullopt on transient errors.
    ss::future<std::optional<kafka::offset>> dispatch_list_offsets(
      retry_chain_node& rcn,
      const ::model::topic_partition& tp,
      ::model::timestamp ts) {
        auto api_version = co_await get_list_offset_api_version(rcn);
        if (!api_version) {
            vlog(cllog.warn, "Unable to determine ListOffset API version");
            co_return std::nullopt;
        }
        auto leader_and_epoch = co_await get_leader_and_epoch_for_tp(rcn, tp);
        if (!leader_and_epoch) {
            vlog(cllog.warn, "[{}] No leader found for partition", tp);
            co_return std::nullopt;
        }
        auto [leader_id, leader_epoch] = *leader_and_epoch;
        kafka::list_offsets_request req;
        req.data.replica_id = ::model::node_id{-1}; // normal consumer
        req.data.isolation_level = 1;               // read committed only
        req.data.topics.emplace_back(
          kafka::list_offset_topic{
            .name = tp.topic,
            .partitions = {{
              .partition_index = tp.partition,
              .current_leader_epoch = leader_epoch,
              .timestamp = ts,
            }},
          });

        auto resp = co_await _cluster.dispatch_to(
          leader_id, std::move(req), *api_version);
        if (resp.data.topics.empty()) {
            vlog(cllog.warn, "[{}] No topics in ListOffsets response", tp);
            co_return std::nullopt;
        }
        auto& topic = resp.data.topics[0];
        if (topic.name != tp.topic) {
            vlog(
              cllog.warn,
              "[{}] Topic name mismatch in ListOffsets response: {}",
              tp,
              topic.name);
            co_return std::nullopt;
        }
        if (topic.partitions.empty()) {
            vlog(cllog.warn, "[{}] No partitions in ListOffsets response", tp);
            co_return std::nullopt;
        }
        auto& partition = topic.partitions[0];
        if (partition.partition_index != tp.partition) {
            vlog(
              cllog.warn,
              "[{}] Partition index mismatch in ListOffsets response: {}",
              tp,
              partition.partition_index);
            co_return std::nullopt;
        }
        if (partition.error_code != kafka::error_code::none) {
            vlog(
              cllog.warn,
              "[{}] Error in ListOffsets response: {}",
              tp,
              partition.error_code);
            co_return std::nullopt;
        }
        co_return partition.offset;
    }

    ss::future<std::optional<kafka::offset>> fetch_offset_for_timestamp(
      retry_chain_node& rcn,
      const ::model::topic_partition& tp,
      ::model::timestamp ts) {
        auto offset = co_await dispatch_list_offsets(rcn, tp, ts);
        if (!offset) {
            co_return std::nullopt;
        }
        if (*offset >= kafka::offset{0}) {
            vlog(
              cllog.debug,
              "[{}] Fetched offset {} for timestamp {}",
              tp,
              *offset,
              ts);
            co_return offset;
        }
        // ListOffsets returns offset -1 when the timestamp is past the end
        // of the log (or the partition is empty). Fall back to the last
        // stable offset (LSO) so we start replicating only new committed
        // data. We query with latest_timestamp which, combined with our
        // read_committed isolation level, returns the LSO.
        auto lso = co_await dispatch_list_offsets(
          rcn, tp, kafka::list_offsets_request::latest_timestamp);
        if (lso) {
            vlog(
              cllog.info,
              "[{}] Timestamp {} is past the end of the source log, "
              "falling back to last stable offset {}",
              tp,
              ts,
              *lso);
        }
        co_return lso;
    }

    ss::future<std::optional<kafka::api_version>>
    get_list_offset_api_version(retry_chain_node& rcn) {
        rcn.check_abort();
        auto supported_versions = co_await _cluster.supported_api_versions(
          kafka::list_offsets_api::key);
        if (!supported_versions) {
            co_return std::nullopt;
        }
        if (
          supported_versions.value().min > kafka::list_offsets_api::max_valid) {
            co_return std::nullopt;
        }
        co_return std::min(
          supported_versions.value().max, kafka::list_offsets_api::max_valid);
    }

    std::optional<std::tuple<::model::node_id, kafka::leader_epoch>>
    do_get_leader_and_epoch_for_tp(const ::model::topic_partition& tp) {
        const auto& topics = _cluster.get_topics().cache();
        auto it = topics.find(tp.topic);
        if (it == topics.end()) {
            vlog(cllog.debug, "[{}] Topic not found in metadata", tp);
            return std::nullopt;
        }
        auto pit = it->second.partitions.find(tp.partition);
        if (pit == it->second.partitions.end()) {
            vlog(cllog.debug, "[{}] Partition not found in metadata", tp);
            return std::nullopt;
        }
        return std::make_tuple(pit->second.leader, pit->second.leader_epoch);
    }

    ss::future<std::optional<std::tuple<::model::node_id, kafka::leader_epoch>>>
    get_leader_and_epoch_for_tp(
      retry_chain_node& rcn, const ::model::topic_partition& tp) {
        auto leader_and_epoch = do_get_leader_and_epoch_for_tp(tp);
        if (leader_and_epoch) {
            co_return leader_and_epoch;
        }
        // refresh metadata and try again
        vlog(cllog.debug, "[{}] NTP has no leader, refreshing metadata", tp);
        rcn.check_abort();
        co_await _cluster.request_metadata_update();
        co_return do_get_leader_and_epoch_for_tp(tp);
    }

    std::optional<::model::timestamp>
    get_start_offset_timestamp(const ::model::ntp& ntp) {
        auto link_md = _mgr->registry()->find_link_by_id(_link_id);
        if (!link_md) {
            return std::nullopt;
        }
        auto mirror_it = link_md->state.mirror_topics.find(ntp.tp.topic);
        if (mirror_it == link_md->state.mirror_topics.end()) {
            return std::nullopt;
        }
        return mirror_it->second.get_starting_offset_ts();
    }

    model::id_t _link_id;
    cluster_link::manager* _mgr;
    kafka::client::cluster& _cluster;
};

class default_link_factory : public link_factory {
public:
    explicit default_link_factory(
      ss::sharded<cluster::partition_manager>* partition_manager,
      ss::sharded<kafka::snc_quota_manager>* snc_quota_mgr,
      ss::sharded<cluster::metadata_cache>* md_cache,
      ss::sharded<cluster::id_allocator_frontend>* id_alloc)
      : link_factory()
      , _partition_manager(partition_manager)
      , _snc_quota_mgr(snc_quota_mgr)
      , _metadata_cache(md_cache)
      , _id_allocator_frontend(id_alloc) {}

    static constexpr auto link_reconciler_period = 5min;
    std::unique_ptr<link> create_link(
      ::model::node_id self,
      model::id_t link_id,
      manager* manager,
      model::metadata_ptr config,
      std::unique_ptr<kafka::client::cluster> cluster_connection) override {
        auto client_id = config->connection.client_id;
        auto cluster = cluster_connection.get();
        kafka::client::direct_consumer_probe::configuration probe_cfg{
          .group_name = link_probe::shadow_link_group,
          .labels = {link_probe::shadow_link_name(config->name)}};
        return std::make_unique<link>(
          self,
          link_id,
          manager,
          link_reconciler_period,
          std::move(config),
          std::move(cluster_connection),
          std::make_unique<default_link_config_provider>(
            link_id, manager, *cluster),
          std::make_unique<remote_data_source_factory>(
            link_id,
            manager,
            std::make_unique<replication::mux_remote_consumer>(
              *cluster_connection,
              _snc_quota_mgr->local(),
              make_remote_consumer_configuration(config->connection),
              std::move(probe_cfg))),
          std::make_unique<local_partition_data_sink_factory>(
            *_partition_manager, *_metadata_cache, *_id_allocator_frontend));
    }

private:
    ss::sharded<cluster::partition_manager>* _partition_manager;
    ss::sharded<kafka::snc_quota_manager>* _snc_quota_mgr;
    ss::sharded<cluster::metadata_cache>* _metadata_cache;
    ss::sharded<cluster::id_allocator_frontend>* _id_allocator_frontend;
};

class kafka_consumer_groups_router : public consumer_groups_router {
public:
    explicit kafka_consumer_groups_router(
      ss::sharded<kafka::group_router>* router)
      : _router(router) {}
    std::optional<::model::partition_id>
    partition_for(const kafka::group_id& group) const final {
        return _router->local().coordinator_mapper().local().partition_for(
          group);
    }

    ss::future<kafka::offset_commit_response>
    offset_commit(kafka::offset_commit_request req) final {
        auto stages = _router->local().offset_commit(std::move(req));

        auto dispatched = co_await ss::coroutine::as_future(
          std::move(stages.dispatched));
        auto result = co_await ss::coroutine::as_future(
          std::move(stages.result));
        std::exception_ptr error = nullptr;
        if (dispatched.failed()) {
            error = dispatched.get_exception();
        }

        if (result.failed()) {
            auto r_err = result.get_exception();
            if (error == nullptr) {
                error = r_err;
            }
        }

        if (error) {
            std::rethrow_exception(error);
        }

        co_return result.get();
    }

    ss::future<bool> assure_topic_exists() final {
        return _router->local().group_initializer().assure_topic_exists(false);
    }

private:
    ss::sharded<kafka::group_router>* _router;
};

class health_monitor_based_partition_metadata_provider
  : public partition_metadata_provider {
public:
    explicit health_monitor_based_partition_metadata_provider(
      ss::sharded<cluster::health_monitor_frontend>* hm_frontend)
      : _hm_frontend(hm_frontend) {}

    ss::future<std::optional<kafka::offset>>
    get_partition_high_watermark(::model::topic_partition_view tp) final {
        auto hwm = co_await _hm_frontend->local().get_partition_high_watermark(
          ::model::topic_namespace_view(::model::kafka_namespace, tp.topic),
          tp.partition);
        if (!hwm) {
            vlog(cllog.warn, "Error getting high watermark for {}", tp);
            co_return std::nullopt;
        }
        co_return hwm.value();
    }

    ss::sharded<cluster::health_monitor_frontend>* _hm_frontend;
};

service::service(
  ::model::node_id self,
  config::binding<bool> enable_shadow_linking,
  ss::sharded<frontend>* plf,
  std::unique_ptr<cluster::partition_change_notifier> notifications,
  ss::sharded<cluster::partition_manager>* partition_manager,
  ss::sharded<cluster::partition_leaders_table>* partition_leaders_table,
  ss::sharded<cluster::shard_table>* shard_table,
  ss::sharded<cluster::metadata_cache>* metadata_cache,
  ss::sharded<::rpc::connection_cache>* connections,
  cluster::controller* controller,
  ss::sharded<kafka::group_router>* group_router,
  ss::sharded<kafka::snc_quota_manager>* snc_quota_mgr,
  ss::sharded<cluster::health_monitor_frontend>* hm_frontend,
  ss::sharded<cluster::security_frontend>* security_fe,
  ss::sharded<kafka::data::rpc::client>* kafka_data_rpc_client,
  ss::sharded<cluster::id_allocator_frontend>* id_alloc,
  ss::smp_service_group smp_group,
  ss::scheduling_group scheduling_group)
  : _self(self)
  , _enable_shadow_linking(std::move(enable_shadow_linking))
  , _plf(plf)
  , _notifications(std::move(notifications))
  , _partition_manager(partition_manager)
  , _partition_leaders_table(partition_leaders_table)
  , _shard_table(shard_table)
  , _metadata_cache(metadata_cache)
  , _connections(connections)
  , _controller(controller)
  , _group_router(group_router)
  , _snc_quota_mgr(snc_quota_mgr)
  , _hm_frontend(hm_frontend)
  , _security_fe(security_fe)
  , _kafka_data_rpc_client(kafka_data_rpc_client)
  , _id_allocator_frontend(id_alloc)
  , _smp_group(smp_group)
  , _scheduling_group(scheduling_group)
  , _queue(_scheduling_group, [](const std::exception_ptr& ex) {
      vlog(cllog.warn, "unexpected shadow link service error: {}", ex);
  }) {
    _enable_shadow_linking.watch(
      [this]() { handle_enable_shadow_link_change(); });
}

service::~service() = default;

ss::future<> service::start() {
    vlog(cllog.info, "Starting cluster link service");
    co_await ss::coroutine::switch_to(_scheduling_group);
    auto units = co_await _shadow_link_config_mutex.get_units(_as);
    if (_enable_shadow_linking()) {
        co_await maybe_start_manager();
    }
}

ss::future<> service::stop() {
    vlog(cllog.info, "Stopping cluster link service");

    _shadow_link_config_mutex.broken();
    co_await _queue.shutdown();
    _as.request_abort();
    co_await _gate.close();

    co_await maybe_stop_manager();
}

ss::future<cl_result<model::metadata_ptr>>
service::upsert_cluster_link(model::metadata md) {
    auto h = _gate.hold();
    return with_manager([md = std::move(md)](manager* mgr) mutable {
        return mgr->upsert_cluster_link(std::move(md));
    });
}

cl_result<model::metadata_ptr>
service::get_cluster_link(const model::name_t& name) {
    return with_manager(
      [&name](manager* mgr) { return mgr->get_cluster_link(name); });
}

cl_result<chunked_vector<model::metadata_ptr>> service::list_cluster_links() {
    return with_manager([](manager* mgr) { return mgr->list_cluster_links(); });
}

ss::future<cl_result<model::metadata_ptr>> service::update_cluster_link(
  model::name_t name, model::update_cluster_link_configuration_cmd cmd) {
    auto h = _gate.hold();
    return with_manager(
      [name = std::move(name), cmd = std::move(cmd)](manager* mgr) mutable {
          return mgr->update_cluster_link(std::move(name), std::move(cmd));
      });
}

ss::future<cl_result<model::metadata_ptr>> service::update_mirror_topic_status(
  model::name_t link_name,
  ::model::topic topic,
  model::mirror_topic_status status,
  bool force_update) {
    auto h = _gate.hold();
    return with_manager([link_name = std::move(link_name),
                         topic = std::move(topic),
                         status,
                         force_update](manager* mgr) mutable {
        return mgr->update_mirror_topic_status(
          std::move(link_name), std::move(topic), status, force_update);
    });
}

ss::future<cl_result<model::metadata_ptr>>
service::failover_link_topics(model::name_t link_name) {
    auto h = _gate.hold();
    return with_manager(
      [link_name = std::move(link_name)](manager* mgr) mutable {
          return mgr->failover_link_topics(std::move(link_name));
      });
}

ss::future<cl_result<void>>
service::delete_cluster_link(model::name_t name, bool force_delete_link) {
    auto h = _gate.hold();
    return with_manager(
      [name = std::move(name), force_delete_link](manager* mgr) mutable {
          return mgr->delete_cluster_link(std::move(name), force_delete_link);
      });
}

ss::future<cl_result<model::metadata_ptr>>
service::delete_shadow_topic_from_shadow_link(
  model::name_t link_name, ::model::topic shadow_topic) {
    auto h = _gate.hold();
    return with_manager(
      [link_name = std::move(link_name),
       shadow_topic = std::move(shadow_topic)](manager* mgr) mutable {
          return mgr->remove_shadow_topic_from_link(
            std::move(link_name), std::move(shadow_topic));
      });
}

void service::register_notifications() {
    auto pl_notif_id = _plf->local().register_for_updates(
      [this](model::id_t id, ::model::revision_id revision) {
          _manager->on_link_change(id, revision);
      });
    _notification_cleanups.emplace_back([this, pl_notif_id] {
        _plf->local().unregister_for_updates(pl_notif_id);
    });

    auto partition_notifications_id
      = _notifications->register_partition_notifications(
        [this](
          cluster::partition_change_notifier::notification_type type,
          const ::model::ntp& ntp,
          std::optional<cluster::partition_change_notifier::partition_state>
            partition) {
            auto is_leader = partition && partition->is_leader ? ntp_leader::yes
                                                               : ntp_leader::no;
            auto term = partition ? std::make_optional(partition->term)
                                  : std::nullopt;
            using ntype = cluster::partition_change_notifier::notification_type;
            switch (type) {
            case ntype::leadership_change:
            case ntype::partition_replica_assigned:
            case ntype::partition_replica_unassigned:
                _manager->handle_partition_state_change(ntp, is_leader, term);
                break;
            case ntype::partition_properties_change:
                // TODO: once we have partition properties
                break;
            }
        },
        cluster::partition_change_notifier::notify_current_state::yes);
    _notification_cleanups.emplace_back([this, partition_notifications_id] {
        _notifications->unregister_partition_notifications(
          partition_notifications_id);
    });
}

void service::unregister_notifications() { _notification_cleanups.clear(); }

errc service::check_manager_state() {
    if (!_manager) {
        return errc::cluster_link_disabled;
    }

    return errc::success;
}

void service::handle_enable_shadow_link_change() {
    _queue.submit([this] {
        return do_handle_enable_shadow_link_change().handle_exception(
          [](std::exception_ptr ex) {
              auto lvl = ssx::is_shutdown_exception(ex) ? ss::log_level::debug
                                                        : ss::log_level::error;
              vlogl(
                cllog,
                lvl,
                "Error occurred while handling change in shadow link config: "
                "{}",
                ex);
          });
    });
}

ss::future<> service::do_handle_enable_shadow_link_change() {
    auto units = co_await _shadow_link_config_mutex.get_units(_as);
    if (_enable_shadow_linking()) {
        vlog(cllog.info, "Enabling shadow linking");
        co_await maybe_start_manager();
    } else {
        vlog(cllog.info, "Disabling shadow linking");
        co_await maybe_stop_manager();
    }
}

ss::future<> service::maybe_start_manager() {
    if (_manager) {
        co_return;
    }
    _manager = std::make_unique<manager>(
      _self,
      partition_leader_cache::make_default(_partition_leaders_table),
      partition_manager::make_default(
        _shard_table, _partition_manager, _smp_group),
      topic_metadata_cache::make_default(_metadata_cache),
      topic_creator::make_default(_controller),
      security_service::make_default(_security_fe),
      std::make_unique<link_registry_adapter>(&_plf->local(), this),
      std::make_unique<default_link_factory>(
        _partition_manager,
        _snc_quota_mgr,
        _metadata_cache,
        _id_allocator_frontend),
      std::make_unique<cluster_factory>(),
      std::make_unique<kafka_consumer_groups_router>(_group_router),
      std::make_unique<health_monitor_based_partition_metadata_provider>(
        _hm_frontend),
      kafka_rpc_client_service::make_default(_kafka_data_rpc_client),
      members_table_provider::make_default(&_controller->get_members_table()),
      30s, // Temporary until we have a proper configuration for this
      config::shard_local_cfg().default_topic_replication.bind(),
      _scheduling_group);
    co_await _manager->register_task_factory<source_topic_syncer_factory>();
    co_await _manager->register_task_factory<group_mirroring_task_factory>();
    co_await _manager->register_task_factory<security_migrator_factory>();

    // Register notifications before the manager starts.  The manager will
    // have a constructed the underlying workqueue to start in a paused
    // state and will pick up the notifications once it has started
    register_notifications();
    auto manager_start = co_await ss::coroutine::as_future(_manager->start());
    if (manager_start.failed()) {
        auto ex = manager_start.get_exception();
        vlog(cllog.error, "Failed to start cluster link manager: {}", ex);
        unregister_notifications();
        _manager.reset(nullptr);

        std::rethrow_exception(ex);
    }
    std::move(manager_start).get();
}

ss::future<> service::maybe_stop_manager() {
    if (!_manager) {
        co_return;
    }
    unregister_notifications();
    auto mgr = std::exchange(_manager, nullptr);
    co_await mgr->stop();
}

rpc::shadow_topic_report_response service::shard_local_topic_report(
  const model::id_t& link_id, const ::model::topic& topic) {
    auto h = _gate.hold();
    if (auto err = check_manager_state(); err != errc::success) {
        return rpc::shadow_topic_report_response{.err_code = err};
    }
    auto& registry = _manager->registry();
    const auto& md = registry->find_link_by_id(link_id);
    if (!md) {
        return ::cluster_link::rpc::shadow_topic_report_response{
          .err_code = errc::link_id_not_found};
    }
    const auto& topics = md->state.mirror_topics;
    if (topics.find(topic) == topics.end()) {
        return ::cluster_link::rpc::shadow_topic_report_response{
          .err_code = errc::topic_not_being_mirrored};
    }
    auto maybe_rev = registry->get_last_update_revision(link_id);
    if (!maybe_rev.has_value()) {
        vlog(
          cllog.warn,
          "Inconsistent state detected, topic {} is mapped to link id {}, but "
          "the link revision does not exist",
          topic,
          link_id);
        return ::cluster_link::rpc::shadow_topic_report_response{
          .err_code = ::cluster_link::errc::link_id_not_found};
    }
    rpc::shadow_topic_report_response result;
    result.err_code = ::cluster_link::errc::success;
    result.link_update_revision = maybe_rev.value();
    auto local_partitions
      = _partition_manager->local().get_topic_partition_table(
        {::model::kafka_namespace, topic});
    for (const auto& [ntp, partition] : local_partitions) {
        if (!partition->is_leader()) {
            continue;
        }
        result.leaders.push_back(
          ::cluster_link::rpc::shadow_topic_partition_leader_report{
            .partition = ntp.tp.partition});
    }
    return result;
}

ss::future<rpc::shadow_topic_report_response>
service::node_local_shadow_topic_report(
  rpc::shadow_topic_report_request request) {
    auto h = _gate.hold();
    if (auto err = check_manager_state(); err != errc::success) {
        co_return rpc::shadow_topic_report_response{.err_code = err};
    }
    shard_report_reducer reducer{};
    const auto& link_id = request.link_id;
    const auto& topic = request.topic_name;
    co_await container().map_reduce(
      reducer,
      [](
        service& s,
        const ::cluster_link::model::id_t& link_id,
        const ::model::topic& topic) {
          if (!s.container().local_is_initialized()) {
              return rpc::shadow_topic_report_response{
                .err_code = errc::service_not_ready};
          }
          return s.shard_local_topic_report(link_id, topic);
      },
      link_id,
      topic);
    auto result = std::move(reducer).get();
    if (result) {
        result->node_id = _self;
        co_return std::move(*result);
    }
    vlog(
      cllog.error,
      "No result from shard report reducer for topic: {}, this should never "
      "happen, returning {}",
      topic,
      errc::link_id_not_found);
    // This is effectively unreachable because the reducer always produces a
    // result aggregated from all shards. Here we return a blanket
    // link_id_not_found
    co_return ::cluster_link::rpc::shadow_topic_report_response{
      .err_code = errc::link_id_not_found};
}

ss::future<::cluster_link::rpc::shadow_topic_report_response>
service::shadow_topic_report(
  ::model::node_id node_id, rpc::shadow_topic_report_request request) {
    if (auto err = check_manager_state(); err != errc::success) {
        co_return rpc::shadow_topic_report_response{.err_code = err};
    }
    using resp_t = ::cluster_link::rpc::shadow_topic_report_response;
    if (node_id == _self) {
        co_return co_await node_local_shadow_topic_report(std::move(request));
    }
    static constexpr auto rpc_timeout = 5s;
    co_return co_await _connections->local()
      .with_node_client<rpc::shadow_linking_rpc_client_protocol>(
        _self,
        ss::this_shard_id(),
        node_id,
        ::model::timeout_clock::now() + rpc_timeout,
        [request = std::move(request)](
          rpc::shadow_linking_rpc_client_protocol client) mutable {
            return client
              .shadow_topic_report(
                std::move(request), ::rpc::client_opts(rpc_timeout))
              .then(&::rpc::get_ctx_data<resp_t>);
        })
      .then(
        [](result<::cluster_link::rpc::shadow_topic_report_response> result) {
            if (result.has_error()) {
                vlog(
                  cllog.warn,
                  "Error getting shadow topic report from remote node: {}",
                  result.error());
                return ss::make_ready_future<resp_t>(
                  resp_t{.err_code = ::cluster_link::errc::rpc_error});
            }
            return ss::make_ready_future<resp_t>(std::move(result.value()));
        });
}

ss::future<model::report_result_t>
service::shadow_topic_report(model::id_t link_id, const ::model::topic& topic) {
    auto h = _gate.hold();
    // farms out requests to all nodes with replicas of the topic
    // and then aggregates the results
    // generate a list of brokers with replicas of the topic
    if (auto err = check_manager_state(); err != errc::success) {
        co_return std::unexpected<errc>(err);
    }
    absl::flat_hash_set<::model::node_id> topic_nodes;
    const auto& md_cache = _metadata_cache->local();
    const auto& maybe_tp_md = md_cache.get_topic_metadata_ref(
      ::model::topic_namespace_view{::model::kafka_namespace, topic});
    if (!maybe_tp_md) {
        co_return std::unexpected<errc>(errc::topic_does_not_exist);
    }
    const auto& tp_md = maybe_tp_md.value().get();
    // no scheduling points while looping through partitions
    auto num_partitions = tp_md.get_configuration().partition_count;
    const auto& assignments = tp_md.get_assignments();
    for (const auto& [_, p_assignment] : assignments) {
        for (const auto& r : p_assignment.replicas) {
            topic_nodes.insert(r.node_id);
        }
    }
    if (topic_nodes.empty()) {
        co_return std::unexpected<errc>(errc::topic_metadata_stale);
    }
    ::cluster_link::model::aggregated_shadow_topic_report result;
    result.total_partitions = num_partitions;
    result.brokers.reserve(topic_nodes.size());
    try {
        co_await ss::max_concurrent_for_each(
          topic_nodes,
          32,
          [this, link_id, &topic, &result](::model::node_id node_id) {
              ::cluster_link::rpc::shadow_topic_report_request request;
              request.link_id = link_id;
              request.topic_name = topic;
              return shadow_topic_report(node_id, std::move(request))
                .then([node_id, &result](
                        ::cluster_link::rpc::shadow_topic_report_response r) {
                    if (r.err_code != ::cluster_link::errc::success) {
                        vlog(
                          cllog.warn,
                          "Error getting shadow topic report from node {}: {}",
                          node_id,
                          r.err_code);
                        return ss::now();
                    }
                    ::cluster_link::model::aggregated_shadow_topic_report::
                      broker_report broker_report;
                    broker_report.broker = node_id;
                    broker_report.link_update_revision = r.link_update_revision;
                    for (auto& leader : r.leaders) {
                        broker_report.leaders.push_back(
                          {.partition = leader.partition});
                    }
                    result.brokers.push_back(std::move(broker_report));
                    return ss::now();
                });
          });
    } catch (...) {
        vlog(
          cllog.warn,
          "Exception during shadow topic reporting {}",
          std::current_exception());
        co_return std::unexpected<errc>(errc::rpc_error);
    }
    co_return result;
}

ss::future<rpc::shadow_link_status_report_response>
service::node_local_shadow_link_report(
  rpc::shadow_link_status_report_request req) {
    shard_link_report_reducer reducer{};
    const auto& link_id = req.link_id;

    co_await container().map_reduce(
      reducer,
      [](service& s, const model::id_t& id) {
          if (!s.container().local_is_initialized()) {
              return rpc::shadow_link_status_report_response{
                .err_code = errc::service_not_ready};
          }
          return s.shard_local_shadow_link_report(id);
      },
      link_id);
    auto result = std::move(reducer).get();
    if (result) {
        vlog(cllog.trace, "shadow link report for node {}: {}", _self, *result);
        co_return std::move(*result);
    }
    vlog(
      cllog.error,
      "No result from shard link report reducer for link {}",
      link_id);

    co_return rpc::shadow_link_status_report_response{
      .err_code = errc::link_id_not_found, .link_id = link_id};
}

rpc::shadow_link_status_report_response
service::shard_local_shadow_link_report(model::id_t id) {
    if (auto err = check_manager_state(); err != errc::success) {
        return rpc::shadow_link_status_report_response{.err_code = err};
    }
    auto on_controller_leader = [this]() -> bool {
        if (ss::this_shard_id() != cluster::controller_stm_shard) {
            return false;
        }
        return _self
               == _partition_leaders_table->local().get_leader(
                 ::model::controller_ntp);
    }();
    rpc::shadow_link_status_report_response result;
    result.link_id = id;

    auto res = _manager->get_partition_offsets_report_for_link(id);
    if (!res.has_value()) {
        vlog(
          cllog.warn,
          "Failed to get shard local shadow link report for link {}: {} ({})",
          id,
          res.assume_error().code(),
          res.assume_error().message());
        result.err_code = res.assume_error().code();
        return result;
    }
    auto value = std::move(res.assume_value());
    result.err_code = errc::success;

    for (const auto& [topic, offsets] : value) {
        auto& report = result.topic_responses[topic.tp.topic]
                         .partition_reports[topic.tp.partition];
        report.partition = topic.tp.partition;
        report.shadow_partition_high_watermark = offsets.shadow_hwm;
        // If last update time is default time_point, then we have not received
        // a new offset report from the source cluster for this partition
        if (offsets.update_time != ss::lowres_clock::time_point{}) {
            report.source_partition_start_offset = offsets.source_start_offset;
            report.source_partition_high_watermark = offsets.source_hwm;
            report.source_partition_last_stable_offset = offsets.source_lso;
            report.last_update_time
              = std::chrono::duration_cast<std::chrono::milliseconds>(
                offsets.update_time.time_since_epoch());
        }
    }

    auto task_report_res = _manager->get_task_status_report(id);
    if (!task_report_res.has_value()) {
        vlog(
          cllog.warn,
          "Failed to get shard local shadow link task report for link {}: {} "
          "({})",
          id,
          task_report_res.assume_error().code(),
          task_report_res.assume_error().message());
        result.err_code = task_report_res.assume_error().code();
        return result;
    }

    auto task_report = std::move(task_report_res).assume_value();
    for (auto& [name, report] : task_report.task_status_reports) {
        if (
          !on_controller_leader && report.is_controller_locked_task
          && report.task_state == model::task_state::stopped) {
            // skip reporting stopped controller locked tasks on non-leader
            // nodes
            continue;
        }
        result.task_status_reports[name].emplace_back(std::move(report));
    }

    vlog(
      cllog.trace,
      "shadow link report for shard {}/{}: {}",
      _self,
      ss::this_shard_id(),
      result);

    return result;
}

ss::future<model::status_report_ret_t>
service::shadow_link_report(model::name_t name) {
    vlog(cllog.trace, "Generating shadow link report for link {}", name);
    auto link_id = _plf->local().find_link_id_by_name(name);
    if (!link_id.has_value()) {
        co_return std::unexpected<errc>(errc::link_id_not_found);
    }
    auto& members_table = _controller->get_members_table();
    const auto& node_ids = members_table.local().node_ids();
    vlog(cllog.trace, "Issuing rpcs to nodes {}", node_ids);
    model::shadow_link_status_report results;
    results.link_id = link_id.value();
    try {
        co_await ss::max_concurrent_for_each(
          node_ids,
          32,
          [this, &results, link_id = link_id.value()](::model::node_id node) {
              rpc::shadow_link_status_report_request request{
                .link_id = link_id};
              return shadow_link_report(node, std::move(request))
                .then([&results](rpc::shadow_link_status_report_response resp) {
                    for (const auto& [topic, topic_response] :
                         resp.topic_responses) {
                        auto& existing = results.topic_responses[topic];
                        for (const auto& [pid, report] :
                             topic_response.partition_reports) {
                            existing.partition_reports.emplace(pid, report);
                        }
                    }
                    for (auto& [task_name, reports] :
                         resp.task_status_reports) {
                        auto& existing_reports
                          = results.task_status_reports[task_name];
                        for (auto& r : reports) {
                            existing_reports.push_back(std::move(r));
                        }
                    }
                });
          });
    } catch (const std::exception& e) {
        vlog(cllog.warn, "Exception during shadow link reporting: {}", e);
        co_return std::unexpected<errc>(errc::rpc_error);
    }

    co_return results;
}

ss::future<rpc::shadow_link_status_report_response> service::shadow_link_report(
  ::model::node_id node, rpc::shadow_link_status_report_request req) {
    using resp_t = rpc::shadow_link_status_report_response;
    if (node == _self) {
        co_return co_await node_local_shadow_link_report(std::move(req));
    }

    static constexpr auto rpc_timeout = 5s;
    vlog(cllog.trace, "Issuing rpc to node {}", node);
    auto resp = co_await _connections->local()
                  .with_node_client<rpc::shadow_linking_rpc_client_protocol>(
                    _self,
                    ss::this_shard_id(),
                    node,
                    ::model::timeout_clock::now() + rpc_timeout,
                    [request = std::move(req)](
                      rpc::shadow_linking_rpc_client_protocol client) mutable {
                        return client
                          .shadow_link_report(
                            std::move(request), ::rpc::client_opts(rpc_timeout))
                          .then(&::rpc::get_ctx_data<resp_t>);
                    });
    if (resp.has_error()) {
        vlog(
          cllog.warn,
          "Error getting shadow link report for node {}: {}",
          node,
          resp.error());
        co_return resp_t{.err_code = errc::rpc_error};
    }
    co_return std::move(resp.value());
}

} // namespace cluster_link
