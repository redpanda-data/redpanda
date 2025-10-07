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
#include "cluster/health_monitor_frontend.h"
#include "cluster/members_table.h"
#include "cluster/partition_manager.h"
#include "cluster_link/group_mirroring_task.h"
#include "cluster_link/link.h"
#include "cluster_link/logger.h"
#include "cluster_link/manager.h"
#include "cluster_link/model/types.h"
#include "cluster_link/replication/deps_impl.h"
#include "cluster_link/replication/mux_remote_consumer.h"
#include "cluster_link/security_migrator.h"
#include "cluster_link/shadow_linking_rpc_service.h"
#include "cluster_link/source_topic_syncer.h"
#include "kafka/client/direct_consumer/direct_consumer.h"
#include "kafka/server/group_router.h"
#include "kafka/server/snc_quota_manager.h"
#include "ssx/future-util.h"

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
using data_src_factory = replication::remote_data_source_factory;
using data_sink_factory = replication::local_partition_data_sink_factory;

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

    std::optional<std::reference_wrapper<const model::metadata>>
    find_link_by_id(model::id_t id) const override {
        return _plf->find_link_by_id(id);
    }

    std::optional<std::reference_wrapper<const model::metadata>>
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
      model::id_t id, ::model::timeout_clock::time_point timeout) override {
        return _plf->failover_link_topics(id, timeout);
    }

private:
    frontend* _plf;
    service* _svc;
};

class default_link_factory : public link_factory {
public:
    explicit default_link_factory(
      ss::sharded<cluster::partition_manager>* partition_manager,
      ss::sharded<kafka::snc_quota_manager>* snc_quota_mgr)
      : link_factory()
      , _partition_manager(partition_manager)
      , _snc_quota_mgr(snc_quota_mgr) {}

    static constexpr auto link_reconciler_period = 5min;
    std::unique_ptr<link> create_link(
      ::model::node_id self,
      model::id_t link_id,
      manager* manager,
      model::metadata config,
      std::unique_ptr<kafka::client::cluster> cluster_connection) override {
        auto client_id = config.connection.client_id;
        return std::make_unique<link>(
          self,
          link_id,
          manager,
          link_reconciler_period,
          std::move(config),
          std::move(cluster_connection),
          std::make_unique<data_src_factory>(make_remote_consumer(
            std::move(client_id),
            *cluster_connection,
            _snc_quota_mgr->local(),
            config.connection)),
          std::make_unique<data_sink_factory>(*_partition_manager));
    }

private:
    std::unique_ptr<replication::mux_remote_consumer> make_remote_consumer(
      ss::sstring client_id,
      kafka::client::cluster& cluster,
      kafka::snc_quota_manager& snc_quota_mgr,
      const model::connection_config& conn_cfg) {
        // todo0: make more these configurable at connection level
        // todo1: make these dynamic
        kafka::client::direct_consumer::configuration cfg;
        cfg.min_bytes = conn_cfg.get_fetch_min_bytes();
        cfg.max_fetch_size = conn_cfg.get_fetch_max_bytes();
        cfg.partition_max_bytes = 512_KiB;
        cfg.max_wait_time = 200ms;
        cfg.isolation_level = ::model::isolation_level::read_committed;
        cfg.max_buffered_bytes = 5_MiB;
        cfg.max_buffered_elements = std::numeric_limits<size_t>::max();
        cfg.with_sessions = kafka::client::fetch_sessions_enabled::yes;
        static constexpr size_t partition_max_buffered_bytes = 5_MiB;
        static constexpr auto fetch_max_wait = 100ms;
        auto direct_consumer = std::make_unique<kafka::client::direct_consumer>(
          cluster, cfg);

        return std::make_unique<replication::mux_remote_consumer>(
          std::move(client_id),
          std::move(direct_consumer),
          snc_quota_mgr,
          partition_max_buffered_bytes,
          fetch_max_wait);
    }
    ss::sharded<cluster::partition_manager>* _partition_manager;
    ss::sharded<kafka::snc_quota_manager>* _snc_quota_mgr;
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

ss::future<cl_result<model::metadata>>
service::upsert_cluster_link(model::metadata md) {
    auto h = _gate.hold();
    return with_manager([md = std::move(md)](manager* mgr) mutable {
        return mgr->upsert_cluster_link(std::move(md));
    });
}

cl_result<model::metadata>
service::get_cluster_link(const model::name_t& name) {
    return with_manager(
      [&name](manager* mgr) { return mgr->get_cluster_link(name); });
}

cl_result<chunked_vector<model::metadata>> service::list_cluster_links() {
    return with_manager([](manager* mgr) { return mgr->list_cluster_links(); });
}

ss::future<cl_result<model::metadata>> service::update_cluster_link(
  model::name_t name, model::update_cluster_link_configuration_cmd cmd) {
    auto h = _gate.hold();
    return with_manager(
      [name = std::move(name), cmd = std::move(cmd)](manager* mgr) mutable {
          return mgr->update_cluster_link(std::move(name), std::move(cmd));
      });
}

ss::future<cl_result<model::metadata>> service::update_mirror_topic_status(
  model::name_t link_name,
  ::model::topic topic,
  model::mirror_topic_status status) {
    auto h = _gate.hold();
    return with_manager([link_name = std::move(link_name),
                         topic = std::move(topic),
                         status](manager* mgr) mutable {
        return mgr->update_mirror_topic_status(
          std::move(link_name), std::move(topic), status);
    });
}

ss::future<cl_result<model::metadata>>
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
        _partition_manager, _snc_quota_mgr),
      std::make_unique<cluster_factory>(),
      std::make_unique<kafka_consumer_groups_router>(_group_router),
      std::make_unique<health_monitor_based_partition_metadata_provider>(
        _hm_frontend),
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
    if (!md.has_value()) {
        return ::cluster_link::rpc::shadow_topic_report_response{
          .err_code = errc::link_id_not_found};
    }
    const auto& topics = md->get().state.mirror_topics;
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
        if (offsets.update_time == ss::lowres_clock::time_point{}) {
            // no offsets have been set for this partition
            continue;
        }
        result.topic_responses[topic.tp.topic]
          .partition_reports[topic.tp.partition]
          = rpc::shadow_topic_partition_leader_report{
            .partition = topic.tp.partition,
            .source_partition_start_offset = offsets.source_start_offset,
            .source_partition_high_watermark = offsets.source_hwm,
            .source_partition_last_stable_offset = offsets.source_lso,
            .last_update_time
            = std::chrono::duration_cast<std::chrono::milliseconds>(
              offsets.update_time.time_since_epoch()),
            .shadow_partition_high_watermark = offsets.shadow_hwm,
          };
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
