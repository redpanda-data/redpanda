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
#include "cluster/health_monitor_frontend.h"
#include "cluster/partition_manager.h"
#include "cluster_link/group_mirroring_task.h"
#include "cluster_link/link.h"
#include "cluster_link/logger.h"
#include "cluster_link/manager.h"
#include "cluster_link/model/types.h"
#include "cluster_link/replication/deps_impl.h"
#include "cluster_link/replication/mux_remote_consumer.h"
#include "cluster_link/source_topic_syncer.h"
#include "kafka/client/direct_consumer/direct_consumer.h"
#include "kafka/server/group_router.h"

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
    explicit link_registry_adapter(frontend* plf)
      : _plf(plf) {}

    ss::future<::cluster::cluster_link::errc> upsert_link(
      model::metadata md, ::model::timeout_clock::time_point timeout) override {
        return _plf->upsert_cluster_link(std::move(md), timeout);
    }

    std::optional<std::reference_wrapper<const model::metadata>>
    find_link_by_id(model::id_t id) const override {
        return _plf->find_link_by_id(id);
    }

    std::optional<std::reference_wrapper<const model::metadata>>
    find_link_by_name(const model::name_t& name) const override {
        return _plf->find_link_by_name(name);
    }

    chunked_vector<model::id_t> get_all_link_ids() const override {
        return _plf->get_all_link_ids();
    }

    ss::future<::cluster::cluster_link::errc> add_mirror_topic(
      model::id_t id,
      model::add_mirror_topic_cmd cmd,
      ::model::timeout_clock::time_point timeout) override {
        return _plf->add_mirror_topic(id, std::move(cmd), timeout);
    }

    ss::future<::cluster::cluster_link::errc> update_mirror_topic_state(
      model::id_t id,
      model::update_mirror_topic_state_cmd cmd,
      ::model::timeout_clock::time_point timeout) override {
        return _plf->update_mirror_topic_state(id, std::move(cmd), timeout);
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

private:
    frontend* _plf;
};

class default_link_factory : public link_factory {
public:
    explicit default_link_factory(
      ss::sharded<cluster::partition_manager>* partition_manager)
      : link_factory()
      , _partition_manager(partition_manager) {}

    static constexpr auto link_reconciler_period = 5min;
    std::unique_ptr<link> create_link(
      ::model::node_id self,
      model::id_t link_id,
      manager* manager,
      model::metadata config,
      std::unique_ptr<kafka::client::cluster> cluster_connection) override {
        return std::make_unique<link>(
          self,
          link_id,
          manager,
          link_reconciler_period,
          std::move(config),
          std::move(cluster_connection),
          std::make_unique<data_src_factory>(
            make_remote_consumer(*cluster_connection, config.connection)),
          std::make_unique<data_sink_factory>(*_partition_manager));
    }

private:
    std::unique_ptr<replication::mux_remote_consumer> make_remote_consumer(
      kafka::client::cluster& cluster,
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
          std::move(direct_consumer),
          partition_max_buffered_bytes,
          fetch_max_wait);
    }
    ss::sharded<cluster::partition_manager>* _partition_manager;
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
  ss::sharded<frontend>* plf,
  std::unique_ptr<cluster::partition_change_notifier> notifications,
  ss::sharded<cluster::partition_manager>* partition_manager,
  ss::sharded<cluster::partition_leaders_table>* partition_leaders_table,
  ss::sharded<cluster::shard_table>* shard_table,
  ss::sharded<cluster::metadata_cache>* metadata_cache,
  cluster::controller* controller,
  ss::sharded<kafka::group_router>* group_router,
  ss::sharded<cluster::health_monitor_frontend>* hm_frontend,
  ss::smp_service_group smp_group)
  : _self(self)
  , _plf(plf)
  , _notifications(std::move(notifications))
  , _partition_manager(partition_manager)
  , _partition_leaders_table(partition_leaders_table)
  , _shard_table(shard_table)
  , _metadata_cache(metadata_cache)
  , _controller(controller)
  , _group_router(group_router)
  , _hm_frontend(hm_frontend)
  , _smp_group(smp_group) {}

service::~service() = default;

ss::future<> service::start() {
    vlog(cllog.info, "Starting cluster link service");
    _manager = std::make_unique<manager>(
      _self,
      partition_leader_cache::make_default(_partition_leaders_table),
      partition_manager::make_default(
        _shard_table, _partition_manager, _smp_group),
      topic_metadata_cache::make_default(_metadata_cache),
      topic_creator::make_default(_controller),
      std::make_unique<link_registry_adapter>(&_plf->local()),
      std::make_unique<default_link_factory>(_partition_manager),
      std::make_unique<cluster_factory>(),
      std::make_unique<kafka_consumer_groups_router>(_group_router),
      std::make_unique<health_monitor_based_partition_metadata_provider>(
        _hm_frontend),
      30s, // Temporary until we have a proper configuration for this
      config::shard_local_cfg().default_topic_replication.bind());

    co_await _manager->register_task_factory<source_topic_syncer_factory>();
    co_await _manager->register_task_factory<group_mirroring_task_factory>();

    // Register notifications before the manager starts.  The manager will have
    // a constructed the underlying workqueue to start in a paused state and
    // will pick up the notifications once it has started
    register_notifications();
    co_await _manager->start();
}

ss::future<> service::stop() {
    vlog(cllog.info, "Stopping cluster link service");

    if (_manager) {
        co_await _manager->stop();
    }
}

ss::future<result<model::metadata>>
service::upsert_cluster_link(model::metadata md) {
    return _manager->upsert_cluster_link(std::move(md));
}

result<model::metadata> service::get_cluster_link(const model::name_t& name) {
    return _manager->get_cluster_link(name);
}

result<chunked_vector<model::metadata>> service::list_cluster_links() {
    return _manager->list_cluster_links();
}

void service::register_notifications() {
    auto pl_notif_id = _plf->local().register_for_updates(
      [this](model::id_t id) { _manager->on_link_change(id); });
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
} // namespace cluster_link
