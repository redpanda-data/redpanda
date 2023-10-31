/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "transform/api.h"

#include "cluster/errc.h"
#include "cluster/partition_manager.h"
#include "cluster/plugin_frontend.h"
#include "cluster/types.h"
#include "features/feature_table.h"
#include "kafka/server/partition_proxy.h"
#include "kafka/server/replicated_partition.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "model/transform.h"
#include "resource_mgmt/io_priority.h"
#include "transform/io.h"
#include "transform/logger.h"
#include "transform/rpc/client.h"
#include "transform/rpc/deps.h"
#include "transform/transform_manager.h"
#include "transform/transform_processor.h"
#include "wasm/api.h"
#include "wasm/cache.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/optimized_optional.hh>

#include <memory>

namespace transform {

namespace {
constexpr auto wasm_binary_timeout = std::chrono::seconds(3);
constexpr auto metadata_timeout = std::chrono::seconds(1);

class rpc_client_sink final : public sink {
public:
    rpc_client_sink(model::ntp ntp, rpc::client* c)
      : _ntp(std::move(ntp))
      , _client(c) {}

    ss::future<> write(ss::chunked_fifo<model::record_batch> batches) override {
        auto ec = co_await _client->produce(_ntp.tp, std::move(batches));
        if (ec != cluster::errc::success) {
            throw std::runtime_error(ss::format(
              "failure to produce transform data: {}",
              cluster::error_category().message(int(ec))));
        }
    }

private:
    model::ntp _ntp;
    rpc::client* _client;
};

class partition_source final : public source {
public:
    explicit partition_source(kafka::partition_proxy p)
      : _partition(std::move(p)) {}

    kafka::offset latest_offset() final {
        auto result = _partition.last_stable_offset();
        if (result.has_error()) {
            throw std::runtime_error(
              kafka::make_error_code(result.error()).message());
        }
        return model::offset_cast(result.value());
    }

    ss::future<model::record_batch_reader>
    read_batch(kafka::offset offset, ss::abort_source* as) final {
        auto translater = co_await _partition.make_reader(
          storage::log_reader_config(
            /*start_offset=*/kafka::offset_cast(offset),
            /*max_offset=*/model::offset::max(),
            /*prio=*/wasm_read_priority(),
            /*as=*/*as));
        co_return std::move(translater).reader;
    }

private:
    kafka::partition_proxy _partition;
};

class registry_adapter : public registry {
public:
    registry_adapter(
      cluster::plugin_frontend* pf, cluster::partition_manager* m)
      : _pf(pf)
      , _manager(m) {}

    absl::flat_hash_set<model::partition_id>
    get_leader_partitions(model::topic_namespace_view tp_ns) const override {
        absl::flat_hash_set<model::partition_id> p;
        for (const auto& entry : _manager->get_topic_partition_table(tp_ns)) {
            if (entry.second->is_elected_leader()) {
                p.emplace(entry.first.tp.partition);
            }
        }
        return p;
    }

    absl::flat_hash_set<model::transform_id>
    lookup_by_input_topic(model::topic_namespace_view tp_ns) const override {
        auto entries = _pf->lookup_transforms_by_input_topic(tp_ns);
        absl::flat_hash_set<model::transform_id> result;
        for (const auto& [id, _] : entries) {
            result.emplace(id);
        }
        return result;
    }

    std::optional<model::transform_metadata>
    lookup_by_id(model::transform_id id) const override {
        return _pf->lookup_transform(id);
    }

private:
    cluster::plugin_frontend* _pf;
    cluster::partition_manager* _manager;
};

class offset_tracker_impl : public offset_tracker {
public:
    offset_tracker_impl(
      model::transform_id tid, model::partition_id pid, rpc::client* client)
      : _tid(tid)
      , _pid(pid)
      , _client(client) {}

    ss::future<std::optional<kafka::offset>> load_committed_offset() override {
        auto result = co_await _client->offset_fetch(
          {.id = _tid, .partition = _pid});
        if (result.has_error()) {
            cluster::errc ec = result.error();
            throw std::runtime_error(ss::format(
              "error committing offset: {}",
              cluster::error_category().message(int(ec))));
        }
        auto value = result.value();
        if (!value) {
            co_return std::nullopt;
        }
        co_return value->offset;
    }

    ss::future<> commit_offset(kafka::offset offset) override {
        // TODO(rockwood): We should be batching commits so commiting scales
        // with the number of cores, not the number of partitions.
        cluster::errc ec = co_await _client->offset_commit(
          {.id = _tid, .partition = _pid}, {.offset = offset});
        if (ec != cluster::errc::success) {
            throw std::runtime_error(ss::format(
              "error committing offset: {}",
              cluster::error_category().message(int(ec))));
        }
    }

private:
    model::transform_id _tid;
    model::partition_id _pid;
    rpc::client* _client;
};

using wasm_engine_factory = ss::noncopyable_function<
  ss::future<ss::optimized_optional<ss::shared_ptr<wasm::engine>>>(
    model::transform_metadata)>;

class proc_factory : public processor_factory {
public:
    proc_factory(
      wasm_engine_factory factory,
      cluster::partition_manager* partition_manager,
      rpc::client* client)
      : _wasm_engine_factory(std::move(factory))
      , _partition_manager(partition_manager)
      , _client(client) {}

    ss::future<std::unique_ptr<processor>> create_processor(
      model::transform_id id,
      model::ntp ntp,
      model::transform_metadata meta,
      processor::error_callback cb,
      probe* p) final {
        auto engine = co_await _wasm_engine_factory(meta);
        if (!engine) {
            throw std::runtime_error("unable to create wasm engine");
        }
        auto partition = kafka::make_partition_proxy(ntp, *_partition_manager);
        if (!partition) {
            throw std::runtime_error("unable to create transform source");
        }
        auto src = std::make_unique<partition_source>(*std::move(partition));
        vassert(
          meta.output_topics.size() == 1,
          "only a single output topic is supported");
        const auto& output_topic = meta.output_topics[0];
        std::vector<std::unique_ptr<sink>> sinks;
        auto sink = std::make_unique<rpc_client_sink>(
          model::ntp(output_topic.ns, output_topic.tp, ntp.tp.partition),
          _client);
        sinks.push_back(std::move(sink));

        auto offset_tracker = std::make_unique<offset_tracker_impl>(
          id, ntp.tp.partition, _client);

        co_return std::make_unique<processor>(
          id,
          ntp,
          meta,
          *std::move(engine),
          std::move(cb),
          std::move(src),
          std::move(sinks),
          std::move(offset_tracker),
          p);
    }

private:
    mutex _mu;
    wasm_engine_factory _wasm_engine_factory;
    cluster::partition_manager* _partition_manager;
    rpc::client* _client;
    absl::flat_hash_map<model::offset, std::unique_ptr<wasm::engine>> _cache;
};

} // namespace

class wrapped_service_reporter : public rpc::reporter {
public:
    explicit wrapped_service_reporter(ss::sharded<service>* service)
      : _service(service) {}

    ss::future<model::cluster_transform_report> compute_report() override {
        // It's the RPC server is started before the transform subsystem boots
        // up, so we need to check for initialization here.
        // TODO(rockwood): Fix the bootup sequence so this doesn't happen.
        if (!_service->local_is_initialized()) {
            return ss::make_exception_future<model::cluster_transform_report>(
              std::make_exception_ptr(
                std::runtime_error("transforms are disabled")));
        }
        return _service->local().compute_node_local_report();
    };

private:
    ss::sharded<service>* _service;
};

service::service(
  wasm::caching_runtime* runtime,
  model::node_id self,
  ss::sharded<cluster::plugin_frontend>* plugin_frontend,
  ss::sharded<features::feature_table>* feature_table,
  ss::sharded<raft::group_manager>* group_manager,
  ss::sharded<cluster::partition_manager>* partition_manager,
  ss::sharded<rpc::client>* rpc_client)
  : _runtime(runtime)
  , _self(self)
  , _plugin_frontend(plugin_frontend)
  , _feature_table(feature_table)
  , _group_manager(group_manager)
  , _partition_manager(partition_manager)
  , _rpc_client(rpc_client) {}

service::~service() = default;

ss::future<> service::start() {
    _manager = std::make_unique<manager<ss::lowres_clock>>(
      _self,
      std::make_unique<registry_adapter>(
        &_plugin_frontend->local(), &_partition_manager->local()),
      std::make_unique<proc_factory>(
        [this](model::transform_metadata meta) {
            return create_engine(std::move(meta));
        },
        &_partition_manager->local(),
        &_rpc_client->local()));
    co_await _manager->start();
    register_notifications();
}

void service::register_notifications() {
    auto plugin_notif_id = _plugin_frontend->local().register_for_updates(
      [this](model::transform_id id) { _manager->on_plugin_change(id); });
    _notification_cleanups.emplace_back([this, plugin_notif_id] {
        _plugin_frontend->local().unregister_for_updates(plugin_notif_id);
    });
    auto leadership_notif_id
      = _group_manager->local().register_leadership_notification(
        [this](
          raft::group_id group_id,
          model::term_id,
          std::optional<model::node_id> leader) {
            auto partition = _partition_manager->local().partition_for(
              group_id);
            if (!partition) {
                vlog(
                  tlog.debug,
                  "got leadership notification for unknown partition: {}",
                  group_id);
                return;
            }
            bool node_is_leader = leader.has_value() && leader == _self;
            if (!node_is_leader) {
                _manager->on_leadership_change(
                  partition->ntp(), ntp_leader::no);
                return;
            }
            if (partition->ntp().ns != model::kafka_namespace) {
                return;
            }
            ntp_leader is_leader = partition && partition->is_elected_leader()
                                     ? ntp_leader::yes
                                     : ntp_leader::no;
            _manager->on_leadership_change(partition->ntp(), is_leader);
        });
    _notification_cleanups.emplace_back([this, leadership_notif_id] {
        _group_manager->local().unregister_leadership_notification(
          leadership_notif_id);
    });
    auto unmanage_notification_id
      = _partition_manager->local().register_unmanage_notification(
        model::kafka_namespace, [this](model::topic_partition_view tp) {
            _manager->on_leadership_change(
              model::ntp(model::kafka_namespace, tp.topic, tp.partition),
              ntp_leader::no);
        });
    _notification_cleanups.emplace_back([this, unmanage_notification_id] {
        _partition_manager->local().unregister_unmanage_notification(
          unmanage_notification_id);
    });
    // NOTE: this will also trigger notifications for existing partitions, which
    // will effectively bootstrap the transform manager.
    auto manage_notification_id
      = _partition_manager->local().register_manage_notification(
        model::kafka_namespace,
        [this](const ss::lw_shared_ptr<cluster::partition>& p) {
            ntp_leader is_leader = p->is_elected_leader() ? ntp_leader::yes
                                                          : ntp_leader::no;
            _manager->on_leadership_change(p->ntp(), is_leader);
        });
    _notification_cleanups.emplace_back([this, manage_notification_id] {
        _partition_manager->local().unregister_manage_notification(
          manage_notification_id);
    });
}

void service::unregister_notifications() { _notification_cleanups.clear(); }

ss::future<> service::stop() {
    unregister_notifications();
    co_await _gate.close();
    // It's possible to call stop before start, so make sure we created the
    // manager.
    if (_manager) {
        co_await _manager->stop();
    }
}

ss::future<cluster::errc>
service::delete_transform(model::transform_name name) {
    if (!_feature_table->local().is_active(
          features::feature::wasm_transforms)) {
        co_return cluster::errc::feature_disabled;
    }
    auto _ = _gate.hold();

    vlog(tlog.info, "deleting transform {}", name);
    auto result = co_await _plugin_frontend->local().remove_transform(
      name, model::timeout_clock::now() + metadata_timeout);

    // Make deletes itempotent by translating does not exist into success
    if (result.ec == cluster::errc::transform_does_not_exist) {
        co_return cluster::errc::success;
    }
    if (result.ec != cluster::errc::success) {
        co_return result.ec;
    }
    co_await cleanup_wasm_binary(result.uuid);
    co_return cluster::errc::success;
}

ss::future<cluster::errc>
service::deploy_transform(model::transform_metadata meta, iobuf binary) {
    if (!_feature_table->local().is_active(
          features::feature::wasm_transforms)) {
        co_return cluster::errc::feature_disabled;
    }
    auto _ = _gate.hold();

    vlog(
      tlog.info,
      "deploying wasm binary (size={}) for transform {}",
      binary.size_bytes(),
      meta.name);
    // TODO(rockwood): Validate that the wasm adheres to our ABI
    auto result = co_await _rpc_client->local().store_wasm_binary(
      std::move(binary), wasm_binary_timeout);
    if (result.has_error()) {
        vlog(
          tlog.warn, "storing wasm binary for transform {} failed", meta.name);
        co_return result.error();
    }
    auto [key, offset] = result.value();
    meta.uuid = key;
    meta.source_ptr = offset;
    vlog(
      tlog.debug,
      "stored wasm binary for transform {} at offset {}",
      meta.name,
      offset);
    cluster::errc ec = co_await _plugin_frontend->local().upsert_transform(
      meta, model::timeout_clock::now() + metadata_timeout);
    vlog(
      tlog.debug,
      "deploying transform {} result: {}",
      meta.name,
      cluster::error_category().message(int(ec)));
    if (ec != cluster::errc::success) {
        co_await cleanup_wasm_binary(key);
    }
    co_return ec;
}

ss::future<model::cluster_transform_report> service::list_transforms() {
    if (!_feature_table->local().is_active(
          features::feature::wasm_transforms)) {
        co_return model::cluster_transform_report{};
    }
    auto _ = _gate.hold();
    co_return co_await _rpc_client->local().generate_report();
}

ss::future<> service::cleanup_wasm_binary(uuid_t key) {
    // TODO(rockwood): This is a best effort cleanup, we should also have
    // some sort of GC process as well.
    auto result = co_await ss::coroutine::as_future<cluster::errc>(
      _rpc_client->local().delete_wasm_binary(key, wasm_binary_timeout));
    if (result.failed()) {
        vlog(
          tlog.debug,
          "cleaning up wasm binary failed: {}",
          result.get_exception());
        co_return;
    }
    auto ec = result.get();
    if (ec == cluster::errc::success) {
        co_return;
    }
    vlog(
      tlog.debug,
      "cleaning up wasm binary failed: {}",
      cluster::error_category().message(int(ec)));
}

ss::future<ss::optimized_optional<ss::shared_ptr<wasm::engine>>>
service::create_engine(model::transform_metadata meta) {
    auto factory = co_await get_factory(std::move(meta));
    if (!factory) {
        co_return ss::shared_ptr<wasm::engine>(nullptr);
    }
    co_return co_await (*factory)->make_engine();
}

ss::future<
  ss::optimized_optional<ss::foreign_ptr<ss::shared_ptr<wasm::factory>>>>
service::get_factory(model::transform_metadata meta) {
    constexpr ss::shard_id creation_shard = 0;
    // TODO(rockwood): Consider caching factories core local (or moving that
    // optimization into the caching runtime).
    if (ss::this_shard_id() != creation_shard) {
        co_return co_await container().invoke_on(
          creation_shard,
          [](service& s, model::transform_metadata meta) {
              return s.get_factory(std::move(meta));
          },
          std::move(meta));
    }
    auto cached = _runtime->get_cached_factory(meta);
    if (cached) {
        co_return ss::make_foreign(*cached);
    }
    auto result = co_await _rpc_client->local().load_wasm_binary(
      meta.source_ptr, wasm_binary_timeout);
    if (result.has_error()) {
        vlog(
          tlog.warn,
          "unable to load wasm binary for transform {}: {}",
          meta.name,
          cluster::error_category().message(int(result.error())));
        co_return ss::foreign_ptr<ss::shared_ptr<wasm::factory>>(nullptr);
    }
    auto factory = co_await _runtime->make_factory(
      std::move(meta), std::move(result).value(), &tlog);
    co_return ss::make_foreign(factory);
}

ss::future<model::cluster_transform_report>
service::compute_node_local_report() {
    co_return co_await container().map_reduce0(
      [](service& s) { return s._manager->compute_report(); },
      model::cluster_transform_report{},
      [](
        model::cluster_transform_report agg,
        const model::cluster_transform_report& local) {
          agg.merge(local);
          return agg;
      });
}

std::unique_ptr<rpc::reporter>
service::create_reporter(ss::sharded<service>* s) {
    return std::make_unique<wrapped_service_reporter>(s);
}

} // namespace transform
