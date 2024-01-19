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
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "kafka/server/partition_proxy.h"
#include "kafka/server/replicated_partition.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "model/transform.h"
#include "resource_mgmt/io_priority.h"
#include "ssx/future-util.h"
#include "transform/commit_batcher.h"
#include "transform/io.h"
#include "transform/logger.h"
#include "transform/rpc/client.h"
#include "transform/rpc/deps.h"
#include "transform/transform_logger.h"
#include "transform/transform_manager.h"
#include "transform/transform_processor.h"
#include "transform/txn_reader.h"
#include "wasm/api.h"
#include "wasm/cache.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/optimized_optional.hh>

namespace transform {

namespace {
constexpr auto wasm_binary_timeout = std::chrono::seconds(3);
constexpr auto metadata_timeout = std::chrono::seconds(1);

class rpc_client_sink final : public sink {
public:
    rpc_client_sink(
      model::topic topic,
      model::partition_id input_partition_id,
      cluster::topic_table* topic_table,
      rpc::client* client)
      : _topic(std::move(topic))
      , _input_partition_id(input_partition_id)
      , _topic_table(topic_table)
      , _client(client) {}

    ss::future<> write(ss::chunked_fifo<model::record_batch> batches) override {
        model::partition_id partition = compute_output_partition();
        auto ec = co_await _client->produce(
          {_topic, partition}, std::move(batches));
        if (ec != cluster::errc::success) {
            throw std::runtime_error(ss::format(
              "failure to produce transform data: {}",
              cluster::error_category().message(int(ec))));
        }
    }

private:
    model::partition_id compute_output_partition() {
        model::topic_namespace_view ns_tp{model::kafka_namespace, _topic};
        const auto& config = _topic_table->get_topic_cfg(ns_tp);
        if (!config) {
            throw std::runtime_error(ss::format(
              "unable to compute output partition for topic: {}", _topic));
        }

        const auto* disabled_set = _topic_table->get_topic_disabled_set(ns_tp);
        if (!(disabled_set && disabled_set->is_fully_disabled())) {
            // Do linear probing to find a non-disabled partition. The
            // expectation is that most of the times we'll need just a few
            // probes.
            for (int32_t i = 0; i < config->partition_count; ++i) {
                model::partition_id candidate(
                  (_input_partition_id + i) % config->partition_count);
                if (!(disabled_set && disabled_set->is_disabled(candidate))) {
                    return candidate;
                }
            }
        }

        throw std::runtime_error(ss::format(
          "unable to compute output partition for topic: {}, all output "
          "partitions disabled",
          _topic));
    }

    model::topic _topic;
    model::partition_id _input_partition_id;
    cluster::topic_table* _topic_table;
    rpc::client* _client;
};

class partition_source final : public source {
public:
    explicit partition_source(kafka::partition_proxy p)
      : _partition(std::move(p)) {}

    ss::future<> start() final {
        _gate = {};
        return ss::now();
    }

    // It is important that all outstanding readers have been deleted before
    // stopping.
    ss::future<> stop() final { return _gate.close(); }

    kafka::offset latest_offset() final {
        auto result = _partition.last_stable_offset();
        if (result.has_error()) {
            throw std::runtime_error(
              kafka::make_error_code(result.error()).message());
        }
        return model::offset_cast(model::prev_offset(result.value()));
    }

    ss::future<model::record_batch_reader>
    read_batch(kafka::offset offset, ss::abort_source* as) final {
        auto _ = _gate.hold();
        // There currently no way to abort the call to get the sync start, so
        // instead we wrap the resulting future in our abort source.
        auto result = co_await ssx::with_timeout_abortable(
          // Ensure we don't delete the partition until this has resolved if we
          // end up timing out.
          _partition.sync_effective_start().finally([holder = std::move(_)] {}),
          model::no_timeout,
          *as);
        if (result.has_error()) {
            throw std::runtime_error(
              kafka::make_error_code(result.error()).message());
        }
        // It's possible to have the local log was truncated due to delete
        // records, retention, etc. In this event, simply resume from the start
        // of the log.
        model::offset start_offset = std::max(
          result.value(), kafka::offset_cast(offset));
        // Clamp reads to only committed transactions.
        auto maybe_lso = _partition.last_stable_offset();
        if (!maybe_lso) {
            throw std::runtime_error(
              kafka::make_error_code(maybe_lso.error()).message());
        }
        // It's possible for LSO to be 0, which in this case the previous offset
        // is model::offset::min(), this is the same as the kafka fetch path.
        model::offset max_offset = model::prev_offset(maybe_lso.value());
        // If the max offset is less than the start, it's always going to be an
        // empty read, short circuit here.
        if (max_offset < start_offset) {
            co_return model::make_memory_record_batch_reader(
              model::record_batch_reader::data_t{});
        }
        // TODO(rockwood): This is currently an arbitrary value, but we should
        // dynamically update this based on how much memory is available in the
        // transform subsystem.
        constexpr static size_t max_bytes = 128_KiB;
        auto translater = co_await _partition.make_reader(
          storage::log_reader_config(
            /*start_offset=*/start_offset,
            /*max_offset=*/max_offset,
            /*min_bytes=*/0,
            /*max_bytes=*/max_bytes,
            /*prio=*/wasm_read_priority(),
            /*type_filter=*/std::nullopt, // Overridden by partition
            /*time=*/std::nullopt,        // Not doing a timequery
            /*as=*/*as));

        // NOTE: It's a very important property that the source always outlives
        // all readers it makes, as this takes a pointer to a member.
        //
        // This is documented as part of the contract for the source interface.
        auto tracker = aborted_transaction_tracker::create_default(
          &_partition, std::move(translater.ot_state));
        co_return model::make_record_batch_reader<read_committed_reader>(
          std::move(tracker), std::move(translater.reader));
    }

private:
    // This gate is only to guard against the case when the abort has fired and
    // there is still a live future that holds a reference to _partition.
    ss::gate _gate;
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
      model::transform_id tid,
      model::partition_id pid,
      rpc::client* client,
      commit_batcher<>* batcher)
      : _key({.id = tid, .partition = pid})
      , _client(client)
      , _batcher(batcher) {}

    ss::future<> start() override { return ss::now(); }

    ss::future<> stop() override {
        _batcher->unload(_key);
        return ss::now();
    }

    ss::future<> wait_for_previous_flushes(ss::abort_source* as) override {
        return _batcher->wait_for_previous_flushes(_key, as);
    }

    ss::future<std::optional<kafka::offset>> load_committed_offset() override {
        auto result = co_await _client->offset_fetch(_key);
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
        return _batcher->commit_offset(_key, {.offset = offset});
    }

private:
    model::transform_offsets_key _key;
    rpc::client* _client;
    commit_batcher<>* _batcher;
};

using wasm_engine_factory = ss::noncopyable_function<
  ss::future<ss::optimized_optional<ss::shared_ptr<wasm::engine>>>(
    model::transform_metadata)>;

class proc_factory : public processor_factory {
public:
    proc_factory(
      wasm_engine_factory factory,
      cluster::topic_table* topic_table,
      cluster::partition_manager* partition_manager,
      rpc::client* client,
      commit_batcher<>* batcher)
      : _wasm_engine_factory(std::move(factory))
      , _partition_manager(partition_manager)
      , _client(client)
      , _batcher(batcher)
      , _topic_table(topic_table) {}

    ss::future<std::unique_ptr<processor>> create_processor(
      model::transform_id id,
      model::ntp ntp,
      model::transform_metadata meta,
      processor::state_callback cb,
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
          output_topic.tp, ntp.tp.partition, _topic_table, _client);
        sinks.push_back(std::move(sink));

        auto offset_tracker = std::make_unique<offset_tracker_impl>(
          id, ntp.tp.partition, _client, _batcher);

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
    commit_batcher<>* _batcher;
    cluster::topic_table* _topic_table;
};

class rpc_offset_committer : public offset_committer {
public:
    explicit rpc_offset_committer(rpc::client* client)
      : _client(client) {}

    ss::future<result<model::partition_id, cluster::errc>>
    find_coordinator(model::transform_offsets_key key) override {
        return _client->find_coordinator(key);
    }

    ss::future<cluster::errc> batch_commit(
      model::partition_id coordinator,
      absl::btree_map<
        model::transform_offsets_key,
        model::transform_offsets_value> batch) override {
        return _client->batch_offset_commit(coordinator, std::move(batch));
    }

private:
    rpc::client* _client;
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
  ss::sharded<cluster::topic_table>* topic_table,
  ss::sharded<cluster::partition_manager>* partition_manager,
  ss::sharded<rpc::client>* rpc_client,
  ss::scheduling_group sg)
  : _runtime(runtime)
  , _self(self)
  , _plugin_frontend(plugin_frontend)
  , _feature_table(feature_table)
  , _group_manager(group_manager)
  , _topic_table(topic_table)
  , _partition_manager(partition_manager)
  , _rpc_client(rpc_client)
  , _sg(sg) {}

service::~service() = default;

ss::future<> service::start() {
    _batcher = std::make_unique<commit_batcher<ss::lowres_clock>>(
      config::shard_local_cfg().data_transforms_commit_interval_ms.bind(),
      std::make_unique<rpc_offset_committer>(&_rpc_client->local()));

    _manager = std::make_unique<manager<ss::lowres_clock>>(
      _self,
      std::make_unique<registry_adapter>(
        &_plugin_frontend->local(), &_partition_manager->local()),
      std::make_unique<proc_factory>(
        [this](model::transform_metadata meta) {
            return create_engine(std::move(meta));
        },
        &_topic_table->local(),
        &_partition_manager->local(),
        &_rpc_client->local(),
        _batcher.get()),
      _sg);
    co_await _batcher->start();
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
    if (_batcher) {
        co_await _batcher->stop();
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
    // The default report marks all transform's partitions in the unknown state,
    // then update the report with the information that we gather from all the
    // nodes in the cluster's in memory state. This allows us to report on
    // partitions that may not be actively in memory on a node somewhere.
    auto report = compute_default_report();
    report.merge(co_await _rpc_client->local().generate_report());
    co_return report;
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
    auto logger = std::make_unique<transform::logger>(meta.name, &tlog);
    auto factory = co_await get_factory(std::move(meta));
    if (!factory) {
        co_return ss::shared_ptr<wasm::engine>(nullptr);
    }
    co_return co_await (*factory)->make_engine(std::move(logger));
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
      std::move(meta), std::move(result).value());
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

model::cluster_transform_report service::compute_default_report() {
    using state = model::transform_report::processor::state;
    model::cluster_transform_report report;
    // Mark all transforms in an unknown state if they don't get an update
    for (auto [id, transform] : _plugin_frontend->local().all_transforms()) {
        auto cfg = _topic_table->local().get_topic_cfg(transform.input_topic);
        if (!cfg) {
            continue;
        }
        for (int32_t i = 0; i < cfg->partition_count; ++i) {
            report.add(
              id,
              transform,
              {
                .id = model::partition_id(i),
                .status = state::unknown,
                .node = _self,
                .lag = 0,
              });
        }
    }
    return report;
}

std::unique_ptr<rpc::reporter>
service::create_reporter(ss::sharded<service>* s) {
    return std::make_unique<wrapped_service_reporter>(s);
}

ss::future<
  result<ss::chunked_fifo<model::transform_committed_offset>, cluster::errc>>
service::list_committed_offsets(list_committed_offsets_options options) {
    if (!_feature_table->local().is_active(
          features::feature::wasm_transforms)) {
        co_return cluster::errc::feature_disabled;
    }
    auto _ = _gate.hold();
    auto result = co_await _rpc_client->local().list_committed_offsets();
    if (result.has_error()) {
        co_return result.error();
    }
    auto all_transforms = _plugin_frontend->local().all_transforms();
    ss::chunked_fifo<model::transform_committed_offset> commits;
    for (const auto& [k, v] : result.value()) {
        auto it = all_transforms.find(k.id);
        if (it != all_transforms.end()) {
            commits.emplace_back(it->second.name, k.partition, v.offset);
        } else if (options.show_unknown) {
            commits.emplace_back(
              model::transform_name(), k.partition, v.offset);
        }
        co_await ss::coroutine::maybe_yield();
    }
    co_return commits;
}

} // namespace transform
