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
#include "features/feature_table.h"
#include "kafka/server/replicated_partition.h"
#include "model/timeout_clock.h"
#include "resource_mgmt/io_priority.h"
#include "transform/io.h"
#include "transform/logger.h"
#include "transform/rpc/client.h"
#include "transform/transform_manager.h"
#include "transform/transform_processor.h"
#include "wasm/api.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/util/optimized_optional.hh>

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

class rpc_client_factory final : public sink::factory {
public:
    explicit rpc_client_factory(rpc::client* c)
      : _client(c) {}

    std::optional<std::unique_ptr<sink>> create(model::ntp ntp) override {
        return std::make_unique<rpc_client_sink>(ntp, _client);
    };

private:
    rpc::client* _client;
};

class partition_source final : public source {
public:
    explicit partition_source(kafka::partition_proxy p)
      : _partition(std::move(p)) {}

    ss::future<model::offset> load_latest_offset() final {
        auto result = _partition.last_stable_offset();
        if (result.has_error()) {
            throw std::runtime_error(
              kafka::make_error_code(result.error()).message());
        }
        co_return result.value();
    }

    ss::future<model::record_batch_reader>
    read_batch(model::offset offset, ss::abort_source* as) final {
        auto translater = co_await _partition.make_reader(
          storage::log_reader_config(
            /*start_offset=*/offset,
            /*max_offset=*/model::offset::max(),
            // TODO: Make a new priority for WASM transforms
            /*prio=*/kafka_read_priority(),
            /*as=*/*as));
        co_return std::move(translater).reader;
    }

private:
    kafka::partition_proxy _partition;
};

class partition_source_factory final : public source::factory {
public:
    explicit partition_source_factory(cluster::partition_manager* manager)
      : _manager(manager) {}

    std::optional<std::unique_ptr<source>> create(model::ntp ntp) final {
        auto p = _manager->get(ntp);
        if (!p) {
            return std::nullopt;
        }
        return std::make_unique<partition_source>(kafka::partition_proxy(
          std::make_unique<kafka::replicated_partition>(p)));
    };

private:
    cluster::partition_manager* _manager;
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

using wasm_engine_factory = ss::noncopyable_function<
  ss::future<ss::optimized_optional<ss::shared_ptr<wasm::engine>>>(
    model::transform_metadata)>;

class proc_factory : public processor_factory {
public:
    proc_factory(
      wasm_engine_factory factory,
      std::unique_ptr<source::factory> source_factory,
      std::unique_ptr<sink::factory> sink_factory)
      : _wasm_engine_factory(std::move(factory))
      , _source_factory(std::move(source_factory))
      , _sink_factory(std::move(sink_factory)) {}

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
        auto src = _source_factory->create(ntp);
        if (!src) {
            throw std::runtime_error("unable to create transform source");
        }
        vassert(
          meta.output_topics.size() == 1,
          "only a single output topic is supported");
        const auto& output_topic = meta.output_topics[0];
        std::vector<std::unique_ptr<sink>> sinks;
        auto sink = _sink_factory->create(
          model::ntp(output_topic.ns, output_topic.tp, ntp.tp.partition));
        if (!sink) {
            throw std::runtime_error("unable to create transform sink");
        }
        sinks.push_back(*std::move(sink));
        co_return std::make_unique<processor>(
          id,
          ntp,
          meta,
          *std::move(engine),
          std::move(cb),
          *std::move(src),
          std::move(sinks),
          p);
    }

private:
    mutex _mu;
    wasm_engine_factory _wasm_engine_factory;
    std::unique_ptr<source::factory> _source_factory;
    std::unique_ptr<sink::factory> _sink_factory;
    absl::flat_hash_map<model::offset, std::unique_ptr<wasm::engine>> _cache;
};

} // namespace

service::service(
  wasm::runtime* runtime,
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

ss::future<> service::start() { throw std::runtime_error("unimplemented"); }

ss::future<> service::stop() { throw std::runtime_error("unimplemented"); }

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

} // namespace transform
