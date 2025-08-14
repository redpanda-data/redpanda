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

#pragma once

#include "cluster/cluster_link/table.h"
#include "cluster/cluster_link/tests/utils.h"
#include "cluster_link/manager.h"
#include "cluster_link/utils.h"
#include "kafka/client/test/cluster_mock.h"
#include "kafka/data/rpc/deps.h"

#include <seastar/util/defer.hh>
namespace cluster_link::tests {

class test_link_factory : public link_factory {
public:
    explicit test_link_factory(
      ss::lowres_clock::duration task_reconciler_interval)
      : _task_reconciler_interval(task_reconciler_interval) {}
    std::unique_ptr<link> create_link(
      ::model::node_id self,
      model::id_t link_id,
      manager* manager,
      model::metadata metadata,
      kafka::client::cluster cluster_connection) override {
        auto name = metadata.name;
        auto created_link = std::make_unique<link>(
          self,
          link_id,
          manager,
          _task_reconciler_interval,
          std::move(metadata),
          std::move(cluster_connection));

        _links.emplace(std::move(name), created_link.get());
        return created_link;
    }

    std::optional<link*> get_link(const model::name_t& name) const {
        auto it = _links.find(name);
        if (it != _links.end()) {
            return it->second;
        }
        return std::nullopt;
    }

private:
    ss::lowres_clock::duration _task_reconciler_interval;
    chunked_hash_map<model::name_t, link*> _links;
};

class test_link_registry : public link_registry {
public:
    explicit test_link_registry(cluster::cluster_link::table* table)
      : _table(table) {}

    std::optional<std::reference_wrapper<const model::metadata>>
    find_link_by_id(model::id_t id) const override {
        return _table->find_link_by_id(id);
    }

    std::optional<std::reference_wrapper<const model::metadata>>
    find_link_by_name(const model::name_t& name) const override {
        return _table->find_link_by_name(name);
    }

    chunked_vector<model::id_t> get_all_link_ids() const override {
        return _table->get_all_link_ids();
    }

    ss::future<::cluster::cluster_link::errc> add_mirror_topic(
      model::id_t id,
      model::add_mirror_topic_cmd cmd,
      ::model::timeout_clock::time_point) override {
        auto link = _table->find_link_by_id(id);
        if (!link) {
            co_return ::cluster::cluster_link::errc::does_not_exist;
        }
        auto batch
          = ::cluster::cluster_link::testing::create_add_mirror_topic_command(
            id, std::move(cmd));

        auto ec = co_await _table->apply_update(std::move(batch));
        co_return ec.value();
    }

    ss::future<::cluster::cluster_link::errc> update_mirror_topic_state(
      model::id_t id,
      model::update_mirror_topic_state_cmd cmd,
      ::model::timeout_clock::time_point) override {
        auto link = _table->find_link_by_id(id);
        if (!link) {
            co_return ::cluster::cluster_link::errc::does_not_exist;
        }
        auto batch = ::cluster::cluster_link::testing::
          create_update_mirror_topic_state_command(id, std::move(cmd));

        auto ec = co_await _table->apply_update(std::move(batch));
        co_return ec.value();
    }

    ss::future<::cluster::cluster_link::errc> update_mirror_topic_properties(
      model::id_t id,
      model::update_mirror_topic_properties_cmd cmd,
      ::model::timeout_clock::time_point) override {
        auto link = _table->find_link_by_id(id);
        if (!link) {
            co_return ::cluster::cluster_link::errc::does_not_exist;
        }
        auto batch = ::cluster::cluster_link::testing::
          create_update_mirror_topic_properties_command(id, std::move(cmd));
        auto ec = co_await _table->apply_update(std::move(batch));
        co_return ec.value();
    }

    std::optional<chunked_hash_map<
      ::model::topic,
      ::cluster_link::model::mirror_topic_metadata>>
    get_mirror_topics_for_link(model::id_t id) const override {
        auto link = _table->find_link_by_id(id);
        if (!link) {
            return std::nullopt;
        }
        chunked_hash_map<
          ::model::topic,
          ::cluster_link::model::mirror_topic_metadata>
          mirror_topics;
        mirror_topics.reserve(link->get().state.mirror_topics.size());
        for (const auto& [topic, metadata] : link->get().state.mirror_topics) {
            mirror_topics.emplace(topic, metadata.copy());
        }
        return mirror_topics;
    }

private:
    cluster::cluster_link::table* _table;
};
class fake_partition_manager_proxy {
public:
    std::optional<ss::shard_id> shard_owner(const ::model::ktp& ktp) {
        auto it = _shard_locations.find(ktp);
        if (it == _shard_locations.end()) {
            return std::nullopt;
        }
        return it->second;
    }
    std::optional<ss::shard_id> shard_owner(const ::model::ntp& ntp) {
        auto it = _shard_locations.find(ntp);
        if (it == _shard_locations.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    void set_shard_owner(const ::model::ntp& ntp, ss::shard_id shard_id) {
        _shard_locations.insert_or_assign(ntp, shard_id);
    }

    void remove_shard_owner(const ::model::ntp& ntp) {
        _shard_locations.erase(ntp);
    }

    template<typename R, typename N>
    ss::future<::result<R, cluster::errc>> invoke_on_shard_impl(
      ss::shard_id,
      const N&,
      ss::noncopyable_function<
        ss::future<::result<R, cluster::errc>>(kafka::partition_proxy*)>) {
        throw std::runtime_error("not implemented");
    }

private:
    ::model::ntp_map_type<ss::shard_id> _shard_locations;
};

class fake_partition_manager : public kafka::data::rpc::partition_manager {
public:
    explicit fake_partition_manager(fake_partition_manager_proxy* impl)
      : _impl(impl) {}

    std::optional<ss::shard_id> shard_owner(const ::model::ktp& ktp) override {
        return _impl->shard_owner(ktp);
    }

    std::optional<ss::shard_id> shard_owner(const ::model::ntp& ntp) override {
        return _impl->shard_owner(ntp);
    }

    void set_shard_owner(const ::model::ntp& ntp, ss::shard_id shard_id) {
        _impl->set_shard_owner(ntp, shard_id);
    }

    void remove_shard_owner(const ::model::ntp& ntp) {
        _impl->remove_shard_owner(ntp);
    }

    ss::future<::result<::model::offset, cluster::errc>> invoke_on_shard(
      ss::shard_id shard_id,
      const ::model::ktp& ktp,
      ss::noncopyable_function<
        ss::future<::result<::model::offset, cluster::errc>>(
          kafka::partition_proxy*)> fn) final {
        return _impl->invoke_on_shard_impl(shard_id, ktp, std::move(fn));
    }
    ss::future<::result<::model::offset, cluster::errc>> invoke_on_shard(
      ss::shard_id shard_id,
      const ::model::ntp& ntp,
      ss::noncopyable_function<
        ss::future<::result<::model::offset, cluster::errc>>(
          kafka::partition_proxy*)> fn) final {
        return _impl->invoke_on_shard_impl(shard_id, ntp, std::move(fn));
    }

    ss::future<::result<kafka::data::rpc::partition_offsets, cluster::errc>>
    get_offsets_from_shard(
      ss::shard_id shard_id,
      const ::model::ktp& ktp,
      ss::noncopyable_function<ss::future<
        ::result<kafka::data::rpc::partition_offsets, cluster::errc>>(
        kafka::partition_proxy*)> fn) final {
        return _impl->invoke_on_shard_impl(shard_id, ktp, std::move(fn));
    }

private:
    fake_partition_manager_proxy* _impl;
};
class fake_partition_leader_cache_impl
  : public kafka::data::rpc::partition_leader_cache {
public:
    std::optional<::model::node_id> get_leader_node(
      ::model::topic_namespace_view tp_ns, ::model::partition_id pid) const {
        auto ntp = ::model::ntp(tp_ns.ns, tp_ns.tp, pid);
        auto it = _leader_map.find(ntp);
        if (it == _leader_map.end()) {
            return std::nullopt;
        }
        return it->second;
    }
    void set_leader_node(const ::model::ntp& ntp, ::model::node_id node_id) {
        _leader_map.insert_or_assign(ntp, node_id);
    }

private:
    chunked_hash_map<::model::ntp, ::model::node_id> _leader_map;
};

class fake_partition_leader_cache
  : public kafka::data::rpc::partition_leader_cache {
public:
    explicit fake_partition_leader_cache(fake_partition_leader_cache_impl* impl)
      : _impl(impl) {}
    std::optional<::model::node_id> get_leader_node(
      ::model::topic_namespace_view tp_ns,
      ::model::partition_id pid) const final {
        return _impl->get_leader_node(tp_ns, pid);
    }

private:
    fake_partition_leader_cache_impl* _impl;
};

class cluster_mock_factory : public cluster_factory {
public:
    explicit cluster_mock_factory(kafka::client::cluster_mock* cluster_mock)
      : _cluster_mock(cluster_mock) {}

    kafka::client::cluster create_cluster(const model::metadata& md) final {
        return {
          metadata_to_kafka_config(md),
          std::make_unique<kafka::client::broker_mock_factory>(_cluster_mock)};
    }

private:
    kafka::client::cluster_mock* _cluster_mock;
};

class fake_topic_metadata_cache
  : public kafka::data::rpc::topic_metadata_cache {
public:
    std::optional<cluster::topic_configuration>
    find_topic_cfg(::model::topic_namespace_view tp_ns) const final {
        auto it = _topic_cfgs.find(::model::topic_namespace(tp_ns));
        if (it == _topic_cfgs.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    void set_topic_config(cluster::topic_configuration cfg) {
        auto tp_ns = cfg.tp_ns;
        _topic_cfgs.insert_or_assign(tp_ns, std::move(cfg));
    }

    void update_topic_config(const cluster::topic_properties_update& update) {
        auto it = _topic_cfgs.find(update.tp_ns);
        if (it == _topic_cfgs.end()) {
            throw std::runtime_error(
              ss::format("unknown topic: {}", update.tp_ns));
        }
        auto& config = it->second;
        // NOTE: We just support batch_max_bytes because that's all we use in
        // tests.
        const auto& prop_update = update.properties.batch_max_bytes;
        switch (prop_update.op) {
        case cluster::incremental_update_operation::none:
            return;
        case cluster::incremental_update_operation::set:
            config.properties.batch_max_bytes
              = update.properties.batch_max_bytes.value;
            break;
        case cluster::incremental_update_operation::remove:
            config.properties.batch_max_bytes.reset();
            break;
        }
    }

    uint32_t get_default_batch_max_bytes() const final { return 1_MiB; };

private:
    absl::flat_hash_map<::model::topic_namespace, cluster::topic_configuration>
      _topic_cfgs;
};

class cluster_link_manager_test_fixture {
public:
    explicit cluster_link_manager_test_fixture(::model::node_id self);
    ~cluster_link_manager_test_fixture() = default;

    cluster_link_manager_test_fixture(const cluster_link_manager_test_fixture&)
      = delete;
    cluster_link_manager_test_fixture&
    operator=(const cluster_link_manager_test_fixture&)
      = delete;
    cluster_link_manager_test_fixture(cluster_link_manager_test_fixture&&)
      = delete;
    cluster_link_manager_test_fixture&
    operator=(cluster_link_manager_test_fixture&&)
      = delete;

    ss::future<> wire_up_and_start(std::unique_ptr<link_factory>);

    ss::future<> reset();

    fake_partition_manager_proxy* partition_manager_proxy() {
        return _fpmp.get();
    }

    ss::sharded<manager>& get_manager() { return _manager; }

    void elect_leader(
      const ::model::ntp& ntp,
      ::model::node_id node_id,
      std::optional<ss::shard_id> shard_id);

    fake_partition_leader_cache_impl* partition_leader_cache() {
        return _fplci;
    }

    fake_partition_manager* partition_manager() { return _fpm; }

    ss::future<> upsert_link(model::metadata metadata);

    std::optional<std::reference_wrapper<const model::metadata>>
    find_link_by_id(model::id_t id);

    std::optional<std::reference_wrapper<const model::metadata>>
    find_link_by_name(const model::name_t& name);

    link_factory* get_link_factory() { return _lf; }

    ss::future<std::optional<model::cluster_link_task_status_report>>
    await_status_report(
      ss::lowres_clock::duration timeout,
      ss::lowres_clock::duration backoff,
      std::function<bool(const model::cluster_link_task_status_report&)>
        predicate,
      std::optional<ss::abort_source> as = std::nullopt);

    kafka::client::cluster_mock& get_cluster_mock() { return _cluster_mock; }

    void set_topic_config(cluster::topic_configuration cfg);

private:
    void setup_cluster_mock();

private:
    kafka::client::cluster_mock _cluster_mock;
    std::unique_ptr<cluster_factory> _cluster_factory;
    chunked_vector<ss::deferred_action<ss::noncopyable_function<void()>>>
      _notification_cleanups;
    ss::sharded<cluster::cluster_link::table> _table;
    std::unique_ptr<fake_partition_manager_proxy> _fpmp;
    fake_partition_manager* _fpm{nullptr};
    fake_partition_leader_cache_impl* _fplci{nullptr};
    fake_topic_metadata_cache* _tmc{nullptr};
    link_factory* _lf{nullptr};
    ss::sharded<manager> _manager;

    ::model::node_id _self;
    model::id_t _next_link_id{0};
};
} // namespace cluster_link::tests
