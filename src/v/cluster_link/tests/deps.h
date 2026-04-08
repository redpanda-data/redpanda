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
#include "cluster_link/replication/tests/deps_test_impl.h"
#include "cluster_link/utils.h"
#include "config/mock_property.h"
#include "kafka/client/test/cluster_mock.h"
#include "kafka/data/rpc/deps.h"
#include "kafka/data/rpc/test/deps.h"
#include "security/acl_entry_set.h"

#include <seastar/util/defer.hh>

using test_config_provider
  = cluster_link::replication::tests::test_config_provider;
using data_src_factory
  = cluster_link::replication::tests::random_data_source_factory;
using data_sink_factory
  = cluster_link::replication::tests::accounting_sink_factory;
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
      model::metadata_ptr metadata,
      std::unique_ptr<kafka::client::cluster> cluster_connection) override {
        auto name = metadata->name;
        auto created_link = std::make_unique<link>(
          self,
          link_id,
          manager,
          _task_reconciler_interval,
          std::move(metadata),
          std::move(cluster_connection),
          std::make_unique<test_config_provider>(),
          std::make_unique<data_src_factory>(),
          std::make_unique<data_sink_factory>());

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

    ss::future<::cluster::cluster_link::errc> upsert_link(
      model::metadata md, ::model::timeout_clock::time_point) override {
        auto batch = ::cluster::cluster_link::testing::create_upsert_command(
          _last_offset++, std::move(md));
        auto ec = co_await _table->apply_update(std::move(batch));
        co_return ec.value();
    }

    ss::future<::cluster::cluster_link::errc> delete_link(
      model::name_t name,
      bool force,
      ::model::timeout_clock::time_point) override {
        auto batch = ::cluster::cluster_link::testing::create_remove_command(
          std::move(name), force);
        auto ec = co_await _table->apply_update(std::move(batch));
        co_return ec.value();
    }

    model::metadata_ptr find_link_by_id(model::id_t id) const override {
        return _table->find_link_by_id(id);
    }

    model::metadata_ptr
    find_link_by_name(const model::name_t& name) const override {
        return _table->find_link_by_name(name);
    }

    std::optional<model::id_t>
    find_link_id_by_name(const model::name_t& name) const override {
        return _table->find_id_by_name(name);
    }

    chunked_vector<model::id_t> get_all_link_ids() const override {
        return _table->get_all_link_ids();
    }

    std::optional<::model::revision_id>
    get_last_update_revision(const model::id_t& id) const override {
        return _table->get_link_last_update_revision(id);
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
      model::update_mirror_topic_status_cmd cmd,
      ::model::timeout_clock::time_point) override {
        auto link = _table->find_link_by_id(id);
        if (!link) {
            co_return ::cluster::cluster_link::errc::does_not_exist;
        }
        auto batch = ::cluster::cluster_link::testing::
          create_update_mirror_topic_status_command(id, std::move(cmd));

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
        mirror_topics.reserve(link->state.mirror_topics.size());
        for (const auto& [topic, metadata] : link->state.mirror_topics) {
            mirror_topics.emplace(topic, metadata.copy());
        }
        return mirror_topics;
    }

    ss::future<::cluster::cluster_link::errc> update_cluster_link_configuration(
      model::id_t id,
      model::update_cluster_link_configuration_cmd cmd,
      ::model::timeout_clock::time_point) override {
        auto link = _table->find_link_by_id(id);
        if (!link) {
            co_return ::cluster::cluster_link::errc::does_not_exist;
        }
        auto batch = ::cluster::cluster_link::testing::
          create_update_cluster_link_configuration_command(id, std::move(cmd));
        auto ec = co_await _table->apply_update(std::move(batch));
        co_return ec.value();
    }

    ss::future<std::expected<
      ::cluster_link::model::aggregated_shadow_topic_report,
      errc>>
    shadow_topic_report(const model::id_t&, const ::model::topic&) override {
        // unused in unit tests.
        co_return std::unexpected(errc::link_id_not_found);
    }

    ss::future<::cluster::cluster_link::errc> failover_link_topics(
      model::id_t id, ::model::timeout_clock::duration timeout) override {
        auto link = _table->find_link_by_id(id);
        if (!link) {
            co_return ::cluster::cluster_link::errc::does_not_exist;
        }
        chunked_vector<::model::topic> topics_to_failover;
        for (const auto& [t, info] : link->state.mirror_topics) {
            if (info.status == model::mirror_topic_status::active) {
                // only active topics can be failed over.
                topics_to_failover.push_back(t);
            }
        }
        for (const auto& t : topics_to_failover) {
            auto err = co_await update_mirror_topic_state(
              id,
              {.topic = t, .status = model::mirror_topic_status::failing_over},
              ::model::timeout_clock::now() + timeout);
            if (err != ::cluster::cluster_link::errc::success) {
                co_return err;
            }
        }
        co_return ::cluster::cluster_link::errc::success;
    }

    ss::future<::cluster::cluster_link::errc> delete_shadow_topic(
      model::id_t id,
      model::delete_mirror_topic_cmd cmd,
      ::model::timeout_clock::time_point) override {
        auto link = _table->find_link_by_id(id);
        if (!link) {
            co_return ::cluster::cluster_link::errc::does_not_exist;
        }
        auto batch = ::cluster::cluster_link::testing::
          create_delete_mirror_topic_command(id, std::move(cmd));
        auto ec = co_await _table->apply_update(std::move(batch));
        co_return ec.value();
    }

private:
    cluster::cluster_link::table* _table;
    ::model::offset _last_offset{0};
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
        ss::future<::result<R, cluster::errc>>(kafka::partition_proxy*)>,
      kafka::data::rpc::require_leader) {
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

    bool is_current_shard_leader(const ::model::ntp& ntp) const final {
        return _impl->shard_owner(ntp) == ss::this_shard_id();
    }

    ss::future<::result<::model::offset, cluster::errc>> invoke_on_shard(
      ss::shard_id shard_id,
      const ::model::ktp& ktp,
      ss::noncopyable_function<ss::future<
        ::result<::model::offset, cluster::errc>>(kafka::partition_proxy*)> fn,
      kafka::data::rpc::require_leader require_leader) final {
        return _impl->invoke_on_shard_impl(
          shard_id, ktp, std::move(fn), require_leader);
    }
    ss::future<::result<::model::offset, cluster::errc>> invoke_on_shard(
      ss::shard_id shard_id,
      const ::model::ntp& ntp,
      ss::noncopyable_function<ss::future<
        ::result<::model::offset, cluster::errc>>(kafka::partition_proxy*)> fn,
      kafka::data::rpc::require_leader require_leader) final {
        return _impl->invoke_on_shard_impl(
          shard_id, ntp, std::move(fn), require_leader);
    }

    ss::future<::result<kafka::data::rpc::partition_offsets, cluster::errc>>
    get_offsets_from_shard(
      ss::shard_id shard_id,
      const ::model::ktp& ktp,
      ss::noncopyable_function<ss::future<
        ::result<kafka::data::rpc::partition_offsets, cluster::errc>>(
        kafka::partition_proxy*)> fn,
      kafka::data::rpc::require_leader require_leader) final {
        return _impl->invoke_on_shard_impl(
          shard_id, ktp, std::move(fn), require_leader);
    }

    ss::future<::result<chunked_vector<::model::record_batch>, cluster::errc>>
    consume_from_shard(
      ss::shard_id shard_id,
      const ::model::ktp& ktp,
      kafka::data::rpc::consume_fn fn,
      kafka::data::rpc::require_leader require_leader) final {
        return _impl->invoke_on_shard_impl(
          shard_id, ktp, std::move(fn), require_leader);
    }

    std::optional<::model::term_id> get_term(const ::model::ntp&) const final {
        return std::nullopt;
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

    std::optional<int32_t>
    partition_count(::model::topic_namespace_view tp_ns) const {
        int32_t count = 0;
        bool found_ntp = false;
        for (const auto& [ntp, _] : _leader_map) {
            if (ntp.ns == tp_ns.ns && ntp.tp.topic == tp_ns.tp) {
                found_ntp = true;
                count++;
            }
        }
        return found_ntp ? std::make_optional(count) : std::nullopt;
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

    std::unique_ptr<kafka::client::cluster>
    create_cluster(const model::metadata& md) final {
        return std::make_unique<kafka::client::cluster>(
          metadata_to_kafka_config(md),
          std::make_unique<kafka::client::broker_mock_factory>(_cluster_mock));
    }

private:
    kafka::client::cluster_mock* _cluster_mock;
};

class fake_topic_metadata_cache
  : public kafka::data::rpc::topic_metadata_cache {
public:
    struct partition_offsets {
        ::model::offset log_start_offset{0};
        ::model::offset high_watermark{0};
    };
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
        auto part_count = cfg.partition_count;
        _topic_cfgs.insert_or_assign(tp_ns, std::move(cfg));
        for (auto i = 0; i < part_count; ++i) {
            auto it = _partition_offsets.find(
              ::model::ntp(tp_ns.ns, tp_ns.tp, ::model::partition_id(i)));
            if (it == _partition_offsets.end()) {
                _partition_offsets.emplace(
                  ::model::ntp(tp_ns.ns, tp_ns.tp, ::model::partition_id(i)),
                  partition_offsets{});
            }
        }
    }

    void update_topic_config(const cluster::topic_properties_update& update) {
        auto it = _topic_cfgs.find(update.tp_ns);
        if (it == _topic_cfgs.end()) {
            throw std::runtime_error(
              ss::format("unknown topic: {}", update.tp_ns));
        }
        auto& config = it->second;
        const auto& prop_update = update.properties;
        if (
          prop_update.batch_max_bytes.op
          == cluster::incremental_update_operation::set) {
            config.properties.batch_max_bytes
              = prop_update.batch_max_bytes.value;
        }

        if (
          prop_update.cleanup_policy_bitflags.op
          == cluster::incremental_update_operation::set) {
            config.properties.cleanup_policy_bitflags
              = prop_update.cleanup_policy_bitflags.value;
        }

        if (
          prop_update.timestamp_type.op
          == cluster::incremental_update_operation::set) {
            config.properties.timestamp_type = prop_update.timestamp_type.value;
        }

        if (
          update.custom_properties.replication_factor.op
          == cluster::incremental_update_operation::set) {
            config.replication_factor
              = update.custom_properties.replication_factor.value.value()();
        }
    }

    void
    set_partition_count(::model::topic_namespace_view tp_ns, int32_t count) {
        auto it = _topic_cfgs.find(::model::topic_namespace(tp_ns));
        if (it == _topic_cfgs.end()) {
            throw std::runtime_error(ss::format("unknown topic: {}", tp_ns));
        }
        it->second.partition_count = count;

        for (auto i = 0; i < count; i++) {
            auto ntp = ::model::ntp(
              tp_ns.ns, tp_ns.tp, ::model::partition_id(i));
            auto it = _partition_offsets.find(ntp);
            if (it == _partition_offsets.end()) {
                _partition_offsets.emplace(ntp, partition_offsets{});
            }
        }
    }

    uint32_t get_default_batch_max_bytes() const final { return 1_MiB; };

    std::optional<partition_offsets>
    get_partition_offsets(const ::model::ntp& ntp) const {
        auto it = _partition_offsets.find(ntp);
        if (it == _partition_offsets.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    void
    set_partition_offsets(const ::model::ntp& ntp, partition_offsets offsets) {
        auto it = _partition_offsets.find(ntp);
        if (it == _partition_offsets.end()) {
            throw std::runtime_error("unknown ntp");
        }
        it->second = std::move(offsets);
    }

private:
    absl::flat_hash_map<::model::topic_namespace, cluster::topic_configuration>
      _topic_cfgs;
    chunked_hash_map<::model::ntp, partition_offsets> _partition_offsets;
};

struct test_consumer_group_router : public consumer_groups_router {
    struct group_state {
        chunked_hash_map<
          ::model::topic,
          chunked_hash_map<::model::partition_id, kafka::offset>>
          offsets;
    };

    std::optional<::model::partition_id>
    partition_for(const kafka::group_id&) const override;

    ss::future<kafka::offset_commit_response>
      offset_commit(kafka::offset_commit_request) override;

    ss::future<bool> assure_topic_exists() override { co_return true; }

    chunked_hash_map<kafka::group_id, group_state> groups;

    int partition_count = 1;
};

struct test_partition_metadata_provider : public partition_metadata_provider {
    ss::future<std::optional<kafka::offset>>
      get_partition_high_watermark(::model::topic_partition_view) final;

    chunked_hash_map<::model::topic_partition, kafka::offset> hwms;
};

struct test_kafka_rpc_client_service : public kafka_rpc_client_service {
    explicit test_kafka_rpc_client_service(fake_topic_metadata_cache* ftmc)
      : _ftmc(ftmc) {}
    ss::future<result<kafka::data::rpc::partition_offsets_map, cluster::errc>>
      get_partition_offsets(
        chunked_vector<kafka::data::rpc::topic_partitions>) final;

    std::optional<cluster::errc> inserted_get_partition_offsets_error;
    std::optional<kafka::data::rpc::partition_offsets_map>
      inserted_get_partition_offsets_response;

private:
    fake_topic_metadata_cache* _ftmc{nullptr};
};

class fake_security_service : public security_service {
public:
    ss::future<std::vector<cluster::errc>> create_acls(
      std::vector<security::acl_binding> bindings,
      ::model::timeout_clock::duration) final {
        std::vector<cluster::errc> results;
        results.reserve(bindings.size());
        for (auto& binding : bindings) {
            auto& entries = _acls[binding.pattern()];
            entries.insert(binding.entry());
            entries.rehash();
            results.emplace_back(cluster::errc::success);
        }

        co_return results;
    }

    const auto& acls() { return _acls; }

private:
    using container_type
      = chunked_hash_map<security::resource_pattern, security::acl_entry_set>;

    container_type _acls;
};

class fake_members_table_provider : public members_table_provider {
public:
    void set_node_count(size_t count) { _node_count = count; }
    size_t node_count() const final { return _node_count; }

private:
    size_t _node_count{3};
};

class cluster_link_manager_test_fixture {
public:
    explicit cluster_link_manager_test_fixture(::model::node_id self);
    ~cluster_link_manager_test_fixture() = default;

    cluster_link_manager_test_fixture(const cluster_link_manager_test_fixture&)
      = delete;
    cluster_link_manager_test_fixture&
    operator=(const cluster_link_manager_test_fixture&) = delete;
    cluster_link_manager_test_fixture(cluster_link_manager_test_fixture&&)
      = delete;
    cluster_link_manager_test_fixture&
    operator=(cluster_link_manager_test_fixture&&) = delete;

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

    cluster::errc update_partition_count(
      ::model::topic_namespace_view tp_ns,
      int32_t partition_count,
      ::model::node_id node_id);

    fake_partition_leader_cache_impl* partition_leader_cache() {
        return _fplci;
    }

    fake_partition_manager* partition_manager() { return _fpm; }

    ss::future<> upsert_link(model::metadata metadata);

    ss::future<> update_link(model::id_t id, model::metadata metadata);

    model::metadata_ptr find_link_by_id(model::id_t id);

    model::metadata_ptr find_link_by_name(const model::name_t& name);

    std::optional<model::id_t> find_link_id_by_name(const model::name_t& name);

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

    test_consumer_group_router* consumer_group_router() {
        return _consumer_group_router;
    }

    test_partition_metadata_provider* partition_metadata_provider() {
        return _partition_metadata_provider;
    }

    ss::future<bool> wait_for_report_to_match(
      ss::lowres_clock::duration timeout,
      ss::lowres_clock::duration backoff,
      std::function<bool(const model::cluster_link_task_status_report&)>
        predicate);

    fake_security_service& security_service() { return *_fss; }

    test_kafka_rpc_client_service& kafka_rpc_client_service() {
        return *_tkrcs;
    }

    void set_partition_hwm(::model::topic_partition_view tp, kafka::offset hwm);

    fake_topic_metadata_cache& topic_metadata_cache() { return *_tmc; }

    kafka::data::rpc::test::fake_topic_creator& topic_creator() {
        return *_ftpc;
    }

    fake_members_table_provider& members_table_provider() { return *_fmtp; }

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
    kafka::data::rpc::test::fake_topic_creator* _ftpc{nullptr};
    fake_security_service* _fss{nullptr};
    link_factory* _lf{nullptr};
    test_consumer_group_router* _consumer_group_router{nullptr};
    test_partition_metadata_provider* _partition_metadata_provider{nullptr};
    test_kafka_rpc_client_service* _tkrcs{nullptr};
    fake_members_table_provider* _fmtp{nullptr};
    ss::sharded<manager> _manager;
    config::mock_property<int16_t> _default_topic_replication{1};

    ::model::node_id _self;
    model::id_t _next_link_id{0};
    int64_t _term_counter{0};
};
} // namespace cluster_link::tests
