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

#include "bytes/bytes.h"
#include "cluster/errc.h"
#include "cluster/types.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "kafka/server/partition_proxy.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/timeout_clock.h"
#include "net/server.h"
#include "net/types.h"
#include "net/unresolved_address.h"
#include "rpc/backoff_policy.h"
#include "rpc/connection_cache.h"
#include "rpc/rpc_server.h"
#include "test_utils/test.h"
#include "transform/rpc/client.h"
#include "transform/rpc/deps.h"
#include "transform/rpc/logger.h"
#include "transform/rpc/service.h"
#include "transform/tests/cluster_fixture.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/flat_hash_map.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iterator>
#include <memory>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <vector>

namespace transform::rpc {

namespace {

// A small helper struct to allow copies for easier to read tests and
// integration with gmock matchers.
struct record_batches {
    static record_batches make() {
        record_batches batches;
        batches.underlying.emplace_back(
          model::test::make_random_batch({.count = 1}));
        return batches;
    }

    record_batches() = default;
    explicit record_batches(ss::chunked_fifo<model::record_batch> underlying)
      : underlying(std::move(underlying)) {}
    record_batches(record_batches&&) = default;
    record_batches& operator=(record_batches&&) = default;
    record_batches(const record_batches& b)
      : underlying(copy(b.underlying)) {}
    record_batches& operator=(const record_batches& b) {
        if (this != &b) {
            underlying = copy(b.underlying);
        }
        return *this;
    }
    ~record_batches() = default;

    bool operator==(const record_batches& other) const {
        return std::equal(
          underlying.begin(),
          underlying.end(),
          other.underlying.begin(),
          other.underlying.end());
    }

    friend std::ostream& operator<<(std::ostream& os, const record_batches& b) {
        return os << ss::format("{}", b.underlying);
    }

    bool empty() const { return underlying.empty(); }
    size_t size() const { return underlying.size(); }
    auto begin() const { return underlying.begin(); }
    auto end() const { return underlying.end(); }

    ss::chunked_fifo<model::record_batch> underlying;

private:
    ss::chunked_fifo<model::record_batch>
    copy(const ss::chunked_fifo<model::record_batch>& batches) {
        ss::chunked_fifo<model::record_batch> copied;
        for (const auto& b : batches) {
            copied.push_back(b.copy());
        }
        return copied;
    }
};

class fake_partition_leader_cache : public partition_leader_cache {
public:
    std::optional<model::node_id> get_leader_node(
      model::topic_namespace_view tp_ns, model::partition_id p) const final {
        auto ntp = model::ntp(tp_ns.ns, tp_ns.tp, p);
        auto it = _leader_map.find(ntp);
        if (it == _leader_map.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    void set_leader_node(const model::ntp& ntp, model::node_id nid) {
        _leader_map.insert_or_assign(ntp, nid);
        vassert(_leader_map.find(ntp) != _leader_map.end(), "what??");
    }

private:
    absl::flat_hash_map<model::ntp, model::node_id> _leader_map;
};

class fake_topic_metadata_cache : public topic_metadata_cache {
public:
    std::optional<cluster::topic_configuration>
    find_topic_cfg(model::topic_namespace_view tp_ns) const final {
        auto it = _topic_cfgs.find(model::topic_namespace(tp_ns));
        if (it == _topic_cfgs.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    void set_topic_cfg(cluster::topic_configuration cfg) {
        auto tp_ns = cfg.tp_ns;
        _topic_cfgs.insert_or_assign(tp_ns, std::move(cfg));
    }

    uint32_t get_default_batch_max_bytes() const final { return 1_MiB; };

private:
    absl::flat_hash_map<model::topic_namespace, cluster::topic_configuration>
      _topic_cfgs;
};

class delegating_fake_topic_metadata_cache : public topic_metadata_cache {
public:
    explicit delegating_fake_topic_metadata_cache(
      fake_topic_metadata_cache* cache)
      : _delegator(cache) {}

    std::optional<cluster::topic_configuration>
    find_topic_cfg(model::topic_namespace_view tp_ns) const final {
        return _delegator->find_topic_cfg(tp_ns);
    }

    uint32_t get_default_batch_max_bytes() const final {
        return _delegator->get_default_batch_max_bytes();
    };

private:
    fake_topic_metadata_cache* _delegator;
};

struct produced_batch {
    model::ntp ntp;
    model::record_batch batch;
};

class fake_topic_creator : public topic_creator {
public:
    fake_topic_creator(
      ss::noncopyable_function<void(const cluster::topic_configuration&)>
        new_topic_cb,
      ss::noncopyable_function<void(const model::ntp&, model::node_id)>
        new_ntp_cb)
      : _new_topic_cb(std::move(new_topic_cb))
      , _new_ntp_cb(std::move(new_ntp_cb)) {}

    ss::future<cluster::errc> create_topic(
      model::topic_namespace_view tp_ns,
      int32_t partition_count,
      cluster::topic_properties properties) final {
        cluster::topic_configuration tcfg{
          tp_ns.ns,
          tp_ns.tp,
          partition_count,
          /*replication_factor=*/1,
        };
        tcfg.properties = properties;
        _new_topic_cb(tcfg);
        for (int32_t i = 0; i < partition_count; ++i) {
            _new_ntp_cb(
              model::ntp(tp_ns.ns, tp_ns.tp, model::partition_id(i)),
              _default_new_topic_leader);
        }
        co_return cluster::errc::success;
    }

    void set_default_new_topic_leader(model::node_id node_id) {
        _default_new_topic_leader = node_id;
    }

private:
    model::node_id _default_new_topic_leader;
    ss::noncopyable_function<void(const cluster::topic_configuration&)>
      _new_topic_cb;
    ss::noncopyable_function<void(const model::ntp&, model::node_id)>
      _new_ntp_cb;
};

class fake_partition_manager : public partition_manager {
public:
    std::optional<ss::shard_id> shard_owner(const model::ntp& ntp) final {
        auto it = _shard_locations.find(ntp);
        if (it == _shard_locations.end()) {
            return std::nullopt;
        }
        return it->second;
    };

    void set_shard_owner(const model::ntp& ntp, ss::shard_id shard_id) {
        _shard_locations.insert_or_assign(ntp, shard_id);
    }
    void remove_shard_owner(const model::ntp& ntp) {
        _shard_locations.erase(ntp);
    }

    record_batches partition_records(const model::ntp& ntp) {
        record_batches batches;
        for (const auto& produced : _produced_batches) {
            if (produced.ntp == ntp) {
                batches.underlying.emplace_back(produced.batch.copy());
            }
        }
        return batches;
    }

    ss::future<cluster::errc> invoke_on_shard(
      ss::shard_id shard_id,
      const model::ntp& ntp,
      ss::noncopyable_function<
        ss::future<cluster::errc>(kafka::partition_proxy*)> fn) final {
        auto owner = shard_owner(ntp);
        if (!owner || shard_id != *owner) {
            co_return cluster::errc::not_leader;
        }
        auto pp = kafka::partition_proxy(
          std::make_unique<in_memory_proxy>(ntp, &_produced_batches));
        co_return co_await fn(&pp);
    }

    ss::future<find_coordinator_response> invoke_on_shard(
      ss::shard_id,
      ss::noncopyable_function<ss::future<find_coordinator_response>(
        cluster::partition_manager&)>) override final {
        return ss::make_exception_future<find_coordinator_response>({});
    }

    ss::future<offset_commit_response> invoke_on_shard(
      ss::shard_id,
      ss::noncopyable_function<ss::future<offset_commit_response>(
        cluster::partition_manager&)>) override final {
        return ss::make_exception_future<offset_commit_response>({});
    }

    ss::future<offset_fetch_response> invoke_on_shard(
      ss::shard_id,
      ss::noncopyable_function<ss::future<offset_fetch_response>(
        cluster::partition_manager&)>) override final {
        return ss::make_exception_future<offset_fetch_response>({});
    }

private:
    class in_memory_proxy : public kafka::partition_proxy::impl {
    public:
        in_memory_proxy(
          model::ntp ntp, ss::chunked_fifo<produced_batch>* produced_batches)
          : _ntp(std::move(ntp))
          , _produced_batches(produced_batches) {}

        const model::ntp& ntp() const final { return _ntp; }
        ss::future<result<model::offset, kafka::error_code>>
        sync_effective_start(model::timeout_clock::duration) final {
            throw std::runtime_error("unimplemented");
        }
        model::offset start_offset() const final {
            throw std::runtime_error("unimplemented");
        }
        model::offset high_watermark() const final {
            throw std::runtime_error("unimplemented");
        }
        checked<model::offset, kafka::error_code>
        last_stable_offset() const final {
            throw std::runtime_error("unimplemented");
        }
        kafka::leader_epoch leader_epoch() const final {
            throw std::runtime_error("unimplemented");
        }
        ss::future<std::optional<model::offset>>
        get_leader_epoch_last_offset(kafka::leader_epoch) const final {
            throw std::runtime_error("unimplemented");
        }
        bool is_elected_leader() const final { return true; }
        bool is_leader() const final { return true; }
        ss::future<std::error_code> linearizable_barrier() final {
            throw std::runtime_error("unimplemented");
        }
        ss::future<kafka::error_code>
        prefix_truncate(model::offset, ss::lowres_clock::time_point) final {
            throw std::runtime_error("unimplemented");
        }
        ss::future<storage::translating_reader> make_reader(
          storage::log_reader_config config,
          std::optional<model::timeout_clock::time_point>) final {
            if (
              config.first_timestamp.has_value()
              || config.type_filter.has_value()) {
                throw std::runtime_error("unimplemented");
            }
            model::record_batch_reader::data_t read_batches;
            for (auto& b : *_produced_batches) {
                if (b.ntp != _ntp) {
                    continue;
                }
                if (b.batch.base_offset() < config.start_offset) {
                    continue;
                }
                read_batches.push_back(b.batch.copy());
                if (b.batch.last_offset() > config.max_offset) {
                    break;
                }
            }
            co_return model::make_memory_record_batch_reader(
              std::move(read_batches));
        }
        ss::future<std::optional<storage::timequery_result>>
        timequery(storage::timequery_config) final {
            throw std::runtime_error("unimplemented");
        }
        ss::future<std::vector<cluster::rm_stm::tx_range>> aborted_transactions(
          model::offset,
          model::offset,
          ss::lw_shared_ptr<const storage::offset_translator_state>) final {
            throw std::runtime_error("unimplemented");
        }
        ss::future<kafka::error_code> validate_fetch_offset(
          model::offset, bool, model::timeout_clock::time_point) final {
            throw std::runtime_error("unimplemented");
        }

        ss::future<result<model::offset>> replicate(
          model::record_batch_reader rdr, raft::replicate_options) final {
            auto batches = co_await model::consume_reader_to_memory(
              std::move(rdr), model::no_timeout);
            auto offset = latest_offset();
            for (const auto& batch : batches) {
                auto b = batch.copy();
                b.header().base_offset = offset++;
                _produced_batches->emplace_back(_ntp, std::move(b));
            }
            co_return _produced_batches->back().batch.last_offset();
        }

        raft::replicate_stages replicate(
          model::batch_identity,
          model::record_batch_reader&&,
          raft::replicate_options) final {
            throw std::runtime_error("unimplemented");
        }

        result<kafka::partition_info> get_partition_info() const override {
            throw std::runtime_error("unimplemented");
        }
        cluster::partition_probe& probe() override {
            throw std::runtime_error("unimplemented");
        }

    private:
        model::offset latest_offset() {
            auto o = model::offset(0);
            for (const auto& b : *_produced_batches) {
                if (b.ntp == _ntp) {
                    o = b.batch.last_offset();
                }
            }
            return o;
        }

        model::ntp _ntp;
        ss::chunked_fifo<produced_batch>* _produced_batches;
    };

    ss::chunked_fifo<produced_batch> _produced_batches;
    model::ntp_flat_map_type<ss::shard_id> _shard_locations;
};

constexpr uint16_t test_server_port = 8080;
constexpr auto test_timeout = std::chrono::seconds(10);
constexpr model::node_id self_node = model::node_id(1);
constexpr model::node_id other_node = model::node_id(2);

struct test_parameters {
    model::node_id leader_node;
    model::node_id non_leader_node;

    friend std::ostream&
    operator<<(std::ostream& os, const test_parameters& tp) {
        return os << "{leader_node: " << tp.leader_node
                  << " non_leader_node: " << tp.non_leader_node << "}";
    }
};

class TransformRpcTest : public ::testing::TestWithParam<test_parameters> {
public:
    void SetUp() override {
        _as.start().get();

        // remote node start
        _remote_services
          .start_single(
            ss::sharded_parameter([this]() {
                auto ftmc = std::make_unique<fake_topic_metadata_cache>();
                _remote_ftmc = ftmc.get();
                return ftmc;
            }),
            ss::sharded_parameter([this]() {
                auto fpm = std::make_unique<fake_partition_manager>();
                _remote_fpm = fpm.get();
                return fpm;
            }))
          .get();

        net::server_configuration scfg("transform_test_rpc_server");
        scfg.addrs.emplace_back(ss::socket_address(test_server_port));
        scfg.max_service_memory_per_core = 1_GiB;
        scfg.disable_metrics = net::metrics_disabled::yes;
        scfg.disable_public_metrics = net::public_metrics_disabled::yes;
        _server = std::make_unique<::rpc::rpc_server>(scfg);
        std::vector<std::unique_ptr<::rpc::service>> rpc_services;
        rpc_services.push_back(std::make_unique<network_service>(
          ss::default_scheduling_group(),
          ss::default_smp_service_group(),
          &_remote_services));
        _server->add_services(std::move(rpc_services));
        _server->start();

        // start local node
        _local_services
          .start_single(
            ss::sharded_parameter([this]() {
                auto ftmc = std::make_unique<fake_topic_metadata_cache>();
                _local_ftmc = ftmc.get();
                return ftmc;
            }),
            ss::sharded_parameter([this]() {
                auto fpm = std::make_unique<fake_partition_manager>();
                _local_fpm = fpm.get();
                return fpm;
            }))
          .get();
        _conn_cache.start(std::ref(_as), std::nullopt).get();
        ::rpc::transport_configuration tcfg(
          net::unresolved_address("localhost", test_server_port));
        tcfg.disable_metrics = net::metrics_disabled::yes;
        _conn_cache.local()
          .emplace(
            other_node,
            tcfg,
            ::rpc::make_exponential_backoff_policy<ss::lowres_clock>(1s, 3s))
          .get();

        auto fplc = std::make_unique<fake_partition_leader_cache>();
        _fplc = fplc.get();
        auto ftpc = std::make_unique<fake_topic_creator>(
          [this](const cluster::topic_configuration& tp_cfg) {
              remote_metadata_cache()->set_topic_cfg(tp_cfg);
              local_metadata_cache()->set_topic_cfg(tp_cfg);
          },
          [this](const model::ntp& ntp, model::node_id leader) {
              elect_leader(ntp, leader);
          });
        _ftpc = ftpc.get();
        _client = std::make_unique<rpc::client>(
          self_node,
          std::move(fplc),
          std::make_unique<delegating_fake_topic_metadata_cache>(_local_ftmc),
          std::move(ftpc),
          &_conn_cache,
          &_local_services);
    }
    void TearDown() override {
        _client->stop().get();
        _client.reset();
        _conn_cache.stop().get();
        _server->stop().get();
        _server.reset();
        _local_services.stop().get();
        _remote_services.stop().get();
        _as.stop().get();
        _local_ftmc = nullptr;
        _local_fpm = nullptr;
        _remote_ftmc = nullptr;
        _remote_fpm = nullptr;
        _fplc = nullptr;
    }

    void create_topic(const model::topic_namespace& tp_ns) {
        cluster::topic_configuration tcfg{
          tp_ns.ns,
          tp_ns.tp,
          /*partition_count=*/1,
          /*replication_factor=*/1,
        };
        remote_metadata_cache()->set_topic_cfg(tcfg);
        local_metadata_cache()->set_topic_cfg(tcfg);
    }

    void elect_leader(const model::ntp& ntp, model::node_id node_id) {
        partition_leader_cache()->set_leader_node(ntp, node_id);
        if (node_id == self_node) {
            local_partition_manager()->set_shard_owner(
              ntp, ss::this_shard_id());
            remote_partition_manager()->remove_shard_owner(ntp);
        } else if (node_id == other_node) {
            remote_partition_manager()->set_shard_owner(
              ntp, ss::this_shard_id());
            local_partition_manager()->remove_shard_owner(ntp);
        } else {
            throw std::runtime_error(ss::format("unknown node_id {}", node_id));
        }
    }

    void set_default_new_topic_leader(model::node_id node_id) {
        _ftpc->set_default_new_topic_leader(node_id);
    }

    cluster::errc produce(const model::ntp& ntp, record_batches batches) {
        return client()->produce(ntp.tp, std::move(batches.underlying)).get();
    }

    result<stored_wasm_binary_metadata, cluster::errc>
    store_wasm_binary(iobuf b) {
        return client()->store_wasm_binary(std::move(b), test_timeout).get();
    }
    result<iobuf, cluster::errc> load_wasm_binary(model::offset o) {
        return client()->load_wasm_binary(o, test_timeout).get();
    }
    cluster::errc delete_wasm_binary(uuid_t key) {
        return client()->delete_wasm_binary(key, test_timeout).get();
    }

    model::node_id leader_node() const { return GetParam().leader_node; }
    model::node_id non_leader_node() const {
        return GetParam().non_leader_node;
    }

    record_batches non_leader_batches(const model::ntp& ntp) {
        return batches_for(non_leader_node(), ntp);
    }
    record_batches leader_batches(const model::ntp& ntp) {
        return batches_for(leader_node(), ntp);
    }

    // local node state
    fake_topic_metadata_cache* local_metadata_cache() { return _local_ftmc; }
    fake_partition_manager* local_partition_manager() { return _local_fpm; }
    fake_partition_leader_cache* partition_leader_cache() { return _fplc; }
    rpc::local_service* local_service() { return &_local_services.local(); }
    client* client() { return _client.get(); }

    // remote node state
    fake_topic_metadata_cache* remote_metadata_cache() { return _remote_ftmc; }
    fake_partition_manager* remote_partition_manager() { return _remote_fpm; }
    rpc::local_service* remote_service() { return &_remote_services.local(); }

private:
    record_batches batches_for(model::node_id node, const model::ntp& ntp) {
        auto manager = node == self_node ? local_partition_manager()
                                         : remote_partition_manager();
        return manager->partition_records(ntp);
    }

    std::unique_ptr<::rpc::rpc_server> _server;
    fake_topic_metadata_cache* _local_ftmc = nullptr;
    fake_partition_manager* _local_fpm = nullptr;
    fake_topic_metadata_cache* _remote_ftmc = nullptr;
    fake_partition_manager* _remote_fpm = nullptr;
    fake_partition_leader_cache* _fplc = nullptr;
    fake_topic_creator* _ftpc = nullptr;
    ss::sharded<rpc::local_service> _local_services;
    ss::sharded<rpc::local_service> _remote_services;
    ss::sharded<::rpc::connection_cache> _conn_cache;
    std::unique_ptr<rpc::client> _client;
    ss::sharded<ss::abort_source> _as;
};

} // namespace

model::ntp make_ntp(std::string_view topic) {
    return {
      model::kafka_namespace, model::topic(topic), model::partition_id(0)};
}

using ::testing::IsEmpty;
using ::testing::SizeIs;

TEST_P(TransformRpcTest, ClientCanProduce) {
    auto ntp = make_ntp("foo");
    create_topic(model::topic_namespace(ntp.ns, ntp.tp.topic));
    elect_leader(ntp, leader_node());
    auto batches = record_batches::make();
    cluster::errc ec = produce(ntp, batches);
    EXPECT_EQ(ec, cluster::errc::success)
      << cluster::error_category().message(int(ec));
    EXPECT_THAT(non_leader_batches(ntp), IsEmpty());
    EXPECT_EQ(leader_batches(ntp), batches);
}

TEST_P(TransformRpcTest, WasmBinaryCrud) {
    // clang-format off
    // NOLINTBEGIN(*-magic-numbers)
    iobuf wasm_binary = bytes_to_iobuf(
      {0x00, 0x61, 0x73, 0x6d, 0x01, 0x00,
       0x00, 0x00, 0x00, 0x08, 0x04, 0x6e,
       0x61, 0x6d, 0x65, 0x02, 0x01, 0x00});
    // NOLINTEND(*-magic-numbers)
    // clang-format on
    // The topic is auto created
    set_default_new_topic_leader(leader_node());

    auto stored = store_wasm_binary(wasm_binary.copy());
    ASSERT_TRUE(stored.has_value());
    EXPECT_THAT(
      non_leader_batches(model::wasm_binaries_internal_ntp), IsEmpty());
    EXPECT_THAT(leader_batches(model::wasm_binaries_internal_ntp), SizeIs(1));
    auto [key, offset] = stored.value();
    auto loaded = load_wasm_binary(offset);
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded.value(), wasm_binary);
    auto ec = delete_wasm_binary(key);
    EXPECT_EQ(ec, cluster::errc::success)
      << cluster::error_category().message(int(ec));
}

struct TransformRpcTestParams {
    int num_transform_topic_partitions;
    bool shuffle_leadership;
    friend std::ostream&
    operator<<(std::ostream& os, const TransformRpcTestParams& tp) {
        return os << "{partitions: " << tp.num_transform_topic_partitions
                  << " shuffle_leadership: " << tp.shuffle_leadership << "}";
    }
};

class TransformRpcTestFixture
  : public WasmClusterFixture
  , public ::testing::TestWithParam<TransformRpcTestParams> {
public:
    void shuffle_transform_offsets_leaders() {
        for (auto id = 0; id < GetParam().num_transform_topic_partitions;
             id++) {
            shuffle_leadership(
              {model::kafka_internal_namespace,
               model::transform_offsets_topic,
               id});
        }
    }
};

TEST_P(TransformRpcTestFixture, TestTransformOffsetRPCs) {
    constexpr size_t num_brokers = 3;
    for (int i = 0; i < num_brokers; i++) {
        create_node_application(model::node_id{i});
    }
    wait_for_all_members(5s).get();
    create_transform_offsets_topic(GetParam().num_transform_topic_partitions);
    constexpr size_t num_transforms = 50;
    constexpr size_t num_src_partitions = 100;

    auto& client = transforms_client(model::node_id{0}).local();
    for (int i = 0; i < num_transforms; i++) {
        auto request_key = model::transform_offsets_key{};
        request_key.id = model::transform_id{i};
        for (int32_t j = 0; j < num_src_partitions; j++) {
            request_key.partition = model::partition_id{j};
            auto request_val = model::transform_offsets_value{
              .offset = kafka::offset{j}};
            auto result = client.offset_commit(request_key, request_val).get();
            ASSERT_EQ(result, cluster::errc::success)
              << "request (" << i << "," << j << ")";
            // issue occastional leadership changes and ensure fetch sees a
            // consistent state.
            if (
              GetParam().shuffle_leadership
              && random_generators::get_int(100) <= 10) {
                shuffle_transform_offsets_leaders();
            }
            auto read_result = client.offset_fetch(request_key).get();
            ASSERT_TRUE(!read_result.has_error());
            ASSERT_EQ(read_result.value().offset, request_val.offset);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
  WorksLocallyAndRemotely,
  TransformRpcTest,
  ::testing::Values(
    test_parameters{.leader_node = self_node, .non_leader_node = other_node},
    test_parameters{.leader_node = other_node, .non_leader_node = self_node}));

INSTANTIATE_TEST_SUITE_P(
  TransformRpcSingleAndMultiPartitions,
  TransformRpcTestFixture,
  ::testing::Values(
    TransformRpcTestParams{
      .num_transform_topic_partitions = 1, .shuffle_leadership = false},
    TransformRpcTestParams{
      .num_transform_topic_partitions = 3, .shuffle_leadership = true},
    TransformRpcTestParams{
      .num_transform_topic_partitions = 3, .shuffle_leadership = false}));

} // namespace transform::rpc
