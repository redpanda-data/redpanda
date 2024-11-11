#pragma once

#include "kafka/data/rpc/client.h"
#include "kafka/data/rpc/deps.h"
#include "kafka/data/rpc/service.h"
#include "model/record.h"
#include "model/tests/random_batch.h"

namespace kafka::data::rpc::test {

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

struct produced_batch {
    model::ntp ntp;
    model::record_batch batch;
};

class in_memory_proxy : public kafka::partition_proxy::impl {
public:
    in_memory_proxy(
      const model::ktp& ktp, ss::chunked_fifo<produced_batch>* produced_batches)
      : _ntp(ktp.to_ntp())
      , _produced_batches(produced_batches) {}
    in_memory_proxy(
      model::ntp ntp, ss::chunked_fifo<produced_batch>* produced_batches)
      : _ntp(std::move(ntp))
      , _produced_batches(produced_batches) {}

    const model::ntp& ntp() const final { return _ntp; }
    ss::future<result<model::offset, kafka::error_code>>
    sync_effective_start(model::timeout_clock::duration) final {
        throw std::runtime_error("unimplemented");
    }
    model::offset local_start_offset() const final {
        throw std::runtime_error("unimplemented");
    }
    model::offset start_offset() const final {
        throw std::runtime_error("unimplemented");
    }
    model::offset high_watermark() const final {
        throw std::runtime_error("unimplemented");
    }
    checked<model::offset, kafka::error_code> last_stable_offset() const final {
        throw std::runtime_error("unimplemented");
    }
    kafka::leader_epoch leader_epoch() const final {
        throw std::runtime_error("unimplemented");
    }
    ss::future<std::optional<model::offset>>
    get_leader_epoch_last_offset(kafka::leader_epoch) const final {
        throw std::runtime_error("unimplemented");
    }
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
    ss::future<std::vector<model::tx_range>> aborted_transactions(
      model::offset,
      model::offset,
      ss::lw_shared_ptr<const storage::offset_translator_state>) final {
        throw std::runtime_error("unimplemented");
    }
    ss::future<kafka::error_code> validate_fetch_offset(
      model::offset, bool, model::timeout_clock::time_point) final {
        throw std::runtime_error("unimplemented");
    }

    ss::future<result<model::offset>>
    replicate(model::record_batch_reader rdr, raft::replicate_options) final {
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

    void update_topic_cfg(const cluster::topic_properties_update& update) {
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
        case cluster::none:
            return;
        case cluster::set:
            config.properties.batch_max_bytes
              = update.properties.batch_max_bytes.value;
            break;
        case cluster::remove:
            config.properties.batch_max_bytes.reset();
            break;
        }
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

class fake_topic_creator : public kafka::data::rpc::topic_creator {
public:
    fake_topic_creator(
      ss::noncopyable_function<void(const cluster::topic_configuration&)>
        new_topic_cb,
      ss::noncopyable_function<void(const cluster::topic_properties_update&)>
        update_topic_cb,
      ss::noncopyable_function<void(const model::ntp&, model::node_id)>
        new_ntp_cb)
      : _new_topic_cb(std::move(new_topic_cb))
      , _update_topic_cb(std::move(update_topic_cb))
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

    ss::future<cluster::errc>
    update_topic(cluster::topic_properties_update update) override {
        _update_topic_cb(update);
        co_return cluster::errc::success;
    }

    void set_default_new_topic_leader(model::node_id node_id) {
        _default_new_topic_leader = node_id;
    }

private:
    model::node_id _default_new_topic_leader;
    ss::noncopyable_function<void(const cluster::topic_configuration&)>
      _new_topic_cb;
    ss::noncopyable_function<void(const cluster::topic_properties_update&)>
      _update_topic_cb;
    ss::noncopyable_function<void(const model::ntp&, model::node_id)>
      _new_ntp_cb;
};

class fake_partition_manager : public partition_manager {
public:
    std::optional<ss::shard_id> shard_owner(const model::ktp& ktp) override {
        auto it = _shard_locations.find(ktp);
        if (it == _shard_locations.end()) {
            return std::nullopt;
        }
        return it->second;
    };
    std::optional<ss::shard_id> shard_owner(const model::ntp& ntp) override {
        auto it = _shard_locations.find(ntp);
        if (it == _shard_locations.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    void set_errors(int n) { _errors_to_inject = n; }

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

    ss::future<result<model::offset, cluster::errc>> invoke_on_shard(
      ss::shard_id shard_id,
      const model::ktp& ktp,
      ss::noncopyable_function<ss::future<result<model::offset, cluster::errc>>(
        kafka::partition_proxy*)> fn) final {
        return invoke_on_shard_impl(shard_id, ktp, std::move(fn));
    }
    ss::future<result<model::offset, cluster::errc>> invoke_on_shard(
      ss::shard_id shard_id,
      const model::ntp& ntp,
      ss::noncopyable_function<ss::future<result<model::offset, cluster::errc>>(
        kafka::partition_proxy*)> fn) final {
        return invoke_on_shard_impl(shard_id, ntp, std::move(fn));
    }

private:
    template<typename R, typename N>
    ss::future<result<R, cluster::errc>> invoke_on_shard_impl(
      ss::shard_id shard_id,
      const N& ntp,
      ss::noncopyable_function<
        ss::future<result<R, cluster::errc>>(kafka::partition_proxy*)> fn) {
        auto owner = shard_owner(ntp);
        if (!owner || shard_id != *owner) {
            co_return cluster::errc::not_leader;
        }
        if (_errors_to_inject > 0) {
            --_errors_to_inject;
            co_return cluster::errc::timeout;
        }
        auto pp = kafka::partition_proxy(
          std::make_unique<in_memory_proxy>(ntp, &_produced_batches));
        co_return co_await fn(&pp);
    }
    int _errors_to_inject = 0;
    ss::chunked_fifo<produced_batch> _produced_batches;
    model::ntp_map_type<ss::shard_id> _shard_locations;
};

class kafka_data_test_fixture {
public:
    explicit kafka_data_test_fixture(
      model::node_id sn,
      ss::sharded<::rpc::connection_cache>* cc,
      model::node_id on)
      : self_node(sn)
      , _conn_cache(cc)
      , other_node(on) {}
    ~kafka_data_test_fixture() = default;

    kafka_data_test_fixture(const kafka_data_test_fixture&) = delete;
    kafka_data_test_fixture& operator=(const kafka_data_test_fixture&) = delete;
    kafka_data_test_fixture(kafka_data_test_fixture&&) = delete;
    kafka_data_test_fixture operator=(kafka_data_test_fixture&&) = delete;

    void wire_up_and_start();

    void
    register_services(std::vector<std::unique_ptr<::rpc::service>>& services);

    fake_topic_metadata_cache* remote_metadata_cache() { return _remote_ftmc; }
    fake_partition_manager* remote_partition_manager() { return _remote_fpm; }
    fake_topic_metadata_cache* local_metadata_cache() { return _local_ftmc; }
    fake_partition_manager* local_partition_manager() { return _local_fpm; }
    fake_partition_leader_cache* partition_leader_cache() { return _fplc; }

    void elect_leader(const model::ntp& ntp, model::node_id node_id);

    ss::sharded<client>& client() { return _client; }

    void reset();

private:
    fake_topic_metadata_cache* _local_ftmc = nullptr;
    fake_partition_manager* _local_fpm = nullptr;
    fake_topic_metadata_cache* _remote_ftmc = nullptr;
    fake_partition_manager* _remote_fpm = nullptr;
    fake_partition_leader_cache* _fplc = nullptr;
    fake_topic_creator* _ftpc = nullptr;
    ss::sharded<local_service> _local_services;
    ss::sharded<local_service> _remote_services;
    ss::sharded<kafka::data::rpc::client> _client;

    model::node_id self_node;
    ss::sharded<::rpc::connection_cache>* _conn_cache;
    model::node_id other_node;
};

} // namespace kafka::data::rpc::test
