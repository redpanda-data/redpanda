#pragma once
#include "model/record_batch_reader.h"
#include "raft/consensus.h"
#include "raft/heartbeat_manager.h"
#include "raft/service.h"
#include "random/generators.h"
#include "rpc/connection_cache.h"
#include "rpc/server.h"
#include "storage/log_manager.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/fixture.h"

#include <seastar/core/sleep.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/noncopyable_function.hh>

#include <boost/range/iterator_range_core.hpp>
#include <fmt/core.h>

inline static ss::logger tstlog("raft_test");

using namespace std::chrono_literals; // NOLINT

inline static auto heartbeat_interval = 40ms;
constexpr inline static auto default_storage_type
  = storage::log_manager::storage_type::memory;

struct test_raft_manager {
    explicit test_raft_manager(raft::consensus* ptr)
      : consensus_ptr(ptr) {}
    raft::consensus& consensus_for(raft::group_id) { return *consensus_ptr; };

    raft::consensus* consensus_ptr;
};

struct consume_to_vector {
    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        batches.push_back(std::move(b));
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    std::vector<model::record_batch> end_of_stream() {
        return std::move(batches);
    }

    std::vector<model::record_batch> batches;
};

struct raft_node {
    using log_t = std::vector<model::record_batch>;
    using consensus_ptr = ss::lw_shared_ptr<raft::consensus>;
    using leader_clb_t = ss::noncopyable_function<void(model::node_id)>;

    raft_node(
      model::broker broker,
      raft::group_id gr_id,
      raft::group_configuration cfg,
      raft::timeout_jitter jit,
      storage::log log,
      leader_clb_t l_clb)
      : broker(std::move(broker))
      , log(log)
      , consensus(ss::make_lw_shared<raft::consensus>(
          broker.id(),
          gr_id,
          std::move(cfg),
          std::move(jit),
          log,
          storage::log_append_config::fsync::yes,
          seastar::default_priority_class(),
          std::chrono::seconds(10),
          cache,
          [this](raft::group_id id) { leadership_changed(id); }))
      , leader_callback(std::move(l_clb)) {
        // init cache
        cache.start().get();
        // create connections to initial nodes
        for (const auto& broker : consensus->config().all_brokers()) {
            auto sh = rpc::connection_cache::shard_for(broker.id());
            cache
              .invoke_on(
                sh,
                [&broker, this](rpc::connection_cache& c) {
                    if (c.contains(broker.id())) {
                        return seastar::make_ready_future<>();
                    }
                    return broker.rpc_address().resolve().then(
                      [this, &broker, &c](ss::socket_address addr) {
                          return c.emplace(
                            broker.id(), {.server_addr = addr}, 1ms);
                      });
                })
              .get0();
        }
    }

    raft_node(raft_node&&) = default;

    void start() {
        tstlog.info("Starting node {} stack ", id());
        // start rpc
        server
          .start(rpc::server_configuration{
            .addrs = {broker.rpc_address().resolve().get0()},
            .max_service_memory_per_core = 1024 * 1024 * 1024,
            .credentials = std::nullopt,
            .disable_metrics = rpc::metrics_disabled::yes,
          })
          .get0();
        raft_manager.start(consensus.get()).get0();
        server
          .invoke_on_all([this](rpc::server& s) {
              return s
                .register_service<raft::service<test_raft_manager, raft_node>>(
                  ss::default_scheduling_group(),
                  ss::default_smp_service_group(),
                  raft_manager,
                  *this);
          })
          .get0();
        server.invoke_on_all(&rpc::server::start).get0();
        hbeats = std::make_unique<raft::heartbeat_manager>(
          heartbeat_interval, cache);
        hbeats->start().get0();
        consensus->start().get0();
        hbeats->register_group(consensus);
        started = true;
    }

    ss::future<> stop_node() {
        if (!started) {
            return ss::make_ready_future<>();
        }

        tstlog.info("Stopping node stack {}", broker.id());
        return server.stop()
          .then([this] {
              if (hbeats) {
                  tstlog.info("Stopping heartbets manager at {}", broker.id());
                  hbeats->deregister_group(
                    raft::group_id(consensus->meta().group));
                  return hbeats->stop();
              }
              return ss::make_ready_future<>();
          })
          .then([this] {
              tstlog.info("Stopping raft at {}", broker.id());
              return consensus->stop();
          })
          .then([this] {
              tstlog.info("Raft stopped at node {}", broker.id());
              return raft_manager.stop();
          })
          .then([this] { return cache.stop(); })
          .then([this] {
              tstlog.info("Node {} stopped", broker.id());
              started = false;
          });
    }

    void leadership_changed(raft::group_id id) { leader_callback(broker.id()); }

    ss::shard_id shard_for(raft::group_id) { return ss::shard_id(0); }

    ss::future<log_t> read_log() {
        auto max_offset = model::offset(consensus->meta().commit_index);
        storage::log_reader_config cfg{
          .start_offset = model::offset(0),
          .max_bytes = std::numeric_limits<size_t>::max(),
          .min_bytes = 1,
          .prio = ss::default_priority_class(),
          .type_filter = {},
          .max_offset = max_offset};
        return ss::do_with(
          log.make_reader(std::move(cfg)),
          [this, max_offset](model::record_batch_reader& rdr) {
              tstlog.debug(
                "Reading logs from {} max offset {}, log max offset {}",
                id(),
                max_offset,
                log.max_offset());
              return rdr.consume(consume_to_vector{}, model::no_timeout);
          });
    }

    model::node_id id() { return broker.id(); }

    bool started = false;
    model::broker broker;
    storage::log log;
    ss::sharded<rpc::connection_cache> cache;
    ss::sharded<rpc::server> server;
    ss::sharded<test_raft_manager> raft_manager;
    std::unique_ptr<raft::heartbeat_manager> hbeats;
    consensus_ptr consensus;
    leader_clb_t leader_callback;
};

model::ntp node_ntp(raft::group_id gr_id, model::node_id n_id) {
    return model::ntp{.ns = model::ns("test"),
                      .tp = model::topic_partition{
                        .topic = model::topic(fmt::format("group_{}", gr_id())),
                        .partition = model::partition_id(n_id())}};
}

struct raft_group {
    using members_t = std::unordered_map<model::node_id, raft_node>;
    using logs_t = std::unordered_map<model::node_id, raft_node::log_t>;

    raft_group(
      raft::group_id id,
      int size,
      storage::log_manager::storage_type storage_type = default_storage_type)
      : _id(id)
      , _storage_type(storage_type) {
        _log_manager
          .start(storage::log_config{
            .base_dir = "raft_test_"
                        + random_generators::gen_alphanum_string(6),
            .max_segment_size = 100 * 1024 * 1024, // 100MB
            .should_sanitize = storage::log_config::sanitize_files::yes})
          .get0();
        std::vector<model::broker> brokers;
        for (auto i : boost::irange(0, size)) {
            _initial_brokers.push_back(make_broker(model::node_id(i)));
        }
    };

    raft_node::consensus_ptr member_consensus(model::node_id node) {
        return _members.find(node)->second.consensus;
    }

    model::broker make_broker(model::node_id id) {
        return model::broker(
          model::node_id(id),
          unresolved_address("localhost", 9092),
          unresolved_address("localhost", base_port + id),
          std::nullopt,
          model::broker_properties{
            .cores = 1,
          });
    }

    void enable_node(model::node_id node_id) {
        auto ntp = node_ntp(_id, node_id);
        auto log_optional = _log_manager.local().get(ntp);
        if (!log_optional) {
            log_optional.emplace(
              _log_manager.local()
                .manage(node_ntp(_id, node_id), _storage_type)
                .get0());
        }

        tstlog.info("Enabling node {} in group {}", node_id, _id);
        auto [it, _] = _members.try_emplace(
          node_id,
          make_broker(node_id),
          _id,
          raft::group_configuration{
            .nodes = _initial_brokers,
          },
          raft::timeout_jitter(heartbeat_interval * 2),
          *log_optional,
          [this](model::node_id id) { election_callback(id); });
        it->second.start();
    }

    void disable_node(model::node_id node_id) {
        tstlog.info("Disabling node {} in group {}", node_id, _id);
        _members.find(node_id)->second.stop_node().get0();
        _members.erase(node_id);
    }

    void enable_all() {
        for (auto& br : _initial_brokers) {
            enable_node(br.id());
        }
    }

    std::optional<model::node_id> get_leader_id() {
        std::optional<model::node_id> leader_id{std::nullopt};
        raft::group_id::type leader_term = model::term_id(0);

        for (auto& [id, m] : _members) {
            if (
              m.consensus->is_leader()
              && m.consensus->meta().term > leader_term) {
                leader_id.emplace(id);
            }
        }

        return leader_id;
    }

    void election_callback(model::node_id id) {
        tstlog.info("New leader elected in group {}. Broker {}", _id, id);
        _election_sem.signal();
        _elections_count++;
    }

    void wait_for_next_election() { _election_sem.wait().get0(); }

    ss::future<std::vector<model::record_batch>>
    read_member_log(model::node_id member) {
        return _members.find(member)->second.read_log();
    }

    logs_t read_all_logs() {
        logs_t logs_map;
        for (auto& [id, m] : _members) {
            logs_map.try_emplace(id, m.read_log().get0());
        }
        return logs_map;
    }

    ~raft_group() {
        std::vector<ss::future<>> close_futures;
        for (auto& [_, m] : _members) {
            close_futures.push_back(m.stop_node());
        }
        ss::when_all(close_futures.begin(), close_futures.end()).get0();
        _log_manager.stop().get0();
    }

    members_t& get_members() { return _members; }

    raft_node& get_member(model::node_id n) { return _members.find(n)->second; }

    uint32_t get_elections_count() const { return _elections_count; }

private:
    uint16_t base_port = 35000;
    ss::sharded<storage::log_manager> _log_manager;
    raft::group_id _id;
    members_t _members;
    std::vector<model::broker> _initial_brokers;
    ss::condition_variable _leader_elected;
    ss::semaphore _election_sem{0};
    uint32_t _elections_count{0};
    storage::log_manager::storage_type _storage_type;
};

struct raft_test_fixture {
    raft_test_fixture() { configure_unit_test_logging(); }

    model::record_batch_reader random_batches_entry(int max_batches) {
        auto batches = storage::test::make_random_batches(
          model::offset(0), max_batches);
        return model::make_memory_record_batch_reader(std::move(batches));
    }

    template<typename Rep, typename Period, typename Pred>
    void wait_for(
      std::chrono::duration<Rep, Period> timeout, Pred&& p, ss::sstring msg) {
        using clock_t = std::chrono::system_clock;
        auto start = clock_t::now();
        auto res = p();
        while (!res) {
            auto elapsed = clock_t::now() - start;
            if (elapsed > timeout) {
                BOOST_FAIL(
                  fmt::format("Timeout elapsed while wating for: {}", msg));
            }
            res = p();
            ss::sleep(std::chrono::milliseconds(400)).get0();
        }
    }

    void assert_at_most_one_leader(raft_group& gr) {
        std::unordered_map<long, int> leaders_per_term;
        for (auto& [_, m] : gr.get_members()) {
            auto term = static_cast<long>(m.consensus->meta().term);
            if (auto it = leaders_per_term.find(term);
                it == leaders_per_term.end()) {
                leaders_per_term.try_emplace(term, 0);
            }
            auto it = leaders_per_term.find(m.consensus->meta().term);
            it->second += m.consensus->is_leader();
        }
        for (auto& [term, leaders] : leaders_per_term) {
            BOOST_REQUIRE_LE(leaders, 1);
        }
    }

    bool are_logs_the_same_length(const raft_group::logs_t& logs) {
        auto size = logs.begin()->second.size();
        if (size == 0) {
            return false;
        }
        for (auto& [id, l] : logs) {
            tstlog.debug("Node {} log has {} batches", id, l.size());
            if (size != l.size()) {
                return false;
            }
        }
        return true;
    }

    bool are_all_commit_indexes_the_same(raft_group& gr) {
        auto c_idx
          = gr.get_members().begin()->second.consensus->meta().commit_index;
        for (auto& [id, m] : gr.get_members()) {
            auto current = model::offset(m.consensus->meta().commit_index);
            auto log_offset = m.log.committed_offset();
            tstlog.debug(
              "Node {} commit index {}, log offset {}",
              id,
              current,
              log_offset);
            if (c_idx != current || log_offset != c_idx) {
                return false;
            }
        }
        return true;
    }

    void assert_all_logs_are_the_same(const raft_group::logs_t& logs) {
        auto it = logs.begin();
        auto& reference = logs.begin()->second;
        it++;
        while (it != logs.end()) {
            if (reference.size() != it->second.size()) {
                auto r_size = reference.size() != it->second.size();
                auto it_sz = it->second.size();
                tstlog.error("Different length {}, {}", r_size, it_sz);
            }
            for (int i = 0; i < reference.size(); ++i) {
                BOOST_REQUIRE_EQUAL(reference[i], it->second[i]);
            }
            it++;
        }
    }

    bool are_logs_the_same(const raft_group::logs_t& logs) {
        auto prev = logs.begin()->second.size();
        for (auto& [_, l] : logs) {
            if (prev != l.size()) {
                return false;
            }
        }

        return true;
    }

    void validate_logs_replication(raft_group& gr) {
        auto logs = gr.read_all_logs();
        wait_for(
          10s,
          [this, &gr, &logs] {
              logs = gr.read_all_logs();
              return are_logs_the_same_length(logs);
          },
          "Logs are replicated");

        assert_all_logs_are_the_same(logs);
    }

    model::node_id wait_for_group_leader(raft_group& gr) {
        gr.wait_for_next_election();
        assert_at_most_one_leader(gr);
        auto leader_id = gr.get_leader_id();
        while (!leader_id) {
            assert_at_most_one_leader(gr);
            gr.wait_for_next_election();
            assert_at_most_one_leader(gr);
            leader_id = gr.get_leader_id();
        }

        return leader_id.value();
    }

    void assert_stable_leadership(
      const raft_group& gr, int number_of_intervals = 5) {
        auto before = gr.get_elections_count();
        ss::sleep(heartbeat_interval * number_of_intervals).get0();
        BOOST_TEST(
          before = gr.get_elections_count(),
          "Group leadership is required to be stable");
    }

    void configure_unit_test_logging() {
        ss::global_logger_registry().set_all_loggers_level(
          ss::log_level::trace);
        ss::global_logger_registry().set_logger_level(
          "exception", ss::log_level::debug);

        ss::apply_logging_settings(ss::logging_settings{
          .logger_levels = {{"exception", ss::log_level::debug}},
          .default_level = ss::log_level::trace,
          .stdout_timestamp_style = ss::logger_timestamp_style::real});
    }
};