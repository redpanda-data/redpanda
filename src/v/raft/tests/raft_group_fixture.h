/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "config/configuration.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "net/dns.h"
#include "net/server.h"
#include "raft/consensus.h"
#include "raft/consensus_client_protocol.h"
#include "raft/heartbeat_manager.h"
#include "raft/rpc_client_protocol.h"
#include "raft/service.h"
#include "random/generators.h"
#include "rpc/backoff_policy.h"
#include "rpc/connection_cache.h"
#include "rpc/rpc_server.h"
#include "rpc/types.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/ntp_config.h"
#include "storage/record_batch_builder.h"
#include "storage/types.h"
#include "test_utils/fixture.h"

#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/btree_map.h>
#include <boost/range/iterator_range_core.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <fmt/core.h>

#include <chrono>
#include <vector>

inline ss::logger tstlog("raft_test");

using namespace std::chrono_literals; // NOLINT

inline static std::chrono::milliseconds heartbeat_interval = 40ms;
inline static const raft::replicate_options
  default_replicate_opts(raft::consistency_level::quorum_ack);

using consensus_ptr = ss::lw_shared_ptr<raft::consensus>;
struct test_raft_manager {
    consensus_ptr consensus_for(raft::group_id) { return c; };
    consensus_ptr c = nullptr;
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
    using leader_clb_t
      = ss::noncopyable_function<void(raft::leadership_status)>;

    raft_node(
      model::ntp ntp,
      model::broker broker,
      raft::group_id gr_id,
      raft::group_configuration cfg,
      raft::timeout_jitter jit,
      ss::sstring storage_dir,
      leader_clb_t l_clb,
      model::cleanup_policy_bitflags cleanup_policy,
      size_t segment_size)
      : broker(std::move(broker))
      , leader_callback(std::move(l_clb))
      , recovery_mem_quota([] {
          return raft::recovery_memory_quota::configuration{
            .max_recovery_memory = config::mock_binding<std::optional<size_t>>(
              std::nullopt),
            .default_read_buffer_size = config::mock_binding(512_KiB),
          };
      }) {
        feature_table.start().get();
        feature_table
          .invoke_on_all(
            [](features::feature_table& f) { f.testing_activate_all(); })
          .get();

        as_service.start().get();

        cache.start(std::ref(as_service)).get();

        storage
          .start(
            [storage_dir]() {
                return storage::kvstore_config(
                  1_MiB,
                  config::mock_binding(10ms),
                  storage_dir,
                  storage::make_sanitized_file_config());
            },
            [storage_dir, segment_size]() {
                return storage::log_config(
                  storage_dir,
                  segment_size,
                  ss::default_priority_class(),
                  storage::make_sanitized_file_config());
            },
            std::ref(feature_table))
          .get();
        storage.invoke_on_all(&storage::api::start).get();

        storage::ntp_config::default_overrides overrides;

        overrides.cleanup_policy_bitflags = cleanup_policy;
        overrides.compaction_strategy = model::compaction_strategy::offset;

        storage::ntp_config ntp_cfg = storage::ntp_config(
          std::move(ntp),
          storage.local().log_mgr().config().base_dir,
          std::make_unique<storage::ntp_config::default_overrides>(
            std::move(overrides)));

        log = storage.local().log_mgr().manage(std::move(ntp_cfg)).get0();

        recovery_throttle
          .start(
            ss::sharded_parameter([] {
                return config::shard_local_cfg()
                  .raft_learner_recovery_rate.bind();
            }),
            ss::sharded_parameter([] {
                return config::shard_local_cfg()
                  .raft_recovery_throttle_disable_dynamic_mode.bind();
            }))
          .get();

        // setup consensus
        auto self_id = this->broker.id();
        consensus = ss::make_lw_shared<raft::consensus>(
          self_id,
          gr_id,
          std::move(cfg),
          std::move(jit),
          log,
          raft::scheduling_config(
            seastar::default_scheduling_group(),
            seastar::default_priority_class()),
          config::mock_binding<std::chrono::milliseconds>(10s),
          raft::make_rpc_client_protocol(self_id, cache),
          [this](raft::leadership_status st) { leader_callback(st); },
          storage.local(),
          recovery_throttle.local(),
          recovery_mem_quota,
          feature_table.local(),
          std::nullopt);
    }

    raft_node(const raft_node&) = delete;
    raft_node& operator=(const raft_node&) = delete;
    raft_node(raft_node&&) noexcept = delete;
    raft_node& operator=(raft_node&&) noexcept = delete;

    void start() {
        tstlog.info("Starting node {} stack ", id());
        // start rpc
        net::server_configuration scfg("raft_test_rpc");
        scfg.addrs.emplace_back(net::resolve_dns(broker.rpc_address()).get());
        scfg.max_service_memory_per_core = 1024 * 1024 * 1024;
        scfg.disable_metrics = net::metrics_disabled::yes;
        scfg.disable_public_metrics = net::public_metrics_disabled::yes;
        server.start(std::move(scfg)).get0();
        raft_manager.start().get0();
        raft_manager
          .invoke_on(0, [this](test_raft_manager& mgr) { mgr.c = consensus; })
          .get0();
        server
          .invoke_on_all([this](rpc::rpc_server& s) {
              s.register_service<raft::service<test_raft_manager, raft_node>>(
                ss::default_scheduling_group(),
                ss::default_smp_service_group(),
                raft_manager,
                *this,
                heartbeat_interval);
          })
          .get0();
        server.invoke_on_all(&rpc::rpc_server::start).get0();
        hbeats = std::make_unique<raft::heartbeat_manager>(
          config::mock_binding<std::chrono::milliseconds>(
            std::chrono::milliseconds(heartbeat_interval)),
          raft::make_rpc_client_protocol(broker.id(), cache),
          broker.id(),
          config::mock_binding<std::chrono::milliseconds>(
            heartbeat_interval * 20));
        hbeats->start().get0();
        hbeats->register_group(consensus).get();
        started = true;
        consensus->start().get0();
    }

    ss::future<> stop_node() {
        if (!started) {
            return ss::make_ready_future<>();
        }

        tstlog.info("Stopping node stack {}", broker.id());
        _as.request_abort();
        auto abort_f = as_service.invoke_on_all(
          [](auto& local) { local.request_abort(); });
        return std::move(abort_f)
          .then([this] { return recovery_throttle.stop(); })
          .then([this] { return server.stop(); })
          .then([this] {
              if (hbeats) {
                  tstlog.info("Stopping heartbets manager at {}", broker.id());
                  return hbeats->deregister_group(consensus->group())
                    .then([this] { return hbeats->stop(); });
              }
              return ss::make_ready_future<>();
          })
          .then([this] {
              if (hbeats) {
                  hbeats.reset();
              }
          })
          .then([this] {
              tstlog.info("Stopping raft at {}", broker.id());
              return consensus->stop();
          })
          .then([this] {
              if (kill_eviction_stm_cb) {
                  return (*kill_eviction_stm_cb)().then(
                    [this] { kill_eviction_stm_cb = nullptr; });
              }
              return ss::now();
          })
          .then([this] {
              tstlog.info(
                "Raft stopped at node {} {}",
                broker.id(),
                consensus.use_count());
              return raft_manager.stop();
          })
          .then([this] {
              tstlog.info(
                "consensus destroyed at node {} {}",
                broker.id(),
                consensus.use_count());
              consensus = nullptr;
              return ss::now();
          })
          .then([this] {
              tstlog.info("Stopping cache at node {}", broker.id());
              return cache.stop();
          })
          .then([this] {
              tstlog.info("Stopping storage at node {}", broker.id());
              log = nullptr;
              return storage.stop();
          })
          .then([this] { return feature_table.stop(); })
          .then([this] { return as_service.stop(); })
          .then([this] {
              tstlog.info("Node {} stopped", broker.id());
              started = false;
          });
    }

    std::optional<ss::shard_id> shard_for(raft::group_id) {
        return ss::shard_id(0);
    }

    ss::future<log_t> read_log() {
        auto max_offset = model::offset(consensus->last_visible_index());
        auto lstats = log->offsets();

        storage::log_reader_config cfg(
          lstats.start_offset, max_offset, ss::default_priority_class());

        return log->make_reader(cfg).then(
          [this, max_offset](model::record_batch_reader rdr) {
              tstlog.debug(
                "Reading logs from {} max offset {}, log offsets {}",
                id(),
                max_offset,
                log->offsets());
              return std::move(rdr).consume(
                consume_to_vector{}, model::no_timeout);
          });
    }

    model::node_id id() { return broker.id(); }

    void
    create_connection_to(model::node_id self, const model::broker& broker) {
        for (ss::shard_id i = 0; i < ss::smp::count; ++i) {
            auto sh = rpc::connection_cache::shard_for(self, i, broker.id());
            cache
              .invoke_on(
                sh,
                [&broker](rpc::connection_cache& c) {
                    if (c.contains(broker.id())) {
                        return seastar::make_ready_future<>();
                    }

                    return c.emplace(
                      broker.id(),
                      {.server_addr = broker.rpc_address(),
                       .disable_metrics = net::metrics_disabled::yes},
                      rpc::make_exponential_backoff_policy<rpc::clock_type>(
                        std::chrono::milliseconds(1),
                        std::chrono::milliseconds(1)));
                })
              .get0();
        }
    }

    bool started = false;
    model::broker broker;
    ss::sharded<storage::api> storage;
    ss::sharded<raft::coordinated_recovery_throttle> recovery_throttle;
    ss::shared_ptr<storage::log> log;
    ss::sharded<ss::abort_source> as_service;
    ss::sharded<rpc::connection_cache> cache;
    ss::sharded<rpc::rpc_server> server;
    ss::sharded<test_raft_manager> raft_manager;
    std::unique_ptr<ss::noncopyable_function<ss::future<>()>>
      kill_eviction_stm_cb;
    leader_clb_t leader_callback;
    raft::recovery_memory_quota recovery_mem_quota;
    std::unique_ptr<raft::heartbeat_manager> hbeats;
    consensus_ptr consensus;
    ss::sharded<features::feature_table> feature_table;
    ss::abort_source _as;
};

inline model::ntp node_ntp(raft::group_id gr_id, model::node_id n_id) {
    return model::ntp(
      model::kafka_namespace,
      model::topic(ssx::sformat("group_{}", gr_id())),
      model::partition_id(n_id()));
}

struct raft_group {
    using members_t = std::unordered_map<model::node_id, raft_node>;
    using logs_t = std::unordered_map<model::node_id, raft_node::log_t>;

    raft_group(
      raft::group_id id,
      int size,
      model::cleanup_policy_bitflags cleanup_policy
      = model::cleanup_policy_bitflags::deletion,
      size_t segment_size = 100_MiB)
      : _id(id)
      , _storage_dir("test.raft." + random_generators::gen_alphanum_string(6))
      , _cleanup_policy(cleanup_policy)
      , _segment_size(segment_size) {
        std::vector<model::broker> brokers;
        for (auto i : boost::irange(0, size)) {
            _initial_brokers.push_back(make_broker(model::node_id(i)));
        }
    };

    consensus_ptr member_consensus(model::node_id node) {
        return _members.find(node)->second.consensus;
    }

    model::broker make_broker(model::node_id id) {
        return model::broker(
          model::node_id(id),
          net::unresolved_address("localhost", 9092),
          net::unresolved_address("localhost", base_port + id),
          std::nullopt,
          model::broker_properties{
            .cores = 1,
          });
    }

    void enable_node(model::node_id node_id) {
        auto ntp = node_ntp(_id, node_id);
        auto broker = make_broker(node_id);
        tstlog.info("Enabling node {} in group {}", node_id, _id);
        auto [it, _] = _members.try_emplace(
          node_id,
          ntp,
          broker,
          _id,
          get_raft_cfg(),
          raft::timeout_jitter(heartbeat_interval * 10),
          ssx::sformat("{}/{}", _storage_dir, node_id()),
          [this, node_id](raft::leadership_status st) {
              election_callback(node_id, st);
          },
          _cleanup_policy,
          _segment_size);
        it->second.start();
        for (auto& [_, n] : _members) {
            n.create_connection_to(n.id(), broker);
            it->second.create_connection_to(broker.id(), n.broker);
        }
    }

    model::broker create_new_node(model::node_id node_id) {
        auto ntp = node_ntp(_id, node_id);
        auto broker = make_broker(node_id);
        tstlog.info("Enabling node {} in group {}", node_id, _id);
        auto [it, _] = _members.try_emplace(
          node_id,
          ntp,
          broker,
          _id,
          raft::group_configuration(
            std::vector<raft::vnode>{}, model::revision_id(0)),
          raft::timeout_jitter(heartbeat_interval * 10),
          ssx::sformat("{}/{}", _storage_dir, node_id()),
          [this, node_id](raft::leadership_status st) {
              election_callback(node_id, st);
          },
          _cleanup_policy,
          _segment_size);
        it->second.start();

        for (auto& [_, n] : _members) {
            n.create_connection_to(n.id(), broker);
            it->second.create_connection_to(broker.id(), n.broker);
        }

        return broker;
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

    std::optional<model::node_id> get_leader_id() { return _leader_id; }

    ss::future<model::node_id> wait_for_leader() {
        if (_leader_id) {
            auto it = _members.find(*_leader_id);
            if (
              it != _members.end()
              && it->second.consensus->is_elected_leader()) {
                return ss::make_ready_future<model::node_id>(*_leader_id);
            }
            _leader_id = std::nullopt;
        }

        return _election_cond
          .wait(
            ss::semaphore::clock::now() + 5s,
            [this] { return _leader_id.has_value(); })
          .then([this] { return *_leader_id; });
    }

    void election_callback(model::node_id src, raft::leadership_status st) {
        if (
          !st.current_leader
          || (st.current_leader && st.current_leader->id() != src)) {
            // only accept election callbacks from current leader.
            return;
        }
        tstlog.info(
          "Group {} has new leader {}", st.group, st.current_leader.value());

        _leader_id = st.current_leader->id();
        _election_cond.broadcast();
        _elections_count++;
    }

    ss::future<> wait_for_election(ss::semaphore::time_point tout) {
        return _election_cond.wait(tout);
    }

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
        ss::when_all_succeed(close_futures.begin(), close_futures.end()).get0();
    }

    members_t& get_members() { return _members; }

    raft_node& get_member(model::node_id n) { return _members.find(n)->second; }

    uint32_t get_elections_count() const { return _elections_count; }

    const ss::sstring& get_data_dir() const { return _storage_dir; }

    raft::group_configuration get_raft_cfg() {
        std::vector<raft::vnode> nodes;
        nodes.reserve(_initial_brokers.size());
        for (auto& b : _initial_brokers) {
            nodes.emplace_back(b.id(), model::revision_id(0));
        }

        return {std::move(nodes), model::revision_id(0)};
    }

private:
    uint16_t base_port = 35000;
    raft::group_id _id;
    members_t _members;
    std::vector<model::broker> _initial_brokers;
    ss::condition_variable _election_cond;
    std::optional<model::node_id> _leader_id;
    uint32_t _elections_count{0};
    ss::sstring _storage_dir;
    model::cleanup_policy_bitflags _cleanup_policy;
    size_t _segment_size;
};

inline model::record_batch_reader random_batches_reader(int max_batches) {
    auto batches = model::test::make_random_batches(
      model::test::record_batch_spec{
        .offset = model::offset(0),
        .allow_compression = true,
        .count = max_batches});
    return model::make_memory_record_batch_reader(std::move(batches));
}

inline model::record_batch_reader
random_batch_reader(model::test::record_batch_spec spec) {
    auto batch = model::test::make_random_batch(spec);
    ss::circular_buffer<model::record_batch> batches;
    batches.reserve(1);
    batch.set_term(model::term_id(0));
    batches.push_back(std::move(batch));
    return model::make_memory_record_batch_reader(std::move(batches));
}

inline model::record_batch_reader
random_batches_reader(model::test::record_batch_spec spec) {
    auto batches = model::test::make_random_batches(spec);
    return model::make_memory_record_batch_reader(std::move(batches));
}

template<typename Rep, typename Period, typename Pred>
inline void wait_for(
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

inline void assert_at_most_one_leader(raft_group& gr) {
    std::unordered_map<long, int> leaders_per_term;
    for (auto& [_, m] : gr.get_members()) {
        auto term = static_cast<long>(m.consensus->term());
        if (auto it = leaders_per_term.find(term);
            it == leaders_per_term.end()) {
            leaders_per_term.try_emplace(term, 0);
        }
        auto it = leaders_per_term.find(m.consensus->term());
        it->second += m.consensus->is_elected_leader();
    }
    for (auto& [term, leaders] : leaders_per_term) {
        BOOST_REQUIRE_LE(leaders, 1);
    }
}

inline bool are_logs_the_same_length(const raft_group::logs_t& logs) {
    // if one of the logs is empty all of them have to be empty
    auto empty = std::all_of(
      logs.cbegin(), logs.cend(), [](const raft_group::logs_t::value_type& p) {
          return p.second.empty();
      });

    if (empty) {
        return true;
    }
    if (logs.begin()->second.empty()) {
        return false;
    }
    auto last_offset = logs.begin()->second.back().last_offset();

    return std::all_of(logs.begin(), logs.end(), [last_offset](auto& p) {
        return !p.second.empty()
               && p.second.back().last_offset() == last_offset;
    });
}

inline bool are_all_commit_indexes_the_same(raft_group& gr) {
    auto c_idx = gr.get_members().begin()->second.consensus->committed_offset();
    return std::all_of(
      gr.get_members().begin(),
      gr.get_members().end(),
      [c_idx](raft_group::members_t::value_type& n) {
          auto current = model::offset(n.second.consensus->committed_offset());
          auto log_offset = n.second.log->offsets().dirty_offset;
          return c_idx == current && log_offset == c_idx;
      });
}

inline bool are_all_consumable_offsets_are_the_same(raft_group& gr) {
    auto c_idx
      = gr.get_members().begin()->second.consensus->last_visible_index();
    return std::all_of(
      gr.get_members().begin(),
      gr.get_members().end(),
      [c_idx](raft_group::members_t::value_type& n) {
          auto current = model::offset(
            n.second.consensus->last_visible_index());
          auto log_offset = n.second.log->offsets().dirty_offset;
          return c_idx == current && log_offset == c_idx;
      });
}

inline bool are_logs_equivalent(
  const std::vector<model::record_batch>& a,
  const std::vector<model::record_batch>& b) {
    // both logs are empty - this is ok
    if (a.empty() && b.empty()) {
        return true;
    }
    // one of the logs is empty while second has batches
    if (a.empty() || b.empty()) {
        return false;
    }
    auto [a_it, b_it] = std::mismatch(
      a.rbegin(), a.rend(), b.rbegin(), b.rend());

    return a_it == a.rbegin() || b_it == b.rbegin();
}

inline bool assert_all_logs_are_the_same(const raft_group::logs_t& logs) {
    auto& reference = logs.begin()->second;

    return std::all_of(
      std::next(logs.cbegin()),
      logs.cend(),
      [&reference](const raft_group::logs_t::value_type& p) {
          return !are_logs_equivalent(reference, p.second);
      });
}

inline void validate_logs_replication(raft_group& gr) {
    auto logs = gr.read_all_logs();
    wait_for(
      10s,
      [&gr, &logs] {
          logs = gr.read_all_logs();
          return are_logs_the_same_length(logs);
      },
      "Logs are replicated");

    assert_all_logs_are_the_same(logs);
}

template<typename T>
model::timeout_clock::time_point timeout(std::chrono::duration<T> d) {
    return model::timeout_clock::now() + d;
}

inline model::node_id wait_for_group_leader(raft_group& gr) {
    auto leader_id = gr.wait_for_leader().get0();
    assert_at_most_one_leader(gr);
    return leader_id;
}

inline void
assert_stable_leadership(const raft_group& gr, int number_of_intervals = 5) {
    auto before = gr.get_elections_count();
    ss::sleep(heartbeat_interval * number_of_intervals).get0();
    BOOST_TEST(
      before = gr.get_elections_count(),
      "Group leadership is required to be stable");
}

template<typename Func>
ss::future<bool>
do_with_leader(raft_group& gr, model::timeout_clock::duration tout, Func&& f) {
    return gr.wait_for_leader().then(
      [&gr, f, tout](model::node_id leader_id) mutable {
          auto fut = ss::futurize_invoke(f, gr.get_member(leader_id));

          return ss::with_timeout(
                   model::timeout_clock::now() + tout, std::move(fut))
            .handle_exception([](const std::exception_ptr&) { return false; });
      });
}

template<typename Func>
ss::future<bool> retry_with_leader(
  raft_group& gr,
  int max_retries,
  model::timeout_clock::duration tout,
  Func&& f) {
    struct retry_meta {
        int current_retry = 0;
        bool success = false;
    };
    auto meta = ss::make_lw_shared<retry_meta>();

    return ss::do_until(
             [meta, max_retries] {
                 return meta->success || meta->current_retry >= max_retries;
             },
             [meta, f, &gr, tout]() mutable {
                 tstlog.info(
                   "Leader action - retry attempt: {}", meta->current_retry);
                 return do_with_leader(gr, tout, f)
                   .then([meta](bool success) mutable {
                       meta->current_retry++;
                       meta->success = success;
                   })
                   .handle_exception([meta](const std::exception_ptr&) {
                       meta->success = false;
                       meta->current_retry++;
                   })
                   .then([meta] {
                       if (!meta->success) {
                           return ss::sleep(200ms * meta->current_retry);
                       }
                       return ss::now();
                   });
             })
      .then([meta] { return meta->success; });
}

inline ss::future<bool> replicate_random_batches(
  raft_group& gr,
  int count,
  raft::consistency_level c_lvl = raft::consistency_level::quorum_ack,
  model::timeout_clock::duration tout = 1s) {
    return retry_with_leader(
      gr, 5, tout, [count, c_lvl](raft_node& leader_node) {
          auto rdr = random_batches_reader(count);
          raft::replicate_options opts(c_lvl);

          return leader_node.consensus->replicate(std::move(rdr), opts)
            .then([](result<raft::replicate_result> res) {
                if (!res) {
                    return false;
                }
                return true;
            });
      });
}

inline ss::future<bool> replicate_random_batches(
  model::term_id expected_term,
  raft_group& gr,
  int count,
  raft::consistency_level c_lvl = raft::consistency_level::quorum_ack,
  model::timeout_clock::duration tout = 1s) {
    return retry_with_leader(
      gr, 5, tout, [count, expected_term, c_lvl](raft_node& leader_node) {
          auto rdr = random_batches_reader(count);
          raft::replicate_options opts(c_lvl);

          return leader_node.consensus
            ->replicate(expected_term, std::move(rdr), opts)
            .then([](result<raft::replicate_result> res) {
                if (!res) {
                    return false;
                }
                return true;
            });
      });
}

/**
 * Makes compactible batches, having one record per batch
 */
inline model::record_batch_reader
make_compactible_batches(int keys, size_t batches, model::timestamp ts) {
    ss::circular_buffer<model::record_batch> ret;
    for (size_t b = 0; b < batches; b++) {
        int k = random_generators::get_int(0, keys);
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));
        iobuf k_buf;
        iobuf v_buf;
        ss::sstring k_str = ssx::sformat("key-{}", k);
        ss::sstring v_str = ssx::sformat("key-{}-value-{}", k, b);
        reflection::serialize(k_buf, k_str);
        reflection::serialize(v_buf, v_str);
        builder.add_raw_kv(std::move(k_buf), std::move(v_buf));
        ret.push_back(std::move(builder).build());
    }
    for (auto& b : ret) {
        b.header().first_timestamp = ts;
        b.header().max_timestamp = ts;
    }
    return model::make_memory_record_batch_reader(std::move(ret));
}

inline ss::future<bool> replicate_compactible_batches(
  raft_group& gr,
  model::timestamp ts,
  model::timeout_clock::duration tout = 1s) {
    return retry_with_leader(gr, 5, tout, [ts](raft_node& leader_node) {
        auto rdr = make_compactible_batches(5, 80, ts);
        raft::replicate_options opts(raft::consistency_level::quorum_ack);

        return leader_node.consensus->replicate(std::move(rdr), opts)
          .then([](result<raft::replicate_result> res) {
              if (!res) {
                  return false;
              }
              return true;
          });
    });
}

inline void validate_offset_translation(raft_group& gr) {
    // build reference map
    if (gr.get_members().size() == 1) {
        return;
    }
    absl::btree_map<model::offset, model::offset> reference;

    auto start = gr.get_members().begin()->second.consensus->start_offset();
    auto end = gr.get_members().begin()->second.consensus->last_visible_index();

    for (auto o = start; o < end; o++) {
        reference[o] = gr.get_members()
                         .begin()
                         ->second.consensus->get_offset_translator_state()
                         ->from_log_offset(o);
    }

    for (auto it = std::next(gr.get_members().begin());
         it != gr.get_members().end();
         ++it) {
        auto start = it->second.consensus->start_offset();
        auto end = it->second.consensus->last_visible_index();

        for (auto o = start; o < end; o++) {
            // skip if we do not have this offset in reference map
            if (!reference.contains(o)) {
                continue;
            }
            auto translated = it->second.consensus
                                ->get_offset_translator_state()
                                ->from_log_offset(o);
            tstlog.info(
              "translation for offset {}, validating {} == {}\n",
              o,
              reference[o],
              translated);
            BOOST_REQUIRE_EQUAL(reference[o], translated);
        }
    }
}

struct raft_test_fixture {
    raft_test_fixture() {
        ss::smp::invoke_on_all([] {
            config::shard_local_cfg().get("disable_metrics").set_value(true);
            config::shard_local_cfg()
              .get("disable_public_metrics")
              .set_value(true);
            config::shard_local_cfg()
              .get("raft_heartbeat_disconnect_failures")
              .set_value((size_t)0);
        }).get();
    }

    virtual ~raft_test_fixture(){};

    consensus_ptr get_leader_raft(raft_group& gr) {
        auto leader_id = gr.get_leader_id();
        if (!leader_id) {
            leader_id.emplace(wait_for_group_leader(gr));
        }
        return gr.get_member(*leader_id).consensus;
    }

    uint64_t get_snapshot_size_from_disk(raft_node& node) const {
        std::filesystem::path snapshot_file_path
          = std::filesystem::path(node.log->config().work_directory())
            / storage::simple_snapshot_manager::default_snapshot_filename;
        bool snapshot_file_exists
          = ss::file_exists(snapshot_file_path.string()).get();
        if (snapshot_file_exists) {
            return ss::file_size(snapshot_file_path.string()).get();
        } else {
            return 0;
        }
    }
};
