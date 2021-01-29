/*
 * Copyright 2020 Vectorized, Inc.
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
#include "model/metadata.h"
#include "model/record_batch_reader.h"
#include "raft/consensus.h"
#include "raft/consensus_client_protocol.h"
#include "raft/heartbeat_manager.h"
#include "raft/log_eviction_stm.h"
#include "raft/rpc_client_protocol.h"
#include "raft/service.h"
#include "random/generators.h"
#include "rpc/backoff_policy.h"
#include "rpc/connection_cache.h"
#include "rpc/dns.h"
#include "rpc/server.h"
#include "rpc/simple_protocol.h"
#include "rpc/types.h"
#include "storage/log_manager.h"
#include "storage/ntp_config.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/random_batch.h"
#include "storage/types.h"
#include "test_utils/fixture.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/noncopyable_function.hh>

#include <boost/range/iterator_range_core.hpp>
#include <fmt/core.h>

#include <chrono>

inline ss::logger tstlog("raft_test");

using namespace std::chrono_literals; // NOLINT

inline static auto heartbeat_interval = 40ms;
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
      storage::log_config::storage_type storage_type,
      leader_clb_t l_clb,
      model::cleanup_policy_bitflags cleanup_policy,
      size_t segment_size)
      : broker(std::move(broker))
      , leader_callback(std::move(l_clb)) {
        cache.start().get();

        storage
          .start(
            storage::kvstore_config(
              1_MiB, 10ms, storage_dir, storage::debug_sanitize_files::yes),
            storage::log_config(
              storage_type,
              storage_dir,
              segment_size,
              storage::debug_sanitize_files::yes))
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

        log = std::make_unique<storage::log>(
          storage.local().log_mgr().manage(std::move(ntp_cfg)).get0());

        // setup consensus
        auto self_id = broker.id();
        consensus = ss::make_lw_shared<raft::consensus>(
          self_id,
          gr_id,
          std::move(cfg),
          std::move(jit),
          *log,
          seastar::default_priority_class(),
          std::chrono::seconds(10),
          raft::make_rpc_client_protocol(self_id, cache),
          [this](raft::leadership_status st) { leader_callback(st); },
          storage.local());

        // create connections to initial nodes
        consensus->config().for_each_broker(
          [this, self_id](const model::broker& broker) {
              create_connection_to(self_id, broker);
          });
    }

    raft_node(const raft_node&) = delete;
    raft_node& operator=(const raft_node&) = delete;
    raft_node(raft_node&&) noexcept = delete;
    raft_node& operator=(raft_node&&) noexcept = delete;

    void start() {
        tstlog.info("Starting node {} stack ", id());
        // start rpc
        rpc::server_configuration scfg("raft_test_rpc");
        scfg.addrs.emplace_back(rpc::resolve_dns(broker.rpc_address()).get());
        scfg.max_service_memory_per_core = 1024 * 1024 * 1024;
        scfg.credentials = nullptr;
        scfg.disable_metrics = rpc::metrics_disabled::yes;
        server.start(std::move(scfg)).get0();
        raft_manager.start().get0();
        raft_manager
          .invoke_on(0, [this](test_raft_manager& mgr) { mgr.c = consensus; })
          .get0();
        server
          .invoke_on_all([this](rpc::server& s) {
              auto proto = std::make_unique<rpc::simple_protocol>();
              proto
                ->register_service<raft::service<test_raft_manager, raft_node>>(
                  ss::default_scheduling_group(),
                  ss::default_smp_service_group(),
                  raft_manager,
                  *this);
              s.set_protocol(std::move(proto));
          })
          .get0();
        server.invoke_on_all(&rpc::server::start).get0();
        hbeats = std::make_unique<raft::heartbeat_manager>(
          heartbeat_interval,
          raft::make_rpc_client_protocol(broker.id(), cache),
          broker.id());
        hbeats->start().get0();
        hbeats->register_group(consensus).get();
        started = true;
        consensus->start().get0();
        if (log->config().is_collectable()) {
            _nop_stm = std::make_unique<raft::log_eviction_stm>(
              consensus.get(),
              tstlog,
              ss::make_lw_shared<storage::stm_manager>(),
              _as);
            _nop_stm->start().get0();
        }
    }

    ss::future<> stop_node() {
        if (!started) {
            return ss::make_ready_future<>();
        }

        tstlog.info("Stopping node stack {}", broker.id());
        _as.request_abort();
        return server.stop()
          .then([this] {
              if (hbeats) {
                  tstlog.info("Stopping heartbets manager at {}", broker.id());
                  return hbeats->deregister_group(consensus->group())
                    .then([this] { return hbeats->stop(); });
              }
              return ss::make_ready_future<>();
          })
          .then([this] {
              tstlog.info("Stopping raft at {}", broker.id());
              return consensus->stop();
          })
          .then([this] {
              if (_nop_stm != nullptr) {
                  return _nop_stm->stop();
              }
              return ss::now();
          })
          .then([this] {
              tstlog.info("Raft stopped at node {}", broker.id());
              return raft_manager.stop();
          })
          .then([this] {
              tstlog.info("Stopping cache at node {}", broker.id());
              return cache.stop();
          })
          .then([this] {
              tstlog.info("Stopping storage at node {}", broker.id());
              return storage.stop();
          })
          .then([this] {
              tstlog.info("Node {} stopped", broker.id());
              started = false;
          });
    }

    ss::shard_id shard_for(raft::group_id) { return ss::shard_id(0); }

    bool contains(raft::group_id) { return true; }

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
                [&broker, this](rpc::connection_cache& c) {
                    if (c.contains(broker.id())) {
                        return seastar::make_ready_future<>();
                    }
                    return rpc::resolve_dns(broker.rpc_address())
                      .then([this, &broker, &c](ss::socket_address addr) {
                          return c.emplace(
                            broker.id(),
                            {.server_addr = addr,
                             .disable_metrics = rpc::metrics_disabled::yes},
                            rpc::make_exponential_backoff_policy<
                              rpc::clock_type>(
                              std::chrono::milliseconds(1),
                              std::chrono::milliseconds(1)));
                      });
                })
              .get0();
        }
    }

    bool started = false;
    model::broker broker;
    ss::sharded<storage::api> storage;
    std::unique_ptr<storage::log> log;
    ss::sharded<rpc::connection_cache> cache;
    ss::sharded<rpc::server> server;
    ss::sharded<test_raft_manager> raft_manager;
    std::unique_ptr<raft::heartbeat_manager> hbeats;
    consensus_ptr consensus;
    std::unique_ptr<raft::log_eviction_stm> _nop_stm;
    leader_clb_t leader_callback;
    ss::abort_source _as;
};

static model::ntp node_ntp(raft::group_id gr_id, model::node_id n_id) {
    return model::ntp(
      model::ns("test"),
      model::topic(fmt::format("group_{}", gr_id())),
      model::partition_id(n_id()));
}

struct raft_group {
    using members_t = std::unordered_map<model::node_id, raft_node>;
    using logs_t = std::unordered_map<model::node_id, raft_node::log_t>;

    raft_group(
      raft::group_id id,
      int size,
      storage::log_config::storage_type storage_type
      = storage::log_config::storage_type::disk,
      model::cleanup_policy_bitflags cleanup_policy
      = model::cleanup_policy_bitflags::deletion,
      size_t segment_size = 100_MiB)
      : _id(id)
      , _storage_type(storage_type)
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
          unresolved_address("localhost", 9092),
          unresolved_address("localhost", base_port + id),
          std::nullopt,
          model::broker_properties{
            .cores = 1,
          });
    }

    void enable_node(model::node_id node_id) {
        auto ntp = node_ntp(_id, node_id);
        tstlog.info("Enabling node {} in group {}", node_id, _id);
        auto [it, _] = _members.try_emplace(
          node_id,
          ntp,
          make_broker(node_id),
          _id,
          raft::group_configuration(_initial_brokers, model::revision_id(0)),
          raft::timeout_jitter(heartbeat_interval * 2),
          fmt::format("{}/{}", _storage_dir, node_id()),
          _storage_type,
          [this, node_id](raft::leadership_status st) {
              election_callback(node_id, st);
          },
          _cleanup_policy,
          _segment_size);
        it->second.start();
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
          raft::group_configuration({}, model::revision_id(0)),
          raft::timeout_jitter(heartbeat_interval * 2),
          fmt::format("{}/{}", _storage_dir, node_id()),
          _storage_type,
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
            if (it != _members.end() && it->second.consensus->is_leader()) {
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
          || st.current_leader && st.current_leader->id() != src) {
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
        ss::when_all(close_futures.begin(), close_futures.end()).get0();
    }

    members_t& get_members() { return _members; }

    raft_node& get_member(model::node_id n) { return _members.find(n)->second; }

    uint32_t get_elections_count() const { return _elections_count; }

    const ss::sstring& get_data_dir() const { return _storage_dir; }

private:
    uint16_t base_port = 35000;
    raft::group_id _id;
    members_t _members;
    std::vector<model::broker> _initial_brokers;
    ss::condition_variable _election_cond;
    std::optional<model::node_id> _leader_id;
    uint32_t _elections_count{0};
    storage::log_config::storage_type _storage_type;
    ss::sstring _storage_dir;
    model::cleanup_policy_bitflags _cleanup_policy;
    size_t _segment_size;
};

static model::record_batch_reader random_batches_reader(int max_batches) {
    auto batches = storage::test::make_random_batches(
      model::offset(0), max_batches);
    return model::make_memory_record_batch_reader(std::move(batches));
}

template<typename Rep, typename Period, typename Pred>
static void wait_for(
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

static void assert_at_most_one_leader(raft_group& gr) {
    std::unordered_map<long, int> leaders_per_term;
    for (auto& [_, m] : gr.get_members()) {
        auto term = static_cast<long>(m.consensus->term());
        if (auto it = leaders_per_term.find(term);
            it == leaders_per_term.end()) {
            leaders_per_term.try_emplace(term, 0);
        }
        auto it = leaders_per_term.find(m.consensus->term());
        it->second += m.consensus->is_leader();
    }
    for (auto& [term, leaders] : leaders_per_term) {
        BOOST_REQUIRE_LE(leaders, 1);
    }
}

static bool are_logs_the_same_length(const raft_group::logs_t& logs) {
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

static bool are_all_commit_indexes_the_same(raft_group& gr) {
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

static bool are_all_consumable_offsets_are_the_same(raft_group& gr) {
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

static bool are_logs_equivalent(
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

static bool assert_all_logs_are_the_same(const raft_group::logs_t& logs) {
    auto it = logs.begin();
    auto& reference = logs.begin()->second;

    return std::all_of(
      std::next(logs.cbegin()),
      logs.cend(),
      [&reference](const raft_group::logs_t::value_type& p) {
          return !are_logs_equivalent(reference, p.second);
      });
}

static void validate_logs_replication(raft_group& gr) {
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
static model::timeout_clock::time_point timeout(std::chrono::duration<T> d) {
    return model::timeout_clock::now() + d;
}

static model::node_id wait_for_group_leader(raft_group& gr) {
    auto leader_id = gr.wait_for_leader().get0();
    assert_at_most_one_leader(gr);
    return leader_id;
}

static void
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

static ss::future<bool> replicate_random_batches(
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

static ss::future<bool> replicate_random_batches(
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
static model::record_batch_reader
make_compactible_batches(int keys, size_t batches, model::timestamp ts) {
    ss::circular_buffer<model::record_batch> ret;
    for (size_t b = 0; b < batches; b++) {
        int k = random_generators::get_int(0, keys);
        storage::record_batch_builder builder(
          raft::data_batch_type, model::offset(0));
        iobuf k_buf;
        iobuf v_buf;
        ss::sstring k_str = fmt::format("key-{}", k);
        ss::sstring v_str = fmt::format("key-{}-value-{}", k, b);
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

static ss::future<bool> replicate_compactible_batches(
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

struct raft_test_fixture {
    raft_test_fixture() {
        ss::smp::invoke_on_all([] {
            config::shard_local_cfg().get("disable_metrics").set_value(true);
        }).get();
    }

    consensus_ptr get_leader_raft(raft_group& gr) {
        auto leader_id = gr.get_leader_id();
        if (!leader_id) {
            leader_id.emplace(wait_for_group_leader(gr));
        }
        return gr.get_member(*leader_id).consensus;
    }
};
