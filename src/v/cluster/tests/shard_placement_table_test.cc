/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/logger.h"
#include "cluster/shard_placement_table.h"
#include "features/feature_table.h"
#include "ssx/event.h"
#include "storage/kvstore.h"
#include "storage/storage_resources.h"
#include "test_utils/randoms.h"
#include "test_utils/test.h"

#include <seastar/core/reactor.hh>
#include <seastar/util/file.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace cluster {

namespace {

/// Simplified version of topic_table representing partitions that are expected
/// to exist on this node.
struct ntp_table {
    struct ntp_meta {
        raft::group_id group;
        model::revision_id log_revision;
    };

    absl::flat_hash_map<model::ntp, ntp_meta> ntp2meta;
    model::revision_id revision;
};

/// ntp2shards map is instantiated on shard 0 and is used to check invariants
/// during reconciliation.
///
/// ntp_shards struct is keyed by ntp and holds expected target shard, as well
/// as a set of all shards that have mock "shard-local kvstore state" (this
/// state is virtual and we don't really write it into the actual kvstore). This
/// set is then checked to verify that we don't leave any garbage behind when we
/// move partitions around.
///
/// partition_shards struct is keyed by {ntp, log_revision} pair (although
/// unlikely in practice, presence of several ntp instances with different log
/// revisions is possible on different shards) and used to check the following
/// invariants:
/// * At any given time only the partition is "launched" on max one shard
/// * OR there is max one xshard transfer in progress

struct partition_shards {
    std::optional<ss::shard_id> launched_on;
    std::optional<ss::shard_id> current_state_on;
    std::optional<ss::shard_id> next_state_on;

    bool empty() const {
        return !launched_on && !current_state_on && !next_state_on;
    }
};

struct ntp_shards {
    absl::flat_hash_set<ss::shard_id> shards_with_some_state;
    std::optional<shard_placement_target> target;
    absl::flat_hash_map<model::revision_id, partition_shards> rev2shards;

    bool empty() const {
        return shards_with_some_state.empty() && !target && rev2shards.empty();
    }
};

using ntp2shards_t = absl::flat_hash_map<model::ntp, ntp_shards>;

/// simplified version of controller_backend driving shard_placement_table
/// reconciliation
class reconciliation_backend
  : public ss::peering_sharded_service<reconciliation_backend> {
public:
    explicit reconciliation_backend(
      ss::sharded<ntp_table>& ntpt,
      ss::sharded<shard_placement_table>& spt,
      ss::sharded<ntp2shards_t>& ntp2shards)
      : _ntpt(ntpt.local())
      , _shard_placement(spt.local())
      , _ntp2shards(ntp2shards) {}

    ss::future<> stop() {
        for (auto& [_, rs] : _states) {
            rs->wakeup_event.set();
        }
        co_await _gate.close();
    }

    ss::future<> start() {
        for (const auto& [ntp, _] : _ntpt.ntp2meta) {
            notify_reconciliation(ntp);
        }
        co_return;
    }

    void notify_reconciliation(const model::ntp& ntp) {
        auto [rs_it, inserted] = _states.try_emplace(ntp);
        if (inserted) {
            rs_it->second = ss::make_lw_shared<ntp_reconciliation_state>();
        }
        auto& rs = *rs_it->second;
        rs.pending_notifies += 1;
        rs.wakeup_event.set();
        if (inserted) {
            ssx::background = reconcile_ntp_fiber(ntp, rs_it->second);
        }
    }

    ss::future<bool> is_reconciled() {
        auto shards_reconciled = co_await container().map(
          [](reconciliation_backend& rb) { return rb._states.empty(); });
        co_return std::all_of(
          shards_reconciled.begin(), shards_reconciled.end(), [](bool x) {
              return x;
          });
    }

private:
    struct ntp_reconciliation_state {
        size_t pending_notifies = 0;
        ssx::event wakeup_event{"c/rb/rfwe"};

        bool is_reconciled() const { return pending_notifies == 0; }

        void mark_reconciled(size_t notifies) {
            vassert(
              pending_notifies >= notifies,
              "unexpected pending_notifies: {}",
              pending_notifies);
            pending_notifies -= notifies;
        }
    };

    ss::future<> reconcile_ntp_fiber(
      model::ntp ntp, ss::lw_shared_ptr<ntp_reconciliation_state> rs) {
        if (_gate.is_closed()) {
            co_return;
        }
        auto gate_holder = _gate.hold();

        while (true) {
            co_await rs->wakeup_event.wait(
              1ms * random_generators::get_int(200, 300));
            if (_gate.is_closed()) {
                break;
            }

            try {
                co_await ss::sleep(1ms * random_generators::get_int(30));
                co_await try_reconcile_ntp(ntp, *rs);
                if (rs->is_reconciled()) {
                    _states.erase(ntp);
                    break;
                }
            } catch (...) {
                auto ex = std::current_exception();
                if (!ssx::is_shutdown_exception(ex)) {
                    vlog(
                      clusterlog.error,
                      "[{}] unexpected exception during reconciliation: {}",
                      ntp,
                      ex);
                }
            }
        }
    }

    ss::future<>
    try_reconcile_ntp(const model::ntp& ntp, ntp_reconciliation_state& rs) {
        while (!rs.is_reconciled() && !_gate.is_closed()) {
            size_t notifies = rs.pending_notifies;
            try {
                auto res = co_await reconcile_ntp_step(ntp, rs);
                if (res.has_value()) {
                    if (res.value() == ss::stop_iteration::no) {
                        continue;
                    } else {
                        vlog(
                          clusterlog.trace,
                          "[{}] reconciled, notify count: {}",
                          ntp,
                          notifies);
                        rs.mark_reconciled(notifies);
                    }
                } else {
                    vlog(
                      clusterlog.trace,
                      "[{}] reconciliation attempt error: {}",
                      ntp,
                      res.error());
                }
            } catch (ss::gate_closed_exception const&) {
            } catch (ss::abort_requested_exception const&) {
            } catch (...) {
                vlog(
                  clusterlog.warn,
                  "[{}] exception occured during reconciliation: {}",
                  ntp,
                  std::current_exception());
            }
            break;
        }
    }

    ss::future<result<ss::stop_iteration>>
    reconcile_ntp_step(const model::ntp& ntp, ntp_reconciliation_state& rs) {
        std::optional<shard_placement_table::placement_state> maybe_placement
          = _shard_placement.state_on_this_shard(ntp);
        if (!maybe_placement) {
            co_return ss::stop_iteration::yes;
        }
        auto placement = *maybe_placement;

        std::optional<model::revision_id> expected_log_revision;
        if (auto it = _ntpt.ntp2meta.find(ntp); it != _ntpt.ntp2meta.end()) {
            expected_log_revision = it->second.log_revision;
        }

        vlog(
          clusterlog.trace,
          "[{}] placement state on this shard: {}, expected_log_revision: {}",
          ntp,
          placement,
          expected_log_revision);

        switch (placement.get_reconciliation_action(expected_log_revision)) {
        case shard_placement_table::reconciliation_action::remove: {
            auto cmd_revision = expected_log_revision.value_or(_ntpt.revision);
            auto ec = co_await delete_partition(ntp, placement, cmd_revision);
            if (ec) {
                co_return ec;
            }
            co_return ss::stop_iteration::no;
        }
        case shard_placement_table::reconciliation_action::
          wait_for_target_update:
            co_return errc::waiting_for_shard_placement_update;
        case shard_placement_table::reconciliation_action::transfer: {
            auto ec = co_await transfer_partition(
              ntp,
              expected_log_revision.value(),
              placement.current.has_value());
            if (ec) {
                co_return ec;
            }
            co_return ss::stop_iteration::no;
        }
        case shard_placement_table::reconciliation_action::create: {
            if (!_launched.contains(ntp)) {
                auto ec = co_await create_partition(
                  ntp,
                  expected_log_revision.value(),
                  placement.current.has_value());
                if (ec) {
                    co_return ec;
                }
            }
            co_return ss::stop_iteration::yes;
        }
        }
    }

    ss::future<std::error_code> create_partition(
      const model::ntp& ntp,
      model::revision_id log_revision,
      bool state_expected) {
        auto ec = co_await _shard_placement.prepare_create(ntp, log_revision);
        vlog(clusterlog.trace, "[{}] creating partition: {}", ntp, ec);
        if (ec) {
            co_return ec;
        }

        _launched.insert(ntp);
        co_await ss::sleep(1ms * random_generators::get_int(30));

        co_await _ntp2shards.invoke_on(
          0,
          [ntp, log_revision, state_expected, shard = ss::this_shard_id()](
            ntp2shards_t& ntp2shards) {
              auto& shards = ntp2shards[ntp];
              auto& p_shards = shards.rev2shards[log_revision];

              vassert(
                (!state_expected && !p_shards.current_state_on)
                  || (state_expected && p_shards.current_state_on == shard),
                "[{}] unexpected current: {} (starting on: {})",
                ntp,
                p_shards.current_state_on,
                shard);

              bool inserted
                = shards.shards_with_some_state.insert(shard).second;
              if (!state_expected) {
                  vassert(
                    inserted,
                    "[{}] unexpected set contents, shard: {}, current: {}",
                    ntp,
                    shard,
                    p_shards.current_state_on);
              }
              p_shards.current_state_on = shard;

              vassert(
                !p_shards.next_state_on,
                "[{}] unexpected next: {} (starting on: {})",
                ntp,
                p_shards.next_state_on,
                shard);

              vassert(
                !p_shards.launched_on,
                "[{}] unexpected launched: {} (starting on: {})",
                ntp,
                p_shards.launched_on,
                shard);
              p_shards.launched_on = shard;
          });

        co_return errc::success;
    }

    ss::future<std::error_code> delete_partition(
      const model::ntp& ntp,
      shard_placement_table::placement_state placement,
      model::revision_id cmd_revision) {
        auto ec = co_await _shard_placement.prepare_delete(ntp, cmd_revision);
        vlog(
          clusterlog.trace,
          "[{}] deleting partition at cmd_revision: {}, ec: {}",
          ntp,
          cmd_revision,
          ec);
        if (ec) {
            co_return ec;
        }

        if (!placement.current) {
            // nothing to delete
            co_return errc::success;
        }

        bool launched_expected = _launched.erase(ntp);
        co_await ss::sleep(1ms * random_generators::get_int(30));

        co_await _ntp2shards.invoke_on(
          0,
          [ntp,
           log_revision = placement.current.value().log_revision,
           shard = ss::this_shard_id(),
           launched_expected](ntp2shards_t& ntp2shards) {
              auto& shards = ntp2shards[ntp];

              bool erased = shards.shards_with_some_state.erase(shard);
              if (launched_expected) {
                  vassert(
                    erased,
                    "[{}] unexpected set contents (deleting on: {})",
                    ntp,
                    shard);
              }

              auto& p_shards = shards.rev2shards[log_revision];

              vassert(
                (launched_expected && p_shards.launched_on == shard)
                  || (!launched_expected && !p_shards.launched_on),
                "[{}] unexpected launched: {} (shard: {}, expected: {})",
                ntp,
                p_shards.launched_on,
                shard,
                launched_expected);
              p_shards.launched_on = std::nullopt;

              if (launched_expected) {
                  vassert(
                    p_shards.current_state_on == shard,
                    "[{}] unexpected current: {} (deleting on: {})",
                    ntp,
                    p_shards.current_state_on,
                    shard);
              }
              if (p_shards.current_state_on == shard) {
                  p_shards.current_state_on = std::nullopt;
              }

              if (launched_expected) {
                  vassert(
                    !p_shards.next_state_on,
                    "[{}] unexpected next: {} (deleting on: {})",
                    ntp,
                    p_shards.next_state_on,
                    shard);
              }
              if (p_shards.next_state_on == shard) {
                  p_shards.next_state_on = std::nullopt;
              }
          });

        co_await _shard_placement.finish_delete(
          ntp, placement.current->log_revision);
        co_return ec;
    }

    ss::future<std::error_code> transfer_partition(
      const model::ntp& ntp,
      model::revision_id log_revision,
      bool state_expected) {
        auto maybe_dest = co_await _shard_placement.prepare_transfer(
          ntp, log_revision);
        if (maybe_dest.has_error()) {
            vlog(
              clusterlog.trace,
              "[{}] preparing transfer error: {}",
              ntp,
              maybe_dest.error());
            co_return maybe_dest.error();
        }

        vlog(
          clusterlog.trace,
          "[{}] preparing transfer dest: {}",
          ntp,
          maybe_dest.value());
        ss::shard_id destination = maybe_dest.value();

        bool launched_expected = _launched.erase(ntp);

        co_await _ntp2shards.invoke_on(
          0,
          [ntp,
           log_revision,
           source = ss::this_shard_id(),
           destination,
           launched_expected,
           state_expected](ntp2shards_t& ntp2shards) {
              auto& shards = ntp2shards[ntp];
              auto& p_shards = shards.rev2shards[log_revision];

              vassert(
                (launched_expected && p_shards.launched_on == source)
                  || (!launched_expected && !p_shards.launched_on),
                "[{}] unexpected launched: {} (transferring from: {}, "
                "expected: {})",
                ntp,
                p_shards.launched_on,
                source,
                launched_expected);
              p_shards.launched_on = std::nullopt;

              vassert(
                (state_expected && p_shards.current_state_on == source)
                  || (!state_expected && !p_shards.current_state_on),
                "[{}] unexpected current: {} (transferring from: {}, "
                "expected: {})",
                ntp,
                p_shards.current_state_on,
                source,
                state_expected);

              if (!state_expected) {
                  vassert(
                    !shards.shards_with_some_state.contains(source),
                    "[{}] unexpected set contents, source: {}",
                    ntp,
                    source);
              }

              vassert(
                !p_shards.next_state_on,
                "[{}] unexpected next: {} (transferring from: {})",
                ntp,
                p_shards.next_state_on,
                source);

              vassert(
                shards.shards_with_some_state.insert(destination).second,
                "[{}] unexpected set contents, destination: {}",
                ntp,
                destination);
              p_shards.next_state_on = destination;
          });

        co_await ss::sleep(1ms * random_generators::get_int(30));

        co_await _ntp2shards.invoke_on(
          0, [ntp, log_revision, destination](ntp2shards_t& ntp2shards) {
              auto& shards = ntp2shards[ntp].rev2shards[log_revision];
              vassert(
                !shards.launched_on,
                "[{}] unexpected launched: {} (transferring to: {})",
                ntp,
                shards.launched_on,
                destination);
              vassert(
                shards.next_state_on == destination,
                "[{}] unexpected next: {} (transferring to: {})",
                ntp,
                shards.next_state_on,
                destination);
              shards.current_state_on = destination;
              shards.next_state_on = std::nullopt;
          });

        co_await container().invoke_on(
          destination, [&ntp, log_revision](reconciliation_backend& dest) {
              return dest._shard_placement
                .finish_transfer_on_destination(ntp, log_revision)
                .then([&] {
                    auto it = dest._states.find(ntp);
                    if (it != dest._states.end()) {
                        it->second->wakeup_event.set();
                    }
                });
          });

        co_await _ntp2shards.invoke_on(
          0,
          [ntp, shard = ss::this_shard_id(), state_expected](
            ntp2shards_t& ntp2shards) {
              auto& ntp_shards = ntp2shards[ntp];
              bool erased = ntp_shards.shards_with_some_state.erase(shard);
              if (!state_expected) {
                  vassert(
                    !erased,
                    "[{}] unexpected set contents, source: {}",
                    ntp,
                    shard);
              }
          });

        co_await _shard_placement.finish_transfer_on_source(ntp, log_revision);
        vlog(clusterlog.trace, "[{}] transferred", ntp);
        co_return errc::success;
    }

private:
    ntp_table& _ntpt;
    shard_placement_table& _shard_placement;
    ss::sharded<ntp2shards_t>& _ntp2shards;

    absl::btree_map<model::ntp, ss::lw_shared_ptr<ntp_reconciliation_state>>
      _states;
    absl::flat_hash_set<model::ntp> _launched;
    ss::gate _gate;
};

// Limit concurrency to 4 so that there are more interesting repeats in randomly
// generated shard ids.
ss::shard_id get_max_shard_id() {
    return std::min(ss::smp::count - 1, ss::shard_id(3));
}

} // namespace

class shard_placement_test_fixture : public seastar_test {
public:
    shard_placement_test_fixture()
      : test_dir("test.data." + random_generators::gen_alphanum_string(10)) {}

    ss::future<> quiescent_state_checks() {
        auto shard2states = co_await spt.map(
          [](shard_placement_table& spt) { return spt._states; });

        absl::node_hash_map<
          model::ntp,
          std::map<ss::shard_id, shard_placement_table::placement_state>>
          ntp2shard2state;
        for (size_t s = 0; s < shard2states.size(); ++s) {
            for (const auto& [ntp, state] : shard2states[s]) {
                ntp2shard2state[ntp].emplace(s, state);
            }
        }

        ASSERT_EQ_CORO(ntp2shard2state.size(), ntpt.local().ntp2meta.size());

        auto& ntp2shards = _ntp2shards.local();
        for (auto it = ntp2shards.begin(); it != ntp2shards.end();) {
            auto it_copy = it++;

            auto& rev2shards = it_copy->second.rev2shards;
            for (auto p_it = rev2shards.begin(); p_it != rev2shards.end();) {
                auto p_it_copy = p_it++;
                if (p_it_copy->second.empty()) {
                    rev2shards.erase(p_it_copy);
                }
            }

            if (it_copy->second.empty()) {
                ntp2shards.erase(it_copy);
            }
        }

        ASSERT_EQ_CORO(ntp2shards.size(), ntpt.local().ntp2meta.size());

        for (const auto& [ntp, meta] : ntpt.local().ntp2meta) {
            auto states_it = ntp2shard2state.find(ntp);
            ASSERT_TRUE_CORO(states_it != ntp2shard2state.end())
              << "ntp: " << ntp;
            const auto& shard2state = states_it->second;

            auto target_it = spt.local()._ntp2target.find(ntp);
            ASSERT_TRUE_CORO(target_it != spt.local()._ntp2target.end())
              << "ntp: " << ntp;
            const auto& target = target_it->second;
            ASSERT_EQ_CORO(target.log_revision, meta.log_revision)
              << "ntp: " << ntp;

            auto shards_it = ntp2shards.find(ntp);
            ASSERT_TRUE_CORO(shards_it != ntp2shards.end()) << "ntp: " << ntp;
            const auto& shards = shards_it->second;
            ASSERT_TRUE_CORO(shards.target) << "ntp: " << ntp;
            ASSERT_EQ_CORO(shards.target->log_revision, meta.log_revision)
              << "ntp: " << ntp;
            ASSERT_EQ_CORO(shards.target->shard, target.shard)
              << "ntp: " << ntp;
            ASSERT_EQ_CORO(
              shards.shards_with_some_state,
              absl::flat_hash_set<ss::shard_id>({target.shard}))
              << "ntp: " << ntp;

            ASSERT_TRUE_CORO(shard2state.contains(target.shard))
              << "ntp: " << ntp << ", target shard: " << target.shard;
            for (const auto& [s, placement] : shard2state) {
                if (s == target.shard) {
                    ASSERT_TRUE_CORO(placement.current)
                      << "ntp: " << ntp << ", shard: " << s;
                    ASSERT_EQ_CORO(
                      placement.current->log_revision, meta.log_revision)
                      << "ntp: " << ntp << ", shard: " << s;
                    ASSERT_EQ_CORO(
                      placement.current->status,
                      shard_placement_table::hosted_status::hosted)
                      << "ntp: " << ntp << ", shard: " << s;
                    ASSERT_TRUE_CORO(placement.assigned)
                      << "ntp: " << ntp << ", shard: " << s;
                    ASSERT_EQ_CORO(
                      placement.assigned->log_revision, meta.log_revision)
                      << "ntp: " << ntp << ", shard: " << s;
                } else {
                    ASSERT_TRUE_CORO(!placement.current)
                      << "ntp: " << ntp << ", shard: " << s;
                    ASSERT_TRUE_CORO(!placement.assigned)
                      << "ntp: " << ntp << ", shard: " << s;
                }
            }

            ASSERT_EQ_CORO(shards.rev2shards.size(), 1) << "ntp: " << ntp;
            auto p_shards_it = shards.rev2shards.find(meta.log_revision);
            ASSERT_TRUE_CORO(p_shards_it != shards.rev2shards.end())
              << "ntp: " << ntp;
            const auto& p_shards = p_shards_it->second;
            ASSERT_EQ_CORO(p_shards.launched_on, target.shard)
              << "ntp: " << ntp;
            ASSERT_EQ_CORO(p_shards.current_state_on, target.shard)
              << "ntp: " << ntp;
            ASSERT_EQ_CORO(p_shards.next_state_on, std::nullopt)
              << "ntp: " << ntp;
        }
    }

    ss::future<> start() {
        co_await ft.start();
        co_await ft.invoke_on_all(
          [](features::feature_table& ft) { ft.testing_activate_all(); });

        co_await ntpt.start();

        co_await _ntp2shards.start_single();

        co_await sr.start();

        co_await kvs.start(
          storage::kvstore_config(
            1_MiB,
            config::mock_binding(10ms),
            test_dir,
            storage::make_sanitized_file_config()),
          ss::sharded_parameter([this] { return std::ref(sr.local()); }),
          std::ref(ft));
        co_await kvs.invoke_on_all(
          [](storage::kvstore& kvs) { return kvs.start(); });

        co_await spt.start();

        co_await rb.start(std::ref(ntpt), std::ref(spt), std::ref(_ntp2shards));
        co_await rb.invoke_on_all(
          [](reconciliation_backend& rb) { return rb.start(); });
    }

    ss::future<> stop() {
        co_await rb.stop();
        co_await spt.stop();
        co_await kvs.stop();
        co_await sr.stop();
        co_await _ntp2shards.stop();
        co_await ntpt.stop();
        co_await ft.stop();
    }

    ss::future<> TearDownAsync() override {
        co_await stop();
        co_await ss::recursive_remove_directory(
          std::filesystem::path(test_dir));
    }

    ss::sstring test_dir;
    ss::sharded<features::feature_table> ft;
    ss::sharded<ntp_table> ntpt;
    ss::sharded<ntp2shards_t> _ntp2shards; // only on shard 0
    ss::sharded<storage::storage_resources> sr;
    ss::sharded<storage::kvstore> kvs;
    ss::sharded<shard_placement_table> spt;
    ss::sharded<reconciliation_backend> rb;
};

TEST_F_CORO(shard_placement_test_fixture, StressTest) {
    model::revision_id cur_revision{1};
    model::shard_revision_id cur_shard_revision{1};
    raft::group_id cur_group{1};

    co_await start();

    for (size_t i = 0; i < 10'000; ++i) {
        if (random_generators::get_int(15) == 0) {
            vlog(clusterlog.info, "waiting for reconciliation");
            for (size_t i = 0;; ++i) {
                ASSERT_TRUE_CORO(i < 50) << "taking too long to reconcile";
                if (!co_await rb.local().is_reconciled()) {
                    co_await ss::sleep(100ms);
                } else {
                    break;
                }
            }

            vlog(clusterlog.info, "reconciled");
            co_await quiescent_state_checks();
        }

        // small set of ntps to ensure frequent overlaps
        model::ntp ntp(
          model::kafka_namespace, "test_topic", random_generators::get_int(10));

        auto set_target = [&](std::optional<shard_placement_target> target) {
            if (
              ss::this_shard_id()
              != shard_placement_table::assignment_shard_id) {
                return ss::now();
            }

            return ss::yield().then([&, target] {
                _ntp2shards.local()[ntp].target = target;
                return spt.local().set_target(
                  ntp,
                  target,
                  cur_shard_revision++,
                  [this](const model::ntp& ntp) {
                      rb.local().notify_reconciliation(ntp);
                  });
            });
        };

        auto pt_it = ntpt.local().ntp2meta.find(ntp);
        if (pt_it == ntpt.local().ntp2meta.end()) {
            // add
            auto group = cur_group++;
            auto revision = cur_revision++;
            co_await ntpt.invoke_on_all([&](ntp_table& ntpt) {
                ntpt.ntp2meta[ntp] = ntp_table::ntp_meta{
                  .group = group,
                  .log_revision = revision,
                };
                ntpt.revision = revision;

                return set_target(shard_placement_target(
                  revision, random_generators::get_int(get_max_shard_id())));
            });
        } else {
            auto ntp_meta = pt_it->second;

            enum class op_t {
                transfer,
                remove,
                increase_log_rev,
            };

            op_t op = random_generators::random_choice(
              {op_t::transfer, op_t::remove, op_t::increase_log_rev});
            switch (op) {
            case op_t::transfer:
                co_await set_target(shard_placement_target(
                  ntp_meta.log_revision,
                  random_generators::get_int(get_max_shard_id())));
                break;
            case op_t::remove: {
                auto revision = cur_revision++;
                co_await ntpt.invoke_on_all([&](ntp_table& ntpt) {
                    ntpt.ntp2meta.erase(ntp);
                    ntpt.revision = revision;
                    return set_target(std::nullopt);
                });
                break;
            }
            case op_t::increase_log_rev:
                ntp_meta.log_revision = cur_revision++;
                co_await ntpt.invoke_on_all([&](ntp_table& ntpt) {
                    ntpt.ntp2meta[ntp] = ntp_meta;
                    ntpt.revision = ntp_meta.log_revision;

                    return set_target(shard_placement_target(
                      ntp_meta.log_revision,
                      random_generators::get_int(get_max_shard_id())));
                });
                break;
            }
        }
    }

    vlog(clusterlog.info, "finished");
}

} // namespace cluster
