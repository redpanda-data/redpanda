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
#include "container/chunked_hash_map.h"
#include "features/feature_table.h"
#include "ssx/event.h"
#include "storage/kvstore.h"
#include "storage/storage_resources.h"
#include "test_utils/randoms.h"
#include "test_utils/test.h"
#include "utils/prefix_logger.h"

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
      , _ntp2shards(ntp2shards)
      , _logger(clusterlog, "RB") {}

    ss::future<> stop() {
        for (auto& [_, rs] : _states) {
            rs->wakeup_event.set();
        }
        co_await _gate.close();
    }

    ss::future<> start() {
        for (const auto& [ntp, _] : _shard_placement.shard_local_states()) {
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
        vlog(
          _logger.trace,
          "[{}] notify reconciliation, pending_notifies: {}",
          ntp,
          rs.pending_notifies);
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
                      _logger.error,
                      "[{}] unexpected exception during reconciliation: {}",
                      ntp,
                      ex);
                }
            }
        }
    }

    ss::future<>
    try_reconcile_ntp(const model::ntp& ntp, ntp_reconciliation_state& rs) {
        size_t step = 0;
        while (!rs.is_reconciled() && !_gate.is_closed()) {
            size_t notifies = rs.pending_notifies;
            try {
                // Check that we are not busy-looping with successful steps. In
                // this mock reconciliation process we need max 3 steps:
                // 1. delete previous log revision
                // 2. create/transfer current log revision
                // 3. control step to check that everything is reconciled
                vassert(
                  step <= 3,
                  "[{}] too many reconciliation steps: {}",
                  ntp,
                  step);

                auto res = co_await reconcile_ntp_step(ntp, rs);
                if (res.has_value()) {
                    if (res.value() == ss::stop_iteration::yes) {
                        vlog(
                          _logger.trace,
                          "[{}] reconciled, notify count: {}",
                          ntp,
                          notifies);
                        rs.mark_reconciled(notifies);
                        step = 0;
                    } else {
                        step += 1;
                    }
                    continue;
                } else {
                    vlog(
                      _logger.trace,
                      "[{}] reconciliation attempt error: {}",
                      ntp,
                      res.error());
                }
            } catch (const ss::gate_closed_exception&) {
            } catch (const ss::abort_requested_exception&) {
            } catch (...) {
                vlog(
                  _logger.warn,
                  "[{}] exception occured during reconciliation: {}",
                  ntp,
                  std::current_exception());
            }
            break;
        }
    }

    ss::future<result<ss::stop_iteration>>
    reconcile_ntp_step(const model::ntp& ntp, ntp_reconciliation_state&) {
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
          _logger.trace,
          "[{}] placement state on this shard: {}, expected_log_revision: {}",
          ntp,
          placement,
          expected_log_revision);

        switch (placement.get_reconciliation_action(expected_log_revision)) {
        case shard_placement_table::reconciliation_action::remove_partition: {
            auto cmd_revision = expected_log_revision.value_or(_ntpt.revision);
            auto ec = co_await delete_partition(ntp, placement, cmd_revision);
            if (ec) {
                co_return ec;
            }
            co_return ss::stop_iteration::no;
        }
        case shard_placement_table::reconciliation_action::remove_kvstore_state:
            co_await remove_partition_kvstore_state(
              ntp, placement.current().value().log_revision);
            co_return ss::stop_iteration::no;
        case shard_placement_table::reconciliation_action::
          wait_for_target_update:
            co_return errc::waiting_for_shard_placement_update;
        case shard_placement_table::reconciliation_action::transfer: {
            auto ec = co_await transfer_partition(
              ntp,
              expected_log_revision.value(),
              placement.current().has_value());
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
                  placement.current().has_value());
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
        vlog(_logger.trace, "[{}] creating partition: {}", ntp, ec);
        if (ec) {
            co_return ec;
        }

        _launched.insert(ntp);
        vlog(
          _logger.trace,
          "[{}] started partition log_revision: {}",
          ntp,
          log_revision);
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
          _logger.trace,
          "[{}] deleting partition at cmd_revision: {}, ec: {}",
          ntp,
          cmd_revision,
          ec);
        if (ec) {
            co_return ec;
        }

        if (!placement.current()) {
            // nothing to delete
            co_return errc::success;
        }

        bool was_launched = _launched.erase(ntp);
        if (was_launched) {
            vlog(
              _logger.trace,
              "[{}] stopped partition log_revision: {}",
              ntp,
              placement.current()->log_revision);
        }
        if (
          placement.current()->status
          != shard_placement_table::hosted_status::hosted) {
            vassert(!was_launched, "[{}] unexpected launched", ntp);
        }

        co_await ss::sleep(1ms * random_generators::get_int(30));

        co_await _ntp2shards.invoke_on(
          0,
          [ntp,
           current = placement.current().value(),
           shard = ss::this_shard_id(),
           was_launched](ntp2shards_t& ntp2shards) {
              auto& shards = ntp2shards[ntp];

              bool erased = shards.shards_with_some_state.erase(shard);
              if (was_launched) {
                  vassert(
                    erased,
                    "[{}] unexpected set contents (deleting on: {})",
                    ntp,
                    shard);
              }

              auto& p_shards = shards.rev2shards[current.log_revision];

              if (
                current.status
                != shard_placement_table::hosted_status::obsolete) {
                  vassert(
                    (was_launched && p_shards.launched_on == shard)
                      || (!was_launched && !p_shards.launched_on),
                    "[{}] unexpected launched_on: {} (shard: {}, expected: {})",
                    ntp,
                    p_shards.launched_on,
                    shard,
                    was_launched);
              }
              if (was_launched) {
                  p_shards.launched_on = std::nullopt;
              }

              if (was_launched) {
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

              if (was_launched) {
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
          ntp, placement.current()->log_revision);
        co_return ec;
    }

    ss::future<> remove_partition_kvstore_state(
      const model::ntp& ntp, model::revision_id log_revision) {
        vlog(
          _logger.trace,
          "[{}] removing partition kvstore state, log_revision: {}",
          ntp,
          log_revision);

        vassert(!_launched.contains(ntp), "[{}] unexpected launched", ntp);

        co_await _ntp2shards.invoke_on(
          0, [ntp, shard = ss::this_shard_id()](ntp2shards_t& ntp2shards) {
              auto& shards = ntp2shards[ntp];

              vassert(
                shards.shards_with_some_state.erase(shard),
                "[{}] unexpected set contents, shard: {}",
                ntp,
                shard);
          });

        co_await _shard_placement.finish_delete(ntp, log_revision);
    }

    ss::future<std::error_code> transfer_partition(
      const model::ntp& ntp,
      model::revision_id log_revision,
      bool state_expected) {
        auto transfer_info = co_await _shard_placement.prepare_transfer(
          ntp, log_revision, _shard_placement.container());
        if (transfer_info.source_error != errc::success) {
            vlog(
              _logger.trace,
              "[{}] preparing transfer source error: {}",
              ntp,
              transfer_info.source_error);
            co_return transfer_info.source_error;
        }

        ss::shard_id destination = transfer_info.destination.value();
        vlog(
          _logger.trace,
          "[{}] preparing transfer dest: {} (error: {})",
          ntp,
          destination,
          transfer_info.dest_error);
        if (transfer_info.dest_error != errc::success) {
            co_return transfer_info.dest_error;
        }
        if (transfer_info.is_finished) {
            co_return errc::success;
        }

        bool launched_expected = _launched.erase(ntp);
        if (launched_expected) {
            vlog(
              _logger.trace,
              "[{}] stopped partition for transfer, log_revision: {}",
              ntp,
              log_revision);
        }

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

              if (p_shards.next_state_on == destination) {
                  // transfer was retried
                  vassert(
                    shards.shards_with_some_state.contains(destination),
                    "[{}] unexpected set contents, destination: {}",
                    ntp,
                    destination);
              } else {
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
              }
          });

        co_await ss::sleep(1ms * random_generators::get_int(30));
        if (random_generators::get_int(5) == 0) {
            // simulate partial failure of the transfer.
            throw std::runtime_error{
              fmt_with_ctx(fmt::format, "[{}] transfer failed!", ntp)};
        }

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

        auto shard_callback = [this](const model::ntp& ntp) {
            auto& dest = container().local();
            auto it = dest._states.find(ntp);
            if (it != dest._states.end()) {
                it->second->wakeup_event.set();
            }
        };

        co_await _shard_placement.finish_transfer(
          ntp, log_revision, _shard_placement.container(), shard_callback);
        vlog(_logger.trace, "[{}] transferred", ntp);
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
    prefix_logger _logger;
};

// Limit concurrency to 4 so that there are more interesting repeats in randomly
// generated shard ids.
ss::shard_id get_max_shard_id() {
    return std::min(ss::smp::count - 1, ss::shard_id(3));
}

/// Simplified version of shard_balancer that just assigns ntps to random
/// shards. Runs on shard 0.
class shard_assigner {
public:
    explicit shard_assigner(
      ss::sharded<ntp_table>& ntpt,
      ss::sharded<shard_placement_table>& spt,
      ss::sharded<ntp2shards_t>& ntp2shards,
      ss::sharded<reconciliation_backend>& rb)
      : _ntpt(ntpt.local())
      , _shard_placement(spt.local())
      , _ntp2shards(ntp2shards.local())
      , _rb(rb) {}

    ss::future<> start() {
        for (const auto& [ntp, meta] : _ntpt.ntp2meta) {
            auto maybe_target = _shard_placement.get_target(ntp);
            if (
              !maybe_target
              || maybe_target->log_revision != meta.log_revision) {
                assign_eventually(ntp);
            }
        }

        ssx::background = assign_fiber();
        co_return;
    }

    ss::future<> stop() {
        _wakeup_event.set();
        return _gate.close();
    }

    void enable_persistence_eventually() {
        if (!_enable_persistence) {
            _enable_persistence = true;
            _wakeup_event.set();
        }
    }

    void assign_eventually(const model::ntp& ntp) {
        _to_assign.insert(ntp);
        _wakeup_event.set();
    }

    ss::future<> assign(const model::ntp& ntp) {
        auto gate_holder = _gate.hold();
        auto lock = co_await _mtx.get_units();
        co_await assign_ntp(ntp, lock);
    }

    bool is_reconciled() const { return _mtx.ready() && _to_assign.empty(); }

private:
    ss::future<> assign_fiber() {
        if (_gate.is_closed()) {
            co_return;
        }
        auto gate_holder = _gate.hold();

        while (true) {
            co_await _wakeup_event.wait(1s);
            if (_gate.is_closed()) {
                co_return;
            }
            auto lock = co_await _mtx.get_units();

            if (_enable_persistence) {
                co_await _shard_placement.enable_persistence();
            }

            auto to_assign = std::exchange(_to_assign, {});
            co_await ss::max_concurrent_for_each(
              to_assign, 128, [this, &lock](const model::ntp& ntp) {
                  return assign_ntp(ntp, lock);
              });
        }
    }

    ss::future<> assign_ntp(const model::ntp& ntp, mutex::units& /*lock*/) {
        std::optional<shard_placement_target> target;
        if (auto it = _ntpt.ntp2meta.find(ntp); it != _ntpt.ntp2meta.end()) {
            target = shard_placement_target(
              it->second.group,
              it->second.log_revision,
              random_generators::get_int(get_max_shard_id()));
        }

        try {
            co_await _shard_placement.set_target(
              ntp, target, [this](const model::ntp& ntp) {
                  _rb.local().notify_reconciliation(ntp);
              });
            _ntp2shards[ntp].target = target;
        } catch (...) {
            auto ex = std::current_exception();
            if (!ssx::is_shutdown_exception(ex)) {
                vlog(
                  clusterlog.warn,
                  "[{}] shard_assigner exception: {}",
                  ntp,
                  ex);
                _to_assign.insert(ntp);
            }
        }
    }

private:
    ntp_table& _ntpt;
    shard_placement_table& _shard_placement;
    ntp2shards_t& _ntp2shards;
    ss::sharded<reconciliation_backend>& _rb;

    bool _enable_persistence = false;
    chunked_hash_set<model::ntp> _to_assign;
    ssx::event _wakeup_event{"shard_assigner"};
    mutex _mtx{"shard_assigner"};
    ss::gate _gate;
};

template<typename Left, typename Right>
void assert_key_sets_equal(
  const Left& left,
  std::string_view left_str,
  const Right& right,
  std::string_view right_str) {
    std::vector<typename Left::key_type> keys1;
    for (const auto& kv : left) {
        if (!right.contains(kv.first)) {
            keys1.push_back(kv.first);
        }
    }
    ASSERT_TRUE(keys1.empty()) << "keys in " << left_str << ", but not in "
                               << right_str << ": " << keys1;

    std::vector<typename Right::key_type> keys2;
    for (const auto& kv : right) {
        if (!left.contains(kv.first)) {
            keys2.push_back(kv.first);
        }
    }
    ASSERT_TRUE(keys2.empty()) << "keys in " << right_str << ", but not in "
                               << left_str << ": " << keys2;
}

} // namespace

class shard_placement_test_fixture : public seastar_test {
public:
    shard_placement_test_fixture()
      : test_dir("test.data." + random_generators::gen_alphanum_string(10)) {}

    using ntp2shard2state_t = absl::node_hash_map<
      model::ntp,
      std::map<ss::shard_id, shard_placement_table::placement_state>>;

    ss::future<ntp2shard2state_t> get_ntp2shard2state() const {
        auto shard2states = co_await spt->map(
          [](shard_placement_table& spt) { return spt._states; });

        ntp2shard2state_t ntp2shard2state;
        for (size_t s = 0; s < shard2states.size(); ++s) {
            for (const auto& [ntp, state] : shard2states[s]) {
                ntp2shard2state[ntp].emplace(s, state);
            }
        }

        co_return ntp2shard2state;
    }

    void clean_ntp2shards() {
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
    }

    ss::future<> quiescent_state_checks() {
        auto ntp2shard2state = co_await get_ntp2shard2state();
        assert_key_sets_equal(
          ntp2shard2state,
          "spt placement state map",
          ntpt.local().ntp2meta,
          "ntp2meta map");

        clean_ntp2shards();
        const auto& ntp2shards = _ntp2shards.local();
        assert_key_sets_equal(
          ntp2shards,
          "reference ntp state map",
          ntpt.local().ntp2meta,
          "ntp2meta map");

        for (const auto& [ntp, meta] : ntpt.local().ntp2meta) {
            auto states_it = ntp2shard2state.find(ntp);
            ASSERT_TRUE_CORO(states_it != ntp2shard2state.end())
              << "ntp: " << ntp;
            const auto& shard2state = states_it->second;

            auto entry_it = spt->local()._ntp2entry.find(ntp);
            ASSERT_TRUE_CORO(entry_it != spt->local()._ntp2entry.end())
              << "ntp: " << ntp;
            ASSERT_TRUE_CORO(entry_it->second->target) << "ntp: " << ntp;
            ASSERT_TRUE_CORO(entry_it->second->mtx.ready()) << "ntp: " << ntp;

            const auto& target = entry_it->second->target.value();
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
                    ASSERT_TRUE_CORO(placement.current())
                      << "ntp: " << ntp << ", shard: " << s;
                    ASSERT_EQ_CORO(
                      placement.current()->log_revision, meta.log_revision)
                      << "ntp: " << ntp << ", shard: " << s;
                    ASSERT_EQ_CORO(
                      placement.current()->status,
                      shard_placement_table::hosted_status::hosted)
                      << "ntp: " << ntp << ", shard: " << s;
                    ASSERT_TRUE_CORO(placement.assigned())
                      << "ntp: " << ntp << ", shard: " << s;
                    ASSERT_EQ_CORO(
                      placement.assigned()->log_revision, meta.log_revision)
                      << "ntp: " << ntp << ", shard: " << s;
                } else {
                    ASSERT_TRUE_CORO(!placement.current())
                      << "ntp: " << ntp << ", shard: " << s;
                    ASSERT_TRUE_CORO(!placement.assigned())
                      << "ntp: " << ntp << ", shard: " << s;
                }
            }

            ASSERT_EQ_CORO(shards.rev2shards.size(), 1) << "ntp: " << ntp;
            auto p_shards_it = shards.rev2shards.find(meta.log_revision);
            ASSERT_TRUE_CORO(p_shards_it != shards.rev2shards.end())
              << "ntp: " << ntp << ", log_revision: " << meta.log_revision;
            const auto& p_shards = p_shards_it->second;
            ASSERT_EQ_CORO(p_shards.launched_on, target.shard)
              << "ntp: " << ntp;
            ASSERT_EQ_CORO(p_shards.current_state_on, target.shard)
              << "ntp: " << ntp;
            ASSERT_EQ_CORO(p_shards.next_state_on, std::nullopt)
              << "ntp: " << ntp;
        }
    }

    ss::future<> check_spt_recovery() {
        clean_ntp2shards();
        const auto& ntp2shards = _ntp2shards.local();
        auto ntp2shard2state = co_await get_ntp2shard2state();
        assert_key_sets_equal(
          ntp2shards,
          "reference ntp state map",
          ntp2shard2state,
          "spt placement state map");

        for (const auto& [ntp, expected] : ntp2shards) {
            auto states_it = ntp2shard2state.find(ntp);
            ASSERT_TRUE_CORO(states_it != ntp2shard2state.end())
              << "ntp: " << ntp;
            const auto& shard2state = states_it->second;

            // check main target map
            auto entry_it = spt->local()._ntp2entry.find(ntp);
            if (expected.target) {
                ASSERT_TRUE_CORO(entry_it != spt->local()._ntp2entry.end())
                  << "ntp: " << ntp;
                ASSERT_EQ_CORO(entry_it->second->target, expected.target)
                  << "ntp: " << ntp;
                ASSERT_TRUE_CORO(entry_it->second->mtx.ready())
                  << "ntp: " << ntp;
            } else {
                ASSERT_TRUE_CORO(entry_it == spt->local()._ntp2entry.end())
                  << "ntp: " << ntp;
            }

            // check assigned markers
            if (expected.target) {
                ASSERT_TRUE_CORO(shard2state.contains(expected.target->shard));
            }
            for (const auto& [s, placement] : shard2state) {
                if (expected.target && s == expected.target->shard) {
                    ASSERT_TRUE_CORO(placement.assigned())
                      << "ntp: " << ntp << ", shard: " << s;
                    ASSERT_EQ_CORO(
                      placement.assigned()->log_revision,
                      expected.target->log_revision)
                      << "ntp: " << ntp << ", shard: " << s;
                } else {
                    ASSERT_TRUE_CORO(!placement.assigned())
                      << "ntp: " << ntp << ", shard: " << s;
                }
            }

            // check that all shards with state are known in the placement map.
            for (ss::shard_id s : expected.shards_with_some_state) {
                auto state_it = shard2state.find(s);
                ASSERT_TRUE_CORO(state_it != shard2state.end())
                  << "ntp: " << ntp << ", shard: " << s;
                ASSERT_TRUE_CORO(state_it->second.current())
                  << "ntp: " << ntp << ", shard: " << s;
            }
        }
    }

    ss::future<> start() {
        co_await ft.start();
        co_await ft.invoke_on_all(
          [](features::feature_table& ft) { ft.testing_activate_all(); });
        co_await ntpt.start();
        co_await _ntp2shards.start_single();
        co_await sr.start();

        co_await restart_node(true);
    }

    ss::future<> stop() {
        if (_shard_assigner) {
            co_await _shard_assigner->stop();
        }
        if (rb) {
            co_await rb->stop();
        }
        if (spt) {
            co_await spt->stop();
        }
        if (kvs) {
            co_await kvs->stop();
        }
        co_await sr.stop();
        co_await _ntp2shards.stop();
        co_await ntpt.stop();
        co_await ft.stop();
    }

    ss::future<> restart_node(bool first_start) {
        if (_shard_assigner) {
            co_await _shard_assigner->stop();
        }
        if (rb) {
            co_await rb->stop();
        }
        if (spt) {
            co_await spt->stop();
        }
        if (kvs) {
            co_await kvs->stop();
        }

        for (auto& [ntp, shards] : _ntp2shards.local()) {
            for (auto& [lr, p_shards] : shards.rev2shards) {
                // "stop" mock partitions
                p_shards.launched_on = std::nullopt;
            }
        }

        kvs = std::make_unique<decltype(kvs)::element_type>();
        co_await kvs->start(
          storage::kvstore_config(
            1_MiB,
            config::mock_binding(10ms),
            test_dir,
            storage::make_sanitized_file_config()),
          ss::sharded_parameter([] { return ss::this_shard_id(); }),
          ss::sharded_parameter([this] { return std::ref(sr.local()); }),
          std::ref(ft));
        co_await kvs->invoke_on_all(
          [](storage::kvstore& kvs) { return kvs.start(); });

        spt = std::make_unique<decltype(spt)::element_type>();
        co_await spt->start(
          ss::sharded_parameter([] { return ss::this_shard_id(); }),
          ss::sharded_parameter([this] { return std::ref(kvs->local()); }));

        if (!first_start) {
            chunked_hash_map<raft::group_id, model::ntp> local_group2ntp;
            for (const auto& [ntp, meta] : ntpt.local().ntp2meta) {
                local_group2ntp.emplace(meta.group, ntp);
            }
            co_await spt->local().initialize_from_kvstore(local_group2ntp, {});

            for (auto& [ntp, shards] : _ntp2shards.local()) {
                if (
                  shards.target
                  && !local_group2ntp.contains(shards.target->group)) {
                    // clear obsolete targets
                    shards.target = std::nullopt;
                }
            }
        }

        co_await check_spt_recovery();

        rb = std::make_unique<decltype(rb)::element_type>();
        co_await rb->start(
          std::ref(ntpt), std::ref(*spt), std::ref(_ntp2shards));

        _shard_assigner = std::make_unique<shard_assigner>(
          ntpt, *spt, _ntp2shards, *rb);
        co_await _shard_assigner->start();

        co_await rb->invoke_on_all(
          [](reconciliation_backend& rb) { return rb.start(); });
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
    std::unique_ptr<ss::sharded<storage::kvstore>> kvs;
    std::unique_ptr<ss::sharded<shard_placement_table>> spt;
    std::unique_ptr<ss::sharded<reconciliation_backend>> rb;
    std::unique_ptr<shard_assigner> _shard_assigner;
};

TEST_F_CORO(shard_placement_test_fixture, StressTest) {
    model::revision_id cur_revision{1};
    raft::group_id cur_group{1};
    prefix_logger logger(clusterlog, "TEST");

    co_await start();

    // enable persistence midway through the test
    size_t enable_persistence_at = random_generators::get_int(4'000, 6'000);

    for (size_t i = 0; i < 10'000; ++i) {
        if (i == enable_persistence_at) {
            _shard_assigner->enable_persistence_eventually();
        }

        if (random_generators::get_int(15) == 0) {
            vlog(logger.info, "waiting for reconciliation");
            for (size_t i = 0;; ++i) {
                ASSERT_TRUE_CORO(i < 50) << "taking too long to reconcile";
                if (!(_shard_assigner->is_reconciled()
                      && co_await rb->local().is_reconciled())) {
                    co_await ss::sleep(100ms);
                } else {
                    break;
                }
            }

            vlog(logger.info, "reconciled");
            co_await quiescent_state_checks();
            continue;
        }

        if (
          spt->local().is_persistence_enabled()
          && random_generators::get_int(50) == 0) {
            vlog(logger.info, "restarting");
            co_await restart_node(false);
            vlog(logger.info, "restarted");
            continue;
        }

        // small set of ntps to ensure frequent overlaps
        model::ntp ntp(
          model::kafka_namespace, "test_topic", random_generators::get_int(10));

        auto pt_it = ntpt.local().ntp2meta.find(ntp);
        if (pt_it == ntpt.local().ntp2meta.end()) {
            // add
            auto group = cur_group++;
            auto revision = cur_revision++;
            vlog(
              logger.info,
              "[{}] OP: add, group: {}, log revision: {}",
              ntp,
              group,
              revision);
            co_await ntpt.invoke_on_all([&](ntp_table& ntpt) {
                ntpt.ntp2meta[ntp] = ntp_table::ntp_meta{
                  .group = group,
                  .log_revision = revision,
                };
                ntpt.revision = revision;
                if (ss::this_shard_id() == 0) {
                    _shard_assigner->assign_eventually(ntp);
                }
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
                vlog(logger.info, "[{}] OP: reassign shard", ntp);
                co_await _shard_assigner->assign(ntp);
                break;
            case op_t::remove: {
                vlog(logger.info, "[{}] OP: remove", ntp);
                auto revision = cur_revision++;
                co_await ntpt.invoke_on_all([&](ntp_table& ntpt) {
                    ntpt.ntp2meta.erase(ntp);
                    ntpt.revision = revision;
                    if (ss::this_shard_id() == 0) {
                        _shard_assigner->assign_eventually(ntp);
                    }
                });
                break;
            }
            case op_t::increase_log_rev:
                ntp_meta.log_revision = cur_revision++;
                vlog(
                  logger.info,
                  "[{}] OP: increase log revision to: {}",
                  ntp,
                  ntp_meta.log_revision);
                co_await ntpt.invoke_on_all([&](ntp_table& ntpt) {
                    ntpt.ntp2meta[ntp] = ntp_meta;
                    ntpt.revision = ntp_meta.log_revision;
                    if (ss::this_shard_id() == 0) {
                        _shard_assigner->assign_eventually(ntp);
                    }
                });
                break;
            }
        }
    }

    vlog(logger.info, "finished");
}

} // namespace cluster
