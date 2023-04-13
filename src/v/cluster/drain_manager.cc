#include "cluster/drain_manager.h"

#include "cluster/logger.h"
#include "cluster/partition_manager.h"
#include "cluster/types.h"
#include "random/generators.h"
#include "vlog.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>

namespace cluster {

drain_manager::drain_manager(
  ss::sharded<cluster::partition_manager>& partition_manager)
  : _partition_manager(partition_manager) {}

ss::future<> drain_manager::start() {
    vassert(!_drain.has_value(), "service cannot be restarted");
    vlog(clusterlog.info, "Drain manager starting");
    _drain = task();
    co_return;
}

ss::future<> drain_manager::stop() {
    if (!_drain.has_value()) {
        vlog(clusterlog.info, "Drain manager stopping (was not started)");
        return ss::now();
    }
    vlog(clusterlog.info, "Drain manager stopping");
    _abort.request_abort();
    _sem.signal();
    return _drain.value().handle_exception([](std::exception_ptr e) {
        vlog(clusterlog.warn, "Draining manager task experience error: {}", e);
    });
}

ss::future<> drain_manager::drain() {
    if (_abort.abort_requested()) {
        // handle http requests racing with shutdown
        co_return;
    }

    if (_draining) {
        vlog(clusterlog.info, "Node draining is already active");
        co_return;
    }

    vlog(clusterlog.info, "Node draining is starting");
    _draining = true;
    _status = drain_status{};
    _sem.signal();
}

ss::future<> drain_manager::restore() {
    if (_abort.abort_requested()) {
        co_return;
    }

    if (!_draining) {
        vlog(clusterlog.info, "Node draining is not active");
        co_return;
    }

    vlog(clusterlog.info, "Node draining is stopping");
    _draining = false;
    _sem.signal();
}

ss::future<std::optional<drain_manager::drain_status>> drain_manager::status() {
    if (_abort.abort_requested()) {
        co_return std::nullopt;
    }

    co_return co_await container().map_reduce0(
      [](drain_manager& dm) -> std::optional<drain_status> {
          if (dm._draining) {
              return dm._status;
          }
          return std::nullopt;
      },
      std::optional<drain_status>(std::nullopt),
      [](std::optional<drain_status> res, std::optional<drain_status> update) {
          if (!update.has_value()) {
              return res;
          }
          if (!res.has_value()) {
              res = drain_status{
                .finished = true,
                .errors = false,
              };
          }
          res->finished &= update->finished;
          res->errors |= update->errors;
          if (update->partitions.has_value()) {
              res->partitions = res->partitions.value_or(0)
                                + update->partitions.value();
          }
          if (update->eligible.has_value()) {
              res->eligible = res->eligible.value_or(0)
                              + update->eligible.value();
          }
          if (update->transferring.has_value()) {
              res->transferring = res->transferring.value_or(0)
                                  + update->transferring.value();
          }
          if (update->failed.has_value()) {
              res->failed = res->failed.value_or(0) + update->failed.value();
          }
          return res;
      });
}

ss::future<> drain_manager::task() {
    while (true) {
        co_await _sem.wait();
        _sem.consume(_sem.available_units());

        if (_abort.abort_requested()) {
            break;
        }

        const auto draining = _draining;
        try {
            if (draining) {
                co_await do_drain();
            } else {
                co_await do_restore();
            }
        } catch (...) {
            vlog(
              clusterlog.warn,
              "Draining task {{{}}} experienced error: {}",
              draining ? "drain" : "restore",
              std::current_exception());
            _status.errors = true;
        }
        _status.finished = true;
    }
}

ss::future<> drain_manager::do_drain() {
    vlog(clusterlog.info, "Node draining has started");

    /*
     * Prevent this node from becomming a leader for new and existing raft
     * groups. This does not immediately reliquish existing leadership. it is
     * assumed that all raft groups (e.g. controller/raft0 and kafka data) are
     * represented in the partition manager.
     */
    _partition_manager.local().block_new_leadership();

    while (_draining && !_abort.abort_requested()) {
        /*
         * build a set of eligible partitions. ignore any raft groups that
         * will fail when transferring leadership and which shouldn't be
         * retried. note that above when we block new leadership we don't bother
         * skipping over groups without followers. this is safe because such a
         * group won't lose leadership in the first place.
         */
        std::vector<ss::lw_shared_ptr<cluster::partition>> eligible;
        eligible.reserve(_partition_manager.local().partitions().size());
        for (const auto& p : _partition_manager.local().partitions()) {
            if (!p.second->is_elected_leader() || !p.second->has_followers()) {
                continue;
            }
            eligible.push_back(p.second);
        }
        _status.eligible = eligible.size();
        _status.partitions = _partition_manager.local().partitions().size();

        if (eligible.empty()) {
            break;
        }

        /*
         * choose a random sample from the set of eligible partitions. this is
         * useful when we have a draining policy in which we want to drain as
         * much as possible even if some groups continue to have leadership
         * transfer errors. an alternative approach would be to fence off groups
         * experiencing errors, but then we would have to create some type of
         * retry policy to deal with those partitions.
         */
        std::vector<ss::lw_shared_ptr<cluster::partition>> selected;
        selected.reserve(max_parallel_transfers);
        std::sample(
          eligible.begin(),
          eligible.end(),
          std::back_inserter(selected),
          max_parallel_transfers,
          random_generators::internal::gen);
        eligible.clear();

        /*
         * start a group of transfers
         */
        std::vector<ss::future<std::error_code>> transfers;
        transfers.reserve(selected.size());
        for (auto& p : selected) {
            auto req = transfer_leadership_request{
              .group = p->group(),
            };
            transfers.push_back(p->transfer_leadership(req));
        }
        _status.transferring = transfers.size();

        vlog(
          clusterlog.info,
          "Draining leadership from {} groups",
          transfers.size());

        auto started = ss::lowres_clock::now();

        auto results = co_await ss::when_all(
          transfers.begin(), transfers.end());

        size_t failed = 0;
        for (auto& f : results) {
            try {
                auto err = f.get();
                if (err) {
                    vlog(
                      clusterlog.debug,
                      "Draining leadership failed for group: {}",
                      err);
                    failed++;
                }
            } catch (...) {
                vlog(
                  clusterlog.debug,
                  "Draining leadership failed for group: {}",
                  std::current_exception());
                failed++;
            }
        }
        _status.failed = failed;

        vlog(
          clusterlog.info,
          "Draining leadership from {} groups {} succeeded",
          transfers.size(),
          transfers.size() - failed);

        /*
         * to avoid spinning, cool off if we failed fast
         */
        auto dur = ss::lowres_clock::now() - started;
        if (failed > 0 && dur < transfer_throttle && _draining) {
            try {
                co_await ss::sleep_abortable(transfer_throttle - dur, _abort);
            } catch (ss::sleep_aborted&) {
            }
        }
    }

    vlog(
      clusterlog.info,
      "Node draining has completed on shard {}",
      ss::this_shard_id());
}

/*
 * Unblock this node from new leadership.
 *
 * Currently the unblocking process does not attempt to restore leadership back
 * to the node. This is assumed to be handled at a higher level (e.g. by the
 * operator by enabling or poking the cluster leadership rebalancer). However,
 * we could imagine being more aggresive here in the future.
 */
ss::future<> drain_manager::do_restore() {
    vlog(clusterlog.info, "Node drain stopped");
    _partition_manager.local().unblock_new_leadership();
    co_return;
}

std::ostream&
operator<<(std::ostream& os, const drain_manager::drain_status& ds) {
    fmt::print(
      os,
      "{{finished: {}, errors: {}, partitions: {}, eligible: {}, transferring: "
      "{}, failed: {}}}",
      ds.finished,
      ds.errors,
      ds.partitions,
      ds.eligible,
      ds.transferring,
      ds.failed);
    return os;
}

} // namespace cluster
