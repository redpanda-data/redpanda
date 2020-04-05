#include "raft/heartbeat_manager.h"

#include "outcome_future_utils.h"
#include "raft/consensus_client_protocol.h"
#include "raft/errc.h"
#include "raft/raftgen_service.h"
#include "rpc/reconnect_transport.h"
#include "rpc/types.h"

#include <absl/container/flat_hash_map.h>
#include <bits/stdint-uintn.h>
#include <boost/range/iterator_range.hpp>

namespace raft {
ss::logger hbeatlog{"r/heartbeat"};
using consensus_ptr = heartbeat_manager::consensus_ptr;
using consensus_set = heartbeat_manager::consensus_set;

static ss::future<std::vector<ss::semaphore_units<>>>
locks_for_range(const consensus_set& c) {
    using units_t = ss::semaphore_units<>;
    using opt_t = std::optional<units_t>;

    return ss::map_reduce(
      std::cbegin(c),
      std::cend(c),
      [](consensus_ptr ptr) {
          if (!ptr->is_leader()) {
              // we are only interested in leaders
              return ss::make_ready_future<opt_t>();
          }
          return ptr->op_lock_unit().then([ptr](units_t u) {
              return std::make_optional<units_t>(std::move(u));
          });
      },
      std::vector<units_t>{},
      [](std::vector<units_t> locks, opt_t u) {
          if (u) {
              locks.push_back(std::move(*u));
          }
          return locks;
      });
}

static std::vector<heartbeat_manager::node_heartbeat> requests_for_range(
  const consensus_set& c, clock_type::time_point last_heartbeat) {
    absl::flat_hash_map<model::node_id, std::vector<protocol_metadata>>
      pending_beats;
    if (c.empty()) {
        return {};
    }
    auto self = (*c.begin())->self();
    for (auto& ptr : c) {
        if (!ptr->is_leader()) {
            continue;
        }
        auto& group = ptr->config();
        for (auto& n : group.nodes) {
            // do not send beat to self
            if (n.id() == ptr->self()) {
                continue;
            }

            auto last_hbeat_timestamp = ptr->last_hbeat_timestamp(n.id());
            if (last_hbeat_timestamp > last_heartbeat) {
                hbeatlog.trace(
                  "Skipping sending beat to {} gr: {} last hb {}, last append "
                  "{}",
                  n.id(),
                  ptr->meta().group,
                  last_heartbeat.time_since_epoch().count(),
                  last_hbeat_timestamp.time_since_epoch().count());
                // we already sent heartbeat, skip it
                continue;
            }
            pending_beats[n.id()].push_back(ptr->meta());
        }
        for (auto& n : group.learners) {
            pending_beats[n.id()].push_back(ptr->meta());
        }
    }

    std::vector<heartbeat_manager::node_heartbeat> reqs;
    reqs.reserve(pending_beats.size());
    for (auto& p : pending_beats) {
        reqs.emplace_back(
          p.first, heartbeat_request{self, std::move(p.second)});
    }

    return reqs;
}

heartbeat_manager::heartbeat_manager(
  duration_type interval, consensus_client_protocol proto)
  : _heartbeat_interval(interval)
  , _client_protocol(proto) {
    _heartbeat_timer.set_callback([this] { dispatch_heartbeats(); });
}

ss::future<> heartbeat_manager::send_heartbeats(
  std::vector<ss::semaphore_units<>> locks, std::vector<node_heartbeat> reqs) {
    return ss::do_with(
             std::move(reqs),
             [this,
              u = std::move(locks)](std::vector<node_heartbeat>& reqs) mutable {
                 std::vector<ss::future<>> futures;
                 futures.reserve(reqs.size());
                 for (auto& r : reqs) {
                     futures.push_back(do_heartbeat(std::move(r)));
                 }
                 return _dispatch_sem.wait(reqs.size())
                   .then([u = std::move(u), f = std::move(futures)]() mutable {
                       return std::move(f);
                   });
             })
      .then([](std::vector<ss::future<>> f) {
          return ss::when_all_succeed(f.begin(), f.end());
      });
}

ss::future<>
heartbeat_manager::do_dispatch_heartbeats(clock_type::time_point last_timeout) {
    return locks_for_range(_consensus_groups)
      .then([this, last_timeout](std::vector<ss::semaphore_units<>> locks) {
          auto reqs = requests_for_range(_consensus_groups, last_timeout);
          return send_heartbeats(std::move(locks), std::move(reqs));
      });
}

ss::future<> heartbeat_manager::do_heartbeat(node_heartbeat&& r) {
    std::vector<group_id> groups(r.request.meta.size());
    for (size_t i = 0; i < groups.size(); ++i) {
        groups[i] = group_id(r.request.meta[i].group);
    }
    auto f = _client_protocol.heartbeat(
      r.target,
      std::move(r.request),
      rpc::client_opts(
        next_heartbeat_timeout(),
        rpc::client_opts::sequential_dispatch::yes,
        rpc::compression_type::zstd,
        512));
    _dispatch_sem.signal();
    return f.then([node = r.target, groups = std::move(groups), this](
                    result<heartbeat_reply> ret) mutable {
        process_reply(node, std::move(groups), std::move(ret));
    });
}

void heartbeat_manager::process_reply(
  model::node_id n, std::vector<group_id> groups, result<heartbeat_reply> r) {
    if (!r) {
        hbeatlog.info(
          "Could not send hearbeats to node:{}, reason:{}, message:{}",
          n,
          r,
          r.error().message());
        for (auto g : groups) {
            auto it = std::lower_bound(
              _consensus_groups.begin(),
              _consensus_groups.end(),
              g,
              details::consensus_ptr_by_group_id{});
            if (it == _consensus_groups.end()) {
                hbeatlog.error("cannot find consensus group:{}", g);
                continue;
            }
            // propagate error
            (*it)->process_heartbeat_response(
              n, result<append_entries_reply>(r.error()));
        }
        return;
    }
    hbeatlog.trace("process_reply {}", r);
    for (auto& m : r.value().meta) {
        auto it = std::lower_bound(
          _consensus_groups.begin(),
          _consensus_groups.end(),
          raft::group_id(m.group),
          details::consensus_ptr_by_group_id{});
        if (it == _consensus_groups.end()) {
            hbeatlog.error("Could not find consensus for group:{}", m.group);
            continue;
        }
        (*it)->process_heartbeat_response(
          n, result<append_entries_reply>(std::move(m)));
    }
}

void heartbeat_manager::dispatch_heartbeats() {
    (void)with_gate(
      _bghbeats, [this, old = _hbeat] { return do_dispatch_heartbeats(old); })
      .handle_exception([](const std::exception_ptr& e) {
          hbeatlog.warn("Error dispatching hearbeats - {}", e);
      })
      .then([this] {
          if (!_bghbeats.is_closed()) {
              _heartbeat_timer.arm(next_heartbeat_timeout());
          }
      });

    // update last
    _hbeat = clock_type::now();
}
void heartbeat_manager::deregister_group(group_id g) {
    auto it = std::lower_bound(
      _consensus_groups.begin(),
      _consensus_groups.end(),
      g,
      details::consensus_ptr_by_group_id{});
    if (it != _consensus_groups.end()) {
        _consensus_groups.erase(it);
    }
}
void heartbeat_manager::register_group(ss::lw_shared_ptr<consensus> ptr) {
    _consensus_groups.insert(ptr);
}
ss::future<> heartbeat_manager::start() {
    dispatch_heartbeats();
    return ss::make_ready_future<>();
}
ss::future<> heartbeat_manager::stop() {
    _heartbeat_timer.cancel();
    return _bghbeats.close();
}

clock_type::time_point heartbeat_manager::next_heartbeat_timeout() {
    return clock_type::now() + _heartbeat_interval;
}

} // namespace raft
