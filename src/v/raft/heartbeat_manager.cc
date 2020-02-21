#include "raft/heartbeat_manager.h"

#include "outcome_future_utils.h"
#include "raft/consensus_client_protocol.h"
#include "raft/errc.h"
#include "raft/raftgen_service.h"
#include "rpc/reconnect_transport.h"
#include "rpc/types.h"

#include <boost/range/iterator_range.hpp>

namespace raft {
ss::logger hbeatlog{"r/heartbeat"};
using consensus_ptr = heartbeat_manager::consensus_ptr;
using consensus_set = heartbeat_manager::consensus_set;

static std::vector<heartbeat_manager::node_heartbeat> requests_for_range(
  const consensus_set& c, clock_type::time_point last_heartbeat) {
    std::unordered_map<model::node_id, std::vector<protocol_metadata>>
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
                  "Skipping sending beat to {} last hb {}, last append {}",
                  n.id(),
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

ss::future<> heartbeat_manager::do_dispatch_heartbeats(
  clock_type::time_point last_timeout, clock_type::time_point next_timeout) {
    auto reqs = requests_for_range(_consensus_groups, last_timeout);
    return ss::do_with(
      std::move(reqs), [this, next_timeout](std::vector<node_heartbeat>& reqs) {
          return ss::parallel_for_each(
            reqs.begin(), reqs.end(), [this, next_timeout](node_heartbeat& r) {
                return do_heartbeat(std::move(r), next_timeout);
            });
      });
}

ss::future<> heartbeat_manager::do_heartbeat(
  node_heartbeat&& r, clock_type::time_point next_timeout) {
    auto shard = rpc::connection_cache::shard_for(r.target);
    std::vector<group_id> groups(r.request.meta.size());
    for (size_t i = 0; i < groups.size(); ++i) {
        groups[i] = group_id(r.request.meta[i].group);
    }
    return _client_protocol
      .heartbeat(r.target, std::move(r.request), next_timeout)
      .then([node = r.target, groups = std::move(groups), this](
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
    auto next_timeout = clock_type::now() + _heartbeat_interval;
    (void)with_gate(_bghbeats, [this, old = _hbeat, next_timeout] {
        return do_dispatch_heartbeats(old, next_timeout);
    }).handle_exception([](const std::exception_ptr& e) {
        hbeatlog.warn("Error dispatching hearbeats - {}", e);
    });
    if (!_bghbeats.is_closed()) {
        _heartbeat_timer.arm(next_timeout);
    }
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

} // namespace raft
