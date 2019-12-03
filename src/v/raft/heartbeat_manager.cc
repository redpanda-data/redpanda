#include "raft/heartbeat_manager.h"

#include <boost/range/iterator_range.hpp>

namespace raft {
logger hbeatlog{"r/heartbeat"};
using consensus_ptr = heartbeat_manager::consensus_ptr;
using consensus_set = heartbeat_manager::consensus_set;

static std::vector<heartbeat_request>
requests_for_range(const consensus_set& c) {
    std::unordered_map<model::node_id, std::vector<protocol_metadata>>
      pending_beats;

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
            pending_beats[n.id()].push_back(ptr->meta());
        }
        for (auto& n : group.learners) {
            pending_beats[n.id()].push_back(ptr->meta());
        }
    }

    std::vector<heartbeat_request> reqs;
    reqs.reserve(pending_beats.size());
    for (auto& p : pending_beats) {
        reqs.push_back(heartbeat_request{p.first, std::move(p.second)});
    }

    return reqs;
}

heartbeat_manager::heartbeat_manager(
  duration_type timeout, sharded<client_cache>& cls)
  : _election_duration(timeout)
  , _clients(cls) {
    _heartbeat_timer.set_callback([this] { dispatch_heartbeats(); });
}

future<> heartbeat_manager::do_dispatch_heartbeats(
  clock_type::time_point last_timeout, clock_type::time_point next_timeout) {
    auto reqs = requests_for_range(_consensus_groups);
    return do_with(
      std::move(reqs),
      [this, next_timeout](std::vector<heartbeat_request>& reqs) {
          return parallel_for_each(
            reqs.begin(),
            reqs.end(),
            [this, next_timeout](heartbeat_request& r) {
                return do_heartbeat(std::move(r), next_timeout);
            });
      });
}

future<> heartbeat_manager::do_heartbeat(
  heartbeat_request&& r, clock_type::time_point next_timeout) {
    auto shard = client_cache::shard_for(r.node_id);
    // FIXME: #206
    return smp::submit_to(
             shard,
             [this, r = std::move(r), next_timeout]() mutable {
                 auto& local = _clients.local();
                 if (!local.contains(r.node_id)) {
                     throw std::runtime_error(fmt::format(
                       "Could not find {} in raft::client_cache", r.node_id));
                 }
                 return local.get(r.node_id)->with_client(
                   [next_timeout, r = std::move(r)](
                     reconnect_client::client_type& cli) mutable {
                       hbeatlog.info("DEBUG: sending hbeats {}", r);
                       // FIXME: #137
                       auto f = cli.heartbeat(std::move(r));
                       return with_timeout(next_timeout, std::move(f))
                         .then([](rpc::client_context<heartbeat_reply> r) {
                             return r.data; // copy
                         });
                   });
             })
      .then_wrapped([this](future<heartbeat_reply> fut) {
          try {
              process_reply(fut.get0());
          } catch (...) {
              hbeatlog.error(
                "Could not send heartbeats: {}", std::current_exception());
          }
      });
}

void heartbeat_manager::process_reply(heartbeat_reply&& r) {
    hbeatlog.info("DEBUG: process_reply {}", r);
    for (auto& m : r.meta) {
        auto it = std::lower_bound(
          _consensus_groups.begin(),
          _consensus_groups.end(),
          raft::group_id(m.group),
          details::consensus_ptr_by_group_id{});
        if (it == _consensus_groups.end()) {
            hbeatlog.error("Could not find consensus for group:{}", m.group);
            continue;
        }
        consensus_ptr ptr = *it;
        const raft::group_configuration& cfg = ptr->config();
        if (cfg.contains_machine(model::node_id(m.node_id))) {
            ptr->process_heartbeat(std::move(m));
        }
    }
}

void heartbeat_manager::dispatch_heartbeats() {
    auto next_timeout = clock_type::now() + _election_duration;
    (void)with_gate(_bghbeats, [this, old = _hbeat, next_timeout] {
        return do_dispatch_heartbeats(old, next_timeout);
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
void heartbeat_manager::register_group(lw_shared_ptr<consensus> ptr) {
    _consensus_groups.insert(ptr);
}
future<> heartbeat_manager::start() {
    dispatch_heartbeats();
    return make_ready_future<>();
}
future<> heartbeat_manager::stop() {
    _heartbeat_timer.cancel();
    return _bghbeats.close();
}

} // namespace raft
