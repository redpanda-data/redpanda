#include "raft/heartbeat_manager.h"

#include "outcome_future_utils.h"
#include "raft/errc.h"
#include "raft/raftgen_service.h"
#include "rpc/reconnect_transport.h"

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

static future<result<heartbeat_reply>> send_beat(
  client_cache& local, clock_type::time_point tmo, heartbeat_request&& r) {
    using ret_t = result<heartbeat_reply>;
    if (!local.contains(r.node_id)) {
        return make_ready_future<ret_t>(errc::missing_tcp_client);
    }

    return local.get(r.node_id)->get_connected().then(
      [tmo, r = std::move(r)](result<rpc::transport*> t) mutable {
          if (!t) {
              return make_ready_future<ret_t>(t.error());
          }
          hbeatlog.trace("sending hbeats {}", r);
          auto f
            = raftgen_client_protocol(*(t.value())).heartbeat(std::move(r));
          return result_with_timeout(tmo, errc::timeout, std::move(f))
            .then([](auto r) {
                if (!r) {
                    return make_ready_future<ret_t>(r.error());
                }
                // TODO: wrap in foreign ptr
                return make_ready_future<ret_t>(std::move(r.value().data));
            });
      });
}

future<> heartbeat_manager::do_heartbeat(
  heartbeat_request&& r, clock_type::time_point next_timeout) {
    auto shard = client_cache::shard_for(r.node_id);
    std::vector<group_id> groups(r.meta.size());
    for (size_t i = 0; i < groups.size(); ++i) {
        groups[i] = group_id(r.meta[i].group);
    }
    return smp::submit_to(
             shard,
             [this, r = std::move(r), next_timeout]() mutable {
                 return send_beat(_clients.local(), next_timeout, std::move(r));
             })
      .then([node = r.node_id, groups = std::move(groups), this](
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
            // propagte error
            (*it)->process_heartbeat(
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
        (*it)->process_heartbeat(n, result<append_entries_reply>(std::move(m)));
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
