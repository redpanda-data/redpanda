#include "raft/heartbeat_manager.h"

#include "raft/heartbeat_manager_utils.h"

#include <boost/range/iterator_range.hpp>

namespace raft {
using consensus_ptr = heartbeat_manager::consensus_ptr;
struct group_id_ordering {
    bool operator()(const consensus_ptr& ptr, group_id::type value) const {
        return ptr->meta().group < value;
    }
};

template<typename Container>
bool has_node(const Container& c, model::node_id::type needle) {
    return std::any_of(
      std::begin(c), std::end(c), [needle](const model::broker& b) {
          return needle == b.id();
      });
}

heartbeat_manager::heartbeat_manager(
  duration_type timeout, sharded<client_cache>& cls)
  : _election_duration(timeout)
  , _clients(cls) {
    _heartbeat_timer.set_callback([this] { dispatch_heartbeats(); });
}

future<> heartbeat_manager::do_dispatch_heartbeats(
  clock_type::time_point last_timeout, clock_type::time_point next_timeout) {
    auto reqs = heartbeat_manager_utils::requests_for_range(
      heartbeat_manager_utils::leaders(_iteration_order));
    return do_with(
      std::move(reqs), [this](std::vector<heartbeat_request>& reqs) {
          return parallel_for_each(
            reqs.begin(), reqs.end(), [this](heartbeat_request& r) {
                return do_heartbeat(std::move(r));
            });
      });
}

future<> heartbeat_manager::do_heartbeat(heartbeat_request&& r) {
    auto shard = client_cache::shard_for(r.node_id);
    // FIXME: #206
    // clang-format off
    return smp::submit_to(shard, [this, r = std::move(r)]() mutable {
        auto& local = _clients.local();
        if (!local.contains(r.node_id)) {
            throw std::runtime_error(fmt::format(
              "Could not find {} in raft::client_cache", r.node_id));
        }
        return local.get(r.node_id)->with_client(
          [r = std::move(r)](reconnect_client::client_type& cli) mutable {
              // FIXME: #137
              return cli.heartbeat(std::move(r));
          });
     }).then([this](rpc::client_context<heartbeat_reply> ctx) {
         process_reply(std::move(ctx.data));
     });
    // clang-format on
}

void heartbeat_manager::process_reply(heartbeat_reply&& r) {
    for (auto& m : r.meta) {
        // O( log(n) )
        auto it = std::lower_bound(
          _sorted_groups.begin(),
          _sorted_groups.end(),
          m.group,
          group_id_ordering{});

        // O(1) - about 6 comparisons or so if true

        if (it != _sorted_groups.end()) {
            consensus& c = **it;
            auto& cfg = c.config();
            if (
              has_node(cfg.nodes, m.node_id)
              || has_node(cfg.learners, m.node_id)) {
                c.process_heartbeat(std::move(m));
            }
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
    if (auto it = std::lower_bound(
          _sorted_groups.begin(), _sorted_groups.end(), g, group_id_ordering{});
        it != _sorted_groups.end()) {
        _sorted_groups.erase(it);
    }
    if (auto it = std::find_if(
          _iteration_order.begin(),
          _iteration_order.end(),
          [g](const consensus_ptr& i) { return g == i->meta().group; });
        it != _iteration_order.end()) {
        std::swap(*it, _iteration_order.back());
        _iteration_order.pop_back();
    }
}
void heartbeat_manager::register_group(lw_shared_ptr<consensus> ptr) {
    _iteration_order.push_back(ptr);
    _sorted_groups.insert(ptr);
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
