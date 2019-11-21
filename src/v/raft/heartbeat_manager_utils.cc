#include "raft/heartbeat_manager_utils.h"

#include <boost/range/iterator_range.hpp>

namespace raft {
using consensus_ptr = heartbeat_manager_utils::consensus_ptr;
using container_t = heartbeat_manager_utils::container_t;
using iterator = heartbeat_manager_utils::iterator;
using range = heartbeat_manager_utils::range;

/// \brief returns a range of all leaders
range heartbeat_manager_utils::leaders(std::vector<consensus_ptr>& c) {
    // stable_partition, returns a iterator to the first non-true item.
    // for us that's the end of our collection
    auto it = std::stable_partition(
      std::begin(c), std::end(c), [](const consensus_ptr& i) {
          return i->is_leader();
      });
    return {std::begin(c), it};
}

/// \brief returns a set of requests
std::vector<heartbeat_request>
heartbeat_manager_utils::requests_for_range(range r) {
    std::unordered_map<model::node_id, std::vector<protocol_metadata>>
      pending_beats;

    for (auto iter : boost::make_iterator_range(r.first, r.second)) {
        auto& group = iter->config();
        for (auto& n : group.nodes) {
            // do not send beat to self
            if (n.id() == iter->config().leader_id) {
                continue;
            }

            pending_beats[n.id()].push_back(iter->meta());
        }
        for (auto& n : group.learners) {
            pending_beats[n.id()].push_back(iter->meta());
        }
    }

    std::vector<heartbeat_request> reqs;
    reqs.reserve(pending_beats.size());
    for (auto& p : pending_beats) {
        reqs.push_back(heartbeat_request{p.first, std::move(p.second)});
    }

    return reqs;
}

} // namespace raft
