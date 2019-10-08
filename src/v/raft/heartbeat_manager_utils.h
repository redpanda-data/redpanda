#pragma once

#include "raft/consensus.h"
#include "raft/types.h"

#include <seastar/core/shared_ptr.hh>

#include <vector>

namespace raft {

struct heartbeat_manager_utils {
    using consensus_ptr = lw_shared_ptr<consensus>;
    using container_t = std::vector<consensus_ptr>;
    using iterator = typename container_t::iterator;
    using range = std::pair<iterator, iterator>;

    /// \brief returns a range of all leaders
    static range leaders(std::vector<consensus_ptr>&);

    /// \brief returns a set of requests
    static std::vector<heartbeat_request> requests_for_range(range);
};

} // namespace raft
