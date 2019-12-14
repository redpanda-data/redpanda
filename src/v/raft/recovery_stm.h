#pragma once

#include "model/metadata.h"
#include "raft/consensus.h"

namespace raft {

class recovery_stm {
public:
    recovery_stm(
      consensus* p,
      follower_index_metadata& meta,
      seastar::io_priority_class prio);

    seastar::future<> apply();

private:
    seastar::future<> do_one_read();
    seastar::future<> replicate(std::vector<raft::entry>);

    consensus* _ptr;
    follower_index_metadata& _meta;
    seastar::io_priority_class _prio;
    // needed to early exit. (node down)
    bool _stop_requested = false;
};

} // namespace raft
