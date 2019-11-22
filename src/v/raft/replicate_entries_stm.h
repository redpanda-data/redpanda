#pragma once

#include "raft/consensus.h"

namespace raft {

class replicate_entries_stm {
public:
    replicate_entries_stm(consensus* p) noexcept
      : _ptr(p) {
    }

    /// assumes that this is operating under the consensus::_op_sem lock
    future<> replicate(append_entries_request&&);

private:
    consensus* _ptr;
};
} // namespace raft
