#pragma once

#include "raft/types.h"
#include "random/simple_time_jitter.h"

namespace raft {

using timeout_jitter
  = simple_time_jitter<raft::clock_type, raft::duration_type>;

} // namespace raft
