#pragma once

namespace rp {
struct raft_nstpidx_stats {
  uint64_t earliest_offset{0};
  uint64_t largest_offset{0};
  uint64_t term;
};
}  // namespace rp
