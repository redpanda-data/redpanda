#pragma once

namespace rp {

/// google's jump consistent hash
/// https://arxiv.org/pdf/1406.2294.pdf
inline uint32_t
jump_consistent_hash(uint64_t key, uint32_t num_buckets) {
  int64_t b = -1, j = 0;
  while (j < num_buckets) {
    b = j;
    key = key * 2862933555777941757ULL + 1;
    j = (b + 1) *
        (static_cast<double>(1LL << 31) / static_cast<double>((key >> 33) + 1));
  }
  return static_cast<uint32_t>(b);
}

}  // namespace rp
