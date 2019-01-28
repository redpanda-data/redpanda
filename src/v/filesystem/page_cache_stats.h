#pragma once
#include <cstdint>
#include <iostream>
#include <memory>

#include <smf/histogram.h>
#include <smf/human_bytes.h>
#include <smf/macros.h>

#include "adt/ema.h"

namespace v {
struct page_cache_stats {
  uint64_t fetches{0};
  uint64_t prefetches{0};
  uint64_t cache_hit{0};
  uint64_t cache_miss{0};
  uint64_t stalls{0};
  uint64_t disk_bytes_read{0};
  uint64_t served_bytes{0};
  ema<10> disk_latency;
};
}  // namespace v
namespace std {
inline ostream &
operator<<(ostream &o, const v::page_cache_stats &s) {
  double allreqs = static_cast<double>(s.cache_miss + s.cache_hit + s.stalls);
  double missrate =
    s.cache_miss > 0 ? (static_cast<double>(s.cache_miss) / allreqs) : 0;
  double hitrate =
    s.cache_hit > 0 ? (static_cast<double>(s.cache_hit) / allreqs) : 0;

  o << "v::wal_disk_pager_stats{ prefetches: " << s.prefetches
    << ", disk_bytes_read: " << smf::human_bytes(s.disk_bytes_read)
    << ", served_bytes: " << smf::human_bytes(s.served_bytes)
    << ", cache_hit: " << s.cache_hit << ", cache_miss: " << s.cache_miss
    << ", stalls: " << s.stalls << ", disk_latency: " << s.disk_latency
    << ", hit_rate: " << hitrate << ", miss_rate: " << missrate << " }";
  return o;
}
}  // namespace std
