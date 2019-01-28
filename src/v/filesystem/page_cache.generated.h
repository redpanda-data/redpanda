#pragma once

#include <algorithm>  // for std::lower_bound
#include <array>      // for std::array
#include <cstdint>    // for int32_t
#include <iterator>   // for std::distance
#include <utility>

#include <smf/log.h>  // for debug builds

namespace v {

constexpr std::array<std::pair<int32_t, int32_t>, 8192>
generate_clamp_table() {
  std::array<std::pair<int32_t, int32_t>, 8192> ret = {};
  for (int32_t i = 0; i < 8192; ++i) {
    int32_t j = i * 64;
    ret[i].first = j;
    ret[i].second = j + 64;
  }
  return ret;
}

static constexpr const std::array<std::pair<int32_t, int32_t>, 8192>
  kPageCacheClampTable = generate_clamp_table();

#if __cplusplus > 201703L
// std::lower_bound is only constexpr in c++20
constexpr
#endif
  inline std::pair<int32_t, int32_t>
  page_cache_table_clamp_page(int32_t pageno) {
  DLOG_THROW_IF(pageno > 524288, "Asked for bigger page than possible");
  struct clamp_comparator final {
    constexpr bool
    operator()(const int32_t &pageno,
               const std::pair<int32_t, int32_t> &p) const {
      return pageno < (p.second - 1);
    }
    constexpr bool
    operator()(const std::pair<int32_t, int32_t> &p,
               const int32_t &pageno) const {
      return (p.second - 1) < pageno;
    }
  };

  return *std::lower_bound(kPageCacheClampTable.begin(),
                           kPageCacheClampTable.end(), pageno,
                           clamp_comparator{});
}

}  // namespace v
