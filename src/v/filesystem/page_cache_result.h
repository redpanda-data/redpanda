#pragma once
#include <cstdint>
#include <memory>

#include <gsl/span>
#include <smf/log.h>
#include <smf/macros.h>

struct page_cache_result {
  enum class priority : int8_t {
    low = 0,
    high = 1,
  };
  explicit page_cache_result(int32_t first_page, gsl::span<char> payload,
                             priority p)
    : begin_pageno(first_page), data(payload), prio(p) {}

  page_cache_result(page_cache_result &&o) noexcept
    : begin_pageno(o.begin_pageno), data(std::move(o.data)),
      prio(std::move(o.prio)), marked_for_eviction(o.marked_for_eviction),
      locks(o.locks), pending_reads(o.pending_reads) {}

  page_cache_result &
  operator=(page_cache_result &&o) noexcept {
    if (this != &o) {
      this->~page_cache_result();
      new (this) page_cache_result(std::move(o));
    }
    return *this;
  }
  SMF_DISALLOW_COPY_AND_ASSIGN(page_cache_result);
  ~page_cache_result() {
    DLOG_ERROR_IF(!is_evictable(), "did not finish releasing locks. {}", *this);
  }

  // NOTE: range: (begin, end]
  const int32_t begin_pageno;
  gsl::span<char> data;
  priority prio;
  bool marked_for_eviction{false};
  int32_t locks{0};
  int32_t pending_reads{0};

  int32_t
  end_pageno() const {
    // x >> 12 == x/4096
    static constexpr const int32_t kTrailingZeroes = __builtin_ctz(4096);
    return begin_pageno + std::max<int32_t>(1, data.size() >> kTrailingZeroes);
  }

  bool
  is_page_in_range(int32_t pageno) const {
    return pageno >= begin_pageno && pageno < end_pageno();
  }
  const char *
  get_page_data(int32_t pageno) const {
    DLOG_THROW_IF(!is_page_in_range(pageno),
                  "page number out of bounds: {}. ({}-{}]", pageno,
                  begin_pageno, end_pageno());
    const int32_t slot = pageno - begin_pageno;
    return data.data() + (slot * 4096);
  }
  bool
  is_evictable() const {
    return locks <= 0;
  }
};

struct page_cache_result_lease {
  page_cache_result_lease() : result(nullptr) {}
  explicit page_cache_result_lease(page_cache_result *p) : result(p) {
    if (result != nullptr) result->locks++;
  }

  page_cache_result_lease(page_cache_result_lease &&o) noexcept
    : result(std::move(o.result)) {}

  page_cache_result_lease &
  operator=(const page_cache_result_lease &o) {
    if (this != &o) {
      if (result != nullptr) result->locks--;
      result = o.result;
      if (result != nullptr) result->locks++;
    }
    return *this;
  }

  ~page_cache_result_lease() {
    if (result != nullptr) result->locks--;
    result = nullptr;
  }

  inline operator bool() const { return result != nullptr; }

  page_cache_result *result = nullptr;
};


namespace std {
inline ostream &
operator<<(ostream &o, page_cache_result::priority p) {
  seastar::sstring it = "high";
  if (p == page_cache_result::priority::low) { it = "low"; }
  return o << "page_cache_result::priority{" << it << "}";
}
inline ostream &
operator<<(ostream &o, const page_cache_result &r) {
  return o << "page_cache_result{begin_pageno: " << r.begin_pageno
           << ", end_pageno: " << r.end_pageno()
           << ", data(size): " << r.data.size() << ", prio: " << r.prio
           << ", marked_for_eviction: " << r.marked_for_eviction
           << ", locks: " << r.locks << ", pending_reads: " << r.pending_reads
           << " }";
}
}  // namespace std
