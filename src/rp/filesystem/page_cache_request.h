#pragma once
#include <cstdint>
#include <ostream>

#include <seastar/core/file.hh>

namespace rp {
struct page_cache_request {
  // required
  uint32_t file_id;
  int32_t begin_pageno;
  int32_t end_pageno;

  // hints
  int32_t hint_file_last_pageno;

  // how to fetch data
  seastar::lw_shared_ptr<seastar::file> fptr;
  const seastar::io_priority_class &pc;
};
}  // namespace rp

namespace std {
inline ostream &
operator<<(ostream &o, const rp::page_cache_request &r) {
  return o << "rp::page_cache_request{file_id: " << r.file_id
           << ", begin_pageno: " << r.begin_pageno
           << ", end_pageno: " << r.end_pageno
           << ", hint_file_last_pageno: " << r.hint_file_last_pageno << "}";
}
}  // namespace std
