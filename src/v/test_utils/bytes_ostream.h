#pragma once
#include "bytes/bytes_ostream.h"
#include "utils/fragbuf.h"

fragbuf release_to_fragbuf(bytes_ostream&& bytes) {
    std::vector<temporary_buffer<char>> v;
    std::transform(
      bytes.begin(),
      bytes.end(),
      std::back_inserter(v),
      [](bytes_ostream::fragment& f) { return std::move(f).release(); });
    return fragbuf(std::move(v));
}