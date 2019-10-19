#pragma once
#include "bytes/bytes_ostream.h"
#include "utils/fragbuf.h"

fragbuf copy_to_fragbuf(const bytes_ostream& bytes) {
    std::vector<temporary_buffer<char>> v;
    std::transform(
      bytes.begin(),
      bytes.end(),
      std::back_inserter(v),
      [](const bytes_ostream::fragment& f) {
          return temporary_buffer<char>(f.get(), f.size());
      });
    return fragbuf(std::move(v));
}