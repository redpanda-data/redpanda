#pragma once

#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>

namespace rp {

/// \brief read a full file and returns it in a buf
///
static seastar::future<seastar::temporary_buffer<char>>
readfile(const seastar::sstring &name) {
  return seastar::open_file_dma(name, seastar::open_flags::ro)
    .then([](auto fx) {
      return seastar::do_with(std::move(fx), [](seastar::file &f) {
        return f.size()
          .then([&f](uint64_t size) { return f.dma_read_bulk<char>(0, size); })
          .finally([&f]() { return f.close(); });
      });
    });
}

}  // namespace rp
