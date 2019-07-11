#pragma once

#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>

/// \brief read a full file and returns it in a buf
///
static future<temporary_buffer<char>>
readfile(const sstring& name) {
    return open_file_dma(name, open_flags::ro)
      .then([](auto fx) {
          return do_with(std::move(fx), [](file& f) {
              return f.size()
                .then([&f](uint64_t size) {
                    return f.dma_read_bulk<char>(0, size);
                })
                .finally([&f]() { return f.close(); });
          });
      });
}
