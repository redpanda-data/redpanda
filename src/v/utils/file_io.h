#pragma once

#include "seastarx.h"

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>

#include <bytes/iobuf.h>

static inline ss::future<iobuf> read_fully(ss::sstring name) {
    return ss::open_file_dma(name, ss::open_flags::ro).then([](ss::file f) {
        return f.size()
          .then([f](uint64_t size) mutable {
              return f.dma_read_bulk<char>(0, size);
          })
          .then([f](ss::temporary_buffer<char> buf) mutable {
              return f.close().then([f, buf = std::move(buf)]() mutable {
                  iobuf iob;
                  iob.append(std::move(buf));
                  return iob;
              });
          });
    });
}
