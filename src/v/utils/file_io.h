#pragma once
#include "seastarx.h"

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>

#include <bytes/iobuf.h>

static inline future<iobuf> read_fully(sstring name) {
    return open_file_dma(name, open_flags::ro).then([](file f) {
        return f.size()
          .then([f](uint64_t size) mutable {
              return f.dma_read_bulk<char>(0, size);
          })
          .then([f](temporary_buffer<char> buf) mutable {
              return f.close().then([f, buf = std::move(buf)]() mutable {
                  iobuf iob;
                  iob.append(std::move(buf));
                  return iob;
              });
          });
    });
}
