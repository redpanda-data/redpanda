/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "utils/file_io.h"

#include "bytes/iobuf.h"
#include "bytes/iostream.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/temporary_buffer.hh>

#include <exception>

ss::future<ss::temporary_buffer<char>>
read_fully_tmpbuf(const std::filesystem::path& name) {
    return ss::with_file(
      ss::open_file_dma(name.string(), ss::open_flags::ro), [](ss::file f) {
          return f.size().then([f](uint64_t size) mutable {
              return f.dma_read_bulk<char>(0, size);
          });
      });
}

ss::future<iobuf> read_fully(const std::filesystem::path& name) {
    return read_fully_tmpbuf(name).then([](ss::temporary_buffer<char> buf) {
        iobuf iob;
        iob.append(std::move(buf));
        return iob;
    });
}

/**
 * This helper is useful for YAML loading, because yaml-cpp expects
 * a string.
 *
 * Background on why: https://github.com/jbeder/yaml-cpp/issues/765
 */
ss::future<ss::sstring>
read_fully_to_string(const std::filesystem::path& name) {
    return read_fully_tmpbuf(name).then([](ss::temporary_buffer<char> buf) {
        return ss::to_sstring(std::move(buf));
    });
}

ss::future<> write_fully(const std::filesystem::path& p, iobuf buf) {
    static constexpr const size_t buf_size = 4096;
    auto flags = ss::open_flags::wo | ss::open_flags::create
                 | ss::open_flags::truncate;
    /// Closes file on failure, otherwise file is expected to be closed in the
    /// success case where the ss::output_stream calls close()
    return ss::with_file_close_on_failure(
      ss::open_file_dma(p.string(), flags),
      [buf = std::move(buf)](ss::file f) mutable {
          return ss::make_file_output_stream(std::move(f), buf_size)
            .then([buf = std::move(buf)](ss::output_stream<char> out) mutable {
                return ss::do_with(
                  std::move(out),
                  [buf = std::move(buf)](ss::output_stream<char>& out) mutable {
                      return write_iobuf_to_output_stream(std::move(buf), out)
                        .then([&out]() mutable { return out.close(); });
                  });
            });
      });
}
