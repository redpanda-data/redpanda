/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "utils/file_io.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>

ss::future<ss::temporary_buffer<char>> read_fully_tmpbuf(ss::sstring name) {
    return ss::open_file_dma(
             std::move(name), ss::open_flags::ro | ss::open_flags::create)
      .then([](ss::file f) {
          return f.size()
            .then([f](uint64_t size) mutable {
                return f.dma_read_bulk<char>(0, size);
            })
            .then([f](ss::temporary_buffer<char> buf) mutable {
                return f.close().then([f, buf = std::move(buf)]() mutable {
                    return std::move(buf);
                });
            });
      });
}

ss::future<iobuf> read_fully(ss::sstring name) {
    return read_fully_tmpbuf(std::move(name))
      .then([](ss::temporary_buffer<char> buf) {
          iobuf iob;
          iob.append(std::move(buf));
          return iob;
      });
}

ss::future<> write_fully(const std::filesystem::path& p, iobuf buf) {
    static constexpr const size_t buf_size = 4096;
    auto flags = ss::open_flags::wo | ss::open_flags::create
                 | ss::open_flags::truncate;

    return ss::open_file_dma(p.string(), flags)
      .then([](ss::file f) {
          return ss::make_file_output_stream(std::move(f), buf_size);
      })
      .then([b = std::move(buf)](ss::output_stream<char> out) mutable {
          return ss::do_with(
            std::move(b),
            std::move(out),
            [](iobuf& buf, ss::output_stream<char>& ofs) mutable {
                return seastar::do_for_each(
                         buf.begin(),
                         buf.end(),
                         [&ofs](iobuf::fragment& frag) {
                             return ofs.write(frag.get(), frag.size());
                         })
                  .then([&ofs]() mutable { return ofs.close(); });
            });
      });
}
