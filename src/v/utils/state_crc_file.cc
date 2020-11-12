// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/state_crc_file.h"

#include "hashing/crc32c.h"
#include "vassert.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>

#include <memory>
#include <numeric>

namespace utils {

uint32_t crc_iobuf(const iobuf& buf) {
    crc32 crc;
    for (const auto& frag : buf) {
        crc.extend(frag.get(), frag.size());
    }

    return crc.value();
}

state_crc_file::state_crc_file(ss::sstring name)
  : _filename(std::move(name)) {}

ss::future<iobuf> state_crc_file::read_file() {
    return ss::open_file_dma(
             _filename, ss::open_flags::ro | ss::open_flags::create)
      .then([](ss::file f) {
          return f.size()
            .then([f](uint64_t size) mutable {
                return f.dma_read_bulk<char>(0, size);
            })
            .then([f](ss::temporary_buffer<char> b) mutable {
                return f.close().then([f, b = std::move(b)]() mutable {
                    auto ret = iobuf();
                    ret.append(std::move(b));
                    return ret;
                });
            });
      });
}

ss::future<> state_crc_file::write_file(iobuf buf) {
    auto flags = ss::open_flags::wo | ss::open_flags::create
                 | ss::open_flags::truncate;

    return ss::open_file_dma(_filename, flags)
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
} // namespace utils
