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

#include "reflection/arity.h"
#include "reflection/for_each_field.h"
#include "rpc/demo/types.h"
#include "seastarx.h"
#include "ssx/sformat.h"
#include "utils/hdr_hist.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>

inline ss::future<>
force_write_ptr(ss::sstring filename, const char* ptr, std::size_t len) {
    auto flags = ss::open_flags::rw | ss::open_flags::create
                 | ss::open_flags::truncate;
    return open_file_dma(filename, flags)
      .then(
        [](ss::file f) { return ss::make_file_output_stream(std::move(f)); })
      .then([ptr, len](ss::output_stream<char> o) mutable {
          auto out = ss::make_lw_shared<ss::output_stream<char>>(std::move(o));
          return out->write(ptr, len)
            .then([out] { return out->flush(); })
            .then([out] { return out->close(); })
            .finally([out] {});
      });
}

inline ss::future<>
force_write_buffer(ss::sstring filename, ss::temporary_buffer<char> b) {
    const char* ptr = b.get();
    std::size_t len = b.size();
    return force_write_ptr(std::move(filename), ptr, len)
      .then([b = std::move(b)] {});
}

inline ss::future<> write_histogram(ss::sstring filename, const hdr_hist& h) {
    return force_write_buffer(std::move(filename), h.print_classic());
}

namespace demo {
inline iobuf rand_iobuf(std::size_t chunks, std::size_t chunk_size) {
    iobuf b;
    for (size_t i = 0; i < chunks; ++i) {
        b.append(ss::temporary_buffer<char>(chunk_size));
    }
    return b;
}

inline demo::simple_request
gen_simple_request(size_t data_size, size_t chunk_size) {
    const std::size_t chunks = data_size / chunk_size;
    return demo::simple_request{.data = rand_iobuf(chunks, chunk_size)};
}

inline interspersed_request
gen_interspersed_request(size_t data_size, size_t chunk_size) {
    const std::size_t chunks = data_size / chunk_size / 8;
    // clang-format off
    return interspersed_request{
      .data = interspersed_request::
        payload{._one = i1{.y = rand_iobuf(chunks, chunk_size)},
                ._two = i2{.x = i1{.y = rand_iobuf(chunks, chunk_size)},
                           .y = rand_iobuf(chunks, chunk_size)},
                ._three = i3{.x = i2{.x = i1{.y = rand_iobuf(
                                               chunks, chunk_size)},
                                     .y = rand_iobuf(chunks, chunk_size)},
                             .y = rand_iobuf(chunks, chunk_size)}},
      .x = rand_iobuf(chunks, chunk_size),
      .y = rand_iobuf(chunks, chunk_size)};
    // clang-format on
}

} // namespace demo
