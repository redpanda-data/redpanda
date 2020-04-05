#pragma once

#include "compression/stream_zstd.h"
#include "hashing/xx.h"
#include "likely.h"
#include "reflection/async_adl.h"
#include "rpc/logger.h"
#include "rpc/types.h"
#include "seastarx.h"
#include "vlog.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include <fmt/format.h>

#include <memory>
#include <optional>

namespace rpc {
namespace detail {
static inline void check_out_of_range(size_t got, size_t expected) {
    if (unlikely(got != expected)) {
        throw std::out_of_range(fmt::format(
          "parse_utils out of range. got:{} bytes and expected:{} bytes",
          got,
          expected));
    }
}
} // namespace detail

inline ss::future<std::optional<header>>
parse_header(ss::input_stream<char>& in) {
    return read_iobuf_exactly(in, size_of_rpc_header).then([](iobuf b) {
        if (b.size_bytes() != size_of_rpc_header) {
            return ss::make_ready_future<std::optional<header>>();
        }
        iobuf_parser parser(std::move(b));
        auto h = reflection::adl<header>{}.from(parser);
        if (uint16_t got = checksum_header_only(h);
            unlikely(h.header_checksum != got)) {
            vlog(
              rpclog.info,
              "rpc header missmatching checksums. expected:{}, got:{} - {}",
              h.header_checksum,
              got,
              h);
            return ss::make_ready_future<std::optional<header>>();
        }
        return ss::make_ready_future<std::optional<header>>(h);
    });
}

static inline void
validate_payload_and_header(const iobuf& io, const header& h) {
    detail::check_out_of_range(io.size_bytes(), h.payload_size);
    auto in = iobuf::iterator_consumer(io.cbegin(), io.cend());
    incremental_xxhash64 hasher;
    size_t consumed = in.consume(
      io.size_bytes(), [&hasher](const char* src, size_t sz) {
          hasher.update(src, sz);
          return ss::stop_iteration::no;
      });
    detail::check_out_of_range(consumed, h.payload_size);
    const auto got_checksum = hasher.digest();
    if (h.payload_checksum != got_checksum) {
        throw std::runtime_error(fmt::format(
          "invalid rpc checksum. got:{}, expected:{}",
          got_checksum,
          h.payload_checksum));
    }
}

template<typename T>
ss::future<T> parse_type_wihout_compression(iobuf io) {
    auto p = std::make_unique<iobuf_parser>(std::move(io));
    auto raw = p.get();
    return reflection::async_adl<T>{}.from(*raw).finally([p = std::move(p)] {});
}

template<typename T>
ss::future<T> parse_type(ss::input_stream<char>& in, const header& h) {
    return read_iobuf_exactly(in, h.payload_size).then([h](iobuf io) {
        validate_payload_and_header(io, h);
        if (h.compression == compression_type::none) {
            return rpc::parse_type_wihout_compression<T>(std::move(io));
        }
        if (h.compression == compression_type::zstd) {
            compression::stream_zstd fn;
            io = fn.uncompress(std::move(io));
            return rpc::parse_type_wihout_compression<T>(std::move(io));
        }
        return ss::make_exception_future<T>(std::runtime_error(
          fmt::format("no compression supported. header: {}", h)));
    });
}

} // namespace rpc
