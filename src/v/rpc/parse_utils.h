#pragma once

#include "hashing/xx.h"
#include "likely.h"
#include "reflection/async_adl.h"
#include "rpc/types.h"
#include "seastarx.h"

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
        // clang-format off
         throw std::out_of_range(fmt::format(
           "parse_utils out of range. got:{} bytes and expected:{}"
           " bytes", got, expected));
        // clang-format on
    }
}
} // namespace detail

inline ss::future<std::optional<header>>
parse_header(ss::input_stream<char>& in) {
    constexpr size_t rpc_header_size = sizeof(header);
    return in.read_exactly(rpc_header_size)
      .then([&in](ss::temporary_buffer<char> b) {
          if (b.size() != rpc_header_size) {
              return ss::make_ready_future<std::optional<header>>();
          }
          header h;
          std::copy_n(b.get(), rpc_header_size, reinterpret_cast<char*>(&h));
          return ss::make_ready_future<std::optional<header>>(std::move(h));
      });
}
template<typename T>
ss::future<T>
parse_type_wihout_compression(ss::input_stream<char>& in, const header& h) {
    const auto header_size = h.size;
    const auto header_checksum = h.checksum;
    return read_iobuf_exactly(in, header_size)
      .then([header_checksum, header_size](iobuf io) {
          detail::check_out_of_range(io.size_bytes(), header_size);
          auto in = iobuf::iterator_consumer(io.cbegin(), io.cend());
          incremental_xxhash64 hasher;
          size_t consumed = in.consume(
            io.size_bytes(), [&hasher](const char* src, size_t sz) {
                hasher.update(src, sz);
                return ss::stop_iteration::no;
            });
          detail::check_out_of_range(consumed, header_size);
          const auto got_checksum = hasher.digest();
          if (header_checksum != got_checksum) {
              throw std::runtime_error(fmt::format(
                "invalid rpc checksum. got:{}, expected:{}",
                got_checksum,
                header_checksum));
          }
          auto p = std::make_unique<iobuf_parser>(std::move(io));
          auto raw = p.get();
          return reflection::async_adl<T>{}.from(*raw).finally(
            [p = std::move(p)] {});
      });
}
template<typename T>
ss::future<T> parse_type(ss::input_stream<char>& in, const header& h) {
    if (h.bitflags == 0) {
        return rpc::parse_type_wihout_compression<T>(in, h);
    }
    return ss::make_exception_future<T>(std::runtime_error(
      fmt::format("no compression supported. header: {}", h)));
}

} // namespace rpc
