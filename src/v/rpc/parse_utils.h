#pragma once

#include "rpc/deserialize.h"
#include "rpc/types.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include <fmt/ostream.h>

#include <memory>
#include <optional>

namespace rpc {
inline future<std::optional<header>> parse_header(input_stream<char>& in) {
    constexpr size_t rpc_header_size = sizeof(header);
    return in.read_exactly(rpc_header_size)
      .then([&in](temporary_buffer<char> b) {
          if (b.size() != rpc_header_size) {
              return make_ready_future<std::optional<header>>();
          }
          header h;
          std::copy_n(b.get(), rpc_header_size, reinterpret_cast<char*>(&h));
          return make_ready_future<std::optional<header>>(std::move(h));
      });
}
template<typename T>
future<T>
parse_type_wihout_compression(input_stream<char>& in, const header& h) {
    const auto header_size = h.size;
    const auto header_checksum = h.checksum;
    return do_with(source(in), [header_size, header_checksum](source& src) {
        return deserialize<T>(src).then(
          [header_size, header_checksum, &src](T t) {
              const auto got_checksum = src.checksum();
              if (header_size != src.size_bytes()) {
                  throw deserialize_invalid_argument(
                    src.size_bytes(), header_size);
              }
              if (header_checksum != got_checksum) {
                  throw std::runtime_error(fmt::format(
                    "invalid rpc checksum. got:{}, expected:{}",
                    got_checksum,
                    header_checksum));
              }
              return std::move(t);
          });
    });
}
template<typename T>
future<T> parse_type(input_stream<char>& in, const header& h) {
    if (h.bitflags == 0) {
        return rpc::parse_type_wihout_compression<T>(in, h);
    }
    return make_exception_future<T>(std::runtime_error(
      fmt::format("no compression supported. header: {}", h)));
}

} // namespace rpc
