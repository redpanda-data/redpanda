#pragma once

#include "bytes/iobuf.h"
#include "reflection/arity.h"
#include "reflection/for_each_field.h"
#include "rpc/is_std_helpers.h"
#include "rpc/source.h"
#include "seastarx.h"
#include "utils/copy_range.h"
#include "utils/memory_data_source.h"
#include "utils/named_type.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

#include <boost/iterator/counting_iterator.hpp>
#include <boost/range/irange.hpp>
#include <fmt/format.h>

#include <vector>

namespace rpc {
class deserialize_invalid_argument : public std::invalid_argument {
public:
    deserialize_invalid_argument(size_t got, size_t expected)
      : std::invalid_argument(fmt::format(
        "cannot parse input_stream. got:{} bytes and expected:{} bytes",
        got,
        expected)) {}
};

template<typename T>
ss::future<T> deserialize(source& in) {
    constexpr bool is_optional = is_std_optional_v<T>;
    constexpr bool is_sstring = std::is_same_v<T, ss::sstring>;
    constexpr bool is_vector = is_std_vector_v<T>;
    constexpr bool is_iobuf = std::is_same_v<T, iobuf>;
    constexpr bool is_standard_layout = std::is_standard_layout_v<T>;
    constexpr bool is_trivially_copyable = std::is_trivially_copyable_v<T>;

    if constexpr (is_optional) {
        /// sizeof(bool) is implementation defined, and the standard puts
        /// notable emphasis on this fact.
        //  section: §5.3.3/1 of the standard:
        using value_type = typename std::decay_t<T>::value_type;
        return deserialize<int8_t>(in).then([&in](int8_t is_set) {
            if (is_set == 0) {
                return ss::make_ready_future<T>();
            }
            return deserialize<value_type>(in).then([](value_type t) {
                return ss::make_ready_future<T>(std::move(t));
            });
        });
    } else if constexpr (is_sstring) {
        return deserialize<int32_t>(in).then([&in](int32_t sz) {
            return in.read_exactly(sz).then(
              [&in, sz](ss::temporary_buffer<char> buf) {
                  if (buf.size() != static_cast<size_t>(sz)) {
                      throw deserialize_invalid_argument(buf.size(), sz);
                  }
                  return ss::sstring(buf.get(), sz);
              });
        });
    } else if constexpr (is_vector) {
        using value_type = typename std::decay_t<T>::value_type;
        return deserialize<int32_t>(in).then([&in](int32_t w) {
            return ss::do_with(
              boost::irange(0, static_cast<int>(w)), [&in](auto& r) {
                  return copy_range<T>(
                    r, [&in](int) { return deserialize<value_type>(in); });
              });
        });
    } else if constexpr (is_iobuf) {
        return deserialize<int32_t>(in).then([&in](int32_t max) {
            return in.read_iobuf(max).then([max](iobuf b) {
                if (__builtin_expect(
                      static_cast<size_t>(max) != b.size_bytes(), false)) {
                    throw deserialize_invalid_argument(
                      b.size_bytes(), sizeof(int32_t));
                }
                return std::move(b);
            });
        });
    } else if constexpr (is_standard_layout && is_trivially_copyable) {
        constexpr const size_t sz = sizeof(T);
        return in.read_exactly(sz).then(
          [&in, sz](ss::temporary_buffer<char> buf) {
              if (__builtin_expect(buf.size() != sz, false)) {
                  throw deserialize_invalid_argument(buf.size(), sz);
              }
              T t{};
              std::copy_n(buf.get(), sz, reinterpret_cast<char*>(&t));
              return t;
          });
    } else if constexpr (is_standard_layout) {
        return ss::do_with(T{}, [&in](T& t) mutable {
            auto f = ss::make_ready_future<>();
            reflection::for_each_field(t, [&](auto& field) mutable {
                f = f.then([&in, &field]() mutable {
                    return deserialize<std::decay_t<decltype(field)>>(in).then(
                      [&field](auto value) mutable {
                          field = std::move(value);
                      });
                });
            });
            return f.then([&t] { return std::move(t); });
        });
    }
    static_assert(
      (is_optional || is_sstring || is_vector || is_iobuf
       || (is_standard_layout) || is_trivially_copyable),
      "rpc: no deserializer registered");
}
template<
  typename T,
  typename Tag,
  typename U = std::enable_if_t<std::is_arithmetic_v<T>, T>>
ss::future<named_type<U, Tag>> deserialize(source& in) {
    return deserialize<U>(in).then(
      [](U u) { return named_type<U, Tag>{std::move(u)}; });
}

template<typename T>
ss::future<T> deserialize(iobuf&& fb) {
    auto in = make_iobuf_input_stream(std::move(fb));
    return ss::do_with(std::move(in), [](ss::input_stream<char>& in) {
        return ss::do_with(rpc::source(in), [](rpc::source& src) {
            return rpc::deserialize<T>(src);
        });
    });
}

} // namespace rpc
