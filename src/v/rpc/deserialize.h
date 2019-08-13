#pragma once

#include "rpc/arity.h"
#include "rpc/for_each_field.h"
#include "rpc/is_std_vector.h"
#include "seastarx.h"
#include "utils/fragmented_temporary_buffer.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

#include <boost/iterator/counting_iterator.hpp>
#include <fmt/format.h>

namespace rpc {
class deserialize_invalid_argument : public std::invalid_argument {
public:
    deserialize_invalid_argument(size_t got, size_t expected)
      : std::invalid_argument(fmt::format(
        "cannot parse input_stream. got:{} bytes and expected:{} bytes",
        got,
        expected)) {
    }
};

template<typename T>
future<> deserialize(input_stream<char>& in, T& t) {
    constexpr bool is_sstring = std::is_same_v<T, sstring>;
    constexpr bool is_vector = is_std_vector_v<T>;
    constexpr bool is_fragmented_buffer
      = std::is_same_v<T, fragmented_temporary_buffer>;
    constexpr bool is_standard_layout = std::is_standard_layout_v<T>;
    constexpr bool is_trivially_copyable = std::is_trivially_copyable_v<T>;

    if constexpr (is_sstring) {
        auto i = std::make_unique<int32_t>(0);
        return deserialize<int32_t>(in, *i).then([&in, &t, max = std::move(i)] {
            return in.read_exactly(*max).then(
              [&in, &t, sz = *max](temporary_buffer<char> buf) {
                  if (buf.size() != sz) {
                      throw deserialize_invalid_argument(buf.size(), sz);
                  }
                  t = sstring(sstring::initialized_later(), sz);
                  std::copy_n(buf.get(), sz, t.data());
                  return make_ready_future<>();
              });
        });
    } else if constexpr (is_vector) {
        using value_type = typename std::decay_t<T>::value_type;
        auto i = std::make_unique<int32_t>(0);
        return deserialize<int32_t>(in, *i).then([&in, &t, max = std::move(i)] {
            t.resize(*max);
            return do_for_each(t, [&in](value_type& i) {
                return deserialize<value_type>(in, i);
            });
        });
    } else if constexpr (is_fragmented_buffer) {
        auto i = std::make_unique<int32_t>(0);
        return deserialize<int32_t>(in, *i).then([&in, &t, max = std::move(i)] {
            using ftbr = fragmented_temporary_buffer::reader;
            return do_with(ftbr(), [max = *max, &in, &t](ftbr& r) {
                return r.read_exactly(in, max).then(
                  [&t, max](fragmented_temporary_buffer b) {
                      if (max != b.size_bytes()) {
                          throw deserialize_invalid_argument(
                            b.size_bytes(), sizeof(int32_t));
                      }
                      t = std::move(b);
                      return make_ready_future<>();
                  });
            });
        });
    } else if constexpr (is_standard_layout && is_trivially_copyable) {
        // rever to constexpr
        constexpr const size_t sz = sizeof(T);
        return in.read_exactly(sz).then(
          [&in, &t, sz](temporary_buffer<char> buf) {
              if (buf.size() != sz) {
                  throw deserialize_invalid_argument(buf.size(), sz);
              }
              std::copy_n(buf.get(), sz, reinterpret_cast<char*>(&t));
              return make_ready_future<>();
          });
    } else if constexpr (is_standard_layout) {
        constexpr size_t sz = arity<T>();
        auto sem = make_lw_shared<semaphore>(1);
        sem->ensure_space_for_waiters(sz);
        for_each_field(t, [&in, sem](auto& field) {
            return get_units(*sem, 1).then(
              [&in, &field, sem](semaphore_units<> units) mutable {
                  return deserialize(in, field).finally(
                    [units = std::move(units)] {});
              });
        });
        return sem->wait(1).finally([sem] {});
    }
    throw std::runtime_error(fmt::format(
      "rpc: no deserializer registered. is_vector:{}, is_fragmented_buffer:{}, "
      "is_standard_layout:{}, is_copy_constructible:{}",
      is_vector,
      is_fragmented_buffer,
      is_standard_layout,
      is_trivially_copyable));
}

} // namespace rpc
