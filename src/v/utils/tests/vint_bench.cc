// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "random/generators.h"
#include "utils/vint.h"
#include "utils/vint_iostream.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/later.hh>

#include <array>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <optional>
#include <random>

struct iobuf_reader {
    explicit iobuf_reader(iobuf io)
      : _io(std::move(io)) {}

    bool eof() { return _io.empty() && _pos == _tbuf.size(); }

    ss::future<char> read1() noexcept {
        return ss::make_ready_future<char>(read1_sync());
    }

    char read1_sync() noexcept {
        if (_pos == _tbuf.size()) {
            refill();
        }

        return _tbuf[_pos++];
    }

    template<size_t N>
    std::pair<std::array<char, N>, bool> peek() {
        return {{}, true};
    };

    void skip(size_t) {}

    void refill() {
        if (_io.empty()) {
            vassert(false, "_io.empty()");
        }
        _tbuf = _io.begin()->share();
        _io.pop_front();
        _pos = 0;
    }

    iobuf _io;
    ss::temporary_buffer<char> _tbuf;
    size_t _pos = 0;
};

struct vint_bench {};

static constexpr uint32_t max_vint = 64U * 128 * 128 * 128 * 32 - 1;

using decode_t = std::pair<uint32_t, size_t>;

inline decode_t decode_u32(const char* src) {
    uint32_t result = 0;
    uint64_t shift = 0;
    size_t bytes_read = 0;
    for (; bytes_read < 5; src++) {
        uint64_t byte = *src;
        if (byte & 128) {
            result |= ((byte & 127) << shift);
        } else {
            result |= byte << shift;
            break;
        }
        shift += 7;
        if (++bytes_read == 5) {
            break;
        }
    }

    return {result, bytes_read};
}

iobuf make_vints(size_t count) {
    iobuf ret;
    for (size_t c = 0; c < count; c++) {
        const auto test_number = random_generators::get_int<uint64_t>(
          0, max_vint);
        ret.append(bytes_to_iobuf(unsigned_vint::to_bytes(test_number)));
    }
    return ret;
}

using stream = ss::input_stream<char>;

stream make_vints_stream(size_t count) {
    return make_iobuf_input_stream(make_vints(count));
}

auto make_vints_reader(size_t count) { return iobuf_reader(make_vints(count)); }

namespace unsigned_vint {

constexpr uint8_t limit_bits = ((vint::max_length - 1) * 7);

ss::future<std::pair<uint32_t, size_t>> deserialize_coro(iobuf_reader& s) {
    /// Consume up to 5 iterations (0-4) of reading 7 bits each time
    detail::var_decoder decoder(limit_bits);
    while (true) {
        char c = co_await s.read1();
        if (decoder.accept(c)) {
            break;
        }
    }
    co_return std::make_pair(
      static_cast<uint32_t>(decoder.result), decoder.bytes_read);
}

inline std::pair<uint32_t, size_t> to_pair(detail::var_decoder& decoder) {
    return std::make_pair(
      static_cast<uint32_t>(decoder.result), decoder.bytes_read);
}

auto ready_pair(detail::var_decoder& decoder) {
    return ss::make_ready_future<std::pair<uint32_t, size_t>>(to_pair(decoder));
}

ss::future<std::pair<uint32_t, size_t>>
sdc_recurse(iobuf_reader& s, detail::var_decoder decoder) {
    return s.read1().then([&s, decoder](char c) mutable {
        if (decoder.accept(c)) {
            return ready_pair(decoder);
        }
        return sdc_recurse(s, decoder);
    });
}

ss::future<std::pair<uint32_t, size_t>>
stream_deserialize_cont(iobuf_reader& s) {
    /// Consume up to 5 iterations (0-4) of reading 7 bits each time
    constexpr auto limit = ((max_length - 1) * 7);
    return sdc_recurse(s, detail::var_decoder{limit});
}

ss::future<int> foo() {
    auto f = ss::make_ready_future<>().then([] { return 1; });
    return f;
}

ss::future<std::pair<uint32_t, size_t>>
deserialize_repeat_until_value(iobuf_reader& s) {
    using opt_t = std::optional<decode_t>;

    return ss::repeat_until_value(
      [&s, decoder = detail::var_decoder{limit_bits}]() mutable {
          return s.read1().then([decoder](char c) mutable {
              if (decoder.accept(c)) {
                  return ss::make_ready_future<opt_t>(opt_t{to_pair(decoder)});
              }
              return ss::make_ready_future<opt_t>();
          });
      });
}

ss::future<std::pair<uint32_t, size_t>>
deserialize_repeat_do_with(iobuf_reader& s) {
    return ss::do_with(
      detail::var_decoder{limit_bits}, [&s](detail::var_decoder& decoder) {
          return ss::repeat([&s, &decoder]() mutable {
                     return s.read1().then([decoder](char c) mutable {
                         if (decoder.accept(c)) {
                             return ss::stop_iteration::yes;
                         }
                         return ss::stop_iteration::no;
                     });
                 })
            .then([&decoder]() { return ready_pair(decoder); });
      });
}

inline ss::future<std::pair<uint32_t, size_t>>
sdc_recurse2(iobuf_reader& s, detail::var_decoder decoder) {
    auto f = s.read1();
    while (f.available()) {
        if (decoder.accept(f.get())) {
            return ready_pair(decoder);
        }
        f = s.read1();
    }

    return std::move(f).then([&s, decoder](char c) mutable {
        if (decoder.accept(c)) {
            return ready_pair(decoder);
        }
        return sdc_recurse(s, decoder);
    });
}

using result_type = std::pair<uint32_t, size_t>;

ss::future<std::pair<uint32_t, size_t>>
stream_deserialize_available(iobuf_reader& s) {
    constexpr auto limit = ((max_length - 1) * 7);
    return sdc_recurse2(s, detail::var_decoder{limit});
}

std::pair<uint32_t, size_t> stream_deserialize_sync(iobuf_reader& s) {
    /// Consume up to 5 iterations (0-4) of reading 7 bits each time
    constexpr auto limit = ((max_length - 1) * 7);
    detail::var_decoder decoder(limit);
    while (!s.eof()) {
        auto c = s.read1_sync();
        if (decoder.accept(c)) {
            break;
        }
    }
    return std::make_pair(
      static_cast<uint32_t>(decoder.result), decoder.bytes_read);
}

auto stream_deserialize_optimistic(iobuf_reader& s) {
    auto [buf, filled] = s.peek<5>();

    if (filled) {
        auto result = decode_u32(buf.data());
        s.skip(result.second);
        return ss::make_ready_future<result_type>(result);
    }

    return deserialize_coro(s);
}

} // namespace unsigned_vint

template<typename IS, typename F>
[[gnu::noinline]] ss::future<size_t> decode_stream_coro(IS is, F f) {
    size_t count = 0;
    while (!is.eof()) {
        auto value = co_await f(is);
        perf_tests::do_not_optimize(value);
        count++;
    };
    co_return count;
}

static constexpr size_t STREAM_SIZE = 1000;

[[gnu::noinline]] size_t decode_stream_sync(iobuf_reader is) {
    size_t count = 0;
    while (!is.eof()) {
        auto value = unsigned_vint::stream_deserialize_sync(is);
        perf_tests::do_not_optimize(value);
        count++;
    }
    return count;
}

namespace {
template<typename F>
inline ss::future<size_t> decode_test(F f) {
    auto is = make_vints_reader(STREAM_SIZE);
    perf_tests::start_measuring_time();
    auto s = co_await decode_stream_coro(std::move(is), f);
    perf_tests::stop_measuring_time();
    assert(s == STREAM_SIZE);
    co_return s;
}
} // namespace

PERF_TEST_F(vint_bench, decode_iobuf_reader) {
    return decode_test(unsigned_vint::deserialize_coro);
}

PERF_TEST_F(vint_bench, decode_recursive) {
    return decode_test(unsigned_vint::stream_deserialize_cont);
}

PERF_TEST_F(vint_bench, decode_recursive_available) {
    return decode_test(unsigned_vint::stream_deserialize_available);
}

PERF_TEST_F(vint_bench, decode_repeat_until) {
    return decode_test(unsigned_vint::deserialize_repeat_until_value);
}

PERF_TEST_F(vint_bench, decode_repeat_do_with) {
    return decode_test(unsigned_vint::deserialize_repeat_do_with);
}

PERF_TEST_F(vint_bench, decode_iobuf_sync) {
    auto is = make_vints_reader(STREAM_SIZE);
    perf_tests::start_measuring_time();
    auto s = decode_stream_sync(std::move(is));
    perf_tests::stop_measuring_time();
    assert(s == STREAM_SIZE);
    return s;
}

PERF_TEST(vint_bench, make_stream) {
    const int count = 10000;
    auto b = make_vints_stream(count);
    perf_tests::do_not_optimize(b);
    return count;
}
