#include "bytes/bytes.h"
#include "bytes/bytes_ostream.h"
#include "random/generators.h"
#include "utils/fragbuf.h"
#include "utils/memory_data_source.h"

#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

//#include <boost/test/unit_test.hpp>

#include <cstdint>
#include <vector>

struct {
    [[noreturn]] static void throw_out_of_range(size_t a, size_t b) {
        throw size_t(a + b);
    }
} int_thrower;

std::tuple<std::vector<fragbuf>, uint64_t, uint16_t> get_buffers() {
    uint64_t value1 = 0x1234'5678'abcd'ef02ull;
    uint16_t value2 = 0xfedc;

    auto data = bytes(
      bytes::initialized_later(), sizeof(value1) + sizeof(value2));
    auto dst = std::copy_n(
      reinterpret_cast<const int8_t*>(&value1), sizeof(value1), data.begin());
    std::copy_n(reinterpret_cast<const int8_t*>(&value2), sizeof(value2), dst);

    std::vector<fragbuf> buffers;

    // Everything in a single buffer
    {
        std::vector<temporary_buffer<char>> fragments;
        fragments.emplace_back(
          reinterpret_cast<char*>(data.data()), data.size());
        buffers.emplace_back(std::move(fragments), data.size());
    }

    // One-byte buffers
    {
        std::vector<temporary_buffer<char>> fragments;
        for (auto i = 0u; i < data.size(); i++) {
            fragments.emplace_back(reinterpret_cast<char*>(data.data() + i), 1);
        }
        buffers.emplace_back(std::move(fragments), data.size());
    }

    // Seven bytes and the rest
    {
        std::vector<temporary_buffer<char>> fragments;
        fragments.emplace_back(reinterpret_cast<char*>(data.data()), 7);
        fragments.emplace_back(
          reinterpret_cast<char*>(data.data() + 7), data.size() - 7);
        buffers.emplace_back(std::move(fragments), data.size());
    }

    // 8 bytes and 2 bytes
    {
        std::vector<temporary_buffer<char>> fragments;
        fragments.emplace_back(
          reinterpret_cast<char*>(data.data()), sizeof(uint64_t));
        fragments.emplace_back(
          reinterpret_cast<char*>(data.data() + sizeof(uint64_t)),
          data.size() - sizeof(uint64_t));
        buffers.emplace_back(std::move(fragments), data.size());
    }

    return {std::move(buffers), value1, value2};
}

SEASTAR_THREAD_TEST_CASE(test_empty_istream) {
    auto fbuf = fragbuf();
    auto in = fbuf.get_istream();

    auto linearization_buffer = bytes_ostream();
    BOOST_CHECK_EQUAL(in.bytes_left(), 0);
    BOOST_CHECK_THROW(in.read<char>(), std::out_of_range);
    BOOST_CHECK_THROW(
      in.read_bytes_view(1, linearization_buffer), std::out_of_range);
    BOOST_CHECK_EQUAL(
      in.read_bytes_view(0, linearization_buffer), bytes_view());
    BOOST_CHECK(linearization_buffer.empty());
}

SEASTAR_THREAD_TEST_CASE(test_read_pod) {
    auto test = [&](auto expected_value1, auto expected_value2, fragbuf& ftb) {
        using type1 = std::decay_t<decltype(expected_value1)>;
        using type2 = std::decay_t<decltype(expected_value2)>;
        static_assert(sizeof(type2) < sizeof(type1));

        auto in = ftb.get_istream();
        BOOST_CHECK_EQUAL(in.bytes_left(), sizeof(type1) + sizeof(type2));
        BOOST_CHECK_EQUAL(in.read<type1>(), expected_value1);
        BOOST_CHECK_EQUAL(in.bytes_left(), sizeof(type2));
        BOOST_CHECK_THROW(in.read<type1>(), std::out_of_range);
        BOOST_CHECK_EQUAL(in.bytes_left(), sizeof(type2));
        BOOST_CHECK_EQUAL(in.read<type2>(), expected_value2);
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        BOOST_CHECK_THROW(in.read<type2>(), std::out_of_range);
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        BOOST_CHECK_THROW(in.read<type1>(), std::out_of_range);
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        BOOST_CHECK_THROW(in.read<char>(), std::out_of_range);
        BOOST_CHECK_EXCEPTION(
          in.read<char>(int_thrower), size_t, [](size_t v) { return v == 1; });
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
    };

    auto [buffers, value1, value2] = get_buffers();
    for (auto& frag_buffer : buffers) {
        test(value1, value2, frag_buffer);
    }
}

SEASTAR_THREAD_TEST_CASE(test_read_to) {
    auto test =
      [&](
        bytes_view expected_value1, bytes_view expected_value2, fragbuf& ftb) {
          assert(expected_value2.size() < expected_value1.size());

          bytes actual_value;

          auto in = ftb.get_istream();
          BOOST_CHECK_EQUAL(
            in.bytes_left(), expected_value1.size() + expected_value2.size());
          actual_value = bytes(
            bytes::initialized_later(), expected_value1.size());
          in.read_to(expected_value1.size(), actual_value.begin());
          BOOST_CHECK_EQUAL(actual_value, expected_value1);
          BOOST_CHECK_EQUAL(in.bytes_left(), expected_value2.size());
          BOOST_CHECK_THROW(
            in.read_to(expected_value1.size(), actual_value.begin()),
            std::out_of_range);
          BOOST_CHECK_EQUAL(in.bytes_left(), expected_value2.size());
          actual_value = bytes(
            bytes::initialized_later(), expected_value2.size());
          in.read_to(expected_value2.size(), actual_value.begin());
          BOOST_CHECK_EQUAL(actual_value, expected_value2);
          BOOST_CHECK_EQUAL(in.bytes_left(), 0);
          BOOST_CHECK_THROW(
            in.read_to(expected_value2.size(), actual_value.begin()),
            std::out_of_range);
          BOOST_CHECK_EQUAL(in.bytes_left(), 0);
          BOOST_CHECK_THROW(
            in.read_to(expected_value1.size(), actual_value.begin()),
            std::out_of_range);
          BOOST_CHECK_EQUAL(in.bytes_left(), 0);
          BOOST_CHECK_THROW(
            in.read_to(1, actual_value.begin()), std::out_of_range);
          BOOST_CHECK_EXCEPTION(
            in.read_to(1, actual_value.begin(), int_thrower),
            size_t,
            [](size_t v) { return v == 1; });
          BOOST_CHECK_EQUAL(in.bytes_left(), 0);
      };

    auto [buffers, value1, value2] = get_buffers();
    for (auto& frag_buffer : buffers) {
        auto value1_bv = bytes_view(
          reinterpret_cast<const bytes::value_type*>(&value1), sizeof(value1));
        auto value2_bv = bytes_view(
          reinterpret_cast<const bytes::value_type*>(&value2), sizeof(value2));
        test(value1_bv, value2_bv, frag_buffer);
    }
}

SEASTAR_THREAD_TEST_CASE(test_read_bytes_view) {
    auto linearization_buffer = bytes_ostream();
    auto test =
      [&](
        bytes_view expected_value1, bytes_view expected_value2, fragbuf& ftb) {
          assert(expected_value2.size() < expected_value1.size());

          auto in = ftb.get_istream();
          BOOST_CHECK_EQUAL(
            in.read_bytes_view(0, linearization_buffer), bytes_view());
          BOOST_CHECK_EQUAL(
            in.bytes_left(), expected_value1.size() + expected_value2.size());
          BOOST_CHECK_EQUAL(
            in.read_bytes_view(expected_value1.size(), linearization_buffer),
            expected_value1);
          BOOST_CHECK_EQUAL(in.bytes_left(), expected_value2.size());
          BOOST_CHECK_THROW(
            in.read_bytes_view(expected_value1.size(), linearization_buffer),
            std::out_of_range);
          BOOST_CHECK_EQUAL(in.bytes_left(), expected_value2.size());
          BOOST_CHECK_EQUAL(
            in.read_bytes_view(expected_value2.size(), linearization_buffer),
            expected_value2);
          BOOST_CHECK_EQUAL(in.bytes_left(), 0);
          BOOST_CHECK_THROW(
            in.read_bytes_view(expected_value2.size(), linearization_buffer),
            std::out_of_range);
          BOOST_CHECK_EQUAL(in.bytes_left(), 0);
          BOOST_CHECK_THROW(
            in.read_bytes_view(expected_value1.size(), linearization_buffer),
            std::out_of_range);
          BOOST_CHECK_EQUAL(in.bytes_left(), 0);
          BOOST_CHECK_THROW(
            in.read_bytes_view(1, linearization_buffer), std::out_of_range);
          BOOST_CHECK_EXCEPTION(
            in.read_bytes_view(1, linearization_buffer, int_thrower),
            size_t,
            [](size_t v) { return v == 1; });
          BOOST_CHECK_EQUAL(in.bytes_left(), 0);
          BOOST_CHECK_EQUAL(
            in.read_bytes_view(0, linearization_buffer), bytes_view());
      };

    auto [buffers, value1, value2] = get_buffers();
    for (auto& frag_buffer : buffers) {
        auto value1_bv = bytes_view(
          reinterpret_cast<const bytes::value_type*>(&value1), sizeof(value1));
        auto value2_bv = bytes_view(
          reinterpret_cast<const bytes::value_type*>(&value2), sizeof(value2));
        test(value1_bv, value2_bv, frag_buffer);
    }
}

SEASTAR_THREAD_TEST_CASE(test_read_fragmented_buffer) {
    using tuple_type
      = std::tuple<std::vector<temporary_buffer<char>>, bytes, bytes, bytes>;

    auto generate = [](size_t n) {
        auto prefix = random_generators::get_bytes();
        auto data = random_generators::get_bytes(n);
        auto suffix = random_generators::get_bytes();

        auto linear = bytes(
          bytes::initialized_later(), prefix.size() + n + suffix.size());
        auto dst = linear.begin();
        dst = std::copy(prefix.begin(), prefix.end(), dst);
        dst = std::copy(data.begin(), data.end(), dst);
        std::copy(suffix.begin(), suffix.end(), dst);

        auto src = linear.begin();
        auto left = linear.size();

        std::vector<temporary_buffer<char>> buffers;
        while (left) {
            auto this_size = std::max<size_t>(left / 2, 1);
            buffers.emplace_back(reinterpret_cast<const char*>(src), this_size);
            left -= this_size;
            src += this_size;
        }

        return tuple_type{std::move(buffers),
                          std::move(prefix),
                          std::move(data),
                          std::move(suffix)};
    };

    auto test_cases = std::vector<tuple_type>();

    test_cases.emplace_back();
    test_cases.emplace_back(generate(0));
    test_cases.emplace_back(generate(1024));
    test_cases.emplace_back(generate(512 * 1024));
    for (auto i = 0; i < 16; i++) {
        test_cases.emplace_back(
          generate(random_generators::get_int(16, 16 * 1024)));
    }

    for (auto&& [buffers, expected_prefix, expected_data, expected_suffix] :
         test_cases) {
        auto prefix_size = expected_prefix.size();
        auto size = expected_data.size();
        auto suffix_size = expected_suffix.size();

        auto in = input_stream<char>(data_source(
          std::make_unique<memory_data_source>(std::move(buffers))));

        auto prefix = in.read_exactly(prefix_size).get0();
        BOOST_CHECK_EQUAL(prefix.size(), prefix_size);
        BOOST_CHECK_EQUAL(
          bytes_view(
            reinterpret_cast<const bytes::value_type*>(prefix.get()),
            prefix.size()),
          expected_prefix);

        auto reader = fragbuf::reader();
        auto fbuf = reader.read_exactly(in, size).get0();
        auto buf = bytes_ostream();
        auto view = fbuf.get_istream().read_bytes_view(size, buf);
        BOOST_CHECK_EQUAL(fbuf.size_bytes(), size);
        BOOST_CHECK_EQUAL(view, expected_data);

        auto suffix = in.read_exactly(suffix_size).get0();
        BOOST_CHECK_EQUAL(suffix.size(), suffix_size);
        BOOST_CHECK_EQUAL(
          bytes_view(
            reinterpret_cast<const bytes::value_type*>(suffix.get()),
            suffix.size()),
          expected_suffix);

        in.close().get();
    }
}

SEASTAR_THREAD_TEST_CASE(test_skip) {
    auto test = [&](auto expected_value1, auto expected_value2, fragbuf& ftb) {
        using type1 = std::decay_t<decltype(expected_value1)>;
        using type2 = std::decay_t<decltype(expected_value2)>;

        auto in = ftb.get_istream();
        BOOST_CHECK_EQUAL(in.bytes_left(), sizeof(type1) + sizeof(type2));
        in.skip(sizeof(type1));
        BOOST_CHECK_EQUAL(in.bytes_left(), sizeof(type2));
        BOOST_CHECK_EQUAL(in.read<type2>(), expected_value2);
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        in.skip(sizeof(type2));
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
    };

    auto [buffers, value1, value2] = get_buffers();
    for (auto& frag_buffer : buffers) {
        test(value1, value2, frag_buffer);
    }
}

static void do_test_read_exactly_eof(size_t input_size) {
    std::vector<temporary_buffer<char>> data;
    if (input_size) {
        data.push_back(temporary_buffer<char>(input_size));
    }
    auto ds = data_source(
      std::make_unique<memory_data_source>(std::move(data)));
    auto is = input_stream<char>(std::move(ds));
    auto reader = fragbuf::reader();
    auto result = reader.read_exactly(is, input_size + 1).get0();
    BOOST_CHECK_EQUAL(result.size_bytes(), size_t(0));
}

SEASTAR_THREAD_TEST_CASE(test_read_exactly_eof) {
    do_test_read_exactly_eof(0);
    do_test_read_exactly_eof(1);
}

SEASTAR_THREAD_TEST_CASE(test_istream_iterator) {
    auto linearization_buffer = bytes_ostream();
    auto test =
      [&](
        bytes_view expected_value1, bytes_view expected_value2, fragbuf& ftb) {
          auto in = ftb.get_istream();
          auto it = in.begin();
          for (auto b : expected_value1) {
              BOOST_CHECK_EQUAL(b, *it++);
          }
          for (auto b : expected_value2) {
              BOOST_CHECK_EQUAL(b, *it++);
          }
          BOOST_TEST_PASSPOINT();
          BOOST_CHECK_EQUAL(
            in.read_bytes_view(expected_value1.size(), linearization_buffer),
            expected_value1);
          it = in.begin();
          for (auto b : expected_value2) {
              BOOST_CHECK_EQUAL(b, *it++);
          }
          BOOST_TEST_PASSPOINT();
          BOOST_CHECK_EQUAL(
            in.read_bytes_view(expected_value2.size(), linearization_buffer),
            expected_value2);
          it = in.begin();
          BOOST_REQUIRE(in.begin() == in.end());
      };

    auto [buffers, value1, value2] = get_buffers();
    for (auto& frag_buffer : buffers) {
        auto value1_bv = bytes_view(
          reinterpret_cast<const bytes::value_type*>(&value1), sizeof(value1));
        auto value2_bv = bytes_view(
          reinterpret_cast<const bytes::value_type*>(&value2), sizeof(value2));
        test(value1_bv, value2_bv, frag_buffer);
    }
}

SEASTAR_THREAD_TEST_CASE(test_sharing) {
    uint64_t value = 0x1234'5678'abcd'ef02ull;

    auto data = bytes(bytes::initialized_later(), sizeof(value));
    std::copy_n(
      reinterpret_cast<const int8_t*>(&value), sizeof(value), data.begin());

    std::vector<temporary_buffer<char>> fragments;
    fragments.emplace_back(reinterpret_cast<char*>(data.data()), 3);
    fragments.emplace_back(reinterpret_cast<char*>(data.data() + 3), 2);
    fragments.emplace_back(reinterpret_cast<char*>(data.data() + 5), 3);

    auto ftb = fragbuf(std::move(fragments), sizeof(value));
    auto in = ftb.get_istream();
    BOOST_CHECK_EQUAL(in.bytes_left(), sizeof(value));
    BOOST_CHECK_EQUAL(in.read<uint64_t>(), value);

    auto shared = ftb.share(0, 2);
    in = shared.get_istream();
    BOOST_CHECK_EQUAL(in.bytes_left(), 2);
    BOOST_CHECK_EQUAL(in.read<uint16_t>(), 0xef02);

    shared = ftb.share(1, 6);
    in = shared.get_istream();
    BOOST_CHECK_EQUAL(in.bytes_left(), 6);
    BOOST_CHECK_EQUAL(in.read<uint16_t>(), 0xcdef);
    BOOST_CHECK_EQUAL(in.read<uint32_t>(), 0x345678ab);

    shared = ftb.share(7, 1);
    in = shared.get_istream();
    BOOST_CHECK_EQUAL(in.bytes_left(), 1);
    BOOST_CHECK_EQUAL(in.read<uint8_t>(), 0x12);
}

SEASTAR_THREAD_TEST_CASE(test_istream_sharing) {
    uint64_t value = 0x1234'5678'abcd'ef02ull;

    auto data = bytes(bytes::initialized_later(), sizeof(value));
    std::copy_n(
      reinterpret_cast<const int8_t*>(&value), sizeof(value), data.begin());

    std::vector<temporary_buffer<char>> fragments;
    fragments.emplace_back(reinterpret_cast<char*>(data.data()), 3);
    fragments.emplace_back(reinterpret_cast<char*>(data.data() + 3), 2);
    fragments.emplace_back(reinterpret_cast<char*>(data.data() + 5), 3);

    auto ftb = fragbuf(std::move(fragments), sizeof(value));

    auto to_share = ftb.get_istream();
    auto shared = to_share.read_shared(2);
    auto in = shared.get_istream();
    BOOST_CHECK_EQUAL(in.bytes_left(), 2);
    BOOST_CHECK_EQUAL(in.read<uint16_t>(), 0xef02);

    to_share = ftb.get_istream();
    to_share.skip(1);
    shared = to_share.read_shared(6);
    in = shared.get_istream();
    BOOST_CHECK_EQUAL(in.bytes_left(), 6);
    BOOST_CHECK_EQUAL(in.read<uint16_t>(), 0xcdef);
    BOOST_CHECK_EQUAL(in.read<uint32_t>(), 0x345678ab);

    to_share = ftb.get_istream();
    to_share.skip(7);
    shared = to_share.read_shared(1);
    in = shared.get_istream();
    BOOST_CHECK_EQUAL(in.bytes_left(), 1);
    BOOST_CHECK_EQUAL(in.read<uint8_t>(), 0x12);
}

SEASTAR_THREAD_TEST_CASE(test_release) {
    uint64_t value = 0x1234'5678'abcd'ef02ull;

    auto data = bytes(bytes::initialized_later(), sizeof(value));
    std::copy_n(
      reinterpret_cast<const int8_t*>(&value), sizeof(value), data.begin());

    std::vector<temporary_buffer<char>> fragments;
    fragments.emplace_back(reinterpret_cast<char*>(data.data()), 3);
    fragments.emplace_back(reinterpret_cast<char*>(data.data() + 3), 2);
    fragments.emplace_back(reinterpret_cast<char*>(data.data() + 5), 3);

    auto ftb = fragbuf(std::move(fragments), sizeof(value));
    BOOST_CHECK_EQUAL(std::move(ftb).release().size(), 3);
}

SEASTAR_THREAD_TEST_CASE(test_consume) {
    uint64_t value = 0x1234'5678'abcd'ef02ull;

    auto data = bytes(bytes::initialized_later(), sizeof(value));
    std::copy_n(
      reinterpret_cast<const int8_t*>(&value), sizeof(value), data.begin());

    std::vector<temporary_buffer<char>> fragments;
    fragments.emplace_back(reinterpret_cast<char*>(data.data()), 3);
    fragments.emplace_back(reinterpret_cast<char*>(data.data() + 3), 2);
    fragments.emplace_back(reinterpret_cast<char*>(data.data() + 5), 3);

    auto ftb = fragbuf(std::move(fragments), sizeof(value));
    auto in = ftb.get_istream();
    BOOST_CHECK_EQUAL(in.read<uint16_t>(), 0xef02);

    // Consume the rest of the stream as a sequence of sequences of bytes.
    std::vector<bytes_view> remaining;
    in.consume([&remaining](bytes_view bv) { remaining.push_back(bv); });
    BOOST_CHECK_EQUAL(remaining.size(), 3);
    auto frag = bytes_view(reinterpret_cast<const int8_t*>(&value) + 2, 1);
    BOOST_CHECK_EQUAL(remaining[0], frag);
    frag = bytes_view(reinterpret_cast<const int8_t*>(&value) + 3, 2);
    BOOST_CHECK_EQUAL(remaining[1], frag);
    frag = bytes_view(reinterpret_cast<const int8_t*>(&value) + 5, 3);
    BOOST_CHECK_EQUAL(remaining[2], frag);
}

SEASTAR_THREAD_TEST_CASE(test_fragbuf_auto_size_constructor) {
    std::vector<temporary_buffer<char>> bufs;

    size_t expected = 0;
    for (int i = 0; i < 5; i++) {
        const auto size = i * 10;
        bufs.push_back(temporary_buffer<char>(size));
        expected += size;
    }

    auto f = fragbuf(std::move(bufs));
    BOOST_CHECK_EQUAL(expected, f.size_bytes());
}
