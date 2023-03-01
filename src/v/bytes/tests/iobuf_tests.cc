// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "bytes/details/io_allocation_size.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_istreambuf.h"
#include "bytes/iobuf_ostreambuf.h"
#include "bytes/iobuf_parser.h"
#include "bytes/iostream.h"
#include "bytes/scattered_message.h"
#include "bytes/tests/utils.h"
#include "bytes/utils.h"

#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/range/algorithm/for_each.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>
#include <fmt/format.h>

SEASTAR_THREAD_TEST_CASE(test_copy_equal) {
    iobuf buf;
    buf.reserve_memory(10000000);

    {
        iobuf tmp;
        tmp.append("abcd", 4);
        buf.append_fragments(std::move(tmp));
    }

    auto copy = buf.copy();
    BOOST_CHECK_EQUAL(buf, copy);
}

SEASTAR_THREAD_TEST_CASE(test_appended_data_is_retained) {
    iobuf buf;
    append_sequence(buf, 5);
    BOOST_CHECK_EQUAL(buf.size_bytes(), (5 * characters_per_append));
}

SEASTAR_THREAD_TEST_CASE(test_move_assignment) {
    iobuf buf;
    append_sequence(buf, 7);

    iobuf buf2;
    append_sequence(buf2, 5);

    buf2 = std::move(buf);

    BOOST_REQUIRE(buf2.size_bytes() == 7 * characters_per_append);
}

SEASTAR_THREAD_TEST_CASE(test_move_constructor) {
    iobuf buf;
    append_sequence(buf, 5);

    iobuf buf2(std::move(buf));

    BOOST_REQUIRE(buf2.size_bytes() == 5 * characters_per_append);
}

SEASTAR_THREAD_TEST_CASE(test_size) {
    iobuf buf;
    append_sequence(buf, 5);
    BOOST_REQUIRE_EQUAL(buf.size_bytes(), characters_per_append * 5);
}

SEASTAR_THREAD_TEST_CASE(test_fragment_iteration) {
    size_t count = 52;

    iobuf buf;
    append_sequence(buf, count);

    iobuf buf2;
    for (auto& frag : buf) {
        buf2.append(frag.get(), frag.size());
    }
    BOOST_CHECK_EQUAL(buf.size_bytes(), buf2.size_bytes());
}

SEASTAR_THREAD_TEST_CASE(test_writing_placeholders) {
    iobuf buf;
    ss::sstring s = "hello world";
    const int32_t val = 55;

    auto ph = buf.reserve(sizeof(val));
    buf.append(s.data(), s.size()); // make sure things are legit!
    ph.write(reinterpret_cast<const char*>(&val), sizeof(val));

    const auto& in = *buf.begin();
    const int32_t copy = *reinterpret_cast<const int32_t*>(in.get());
    BOOST_REQUIRE_EQUAL(copy, val);
    BOOST_REQUIRE_EQUAL(buf.size_bytes(), 15);
}

SEASTAR_THREAD_TEST_CASE(test_temporary_buffs) {
    iobuf buf;
    ss::temporary_buffer<char> x(55);
    buf.append(std::move(x));
    BOOST_REQUIRE(buf.size_bytes() == 55);
}
SEASTAR_THREAD_TEST_CASE(test_empty_istream) {
    auto fbuf = iobuf();
    auto in = iobuf::iterator_consumer(fbuf.cbegin(), fbuf.cend());
    BOOST_CHECK_EQUAL(in.bytes_consumed(), 0);

    BOOST_CHECK_THROW(in.consume_type<char>(), std::out_of_range);

    bytes b = ss::uninitialized_string<bytes>(10);
    BOOST_CHECK_THROW(in.consume_to(1, b.begin()), std::out_of_range);
    in.consume_to(0, b.begin());
}
SEASTAR_THREAD_TEST_CASE(test_read_pod) {
    struct pod {
        int x = -1;
        int y = -33;
        int z = 42;
    };

    auto p = pod{};
    iobuf b;
    b.append(reinterpret_cast<const char*>(&p), sizeof(pod));
    auto in = iobuf::iterator_consumer(b.cbegin(), b.cend());

    auto result = in.consume_type<pod>();
    BOOST_CHECK_EQUAL(result.x, p.x);
    BOOST_CHECK_EQUAL(result.y, p.y);
    BOOST_CHECK_EQUAL(result.z, p.z);
    BOOST_CHECK_EQUAL(in.bytes_consumed(), sizeof(pod));
    BOOST_CHECK_THROW(in.consume_type<pod>(), std::out_of_range);
}

SEASTAR_THREAD_TEST_CASE(test_consume_to) {
    // read 75 bytes * i number of times
    // cumulative
    auto fbuf = iobuf();

    for (size_t i = 1; i < 10; ++i) {
        ss::temporary_buffer<char> x(75);
        std::memset(x.get_write(), 'x', x.size());
        fbuf.append(x.get(), x.size());
        auto in = iobuf::iterator_consumer(fbuf.cbegin(), fbuf.cend());

        ss::temporary_buffer<char> y(x.size() * i);
        in.consume_to(y.size(), y.get_write());
        BOOST_CHECK_EQUAL(in.bytes_consumed(), fbuf.size_bytes());
    }
}
SEASTAR_THREAD_TEST_CASE(test_linearization) {
    // read 75 bytes * i number of times
    // cumulative
    auto fbuf = iobuf();
    auto dstbuf = iobuf();

    for (size_t i = 1; i < 10; ++i) {
        ss::temporary_buffer<char> x(136);
        std::memset(x.get_write(), 'x', x.size());
        fbuf.append(x.get(), x.size());
    }
    auto in = iobuf::iterator_consumer(fbuf.cbegin(), fbuf.cend());
    auto ph = dstbuf.reserve(fbuf.size_bytes());
    in.consume_to(ph.remaining_size(), ph);

    BOOST_CHECK_EQUAL(in.bytes_consumed(), fbuf.size_bytes());
    BOOST_CHECK_EQUAL(ph.remaining_size(), 0);
}
SEASTAR_THREAD_TEST_CASE(share_with_byte_comparator) {
    auto fbuf = iobuf();
    for (size_t i = 1; i < 10; ++i) {
        ss::temporary_buffer<char> x(136);
        std::memset(x.get_write(), 'x', x.size());
        fbuf.append(x.get(), x.size());
    }
    BOOST_CHECK_EQUAL(fbuf.share(0, fbuf.size_bytes()), fbuf);
}
SEASTAR_THREAD_TEST_CASE(copy_iobuf_equality_comparator) {
    auto fbuf = iobuf();
    for (size_t i = 1; i < 10; ++i) {
        ss::temporary_buffer<char> x(136);
        std::memset(x.get_write(), 'x', x.size());
        fbuf.append(x.get(), x.size());
    }
    auto fbuf2 = fbuf.copy();
    BOOST_CHECK_EQUAL(fbuf, fbuf2);
}
SEASTAR_THREAD_TEST_CASE(gen_bytes_view) {
    auto fbuf = iobuf();
    auto b = random_generators::get_bytes();
    auto bv = bytes_view(b);
    int32_t x = 42;
    fbuf.append(reinterpret_cast<const char*>(&x), sizeof(x));
    fbuf.append(reinterpret_cast<const char*>(bv.data()), bv.size());
    fbuf.append(reinterpret_cast<const char*>(b.data()), b.size());
    BOOST_CHECK_EQUAL(fbuf.size_bytes(), (2 * 128 * 1024) + sizeof(x));
}
SEASTAR_THREAD_TEST_CASE(traver_all_bytes_one_at_a_time) {
    auto io = iobuf();
    const char* str = "alex";
    io.append(str, std::strlen(str));
    BOOST_CHECK_EQUAL(io.size_bytes(), std::strlen(str));
    auto expected = ss::uninitialized_string(std::strlen(str));
    BOOST_CHECK_EQUAL(expected.size(), std::strlen(str));
    auto begin = iobuf::byte_iterator(io.cbegin(), io.cend());
    auto end = iobuf::byte_iterator(io.cend(), io.cend());
    size_t counter = 0;
    while (begin != end) {
        ++counter;
        ++begin;
    }
    BOOST_CHECK_EQUAL(counter, std::strlen(str));
    begin = iobuf::byte_iterator(io.cbegin(), io.cend());
    end = iobuf::byte_iterator(io.cend(), io.cend());
    auto dst = expected.begin();
    while (begin != end) {
        *dst = *begin;
        ++dst;
        ++begin;
    }
    BOOST_CHECK_EQUAL(str, expected.data());
}
SEASTAR_THREAD_TEST_CASE(not_equal_by_size) {
    auto a = iobuf();
    auto b = iobuf();
    a.append("a", 1);
    b.append("b", 1);
    BOOST_CHECK(a != b);
}
SEASTAR_THREAD_TEST_CASE(test_prepend) {
    auto a = iobuf();
    a.append("a", 1);
    a.prepend(ss::temporary_buffer<char>(1));
    BOOST_CHECK_EQUAL(a.size_bytes(), 2);
}
SEASTAR_THREAD_TEST_CASE(append_each_other) {
    ss::temporary_buffer<char> buf(100);
    auto a = iobuf();
    auto b = iobuf();
    a.append(buf.share());
    b.append(a.share(0, a.size_bytes()));
    for (auto& f : a) {
        // test the underlying buffer remains the same
        BOOST_CHECK(f.share() == buf);
    }
}
SEASTAR_THREAD_TEST_CASE(append_empty) {
    iobuf buf;
    buf.prepend(ss::temporary_buffer<char>(0));
    buf.prepend(ss::temporary_buffer<char>(0));
    buf.prepend(ss::temporary_buffer<char>(0));
    buf.append("alex", 0);
    auto buf2 = buf.share(0, buf.size_bytes());
    BOOST_REQUIRE_EQUAL(buf, buf2);

    iobuf buf3;
    buf3.prepend(std::move(buf2));
    BOOST_REQUIRE_EQUAL(buf, buf3);

    iobuf buf4;
    buf4.append(std::move(buf3));
    BOOST_REQUIRE_EQUAL(buf, buf4);
}

SEASTAR_THREAD_TEST_CASE(share) {
    const auto data = random_generators::gen_alphanum_string(1024);

    iobuf buf;
    buf.append(data.data() + 0, 512);
    buf.append(data.data() + 512, 512);

    for (size_t window_size = 1; window_size <= 128; window_size++) {
        for (size_t off = 0; off < (data.size() - window_size); off++) {
            auto shared = buf.share(off, window_size);

            iobuf other;
            other.append(data.data() + off, window_size);

            BOOST_REQUIRE_MESSAGE(
              shared == other,
              fmt::format("offset {} window_size {}", off, window_size));
        }
    }
}

SEASTAR_THREAD_TEST_CASE(iterator_relationships) {
    iobuf buf;
    BOOST_REQUIRE(buf.begin() == buf.end());
    BOOST_REQUIRE(buf.cbegin() == buf.cend());

    buf.append(ss::temporary_buffer<char>(100));
    BOOST_REQUIRE(buf.begin() != buf.end());
    BOOST_REQUIRE(buf.cbegin() != buf.cend());
    BOOST_REQUIRE(std::next(buf.begin()) == buf.end());
    BOOST_REQUIRE(std::next(buf.cbegin()) == buf.cend());
}

SEASTAR_THREAD_TEST_CASE(is_finished) {
    iobuf buf;
    auto in0 = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
    BOOST_CHECK(in0.is_finished());

    buf.append(ss::temporary_buffer<char>(100));
    auto in1 = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
    BOOST_CHECK(!in1.is_finished());
    in1.skip(50);
    BOOST_CHECK(!in1.is_finished());
    in1.skip(50);
    BOOST_CHECK(in1.is_finished());
}

SEASTAR_THREAD_TEST_CASE(iobuf_as_scattered_message) {
    const auto b = random_generators::gen_alphanum_string(512);
    for (size_t size = 0; size < b.size(); size++) {
        iobuf buf;
        buf.append(b.data(), size);
        auto msg = iobuf_as_scattered(std::move(buf));
        auto packet = std::move(msg).release();
        packet.linearize();
        BOOST_TEST(packet.nr_frags() == 1);
        auto& frag = packet.frag(0);
        BOOST_TEST(frag.size == size);
        BOOST_TEST(std::memcmp(frag.base, b.data(), size) == 0);
    }
}
SEASTAR_THREAD_TEST_CASE(iobuf_as_ostream_basic) {
    const auto b = random_generators::gen_alphanum_string(512);
    iobuf underlying;
    iobuf_ostreambuf obuf(underlying);
    std::ostream os(&obuf);
    os << "hello world" << std::endl /* new line + flush*/;
    BOOST_REQUIRE_EQUAL(underlying.size_bytes(), 12);
}

SEASTAR_THREAD_TEST_CASE(iobuf_as_ostream) {
    const auto b = random_generators::gen_alphanum_string(512);
    iobuf underlying;
    iobuf_ostreambuf obuf(underlying);
    std::ostream os(&obuf);
    // first insertion
    std::copy(b.begin(), b.end(), std::ostreambuf_iterator<char>(os));
    // second insertion
    os << b;
    BOOST_REQUIRE_EQUAL(underlying.size_bytes(), 1024);
}

SEASTAR_THREAD_TEST_CASE(alloctor_forward_progress) {
    static constexpr std::array<uint32_t, 14> src = {{
      512,
      768,
      1152,
      1728,
      2592,
      3888,
      5832,
      8748,
      13122,
      19683,
      29525,
      44288,
      66432,
      99648,
    }};
    static constexpr std::array<uint32_t, 14> expected = {{
      768,
      1152,
      1728,
      2592,
      3888,
      5832,
      8748,
      13122,
      19683,
      29525,
      44288,
      66432,
      99648,
      131072,
    }};
    BOOST_REQUIRE_EQUAL(src.size(), expected.size());
    for (size_t i = 0; i < src.size(); ++i) {
        BOOST_REQUIRE_EQUAL(
          details::io_allocation_size::next_allocation_size(src[i]),
          expected[i]);
    }
}

SEASTAR_THREAD_TEST_CASE(test_next_chunk_allocation_append_temp_buf) {
    const auto b = random_generators::gen_alphanum_string(1024);

    iobuf buf;
    for (size_t i = 0; i < 40000; i++) {
        ss::temporary_buffer<char> tb(b.data(), b.size());
        buf.append(std::move(tb));
    }
    // slow but tha'ts life.
    auto distance = std::distance(buf.begin(), buf.end());
    BOOST_REQUIRE_EQUAL(distance, 324);
    constexpr size_t sz = 40000 * 1024;
    auto msg = iobuf_as_scattered(std::move(buf));
    BOOST_REQUIRE_EQUAL(msg.size(), sz);
}

SEASTAR_THREAD_TEST_CASE(test_next_chunk_allocation_append_iobuf) {
    const auto b = random_generators::gen_alphanum_string(1024);
    iobuf buf;
    for (size_t i = 0; i < 40000; i++) {
        iobuf tmp_buf;
        tmp_buf.append(b.data(), b.size());
        buf.append(std::move(tmp_buf));
    }
    // slow but tha'ts life.
    auto distance = std::distance(buf.begin(), buf.end());
    BOOST_REQUIRE_EQUAL(distance, 324);
    constexpr size_t sz = 40000 * 1024;
    auto msg = iobuf_as_scattered(std::move(buf));
    BOOST_REQUIRE_EQUAL(msg.size(), sz);
}

SEASTAR_THREAD_TEST_CASE(test_appending_frament_takes_ownership) {
    iobuf target;
    const auto b = random_generators::gen_alphanum_string(1024);
    target.append(b.c_str(), b.size());
    auto target_frags_cnt = std::distance(target.begin(), target.end());
    iobuf other = bytes_to_iobuf(random_generators::get_bytes(256));
    auto other_frags_cnt = std::distance(other.begin(), other.end());
    target.append_fragments(std::move(other));

    BOOST_REQUIRE_EQUAL(
      std::distance(target.begin(), target.end()),
      target_frags_cnt + other_frags_cnt);
}

/*
 * testing various trim_front scenarios
 *
 * IMPORTANT: these tests use the leaky abstraction of prepend which as
 * currently implemented unconditionally creates a new fragment compared to
 * other interfaces that may perform coalescing.
 */
SEASTAR_THREAD_TEST_CASE(trim_front) {
    // trimming empty buf
    {
        iobuf buf;
        buf.trim_front(100);
        BOOST_TEST(buf.empty());
        BOOST_TEST(buf.size_bytes() == 0);
    }

    {
        iobuf buf;
        buf.trim_front(0);
        BOOST_TEST(buf.empty());
        BOOST_TEST(buf.size_bytes() == 0);
    }

    // trimming zero bytes
    {
        iobuf buf;
        buf.prepend(ss::temporary_buffer<char>(100));
        BOOST_TEST(buf.size_bytes() == 100);
        buf.trim_front(0);
        BOOST_TEST(buf.size_bytes() == 100);
    }

    {
        iobuf buf;
        buf.prepend(ss::temporary_buffer<char>(0));
        BOOST_TEST(buf.size_bytes() == 0);
        buf.trim_front(0);
        BOOST_TEST(buf.size_bytes() == 0);
    }

    // iobuf frags{100}
    // trim_front(99, 100, 101)
    {
        iobuf buf;
        buf.prepend(ss::temporary_buffer<char>(100));
        buf.trim_front(99);
        BOOST_TEST(buf.size_bytes() == 1);
    }

    {
        iobuf buf;
        buf.prepend(ss::temporary_buffer<char>(100));
        buf.trim_front(100);
        BOOST_TEST(buf.size_bytes() == 0);
    }

    {
        iobuf buf;
        buf.prepend(ss::temporary_buffer<char>(100));
        buf.trim_front(101);
        BOOST_TEST(buf.size_bytes() == 0);
    }

    // iobuf frags{100, 100}
    // trim_front(99, 100, 101)
    {
        iobuf buf;
        buf.prepend(ss::temporary_buffer<char>(100));
        buf.prepend(ss::temporary_buffer<char>(100));
        buf.trim_front(99);
        BOOST_TEST(buf.size_bytes() == 101);
    }

    {
        iobuf buf;
        buf.prepend(ss::temporary_buffer<char>(100));
        buf.prepend(ss::temporary_buffer<char>(100));
        buf.trim_front(100);
        BOOST_TEST(buf.size_bytes() == 100);
    }

    {
        iobuf buf;
        buf.prepend(ss::temporary_buffer<char>(100));
        buf.prepend(ss::temporary_buffer<char>(100));
        buf.trim_front(101);
        BOOST_TEST(buf.size_bytes() == 99);
    }

    {
        iobuf buf;
        buf.prepend(ss::temporary_buffer<char>(100));
        auto it = buf.begin();
        BOOST_TEST(it->size() == 100);
        buf.trim_front(10);
        it = buf.begin();
        BOOST_TEST(it->size() == 90);
    }
}

SEASTAR_THREAD_TEST_CASE(test_iobuf_input_stream_from_trimmed_iobuf) {
    iobuf buf;
    buf.prepend(ss::temporary_buffer<char>(100));
    buf.trim_front(10);
    auto stream = make_iobuf_input_stream(std::move(buf));
    auto res = stream.read().get0();
    BOOST_TEST(res.size() == 90);
}

SEASTAR_THREAD_TEST_CASE(
  test_trim_front_interator_consumer_segment_bytes_left) {
    iobuf buf;
    buf.prepend(ss::temporary_buffer<char>(100));
    buf.trim_front(10);
    details::io_iterator_consumer cons(buf.begin(), buf.end());

    BOOST_TEST(cons.segment_bytes_left() == 90);
}

SEASTAR_THREAD_TEST_CASE(test_trim_front_iterator_consumer_scan) {
    const auto rgen = random_generators::gen_alphanum_string(100);
    iobuf buf;
    buf.append(ss::temporary_buffer<char>(rgen.data(), rgen.size()));
    buf.trim_front(10);
    details::io_iterator_consumer cons(buf.begin(), buf.end());
    std::string expected(rgen.data(), rgen.size());
    expected = expected.substr(10);
    std::string actual;
    for (auto it : cons) {
        actual.push_back(it);
    }
    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());
    BOOST_REQUIRE_EQUAL(expected, actual);
}
SEASTAR_THREAD_TEST_CASE(iobuf_as_istream_basic) {
    iobuf underlying;
    underlying.append("hello world", 11);
    iobuf_istreambuf ibuf(underlying);
    std::istream is(&ibuf);
    std::string h;
    std::string w;
    is >> h;
    is >> w;
    BOOST_REQUIRE_EQUAL(h, "hello");
    BOOST_REQUIRE_EQUAL(w, "world");
}

SEASTAR_THREAD_TEST_CASE(iobuf_as_istream) {
    const auto a = random_generators::gen_alphanum_string(1024);
    const auto b = random_generators::gen_alphanum_string(1024);
    iobuf underlying;
    underlying.append(a.data(), a.size());
    underlying.append(b.data(), b.size());
    iobuf_istreambuf ibuf(underlying);
    std::istream is(&ibuf);
    std::string out;
    std::copy(
      std::istreambuf_iterator<char>(is),
      std::istreambuf_iterator<char>(),
      std::back_inserter(out));
    BOOST_REQUIRE_EQUAL(underlying.size_bytes(), out.size());
    BOOST_REQUIRE_EQUAL(a + b, out);
}

SEASTAR_THREAD_TEST_CASE(iobuf_string_view_cmp) {
    {
        /// string_view to iobuf comparator, random string
        auto a = random_generators::gen_alphanum_string(1024);
        iobuf buf;
        buf.append(a.data(), a.size());
        BOOST_REQUIRE_EQUAL(buf, std::string_view(a));
    }
    {
        /// Same length different content
        static const ss::sstring a = "static_content";
        static const ss::sstring b = "that_is_not_eq";
        iobuf buf;
        buf.append(a.data(), a.size());
        BOOST_REQUIRE_EQUAL(buf, std::string_view(a));
        BOOST_REQUIRE_NE(buf, std::string_view(b));
    }
    {
        /// Different length items
        static const ss::sstring a = "Not the same";
        static const ss::sstring b = "length";
        iobuf buf;
        buf.append(a.data(), a.size());
        BOOST_REQUIRE_NE(a, std::string_view(b));
        BOOST_REQUIRE_EQUAL(a, std::string_view(a));
    }
    {
        /// Multiple fragments
        static const ss::sstring hello = "hello";
        ss::sstring hello_str;
        iobuf buf;
        for (auto i = 0; i < 100; ++i) {
            ss::temporary_buffer<char> a(hello.data(), hello.size());
            hello_str += hello;
            buf.prepend(std::move(a));
        }
        BOOST_REQUIRE_EQUAL(buf, std::string_view(hello_str));
    }
    {
        /// Large data set
        ss::sstring str;
        iobuf buf;
        for (auto i = 0; i < 4096; ++i) {
            str.append("ABC", 3);
            buf.append("ABC", 3);
        }
        BOOST_REQUIRE_EQUAL(buf, std::string_view(str));
    }
}

SEASTAR_THREAD_TEST_CASE(iobuf_parser_peek) {
    const auto a = random_generators::gen_alphanum_string(1024);
    const auto b = random_generators::gen_alphanum_string(1024);
    iobuf src;
    src.append(a.data(), a.size());
    src.append(b.data(), b.size());
    iobuf_parser parser(std::move(src));
    auto dst_a = parser.peek(1000);
    auto dst_b = parser.copy(1000);
    BOOST_REQUIRE(dst_a == dst_b);
}

SEASTAR_THREAD_TEST_CASE(iobuf_is_zero_test) {
    const auto a = random_generators::gen_alphanum_string(1024);
    const auto b = bytes("abc");
    std::array<char, 1024> zeros{0};
    std::array<char, 1> one{1};

    // non zero iobuf
    iobuf non_zero_1;
    non_zero_1.append(a.data(), a.size());
    non_zero_1.append(b.data(), b.size());
    BOOST_REQUIRE_EQUAL(is_zero(non_zero_1), false);

    iobuf non_zero_2;
    non_zero_2.append(zeros.data(), zeros.size());
    non_zero_2.append(one.data(), one.size());
    BOOST_REQUIRE_EQUAL(is_zero(non_zero_2), false);
    // empty iobuf is not zero
    iobuf empty;
    BOOST_REQUIRE_EQUAL(is_zero(empty), false);

    iobuf zero;
    zero.append(zeros.data(), zeros.size());
    zero.append(zeros.data(), zeros.size());
    BOOST_REQUIRE_EQUAL(is_zero(zero), true);
}
