#define BOOST_TEST_MODULE iobuf

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "random/fast_prng.h"
#include "random/generators.h"

#include <boost/range/algorithm/for_each.hpp>
#include <boost/test/unit_test.hpp>

static const constexpr size_t characters_per_append = 10;

void append_sequence(iobuf& buf, size_t count) {
    for (size_t i = 0; i < count; i++) {
        auto str = random_generators::gen_alphanum_string(
          characters_per_append);
        buf.append(str.data(), str.size());
    }
}

BOOST_AUTO_TEST_CASE(test_appended_data_is_retained) {
    iobuf buf;
    append_sequence(buf, 5);
    BOOST_CHECK_EQUAL(buf.size_bytes(), (5 * characters_per_append));
}

BOOST_AUTO_TEST_CASE(test_move_assignment) {
    iobuf buf;
    append_sequence(buf, 7);

    iobuf buf2;
    append_sequence(buf2, 5);

    buf2 = std::move(buf);

    BOOST_REQUIRE(buf2.size_bytes() == 7 * characters_per_append);
}

BOOST_AUTO_TEST_CASE(test_move_constructor) {
    iobuf buf;
    append_sequence(buf, 5);

    iobuf buf2(std::move(buf));

    BOOST_REQUIRE(buf2.size_bytes() == 5 * characters_per_append);
}

BOOST_AUTO_TEST_CASE(test_size) {
    iobuf buf;
    append_sequence(buf, 5);
    BOOST_REQUIRE_EQUAL(buf.size_bytes(), characters_per_append * 5);
}

BOOST_AUTO_TEST_CASE(test_fragment_iteration) {
    size_t count = 52;

    iobuf buf;
    append_sequence(buf, count);

    iobuf buf2;
    for (auto& frag : buf) {
        buf2.append(frag.get(), frag.size());
    }
    BOOST_CHECK_EQUAL(buf.size_bytes(), buf2.size_bytes());
}

BOOST_AUTO_TEST_CASE(test_writing_placeholders) {
    iobuf buf;
    sstring s = "hello world";
    const int32_t val = 55;

    auto ph = buf.reserve(sizeof(val));
    buf.append(s.data(), s.size()); // make sure things are legit!
    ph.write(reinterpret_cast<const char*>(&val), sizeof(val));

    const auto& in = *buf.begin();
    const int32_t copy = *reinterpret_cast<const int32_t*>(in.get());
    BOOST_REQUIRE_EQUAL(copy, val);
    BOOST_REQUIRE_EQUAL(buf.size_bytes(), 15);
}

BOOST_AUTO_TEST_CASE(test_temporary_buffs) {
    iobuf buf;
    temporary_buffer<char> x(55);
    buf.append(std::move(x));
    BOOST_REQUIRE(buf.size_bytes() == 55);
}
BOOST_AUTO_TEST_CASE(test_empty_istream) {
    auto fbuf = iobuf();
    auto in = iobuf::iterator_consumer(fbuf.cbegin(), fbuf.cend());
    BOOST_CHECK_EQUAL(in.bytes_consumed(), 0);

    BOOST_CHECK_THROW(in.consume_type<char>(), std::out_of_range);

    bytes b(bytes::initialized_later(), 10);
    BOOST_CHECK_THROW(in.consume_to(1, b.begin()), std::out_of_range);
    in.consume_to(0, b.begin());
}
BOOST_AUTO_TEST_CASE(test_read_pod) {
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

BOOST_AUTO_TEST_CASE(test_consume_to) {
    // read 75 bytes * i number of times
    // cumulative
    auto fbuf = iobuf();

    for (size_t i = 1; i < 10; ++i) {
        temporary_buffer<char> x(75);
        std::memset(x.get_write(), 'x', x.size());
        fbuf.append(x.get(), x.size());
        auto in = iobuf::iterator_consumer(fbuf.cbegin(), fbuf.cend());

        temporary_buffer<char> y(x.size() * i);
        in.consume_to(y.size(), y.get_write());
        BOOST_CHECK_EQUAL(in.bytes_consumed(), fbuf.size_bytes());
    }
}
BOOST_AUTO_TEST_CASE(test_linearization) {
    // read 75 bytes * i number of times
    // cumulative
    auto fbuf = iobuf();
    auto dstbuf = iobuf();

    for (size_t i = 1; i < 10; ++i) {
        temporary_buffer<char> x(136);
        std::memset(x.get_write(), 'x', x.size());
        fbuf.append(x.get(), x.size());
    }
    auto in = iobuf::iterator_consumer(fbuf.cbegin(), fbuf.cend());
    auto ph = dstbuf.reserve(fbuf.size_bytes());
    in.consume_to(ph.remaining_size(), ph);

    BOOST_CHECK_EQUAL(in.bytes_consumed(), fbuf.size_bytes());
    BOOST_CHECK_EQUAL(ph.remaining_size(), 0);
}
BOOST_AUTO_TEST_CASE(share_with_byte_comparator) {
    auto fbuf = iobuf();
    for (size_t i = 1; i < 10; ++i) {
        temporary_buffer<char> x(136);
        std::memset(x.get_write(), 'x', x.size());
        fbuf.append(x.get(), x.size());
    }
    BOOST_CHECK_EQUAL(fbuf.share(), fbuf);
}
BOOST_AUTO_TEST_CASE(copy_iobuf_equality_comparator) {
    auto fbuf = iobuf();
    for (size_t i = 1; i < 10; ++i) {
        temporary_buffer<char> x(136);
        std::memset(x.get_write(), 'x', x.size());
        fbuf.append(x.get(), x.size());
    }
    auto fbuf2 = fbuf.copy();
    BOOST_CHECK_EQUAL(fbuf, fbuf2);
}
BOOST_AUTO_TEST_CASE(gen_bytes_view) {
    auto fbuf = iobuf();
    auto b = random_generators::get_bytes();
    auto bv = bytes_view(b);
    int32_t x = 42;
    fbuf.append(reinterpret_cast<const char*>(&x), sizeof(x));
    fbuf.append(reinterpret_cast<const char*>(bv.data()), bv.size());
    fbuf.append(reinterpret_cast<const char*>(b.data()), b.size());
    BOOST_CHECK_EQUAL(fbuf.size_bytes(), (2 * 128 * 1024) + sizeof(x));
}
BOOST_AUTO_TEST_CASE(traver_all_bytes_one_at_a_time) {
    auto io = iobuf();
    const char* str = "alex";
    io.append(str, std::strlen(str));
    BOOST_CHECK_EQUAL(io.size_bytes(), std::strlen(str));
    sstring expected(sstring::initialized_later(), std::strlen(str));
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
BOOST_AUTO_TEST_CASE(not_equal_by_size) {
    auto a = iobuf();
    auto b = iobuf();
    a.append("a", 1);
    b.append("b", 1);
    BOOST_CHECK(a != b);
}
