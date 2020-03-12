#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "bytes/tests/utils.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/range/algorithm/for_each.hpp>
#include <boost/test/unit_test.hpp>
#include <fmt/format.h>

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

BOOST_AUTO_TEST_CASE(test_temporary_buffs) {
    iobuf buf;
    ss::temporary_buffer<char> x(55);
    buf.append(std::move(x));
    BOOST_REQUIRE(buf.size_bytes() == 55);
}
BOOST_AUTO_TEST_CASE(test_empty_istream) {
    auto fbuf = iobuf();
    auto in = iobuf::iterator_consumer(fbuf.cbegin(), fbuf.cend());
    BOOST_CHECK_EQUAL(in.bytes_consumed(), 0);

    BOOST_CHECK_THROW(in.consume_type<char>(), std::out_of_range);

    bytes b = ss::uninitialized_string<bytes>(10);
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
        ss::temporary_buffer<char> x(75);
        std::memset(x.get_write(), 'x', x.size());
        fbuf.append(x.get(), x.size());
        auto in = iobuf::iterator_consumer(fbuf.cbegin(), fbuf.cend());

        ss::temporary_buffer<char> y(x.size() * i);
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
BOOST_AUTO_TEST_CASE(share_with_byte_comparator) {
    auto fbuf = iobuf();
    for (size_t i = 1; i < 10; ++i) {
        ss::temporary_buffer<char> x(136);
        std::memset(x.get_write(), 'x', x.size());
        fbuf.append(x.get(), x.size());
    }
    BOOST_CHECK_EQUAL(fbuf.share(0, fbuf.size_bytes()), fbuf);
}
BOOST_AUTO_TEST_CASE(copy_iobuf_equality_comparator) {
    auto fbuf = iobuf();
    for (size_t i = 1; i < 10; ++i) {
        ss::temporary_buffer<char> x(136);
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
BOOST_AUTO_TEST_CASE(not_equal_by_size) {
    auto a = iobuf();
    auto b = iobuf();
    a.append("a", 1);
    b.append("b", 1);
    BOOST_CHECK(a != b);
}
BOOST_AUTO_TEST_CASE(correct_control_structure_sharing) {
    auto a = iobuf();
    a.append("a", 1);
    auto b = a.control_share();
    b.append("b", 1);
    BOOST_CHECK_EQUAL(a.size_bytes(), b.size_bytes());
    BOOST_CHECK_EQUAL(a, b);
}
BOOST_AUTO_TEST_CASE(test_prepend) {
    auto a = iobuf();
    a.append("a", 1);
    a.prepend(ss::temporary_buffer<char>(1));
    BOOST_CHECK_EQUAL(a.size_bytes(), 2);
}
BOOST_AUTO_TEST_CASE(append_each_other) {
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
SEASTAR_THREAD_TEST_CASE(iobuf_cross_shard) {
    // note: run tests w/  > 2 cores for test to exercise
    auto shard = (ss::engine().cpu_id() + 1) % ss::smp::count;
    const ss::sstring data("question authority");
    auto f = ss::smp::submit_to(shard, [data] {
        iobuf src;
        src.append(data.data(), data.size());
        return src;
    });
    auto from = f.get0();
    BOOST_REQUIRE(from.size_bytes() == data.size());
    auto in = iobuf::iterator_consumer(from.cbegin(), from.cend());
    std::equal(data.cbegin(), data.cend(), in.begin());
}
SEASTAR_THREAD_TEST_CASE(iobuf_foreign_copy) {
    iobuf og;
    {
        auto b = random_generators::gen_alphanum_string(1024 * 1024);
        og.append(b.data(), b.size());
    }
    // main tests
    std::vector<iobuf> bufs = iobuf_share_foreign_n(
      std::move(og), ss::smp::count);
    ss::parallel_for_each(
      boost::irange(ss::shard_id(0), ss::smp::count),
      [&bufs](ss::shard_id shard) {
          iobuf io = std::move(bufs[shard]);
          std::vector<iobuf> nested_bufs = iobuf_share_foreign_n(
            std::move(io), ss::smp::count);

          // transitive sharing
          return ss::do_with(
            std::move(nested_bufs), [](std::vector<iobuf>& bufs) {
                return ss::parallel_for_each(
                  boost::irange(ss::shard_id(0), ss::smp::count),
                  [&bufs](ss::shard_id shard) {
                      iobuf io = std::move(bufs[shard]);
                      return ss::smp::submit_to(shard, [io = std::move(io)] {});
                  });
            });
      })
      .get();
}

BOOST_AUTO_TEST_CASE(share) {
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

BOOST_AUTO_TEST_CASE(iterator_relationships) {
    iobuf buf;
    BOOST_REQUIRE(buf.begin() == buf.end());
    BOOST_REQUIRE(buf.cbegin() == buf.cend());

    buf.append(ss::temporary_buffer<char>(100));
    BOOST_REQUIRE(buf.begin() != buf.end());
    BOOST_REQUIRE(buf.cbegin() != buf.cend());
    BOOST_REQUIRE(std::next(buf.begin()) == buf.end());
    BOOST_REQUIRE(std::next(buf.cbegin()) == buf.cend());
}

BOOST_AUTO_TEST_CASE(is_finished) {
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

BOOST_AUTO_TEST_CASE(iobuf_as_scattered_message) {
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
BOOST_AUTO_TEST_CASE(iobuf_as_ostream_basic) {
    const auto b = random_generators::gen_alphanum_string(512);
    iobuf underlying;
    iobuf_ostreambuf obuf(underlying);
    std::ostream os(&obuf);
    os << "hello world" << std::endl /* new line + flush*/;
    BOOST_REQUIRE_EQUAL(underlying.size_bytes(), 12);
}

BOOST_AUTO_TEST_CASE(iobuf_as_ostream) {
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
