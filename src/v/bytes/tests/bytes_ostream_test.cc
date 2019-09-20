#define BOOST_TEST_MODULE bytes

#include "bytes/bytes.h"
#include "bytes/bytes_ostream.h"
#include "random/fast_prng.h"
#include "random/generators.h"

#include <boost/range/algorithm/for_each.hpp>
#include <boost/test/unit_test.hpp>

static const constexpr size_t characters_per_append = 10;

void append_sequence(bytes_ostream& buf, size_t count) {
    for (size_t i = 0; i < count; i++) {
        auto str = random_generators::gen_alphanum_string(
          characters_per_append);
        buf.write(str.data(), str.size());
    }
}

BOOST_AUTO_TEST_CASE(test_appended_data_is_retained) {
    bytes_ostream buf;
    append_sequence(buf, 5);
    BOOST_CHECK_EQUAL(buf.size_bytes(), (5 * characters_per_append));
}

BOOST_AUTO_TEST_CASE(test_move_assignment) {
    bytes_ostream buf;
    append_sequence(buf, 7);

    bytes_ostream buf2;
    append_sequence(buf2, 5);

    buf2 = std::move(buf);

    BOOST_REQUIRE(buf2.size_bytes() == 7 * characters_per_append);
}

BOOST_AUTO_TEST_CASE(test_move_constructor) {
    bytes_ostream buf;
    append_sequence(buf, 5);

    bytes_ostream buf2(std::move(buf));

    BOOST_REQUIRE(buf2.size_bytes() == 5 * characters_per_append);
}

BOOST_AUTO_TEST_CASE(test_size) {
    bytes_ostream buf;
    append_sequence(buf, 5);
    BOOST_REQUIRE_EQUAL(buf.size_bytes(), characters_per_append * 5);
}

BOOST_AUTO_TEST_CASE(test_fragment_iteration) {
    size_t count = 52;

    bytes_ostream buf;
    append_sequence(buf, count);

    bytes_ostream buf2;
    for (auto& frag : buf) {
        buf2.write(frag.get(), frag.size());
    }
    BOOST_CHECK_EQUAL(buf.size_bytes(), buf2.size_bytes());
}

BOOST_AUTO_TEST_CASE(test_writing_placeholders) {
    bytes_ostream buf;
    sstring s = "hello world";
    const int32_t val = 55;

    auto* ph = buf.write_place_holder(sizeof(val));
    buf.write(s.data(), s.size()); // make sure things are legit!
    std::copy_n(reinterpret_cast<const char*>(&val), sizeof(val), ph);

    const auto& in = *buf.begin();
    const int32_t copy = *reinterpret_cast<const int32_t*>(in.get());
    BOOST_REQUIRE_EQUAL(copy, val);
    BOOST_REQUIRE_EQUAL(buf.size_bytes(), 15);
}

BOOST_AUTO_TEST_CASE(test_temporary_buffs) {
    bytes_ostream buf;
    temporary_buffer<char> x(55);
    buf.write(std::move(x));
    BOOST_REQUIRE(buf.size_bytes() == 55);
}
