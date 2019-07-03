#define BOOST_TEST_MODULE bytes

#include "bytes/bytes.h"
#include "bytes/bytes_ostream.h"
#include "random/generators.h"

#include <boost/range/algorithm/for_each.hpp>
#include <boost/test/unit_test.hpp>

void serialize(bytes_ostream& buf, int x) {
    buf.write_non_empty(reinterpret_cast<const char*>(&x), sizeof(x));
}

int deserialize(bytes_view& buf) {
    int x = *reinterpret_cast<const int*>(buf.begin());
    buf.remove_prefix(sizeof(x));
    return x;
}

void append_sequence(bytes_ostream& buf, int count) {
    for (int i = 0; i < count; i++) {
        serialize(buf, i);
    }
}

void assert_sequence(bytes_ostream& buf, int count) {
    assert(buf.size_bytes() == count * sizeof(int));
    auto in = buf.linearize();
    for (int i = 0; i < count; i++) {
        auto val = deserialize(in);
        BOOST_REQUIRE_EQUAL(val, i);
    }
}

BOOST_AUTO_TEST_CASE(test_appended_data_is_retained) {
    bytes_ostream buf;
    append_sequence(buf, 1024);
    assert_sequence(buf, 1024);
}

BOOST_AUTO_TEST_CASE(test_copy_constructor) {
    bytes_ostream buf;
    append_sequence(buf, 1024);

    bytes_ostream buf2(buf);

    BOOST_REQUIRE(buf.size_bytes() == 1024 * sizeof(int));
    BOOST_REQUIRE(buf2.size_bytes() == 1024 * sizeof(int));

    assert_sequence(buf, 1024);
    assert_sequence(buf2, 1024);
}

BOOST_AUTO_TEST_CASE(test_copy_assignment) {
    bytes_ostream buf;
    append_sequence(buf, 512);

    bytes_ostream buf2;
    append_sequence(buf2, 1024);

    buf2 = buf;

    BOOST_REQUIRE(buf.size_bytes() == 512 * sizeof(int));
    BOOST_REQUIRE(buf2.size_bytes() == 512 * sizeof(int));

    assert_sequence(buf, 512);
    assert_sequence(buf2, 512);
}

BOOST_AUTO_TEST_CASE(test_move_assignment) {
    bytes_ostream buf;
    append_sequence(buf, 512);

    bytes_ostream buf2;
    append_sequence(buf2, 1024);

    buf2 = std::move(buf);

    BOOST_REQUIRE(buf.size_bytes() == 0);
    BOOST_REQUIRE(buf2.size_bytes() == 512 * sizeof(int));

    assert_sequence(buf2, 512);
}

BOOST_AUTO_TEST_CASE(test_move_constructor) {
    bytes_ostream buf;
    append_sequence(buf, 1024);

    bytes_ostream buf2(std::move(buf));

    BOOST_REQUIRE(buf.size_bytes() == 0);
    BOOST_REQUIRE(buf2.size_bytes() == 1024 * sizeof(int));

    assert_sequence(buf2, 1024);
}

BOOST_AUTO_TEST_CASE(test_size) {
    bytes_ostream buf;
    append_sequence(buf, 1024);
    BOOST_REQUIRE_EQUAL(buf.size_bytes(), sizeof(int) * 1024);
}

BOOST_AUTO_TEST_CASE(test_is_linearized) {
    bytes_ostream buf;

    BOOST_REQUIRE(buf.is_linearized());

    serialize(buf, 1);

    BOOST_REQUIRE(buf.is_linearized());

    append_sequence(buf, 1024);

    BOOST_REQUIRE(!buf.is_linearized()); // probably
}

BOOST_AUTO_TEST_CASE(test_view) {
    bytes_ostream buf;

    serialize(buf, 1);

    BOOST_REQUIRE(buf.is_linearized());

    auto in = buf.view();
    BOOST_REQUIRE_EQUAL(1, deserialize(in));
}

BOOST_AUTO_TEST_CASE(test_writing_blobs) {
    bytes_ostream buf;

    bytes b("hello");
    bytes_view b_view(b.begin(), b.size());

    buf.write(b_view);
    BOOST_REQUIRE(buf.linearize() == b_view);
}

BOOST_AUTO_TEST_CASE(test_writing_large_blobs) {
    bytes_ostream buf;

    bytes b(bytes::initialized_later(), 1024);
    std::fill(b.begin(), b.end(), 7);
    bytes_view b_view(b.begin(), b.size());

    buf.write(b_view);

    auto buf_view = buf.linearize();
    BOOST_REQUIRE(std::all_of(
      buf_view.begin(), buf_view.end(), [](char c) { return c == 7; }));
}

BOOST_AUTO_TEST_CASE(test_fragment_iteration) {
    int count = 64 * 1024;

    bytes_ostream buf;
    append_sequence(buf, count);

    bytes_ostream buf2;
    for (bytes_view frag : buf.fragments()) {
        buf2.write(frag);
    }

    assert(!buf2.is_linearized());

    assert_sequence(buf2, count);
}

BOOST_AUTO_TEST_CASE(test_writing_empty_blobs) {
    bytes_ostream buf;

    bytes b;
    buf.write(b);

    BOOST_REQUIRE(buf.size_bytes() == 0);
    BOOST_REQUIRE(buf.linearize().empty());
}

BOOST_AUTO_TEST_CASE(test_writing_placeholders) {
    bytes_ostream buf;

    auto* ph = buf.write_place_holder(sizeof(int));
    serialize(buf, 2);
    auto val = 1;
    std::copy_n(reinterpret_cast<const char*>(&val), sizeof(int), ph);

    auto in = buf.view();
    BOOST_REQUIRE_EQUAL(deserialize(in), 1);
    BOOST_REQUIRE_EQUAL(deserialize(in), 2);
    BOOST_REQUIRE(in.size() == 0);
}

BOOST_AUTO_TEST_CASE(test_append_big_and_small_chunks) {
    bytes_ostream small;
    append_sequence(small, 12);

    bytes_ostream big;
    append_sequence(big, 513);

    bytes_ostream buf;
    buf.append(big);
    buf.append(small);
    buf.append(big);
    buf.append(small);
}

BOOST_AUTO_TEST_CASE(test_remove_suffix) {
    auto test = [](size_t length, size_t suffix) {
        BOOST_TEST_MESSAGE(
          "Testing buffer size " << length << " and suffix size " << suffix);

        auto data = random_generators::get_bytes(length);
        bytes_view view = data;

        bytes_ostream bo;
        bo.write(data);

        bo.remove_suffix(suffix);
        view.remove_suffix(suffix);

        BOOST_REQUIRE(view == bytes_ostream(bo).linearize());
        for (bytes_view fragment : bo) {
            BOOST_REQUIRE_LE(fragment.size(), view.size());
            BOOST_REQUIRE(fragment == bytes_view(view.data(), fragment.size()));
            view.remove_prefix(fragment.size());
        }
        BOOST_REQUIRE_EQUAL(view.size(), 0);
    };

    test(0, 0);
    test(16, 0);
    test(1'000'000, 0);

    test(16, 16);
    test(1'000'000, 1'000'000);

    test(16, 1);
    test(16, 15);
    test(1'000'000, 1);
    test(1'000'000, 999'999);

    for (auto i = 0; i < 25; i++) {
        auto a = random_generators::get_int(128 * 1024);
        auto b = random_generators::get_int(128 * 1024);
        test(std::max(a, b), std::min(a, b));
    }
}
