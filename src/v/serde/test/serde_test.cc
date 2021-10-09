// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "hashing/crc32c.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "serde/serde.h"

#include <seastar/core/scheduling.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <chrono>
#include <limits>

struct custom_read_write {
    friend inline void read_nested(
      iobuf_parser& in, custom_read_write& el, size_t const bytes_left_limit) {
        serde::read_nested(in, el._x, bytes_left_limit);
        ++el._x;
    }

    friend inline void write(iobuf& out, custom_read_write el) {
        ++el._x;
        serde::write(out, el._x);
    }

    int _x;
};

enum class my_enum : uint8_t { x, y, z };

inline void
read_nested(iobuf_parser& in, my_enum& el, size_t const bytes_left_limit) {
    serde::read_enum(in, el, bytes_left_limit);
}

inline void write(iobuf& out, my_enum el) { serde::write_enum(out, el); }

SEASTAR_THREAD_TEST_CASE(custom_read_write_test) {
    BOOST_CHECK(
      serde::from_iobuf<custom_read_write>(
        serde::to_iobuf(custom_read_write{123}))
        ._x
      == 125);
}

struct test_msg0
  : serde::envelope<test_msg0, serde::version<1>, serde::compat_version<0>> {
    char _i, _j;
};

struct test_msg1
  : serde::envelope<test_msg1, serde::version<4>, serde::compat_version<0>> {
    int _a;
    test_msg0 _m;
    int _b, _c;
};

struct test_msg1_new
  : serde::
      envelope<test_msg1_new, serde::version<10>, serde::compat_version<5>> {
    int _a;
    test_msg0 _m;
    int _b, _c;
};

struct test_msg1_new_manual {
    using value_t = test_msg1_new_manual;
    static constexpr auto redpanda_serde_version = 10;
    static constexpr auto redpanda_serde_compat_version = 5;

    bool operator==(test_msg1_new_manual const&) const = default;

    int _a;
    test_msg0 _m;
    int _b, _c;
};

struct not_an_envelope {};
static_assert(!serde::is_envelope_v<not_an_envelope>);
static_assert(serde::is_envelope_v<test_msg1>);
static_assert(serde::inherits_from_envelope_v<test_msg1_new>);
static_assert(!serde::inherits_from_envelope_v<test_msg1_new_manual>);
static_assert(test_msg1::redpanda_serde_version == 4);
static_assert(test_msg1::redpanda_serde_compat_version == 0);

SEASTAR_THREAD_TEST_CASE(manual_and_envelope_equal) {
    auto const roundtrip = serde::from_iobuf<test_msg1_new_manual>(
      serde::to_iobuf(test_msg1_new{
        ._a = 77, ._m = test_msg0{._i = 2, ._j = 3}, ._b = 88, ._c = 99}));
    auto const check = test_msg1_new_manual{
      ._a = 77, ._m = test_msg0{._i = 2, ._j = 3}, ._b = 88, ._c = 99};
    BOOST_CHECK(roundtrip == check);
}

SEASTAR_THREAD_TEST_CASE(reserve_test) {
    auto b = iobuf();
    auto p = b.reserve(10);

    auto const a = std::array<char, 3>{'a', 'b', 'c'};
    p.write(&a[0], a.size());

    auto parser = iobuf_parser{std::move(b)};
    auto called = 0U;
    parser.consume(3, [&](const char* data, size_t max) {
        ++called;
        BOOST_CHECK(max == 3);
        BOOST_CHECK(data[0] == a[0]);
        BOOST_CHECK(data[1] == a[1]);
        BOOST_CHECK(data[2] == a[2]);
        return ss::stop_iteration::no;
    });
    BOOST_CHECK(called == 1);
}

SEASTAR_THREAD_TEST_CASE(envelope_too_big_test) {
    struct big
      : public serde::
          envelope<big, serde::version<0>, serde::compat_version<0>> {
        std::vector<char> data_;
    };

    auto too_big = std::make_unique<big>();
    too_big->data_.resize(std::numeric_limits<serde::serde_size_t>::max());
    auto b = iobuf();
    BOOST_CHECK_THROW(serde::write(b, *too_big), std::exception);
}

SEASTAR_THREAD_TEST_CASE(simple_envelope_test) {
    struct msg
      : serde::envelope<msg, serde::version<1>, serde::compat_version<0>> {
        int32_t _i, _j;
    };

    auto b = iobuf();
    serde::write(b, msg{._i = 2, ._j = 3});

    auto parser = iobuf_parser{std::move(b)};
    auto m = serde::read<msg>(parser);
    BOOST_CHECK(m._i == 2);
    BOOST_CHECK(m._j == 3);
}

SEASTAR_THREAD_TEST_CASE(envelope_test) {
    auto b = iobuf();

    serde::write(
      b, test_msg1{._a = 55, ._m = {._i = 'i', ._j = 'j'}, ._b = 33, ._c = 44});

    auto parser = iobuf_parser{std::move(b)};

    auto m = test_msg1{};
    BOOST_CHECK_NO_THROW(m = serde::read<test_msg1>(parser));
    BOOST_CHECK(m._a == 55);
    BOOST_CHECK(m._b == 33);
    BOOST_CHECK(m._c == 44);
    BOOST_CHECK(m._m._i == 'i');
    BOOST_CHECK(m._m._j == 'j');
}

SEASTAR_THREAD_TEST_CASE(envelope_test_version_older_than_compat_version) {
    auto b = iobuf();

    serde::write(
      b,
      test_msg1_new{
        ._a = 55, ._m = {._i = 'i', ._j = 'j'}, ._b = 33, ._c = 44});

    auto parser = iobuf_parser{std::move(b)};

    auto throws = false;
    try {
        serde::read<test_msg1>(parser);
    } catch (std::exception const& e) {
        throws = true;
    }

    BOOST_CHECK(throws);
}

SEASTAR_THREAD_TEST_CASE(envelope_test_buffer_too_short) {
    auto b = iobuf();

    serde::write(
      b,
      test_msg1_new{
        ._a = 55, ._m = {._i = 'i', ._j = 'j'}, ._b = 33, ._c = 44});

    b.trim_back(1); // introduce length mismatch
    auto parser = iobuf_parser{std::move(b)};

    BOOST_CHECK_THROW(serde::read<test_msg1_new>(parser), std::exception);
}

SEASTAR_THREAD_TEST_CASE(vector_test) {
    auto b = iobuf();

    serde::write(b, std::vector{1, 2, 3});

    auto parser = iobuf_parser{std::move(b)};
    auto const m = serde::read<std::vector<int>>(parser);
    BOOST_CHECK((m == std::vector{1, 2, 3}));
}

// struct with differing sizes:
// vector length may take different size (vint)
// vector data may have different size (_ints.size() * sizeof(int))
struct inner_differing_sizes
  : serde::envelope<inner_differing_sizes, serde::version<1>> {
    std::vector<int32_t> _ints;
};

struct complex_msg : serde::envelope<complex_msg, serde::version<3>> {
    std::vector<inner_differing_sizes> _vec;
    int32_t _x;
};

static_assert(serde::is_envelope_v<complex_msg>);

SEASTAR_THREAD_TEST_CASE(complex_msg_test) {
    auto b = iobuf();

    inner_differing_sizes big;
    big._ints.resize(std::numeric_limits<uint8_t>::max() + 1);
    std::fill(begin(big._ints), end(big._ints), 4);

    serde::write(
      b,
      complex_msg{
        ._vec = std::
          vector{inner_differing_sizes{._ints = {1, 2, 3}}, big, inner_differing_sizes{._ints = {1, 2, 3}}, big, inner_differing_sizes{._ints = {1, 2, 3}}, big},
        ._x = 3});

    auto parser = iobuf_parser{std::move(b)};
    auto const m = serde::read<complex_msg>(parser);
    BOOST_CHECK(m._vec.size() == 6);
    for (auto i = 0U; i < m._vec.size(); ++i) {
        if (i % 2 == 0) {
            BOOST_CHECK((m._vec[i]._ints == std::vector{1, 2, 3}));
        } else {
            BOOST_CHECK(m._vec[i]._ints == big._ints);
        }
    }
}

SEASTAR_THREAD_TEST_CASE(all_types_test) {
    {
        using named = named_type<int64_t, struct named_test_tag>;
        auto b = iobuf();
        serde::write(b, named{123});
        auto parser = iobuf_parser{std::move(b)};
        BOOST_CHECK(serde::read<named>(parser) == named{123});
    }

    {
        using ss_bool = ss::bool_class<struct bool_tag>;
        auto b = iobuf();
        serde::write(b, ss_bool{true});
        auto parser = iobuf_parser{std::move(b)};
        BOOST_CHECK(serde::read<ss_bool>(parser) == ss_bool{true});
    }

    {
        auto b = iobuf();
        serde::write(b, std::chrono::milliseconds{123});
        auto parser = iobuf_parser{std::move(b)};
        BOOST_CHECK(
          serde::read<std::chrono::milliseconds>(parser)
          == std::chrono::milliseconds{123});
    }

    {
        auto b = iobuf();
        auto buf = iobuf{};
        buf.append("hello", 5U);
        serde::write(b, std::move(buf));
        auto parser = iobuf_parser{std::move(b)};
        BOOST_CHECK(serde::read<iobuf>(parser).size_bytes() == 5);
    }

    {
        auto b = iobuf();
        serde::write(b, ss::sstring{"123"});
        auto parser = iobuf_parser{std::move(b)};
        BOOST_CHECK(serde::read<ss::sstring>(parser) == ss::sstring{"123"});
    }

    {
        auto b = iobuf();
        auto const v = std::vector<int32_t>{1, 2, 3};
        serde::write(b, v);
        auto parser = iobuf_parser{std::move(b)};
        BOOST_CHECK(serde::read<std::vector<int32_t>>(parser) == v);
    }

    {
        auto b = iobuf();
        serde::write(b, std::optional<int32_t>{});
        auto parser = iobuf_parser{std::move(b)};
        BOOST_CHECK(!serde::read<std::optional<int32_t>>(parser).has_value());
    }

    {
        auto b = iobuf();
        serde::write(b, float{-123.456});
        auto parser = iobuf_parser{std::move(b)};
        BOOST_CHECK(serde::read<float>(parser) == float{-123.456});
    }

    {
        auto b = iobuf();
        serde::write(b, double{-123.456});
        auto parser = iobuf_parser{std::move(b)};
        BOOST_CHECK(serde::read<double>(parser) == double{-123.456});
    }

    {
        auto b = iobuf();
        serde::write(b, model::ns{"abc"});
        auto parser = iobuf_parser{std::move(b)};
        BOOST_CHECK(serde::read<model::ns>(parser) == "abc");
    }
}

struct test_snapshot_header
  : serde::envelope<
      test_snapshot_header,
      serde::version<1>,
      serde::compat_version<0>> {
    ss::future<> serde_async_read(iobuf_parser&, serde::header const&);
    ss::future<> serde_async_write(iobuf&) const;

    model::ns ns_;
    int32_t header_crc;
    int32_t metadata_crc;
    int8_t version;
    int32_t metadata_size;
};

static_assert(serde::is_envelope_v<test_snapshot_header>);
static_assert(serde::has_serde_async_read<test_snapshot_header>);
static_assert(serde::has_serde_async_write<test_snapshot_header>);

ss::future<> test_snapshot_header::serde_async_read(
  iobuf_parser& in, serde::header const& h) {
    ns_ = serde::read_nested<decltype(ns_)>(in, h._bytes_left_limit);
    header_crc = serde::read_nested<decltype(header_crc)>(
      in, h._bytes_left_limit);
    metadata_crc = serde::read_nested<decltype(metadata_crc)>(
      in, h._bytes_left_limit);
    version = serde::read_nested<decltype(version)>(in, h._bytes_left_limit);
    metadata_size = serde::read_nested<decltype(metadata_size)>(
      in, h._bytes_left_limit);

    vassert(metadata_size >= 0, "Invalid metadata size {}", metadata_size);

    crc::crc32c crc;
    crc.extend(ss::cpu_to_le(metadata_crc));
    crc.extend(ss::cpu_to_le(version));
    crc.extend(ss::cpu_to_le(metadata_size));

    if (header_crc != crc.value()) {
        return ss::make_exception_future<>(std::runtime_error(fmt::format(
          "Corrupt snapshot. Failed to verify header crc: {} != "
          "{}: path?",
          crc.value(),
          header_crc)));
    }

    return ss::make_ready_future<>();
}

ss::future<> test_snapshot_header::serde_async_write(iobuf& out) const {
    serde::write(out, ns_);
    serde::write(out, header_crc);
    serde::write(out, metadata_crc);
    serde::write(out, version);
    serde::write(out, metadata_size);
    return ss::make_ready_future<>();
}

SEASTAR_THREAD_TEST_CASE(snapshot_test) {
    auto b = iobuf();
    auto write_future = serde::write_async(
      b,
      test_snapshot_header{
        .header_crc = 1, .metadata_crc = 2, .version = 3, .metadata_size = 4});
    write_future.wait();
    auto parser = iobuf_parser{std::move(b)};
    auto read_future = serde::read_async<test_snapshot_header>(parser);
    read_future.wait();
    BOOST_CHECK(read_future.failed());
    try {
        std::rethrow_exception(read_future.get_exception());
    } catch (std::exception const& e) {
        BOOST_CHECK(
          std::string_view{e.what()}.starts_with("Corrupt snapshot."));
    }
}

struct small
  : public serde::envelope<small, serde::version<0>, serde::compat_version<0>> {
    int a, b, c;
};

struct big
  : public serde::envelope<big, serde::version<1>, serde::compat_version<0>> {
    int a, b, c, d{0x1234};
};

SEASTAR_THREAD_TEST_CASE(compat_test_added_field) {
    auto b = serde::to_iobuf(small{.a = 1, .b = 2, .c = 3});
    auto const deserialized = serde::from_iobuf<big>(std::move(b));
    BOOST_CHECK(deserialized.a == 1);
    BOOST_CHECK(deserialized.b == 2);
    BOOST_CHECK(deserialized.c == 3);
    BOOST_CHECK(deserialized.d == 0x1234);
}

SEASTAR_THREAD_TEST_CASE(compat_test_added_field_vector) {
    auto b = serde::to_iobuf(
      std::vector<small>{{.a = 1, .b = 2, .c = 3}, {.a = 4, .b = 5, .c = 6}});
    auto const deserialized = serde::from_iobuf<std::vector<big>>(std::move(b));
    BOOST_CHECK(deserialized.at(0).a == 1);
    BOOST_CHECK(deserialized.at(0).b == 2);
    BOOST_CHECK(deserialized.at(0).c == 3);
    BOOST_CHECK(deserialized.at(0).d == 0x1234);
    BOOST_CHECK(deserialized.at(1).a == 4);
    BOOST_CHECK(deserialized.at(1).b == 5);
    BOOST_CHECK(deserialized.at(1).c == 6);
    BOOST_CHECK(deserialized.at(1).d == 0x1234);
}

SEASTAR_THREAD_TEST_CASE(compat_test_removed_field) {
    auto b = serde::to_iobuf(big{.a = 1, .b = 2, .c = 3, .d = 4});
    auto const deserialized = serde::from_iobuf<small>(std::move(b));
    BOOST_CHECK(deserialized.a == 1);
    BOOST_CHECK(deserialized.b == 2);
    BOOST_CHECK(deserialized.c == 3);
}

SEASTAR_THREAD_TEST_CASE(compat_test_removed_field_vector) {
    auto b = serde::to_iobuf(std::vector<big>{
      {.a = 1, .b = 2, .c = 3, .d = 0x77},
      {.a = 123, .b = 456, .c = 789, .d = 0x77}});
    auto const deserialized = serde::from_iobuf<std::vector<small>>(
      std::move(b));
    BOOST_CHECK(deserialized.at(0).a == 1);
    BOOST_CHECK(deserialized.at(0).b == 2);
    BOOST_CHECK(deserialized.at(0).c == 3);
    BOOST_CHECK(deserialized.at(1).a == 123);
    BOOST_CHECK(deserialized.at(1).b == 456);
    BOOST_CHECK(deserialized.at(1).c == 789);
}

SEASTAR_THREAD_TEST_CASE(compat_test_half_field_1) {
    struct half_field_1
      : public serde::envelope<
          half_field_1,
          serde::version<1>,
          serde::compat_version<
            0 /* Actually not compatible! Just to catch errors. */>> {
        int a, b;
        short c;
    };

    auto b = serde::to_iobuf(half_field_1{.a = 1, .b = 2, .c = 3});
    BOOST_CHECK_THROW(serde::from_iobuf<small>(std::move(b)), std::exception);
}

SEASTAR_THREAD_TEST_CASE(compat_test_half_field_2) {
    struct half_field_2
      : public serde::
          envelope<half_field_2, serde::version<1>, serde::compat_version<0>> {
        int a, b, c;
        short d;
    };

    auto b = serde::to_iobuf(half_field_2{.a = 1, .b = 2, .c = 3, .d = 0x1234});
    auto const deserialized = serde::from_iobuf<small>(std::move(b));
    BOOST_CHECK(deserialized.a == 1);
    BOOST_CHECK(deserialized.b == 2);
    BOOST_CHECK(deserialized.c == 3);

    auto b1 = serde::to_iobuf(
      half_field_2{.a = 1, .b = 2, .c = 3, .d = 0x1234});
    BOOST_CHECK_THROW(serde::from_iobuf<big>(std::move(b1)), std::exception);
}
