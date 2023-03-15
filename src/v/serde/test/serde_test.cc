// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "hashing/crc32c.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "random/generators.h"
#include "serde/envelope.h"
#include "serde/serde.h"
#include "tristate.h"
#include "utils/fragmented_vector.h"

#include <seastar/core/scheduling.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/functional/hash.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <limits>
#include <optional>
#include <ratio>

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

SEASTAR_THREAD_TEST_CASE(custom_read_write_test) {
    BOOST_CHECK(
      serde::from_iobuf<custom_read_write>(
        serde::to_iobuf(custom_read_write{123}))
        ._x
      == 125);
}

struct test_msg0
  : serde::envelope<test_msg0, serde::version<1>, serde::compat_version<0>> {
    bool operator==(const test_msg0&) const = default;

    char _i, _j;
};

struct test_msg1
  : serde::envelope<test_msg1, serde::version<4>, serde::compat_version<0>> {
    bool operator==(const test_msg1&) const = default;

    int _a;
    test_msg0 _m;
    int _b, _c;
};

struct test_msg1_imcompatible
  : serde::envelope<test_msg1, serde::version<5>, serde::compat_version<5>> {
    bool operator==(const test_msg1_imcompatible&) const = default;

    test_msg0 _m;
};

struct test_msg1_new
  : serde::
      envelope<test_msg1_new, serde::version<10>, serde::compat_version<5>> {
    bool operator==(const test_msg1_new&) const = default;

    int _a;
    test_msg0 _m;
    int _b, _c;
};

struct test_msg1_new_manual {
    using value_t = test_msg1_new_manual;
    static constexpr auto redpanda_serde_version = serde::version_t{10};
    static constexpr auto redpanda_serde_compat_version = serde::version_t{5};

    bool operator==(test_msg1_new_manual const&) const = default;

    int _a;
    test_msg0 _m;
    int _b, _c;
};

struct not_an_envelope {};
static_assert(!serde::is_envelope<not_an_envelope>);
static_assert(serde::is_envelope<test_msg1>);
static_assert(serde::inherits_from_envelope<test_msg1_new>);
static_assert(!serde::inherits_from_envelope<test_msg1_new_manual>);
static_assert(test_msg1::redpanda_serde_version == 4);
static_assert(test_msg1::redpanda_serde_compat_version == 0);

SEASTAR_THREAD_TEST_CASE(incompatible_version_throws) {
    BOOST_CHECK_THROW(
      serde::from_iobuf<test_msg1_imcompatible>(serde::to_iobuf(test_msg1{})),
      serde::serde_exception);
}

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
        bool operator==(const big&) const = default;

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
        bool operator==(const msg&) const = default;

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
  : serde::envelope<
      inner_differing_sizes,
      serde::version<1>,
      serde::compat_version<1>> {
    bool operator==(const inner_differing_sizes&) const = default;

    std::vector<int32_t> _ints;
};

struct complex_msg
  : serde::envelope<complex_msg, serde::version<3>, serde::compat_version<3>> {
    std::vector<inner_differing_sizes> _vec;
    bool operator==(const complex_msg&) const = default;

    int32_t _x;
};

static_assert(serde::is_envelope<complex_msg>);

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

    {
        auto b = iobuf();
        serde::write(b, my_enum::z);
        auto parser = iobuf_parser{std::move(b)};
        BOOST_CHECK(serde::read<my_enum>(parser) == my_enum::z);
    }

    {
        auto b = iobuf();
        serde::write(
          b,
          static_cast<serde::serde_enum_serialized_t>(
            std::numeric_limits<std::underlying_type_t<my_enum>>::max())
            + 1);
        auto parser = iobuf_parser{std::move(b)};
        BOOST_CHECK_THROW(
          { serde::read<my_enum>(parser); }, serde::serde_exception);
    }

    {
        enum class big_enum : uint32_t { x };
        auto b = iobuf();
        BOOST_CHECK_THROW(
          serde::write(
            b,
            static_cast<big_enum>(
              std::numeric_limits<std::underlying_type_t<big_enum>>::max())),
          serde::serde_exception);
    }
}

struct test_snapshot_header
  : serde::envelope<
      test_snapshot_header,
      serde::version<1>,
      serde::compat_version<0>> {
    ss::future<> serde_async_read(iobuf_parser&, serde::header const);
    ss::future<> serde_async_write(iobuf&) const;

    bool operator==(const test_snapshot_header&) const = default;

    model::ns ns_;
    int32_t header_crc;
    int32_t metadata_crc;
    int8_t version;
    int32_t metadata_size;
};

static_assert(serde::is_envelope<test_snapshot_header>);
static_assert(serde::has_serde_async_read<test_snapshot_header>);
static_assert(serde::has_serde_async_write<test_snapshot_header>);

ss::future<> test_snapshot_header::serde_async_read(
  iobuf_parser& in, serde::header const h) {
    ns_ = co_await serde::read_async_nested<decltype(ns_)>(
      in, h._bytes_left_limit);
    header_crc = co_await serde::read_async_nested<decltype(header_crc)>(
      in, h._bytes_left_limit);
    metadata_crc = co_await serde::read_async_nested<decltype(metadata_crc)>(
      in, h._bytes_left_limit);
    version = co_await serde::read_async_nested<decltype(version)>(
      in, h._bytes_left_limit);
    metadata_size = co_await serde::read_async_nested<decltype(metadata_size)>(
      in, h._bytes_left_limit);

    vassert(metadata_size >= 0, "Invalid metadata size {}", metadata_size);

    crc::crc32c crc;
    crc.extend(ss::cpu_to_le(metadata_crc));
    crc.extend(ss::cpu_to_le(version));
    crc.extend(ss::cpu_to_le(metadata_size));

    if (header_crc != crc.value()) {
        throw std::runtime_error(fmt::format(
          "Corrupt snapshot. Failed to verify header crc: {} != "
          "{}: path?",
          crc.value(),
          header_crc));
    }
}

ss::future<> test_snapshot_header::serde_async_write(iobuf& out) const {
    co_await serde::write_async(out, ns_);
    co_await serde::write_async(out, header_crc);
    co_await serde::write_async(out, metadata_crc);
    co_await serde::write_async(out, version);
    co_await serde::write_async(out, metadata_size);
}

SEASTAR_THREAD_TEST_CASE(snapshot_test) {
    {
        auto obj = test_snapshot_header{
          .metadata_crc = 2, .version = 3, .metadata_size = 4};
        crc::crc32c header_crc;
        header_crc.extend(ss::cpu_to_le(obj.metadata_crc));
        header_crc.extend(ss::cpu_to_le(obj.version));
        header_crc.extend(ss::cpu_to_le(obj.metadata_size));
        obj.header_crc = header_crc.value();

        auto b = iobuf();
        serde::write_async(b, obj).get();

        auto parser = iobuf_parser{std::move(b)};
        BOOST_CHECK(
          obj == serde::read_async<test_snapshot_header>(parser).get());
    }

    {
        auto obj_with_bad_crc = test_snapshot_header{
          .header_crc = 1, .metadata_crc = 2, .version = 3, .metadata_size = 4};
        auto b = iobuf();
        serde::write_async(b, obj_with_bad_crc).get();

        auto parser = iobuf_parser{std::move(b)};
        try {
            serde::read_async<test_snapshot_header>(parser).get();
        } catch (std::exception const& e) {
            BOOST_CHECK(
              std::string_view{e.what()}.starts_with("Corrupt snapshot."));
        }
    }
}

struct small
  : public serde::envelope<small, serde::version<0>, serde::compat_version<0>> {
    bool operator==(const small&) const = default;

    int a, b, c;
};

struct big
  : public serde::envelope<big, serde::version<1>, serde::compat_version<0>> {
    bool operator==(const big&) const = default;

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
        bool operator==(const half_field_1&) const = default;

        int a, b;
        short c;
    };

    auto b = serde::to_iobuf(half_field_1{.a = 1, .b = 2, .c = 3});
    // NOLINTNEXTLINE(bugprone-use-after-move)
    BOOST_CHECK_THROW(serde::from_iobuf<small>(std::move(b)), std::exception);
}

SEASTAR_THREAD_TEST_CASE(compat_test_half_field_2) {
    struct half_field_2
      : public serde::
          envelope<half_field_2, serde::version<1>, serde::compat_version<0>> {
        bool operator==(const half_field_2&) const = default;

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
    // NOLINTNEXTLINE(bugprone-use-after-move)
    BOOST_CHECK_THROW(serde::from_iobuf<big>(std::move(b1)), std::exception);
}

SEASTAR_THREAD_TEST_CASE(serde_checksum_envelope_test) {
    struct checksummed
      : public serde::checksum_envelope<
          checksummed,
          serde::version<3>,
          serde::compat_version<2>> {
        bool operator==(const checksummed&) const = default;

        std::vector<test_msg1_new_manual> data_;
    };

    auto const obj = checksummed{
      .data_ = {
        test_msg1_new_manual{
          ._a = 1, ._m = test_msg0{._i = 33, ._j = 44}, ._b = 2, ._c = 3},
        test_msg1_new_manual{
          ._a = 4, ._m = test_msg0{._i = 55, ._j = 66}, ._b = 5, ._c = 6},
        test_msg1_new_manual{
          ._a = 7, ._m = test_msg0{._i = 77, ._j = 88}, ._b = 8, ._c = 9}}};
    auto const vec = std::vector<checksummed>{obj, obj};
    BOOST_CHECK(
      serde::from_iobuf<std::vector<checksummed>>(serde::to_iobuf(vec)) == vec);
}

struct old_no_cs
  : public serde::
      envelope<old_no_cs, serde::version<3>, serde::compat_version<2>> {
    bool operator==(const old_no_cs&) const = default;

    std::vector<test_msg1_new_manual> data_;
};
struct new_cs
  : public serde::
      checksum_envelope<new_cs, serde::version<4>, serde::compat_version<4>> {
    friend inline void
    read_nested(iobuf_parser& in, new_cs& ts, size_t const bytes_left_limit) {
        switch (serde::peek_version(in)) {
        case 1:
        case 2:
            throw std::runtime_error{"not supported"};
        case 3: {
            ts.data_
              = serde::read_nested<old_no_cs>(in, bytes_left_limit).data_;
            break;
        }
        case 4:
        default:
            ts = serde::read_nested<new_cs>(in, bytes_left_limit);
            break;
        }
    }

    bool operator==(const new_cs&) const = default;

    std::vector<test_msg1_new_manual> data_;
};

SEASTAR_THREAD_TEST_CASE(serde_checksum_update) {
    auto const old_obj = old_no_cs{
      .data_ = {
        test_msg1_new_manual{
          ._a = 1, ._m = test_msg0{._i = 33, ._j = 44}, ._b = 2, ._c = 3},
        test_msg1_new_manual{
          ._a = 4, ._m = test_msg0{._i = 55, ._j = 66}, ._b = 5, ._c = 6},
        test_msg1_new_manual{
          ._a = 7, ._m = test_msg0{._i = 77, ._j = 88}, ._b = 8, ._c = 9}}};

    auto const new_obj = new_cs{
      .data_ = {
        test_msg1_new_manual{
          ._a = 1, ._m = test_msg0{._i = 33, ._j = 44}, ._b = 2, ._c = 3},
        test_msg1_new_manual{
          ._a = 4, ._m = test_msg0{._i = 55, ._j = 66}, ._b = 5, ._c = 6},
        test_msg1_new_manual{
          ._a = 7, ._m = test_msg0{._i = 77, ._j = 88}, ._b = 8, ._c = 9}}};

    auto const old_vec = std::vector<old_no_cs>{old_obj, old_obj};
    auto const new_vec = std::vector<new_cs>{new_obj, new_obj};
    BOOST_CHECK(
      serde::from_iobuf<std::vector<new_cs>>(serde::to_iobuf(old_vec))
      == new_vec);
}

struct old_cs
  : public serde::checksum_envelope<
      old_no_cs,
      serde::version<3>,
      serde::compat_version<2>> {
    bool operator==(const old_cs&) const = default;

    std::vector<test_msg1_new_manual> data_;
};
struct new_no_cs
  : public serde::
      envelope<new_cs, serde::version<4>, serde::compat_version<3>> {
    bool operator==(const new_no_cs& other) const {
        return data_ == other.data_;
    }

    serde::checksum_t unchecked_dummy_checksum_{0U};
    std::vector<test_msg1_new_manual> data_;
};

SEASTAR_THREAD_TEST_CASE(serde_checksum_update_1) {
    auto const old_obj = old_cs{
      .data_ = {
        test_msg1_new_manual{
          ._a = 1, ._m = test_msg0{._i = 33, ._j = 44}, ._b = 2, ._c = 3},
        test_msg1_new_manual{
          ._a = 4, ._m = test_msg0{._i = 55, ._j = 66}, ._b = 5, ._c = 6},
        test_msg1_new_manual{
          ._a = 7, ._m = test_msg0{._i = 77, ._j = 88}, ._b = 8, ._c = 9}}};

    auto const new_obj = new_no_cs{
      .data_ = {
        test_msg1_new_manual{
          ._a = 1, ._m = test_msg0{._i = 33, ._j = 44}, ._b = 2, ._c = 3},
        test_msg1_new_manual{
          ._a = 4, ._m = test_msg0{._i = 55, ._j = 66}, ._b = 5, ._c = 6},
        test_msg1_new_manual{
          ._a = 7, ._m = test_msg0{._i = 77, ._j = 88}, ._b = 8, ._c = 9}}};

    auto const old_vec = std::vector<old_cs>{old_obj, old_obj};
    auto const new_vec = std::vector<new_no_cs>{new_obj, new_obj};
    BOOST_CHECK(
      serde::from_iobuf<std::vector<new_no_cs>>(serde::to_iobuf(old_vec))
      == new_vec);
}

struct serde_fields_test_struct
  : serde::
      envelope<test_msg1_new, serde::version<10>, serde::compat_version<5>> {
    serde_fields_test_struct() = default;
    explicit serde_fields_test_struct(int a)
      : _a{a}
      , _m{test_msg0{._i = 11, ._j = 22}}
      , _b{333}
      , _c{444} {}
    bool operator==(const serde_fields_test_struct&) const = default;
    auto serde_fields() { return std::tie(_a, _m, _b, _c); }
    int _a;
    test_msg0 _m;
    int _b, _c;
};

SEASTAR_THREAD_TEST_CASE(serde_fields_test_struct_test) {
    BOOST_CHECK(
      serde::from_iobuf<serde_fields_test_struct>(
        serde::to_iobuf(serde_fields_test_struct{123}))
      == serde_fields_test_struct{123});
}

SEASTAR_THREAD_TEST_CASE(fragmented_vector_test) {
    std::vector<int> sizes(100);
    std::iota(sizes.begin(), sizes.end(), 0);
    sizes.push_back(4095);
    sizes.push_back(4096);
    sizes.push_back(4097);

    for (auto i : sizes) {
        // build input
        fragmented_vector<int> v_in;
        fragmented_vector<int> v_in_copy;
        for (int j = 0; j < i; ++j) {
            v_in.push_back(j);
            v_in_copy.push_back(j);
        }
        BOOST_REQUIRE_EQUAL(v_in.size(), v_in_copy.size());
        BOOST_REQUIRE_EQUAL_COLLECTIONS(
          v_in.begin(), v_in.end(), v_in_copy.begin(), v_in_copy.end());

        iobuf b;
        serde::write(b, std::move(v_in));
        iobuf_parser parser{std::move(b)};
        auto const v_out = serde::read<fragmented_vector<int>>(parser);

        BOOST_REQUIRE_EQUAL(v_out.size(), v_in_copy.size());
        BOOST_REQUIRE_EQUAL_COLLECTIONS(
          v_out.begin(), v_out.end(), v_in_copy.begin(), v_in_copy.end());
    }
}

SEASTAR_THREAD_TEST_CASE(serde_topic_namespace_test) {
    iobuf b;
    {
        model::topic_namespace tn{model::ns{"abc"}, model::topic{"def"}};
        b = serde::to_iobuf(std::move(tn));
    }

    auto tn = serde::from_iobuf<model::topic_namespace>(std::move(b));
    BOOST_CHECK_EQUAL(tn.ns, "abc");
    BOOST_CHECK_EQUAL(tn.tp, "def");
}

SEASTAR_THREAD_TEST_CASE(serde_bytes_test) {
    iobuf b;
    bytes bt = random_generators::get_bytes(128);

    b = serde::to_iobuf(bt);

    auto result = serde::from_iobuf<bytes>(std::move(b));
    BOOST_CHECK_EQUAL(bt, result);
}

SEASTAR_THREAD_TEST_CASE(serde_tristate_test) {
    tristate<ss::sstring> disabled{};
    tristate<ss::sstring> empty(std::nullopt);
    tristate<ss::sstring> set("test-tristate-value");

    iobuf disabled_buf = serde::to_iobuf(disabled);
    iobuf empty_buf = serde::to_iobuf(empty);
    iobuf set_buf = serde::to_iobuf(set);

    BOOST_CHECK_EQUAL(
      disabled,
      // NOLINTNEXTLINE(bugprone-use-after-move)
      serde::from_iobuf<tristate<ss::sstring>>(std::move(disabled_buf)));
    BOOST_CHECK_EQUAL(
      empty,
      // NOLINTNEXTLINE(bugprone-use-after-move)
      serde::from_iobuf<tristate<ss::sstring>>(std::move(empty_buf)));
    BOOST_CHECK_EQUAL(
      set,
      // NOLINTNEXTLINE(bugprone-use-after-move)
      serde::from_iobuf<tristate<ss::sstring>>(std::move(set_buf)));
}

SEASTAR_THREAD_TEST_CASE(seastar_inet_address_test) {
    ss::net::inet_address ipv4("192.168.1.0");
    ss::net::inet_address ipv6("2001:0db8:85a3:0000:0000:8a2e:0370:7334");
    iobuf ipv4_buf = serde::to_iobuf(ipv4);
    iobuf ipv6_buf = serde::to_iobuf(ipv6);
    BOOST_CHECK_EQUAL(
      ipv4,
      // NOLINTNEXTLINE(bugprone-use-after-move)
      serde::from_iobuf<ss::net::inet_address>(std::move(ipv4_buf)));
    BOOST_CHECK_EQUAL(
      ipv6,
      // NOLINTNEXTLINE(bugprone-use-after-move)
      serde::from_iobuf<ss::net::inet_address>(std::move(ipv6_buf)));
}

template<template<class...> class T>
void map_test() {
    T<ss::sstring, int32_t> a = {
      {"asdf", 33},
      {"fooo", 44},
    };

    T<int32_t, int32_t> b = {
      {123, 32},
      {456, 66},
      {959, 11},
    };

    iobuf a_buf = serde::to_iobuf(a);
    iobuf b_buf = serde::to_iobuf(b);

    auto a_from = serde::from_iobuf<T<ss::sstring, int32_t>>(std::move(a_buf));

    auto b_from = serde::from_iobuf<T<int32_t, int32_t>>(std::move(b_buf));

    BOOST_REQUIRE(a == a_from);
    BOOST_REQUIRE(b == b_from);
}

template<template<class...> class T>
void set_test() {
    T<ss::sstring> a = {
      "asdf",
      "fooo",
    };

    T<int32_t> b = {
      123,
      456,
      959,
    };

    iobuf a_buf = serde::to_iobuf(a);
    iobuf b_buf = serde::to_iobuf(b);

    auto a_from = serde::from_iobuf<T<ss::sstring>>(std::move(a_buf));

    auto b_from = serde::from_iobuf<T<int32_t>>(std::move(b_buf));

    BOOST_REQUIRE(a == a_from);
    BOOST_REQUIRE(b == b_from);
}

SEASTAR_THREAD_TEST_CASE(std_unordered_map) { map_test<std::unordered_map>(); }

SEASTAR_THREAD_TEST_CASE(absl_node_hash_map) {
    map_test<absl::node_hash_map>();
}

SEASTAR_THREAD_TEST_CASE(absl_flat_hash_map) {
    map_test<absl::flat_hash_map>();
}

SEASTAR_THREAD_TEST_CASE(absl_node_hash_set) {
    set_test<absl::node_hash_set>();
}

SEASTAR_THREAD_TEST_CASE(absl_btree_set) { set_test<absl::btree_set>(); }

struct foo_bar {};

SEASTAR_THREAD_TEST_CASE(type_str) {
    BOOST_CHECK_EQUAL(serde::type_str<int>(), "int");
    BOOST_CHECK_EQUAL(serde::type_str<void>(), "void");
    // the type part of a parameter in __PRETTY_FUNCTION__ may vary when it
    // comes to a specialization of a template in standard library, but
    // std::basic_string<char> should always have "string" in its name
    BOOST_CHECK_NE(
      serde::type_str<std::string>().find("string"), std::string_view::npos);
    BOOST_CHECK_EQUAL(serde::type_str<foo_bar>(), "foo_bar");
}
// Utility to serialize and deserialize back an input.
template<class T>
T serde_input(T input) {
    return serde::from_iobuf<T>(serde::to_iobuf(std::move(input)));
}

SEASTAR_THREAD_TEST_CASE(duration_type_test) {
    using namespace std::chrono;
    constexpr hours h(1);
    constexpr milliseconds ms{3};
    constexpr milliseconds ms_neg{-1};
    constexpr duration<int, std::kilo> ks(3);
    constexpr duration<int16_t> int16t_d(10);
    constexpr duration<int16_t> int32t_d(10);
    constexpr duration<int64_t, std::pico> int64t_pico(100000);
    constexpr auto max_ns = nanoseconds::max();
    constexpr auto min_ns = nanoseconds::min();

    BOOST_REQUIRE(h == serde_input(h));
    BOOST_REQUIRE(ms == serde_input(ms));
    BOOST_REQUIRE(ms_neg == serde_input(ms_neg));
    BOOST_REQUIRE(ks == serde_input(ks));
    BOOST_REQUIRE(max_ns == serde_input(max_ns));
    BOOST_REQUIRE(int16t_d == serde_input(int16t_d));
    BOOST_REQUIRE(int32t_d == serde_input(int16t_d));
    BOOST_REQUIRE(int64t_pico == serde_input(int64t_pico));
    // Overflows, clamped to ns:max().
    constexpr auto max_ms = milliseconds::max();
    constexpr auto max_ns_in_ms = std::chrono::duration_cast<milliseconds>(
      max_ns);
    constexpr auto max_hrs = hours::max();
    constexpr auto max_ns_in_hrs = std::chrono::duration_cast<hours>(max_ns);
    BOOST_REQUIRE(max_ns_in_ms == serde_input(max_ms));
    BOOST_REQUIRE(max_ns_in_hrs == serde_input(max_hrs));
    // Underflows, clamped to ns::min()
    constexpr auto min_ms = milliseconds::min();
    constexpr auto min_ns_in_ms = std::chrono::duration_cast<milliseconds>(
      min_ns);
    BOOST_REQUIRE(min_ns_in_ms == serde_input(min_ms));

    constexpr auto min_hrs = hours::min();
    constexpr auto min_ns_in_hrs = std::chrono::duration_cast<hours>(min_ns);
    BOOST_REQUIRE(min_ns_in_hrs == serde_input(min_hrs));
}

struct no_default_ctor
  : public serde::
      envelope<no_default_ctor, serde::version<0>, serde::compat_version<0>> {
    no_default_ctor() = delete;

    no_default_ctor(int x)
      : x(x) {}

    auto serde_fields() { return std::tie(x); }

    bool operator==(const no_default_ctor&) const = default;

    static no_default_ctor
    serde_direct_read(iobuf_parser& in, size_t const bytes_left_limit) {
        using serde::read_nested;
        int x;
        read_nested(in, x, bytes_left_limit);
        return no_default_ctor(x);
    }

    int x;
};

static_assert(serde::has_serde_direct_read<no_default_ctor>);

SEASTAR_THREAD_TEST_CASE(no_default_ctor_test) {
    BOOST_CHECK_EQUAL(serde_input(no_default_ctor(37)).x, 37);
}

SEASTAR_THREAD_TEST_CASE(no_default_ctor_vector_test) {
    BOOST_CHECK_EQUAL(
      serde_input(std::vector<no_default_ctor>({no_default_ctor(37)})).at(0).x,
      37);
}

struct async_checksummed
  : public serde::checksum_envelope<
      async_checksummed,
      serde::version<0>,
      serde::compat_version<0>> {
    test_msg0 msg;
    ss::sstring str;

    bool operator==(const async_checksummed&) const = default;

    ss::future<> serde_async_read(iobuf_parser& in, serde::header const h) {
        msg = co_await serde::read_async_nested<decltype(msg)>(
          in, h._bytes_left_limit);
        str = co_await serde::read_async_nested<decltype(str)>(
          in, h._bytes_left_limit);
    }

    ss::future<> serde_async_write(iobuf& out) const {
        co_await serde::write_async(out, msg);
        co_await serde::write_async(out, str);
    }
};

static_assert(serde::is_envelope<async_checksummed>);
static_assert(serde::is_checksum_envelope<async_checksummed>);
static_assert(serde::has_serde_async_read<async_checksummed>);
static_assert(serde::has_serde_async_write<async_checksummed>);

SEASTAR_THREAD_TEST_CASE(test_async_checksummed) {
    auto obj = async_checksummed{
        .msg = test_msg0{
            ._i = 12,
            ._j = 34,
        },
        .str = "hello",
    };

    {
        auto b = iobuf{};
        serde::write_async(b, obj).get();
        auto parser = iobuf_parser{std::move(b)};
        BOOST_CHECK(obj == serde::read_async<async_checksummed>(parser).get());
    }

    {
        auto b = iobuf{};
        serde::write_async(b, obj).get();

        // corrupt the buffer
        BOOST_REQUIRE(b.size_bytes() > 0);
        auto& last_frag = *b.rbegin();
        BOOST_REQUIRE(last_frag.size() > 0);
        last_frag.get_write()[last_frag.size() - 1] += 1;

        auto parser = iobuf_parser{std::move(b)};
        BOOST_CHECK_EXCEPTION(
          serde::read_async<async_checksummed>(parser).get(),
          serde::serde_exception,
          [](const serde::serde_exception& ex) {
              return std::string_view{ex.what()}.find("bad checksum")
                     != std::string_view::npos;
          });
    }
}
