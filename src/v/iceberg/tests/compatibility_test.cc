// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/compatibility.h"
#include "iceberg/datatypes.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <iostream>
#include <vector>

using namespace iceberg;

namespace {

using compat = ss::bool_class<struct compat_tag>;

struct field_test_case {
    field_test_case(
      field_type source, field_type dest, type_check_result expected)
      : source(std::move(source))
      , dest(std::move(dest))
      , expected(expected) {}

    field_test_case(const field_test_case& other)
      : source(make_copy(other.source))
      , dest(make_copy(other.dest))
      , expected(
          other.expected.has_error()
            ? type_check_result{other.expected.error()}
            : type_check_result{other.expected.value()}) {}

    field_test_case(field_test_case&&) = default;
    field_test_case& operator=(const field_test_case& other) = delete;
    field_test_case& operator=(field_test_case&&) = delete;
    ~field_test_case() = default;

    field_type source;
    field_type dest;
    type_check_result expected{compat_errc::mismatch};
};

std::ostream& operator<<(std::ostream& os, const field_test_case& ftc) {
    fmt::print(
      os,
      "{}->{} [expected: {}]",
      ftc.source,
      ftc.dest,
      ftc.expected.has_error()
        ? std::string{"ERROR"}
        : fmt::format("promoted={}", ftc.expected.value()));
    return os;
}
} // namespace

std::vector<field_test_case> generate_test_cases() {
    std::vector<field_test_case> test_data{};

    test_data.emplace_back(int_type{}, long_type{}, type_promoted::yes);
    test_data.emplace_back(int_type{}, boolean_type{}, compat_errc::mismatch);

    test_data.emplace_back(date_type{}, timestamp_type{}, type_promoted::yes);
    test_data.emplace_back(date_type{}, long_type{}, compat_errc::mismatch);

    test_data.emplace_back(float_type{}, double_type{}, type_promoted::yes);
    test_data.emplace_back(
      float_type{}, fixed_type{.length = 64}, compat_errc::mismatch);

    test_data.emplace_back(
      decimal_type{.precision = 10, .scale = 2},
      decimal_type{.precision = 20, .scale = 2},
      type_promoted::yes);
    test_data.emplace_back(
      decimal_type{.precision = 10, .scale = 2},
      decimal_type{.precision = 10, .scale = 2},
      type_promoted::no);
    test_data.emplace_back(
      decimal_type{.precision = 20, .scale = 2},
      decimal_type{.precision = 10, .scale = 2},
      compat_errc::mismatch);

    test_data.emplace_back(
      fixed_type{.length = 32}, fixed_type{.length = 32}, type_promoted::no);
    test_data.emplace_back(
      fixed_type{.length = 32},
      fixed_type{.length = 64},
      compat_errc::mismatch);
    test_data.emplace_back(
      fixed_type{.length = 64},
      fixed_type{.length = 32},
      compat_errc::mismatch);

    struct_type s1{};
    struct_type s2{};
    s2.fields.emplace_back(
      nested_field::create(0, "foo", field_required::yes, int_type{}));
    field_type l1 = list_type::create(0, field_required::yes, int_type{});
    field_type l2 = list_type::create(0, field_required::no, string_type{});
    field_type m1 = map_type::create(
      0, int_type{}, 0, field_required::yes, date_type{});
    field_type m2 = map_type::create(
      0, string_type{}, 0, field_required::no, timestamptz_type{});

    // NOTE: basic type check doesn't descend into non-primitive types
    // Checking stops at type ID - i.e. compat(struct, struct) == true,
    // compat(struct, list) == false.
    test_data.emplace_back(s1.copy(), s1.copy(), type_promoted::no);
    test_data.emplace_back(s1.copy(), s2.copy(), type_promoted::no);
    test_data.emplace_back(make_copy(l1), make_copy(l1), type_promoted::no);
    test_data.emplace_back(make_copy(l1), make_copy(l2), type_promoted::no);
    test_data.emplace_back(make_copy(m1), make_copy(m1), type_promoted::no);
    test_data.emplace_back(make_copy(m1), make_copy(m2), type_promoted::no);

    std::vector<field_type> non_promotable_types;
    non_promotable_types.emplace_back(boolean_type{});
    non_promotable_types.emplace_back(long_type{});
    non_promotable_types.emplace_back(double_type{});
    non_promotable_types.emplace_back(time_type{});
    non_promotable_types.emplace_back(timestamp_type{});
    non_promotable_types.emplace_back(timestamptz_type{});
    non_promotable_types.emplace_back(string_type{});
    non_promotable_types.emplace_back(uuid_type{});
    non_promotable_types.emplace_back(binary_type{});
    non_promotable_types.emplace_back(s1.copy());
    non_promotable_types.emplace_back(make_copy(l1));
    non_promotable_types.emplace_back(make_copy(m1));

    for (const auto& fta : non_promotable_types) {
        for (const auto& ftb : non_promotable_types) {
            if (fta == ftb) {
                continue;
            }
            test_data.emplace_back(
              make_copy(fta), make_copy(ftb), compat_errc::mismatch);
        }
    }

    return test_data;
}

struct CompatibilityTest
  : ::testing::Test
  , testing::WithParamInterface<field_test_case> {};

INSTANTIATE_TEST_SUITE_P(
  PrimitiveTypeCompatibilityTest,
  CompatibilityTest,
  ::testing::ValuesIn(generate_test_cases()));

TEST_P(CompatibilityTest, CompatibleTypesAreCompatible) {
    const auto& p = GetParam();

    auto res = check_types(p.source, p.dest);
    ASSERT_EQ(res.has_error(), p.expected.has_error());
    if (res.has_error()) {
        ASSERT_EQ(res.error(), p.expected.error());
    } else {
        ASSERT_EQ(res.value(), p.expected.value());
    }
}
