/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/compatibility.h"
#include "iceberg/compatibility_utils.h"
#include "iceberg/datatypes.h"
#include "iceberg/field_name_comparison.h"
#include "iceberg/tests/test_schemas.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

using namespace iceberg;

TEST(CompatUtilsTests, SchemasEquivalentNorm) {
    auto make_schema = [](const char* field_name) {
        struct_type s;
        s.fields.push_back(
          nested_field::create(0, field_name, field_required::no, int_type{}));
        return s;
    };

    auto upper = make_schema("UserId");
    auto lower = make_schema("userid");

    // none: case-sensitive
    EXPECT_TRUE(
      schemas_equivalent(upper, upper, field_name_comparison::verbatim));
    EXPECT_FALSE(
      schemas_equivalent(upper, lower, field_name_comparison::verbatim));

    // unicode_lower: case-insensitive
    EXPECT_TRUE(
      schemas_equivalent(upper, upper, field_name_comparison::lower_case));
    EXPECT_TRUE(
      schemas_equivalent(upper, lower, field_name_comparison::lower_case));
}

TEST(CompatUtilsTests, CanForEachField) {
    auto s = std::get<struct_type>(test_nested_schema_type());

    {
        int n = 0;
        auto res = for_each_field(s, [&n](nested_field* f) {
            f->set_evolution_metadata(nested_field::is_new{});
            ++n;
        });
        ASSERT_FALSE(res.has_error());
        EXPECT_EQ(n, 17);
    }

    {
        const auto& s_ref = s;
        auto res = for_each_field(s_ref, [](const nested_field* f) {
            ASSERT_TRUE(f->has_evolution_metadata());
            EXPECT_TRUE(std::holds_alternative<nested_field::is_new>(f->meta));
        });
        ASSERT_FALSE(res.has_error());
    }

    {
        int n = 0;
        auto res = for_each_field(
          s,
          [&n](const nested_field*) { ++n; },
          [](const nested_field* f) -> bool {
              return !f->has_evolution_metadata();
          });
        ASSERT_FALSE(res.has_error());
        EXPECT_EQ(n, 0);
    }

    {
        int n = 0;
        const auto& s_ref = s;
        auto res = for_each_field(
          s_ref,
          [&n](const nested_field*)
            -> checked<std::nullopt_t, schema_evolution_errc> {
              ++n;
              return schema_evolution_errc::invalid_state;
          });
        ASSERT_TRUE(res.has_error());
        EXPECT_EQ(n, 1);
    }
}

TEST(CompatUtilsTests, ForEachFieldHandlesNullFields) {
    struct_type outer{};

    struct_type inner{};
    inner.fields.emplace_back(
      nested_field::create(0, "f1", field_required::no, int_type{}));
    inner.fields.emplace_back(nullptr);
    inner.fields.emplace_back(
      nested_field::create(0, "f1", field_required::no, int_type{}));

    outer.fields.emplace_back(
      nested_field::create(0, "inner", field_required::no, std::move(inner)));

    {
        auto f = nested_field::create(
          0, "outer", field_required::no, outer.copy());

        auto res = for_each_field(*f, [](nested_field* f) {
            // we shouldn't reach here when f is null
            ASSERT_NE(f, nullptr);
        });

        ASSERT_TRUE(res.has_error());
        EXPECT_EQ(res.error(), schema_evolution_errc::null_nested_field);
    }

    {
        outer.fields.emplace_back(nullptr);
        auto res = for_each_field(
          outer, [](nested_field* f) { ASSERT_NE(f, nullptr); });

        ASSERT_TRUE(res.has_error());
        EXPECT_EQ(res.error(), schema_evolution_errc::null_nested_field);
    }
}
