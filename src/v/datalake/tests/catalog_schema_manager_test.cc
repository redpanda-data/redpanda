/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/remote.h"
#include "cloud_io/tests/s3_imposter.h"
#include "cloud_io/tests/scoped_remote.h"
#include "datalake/catalog_schema_manager.h"
#include "features/feature_table.h"
#include "iceberg/datatypes.h"
#include "iceberg/field_collecting_visitor.h"
#include "iceberg/filesystem_catalog.h"
#include "iceberg/table_identifier.h"
#include "iceberg/tests/test_schemas.h"

#include <gtest/gtest.h>

using namespace datalake;
using namespace iceberg;

namespace {
const auto table_ident = table_identifier{.ns = {"redpanda"}, .table = "foo"};
const auto empty_pspec = iceberg::unresolved_partition_spec{};
} // namespace

class CatalogSchemaManagerTest
  : public s3_imposter_fixture
  , public ::testing::Test {
public:
    static constexpr std::string_view base_location{"test"};
    CatalogSchemaManagerTest()
      : sr(cloud_io::scoped_remote::create(10, conf))
      , catalog(remote(), bucket_name, ss::sstring(base_location))
      , schema_mgr(catalog, &features) {
        features.testing_activate_all();
        set_expectations_and_listen({});
    }
    cloud_io::remote& remote() { return sr->remote.local(); }

    void reset_field_ids(struct_type& type) {
        chunked_vector<nested_field*> to_visit;
        for (auto& f : std::ranges::reverse_view(type.fields)) {
            to_visit.emplace_back(f.get());
        }
        while (!to_visit.empty()) {
            auto* f = to_visit.back();
            f->id = nested_field::id_t{0};
            f->meta = std::nullopt;
            to_visit.pop_back();
            std::visit(reverse_field_collecting_visitor{to_visit}, f->type);
        }
    }

    void create_nested_table() {
        create_table(std::get<struct_type>(test_nested_schema_type()));
    }

    void create_table(const struct_type& type) {
        schema s{
          .schema_struct = type.copy(),
          .schema_id = schema::id_t{1},
          .identifier_field_ids{},
        };
        auto create_res
          = catalog.create_table(table_ident, s, partition_spec{}).get();
        ASSERT_FALSE(create_res.has_error());
    }

    ss::future<std::optional<schema>>
    load_table_schema(const table_identifier& table_ident) {
        auto load_res = catalog.load_table(table_ident).get();
        if (!load_res.has_value()) {
            co_return std::nullopt;
        }
        auto& table = load_res.value();
        auto schema_it = std::ranges::find(
          table.schemas, table.current_schema_id, &schema::schema_id);
        if (schema_it == table.schemas.end()) {
            throw std::runtime_error(
              fmt::format(
                "Schema {} not found in table {}",
                table.current_schema_id,
                table_ident));
        }
        co_return std::move(*schema_it);
    }

    features::feature_table features;
    std::unique_ptr<cloud_io::scoped_remote> sr;
    filesystem_catalog catalog;
    catalog_schema_manager schema_mgr;
};

TEST_F(CatalogSchemaManagerTest, TestCreateTable) {
    auto type = std::get<struct_type>(test_nested_schema_type());
    reset_field_ids(type);

    // Create the table
    auto create_res
      = schema_mgr.ensure_table_schema(table_ident, type, empty_pspec).get();
    ASSERT_FALSE(create_res.has_error());

    // Fill the field IDs in `type`.
    auto load_res = schema_mgr.get_table_info(table_ident).get();
    ASSERT_FALSE(load_res.has_error());
    ASSERT_TRUE(load_res.value().fill_registered_ids(type));

    auto schema = load_table_schema(table_ident).get();
    ASSERT_TRUE(schema.has_value());
    EXPECT_EQ(type, schema->schema_struct);
}

TEST_F(CatalogSchemaManagerTest, TestFillFromExistingTable) {
    create_nested_table();
    auto schema = load_table_schema(table_ident).get();
    ASSERT_TRUE(schema.has_value());

    // Even if the table already exists, we should be able to fill fields IDs
    // without trouble.
    auto load_res = schema_mgr.get_table_info(table_ident).get();
    ASSERT_FALSE(load_res.has_error());

    auto type = std::get<struct_type>(test_nested_schema_type());
    reset_field_ids(type);
    ASSERT_TRUE(load_res.value().fill_registered_ids(type));
    EXPECT_EQ(type, schema.value().schema_struct);
}

TEST_F(CatalogSchemaManagerTest, TestFillSubset) {
    create_nested_table();
    auto schema = load_table_schema(table_ident).get();
    ASSERT_TRUE(schema.has_value());

    // Remove a field from the set that we want to fill.
    auto type = std::get<struct_type>(test_nested_schema_type());
    reset_field_ids(type);
    type.fields.pop_back();

    auto load_res = schema_mgr.get_table_info(table_ident).get();
    ASSERT_FALSE(load_res.has_error());
    ASSERT_TRUE(load_res.value().fill_registered_ids(type));

    schema.value().schema_struct.fields.pop_back();
    EXPECT_EQ(type, schema.value().schema_struct);
}

TEST_F(CatalogSchemaManagerTest, TestFillNestedSubset) {
    create_nested_table();
    auto schema = load_table_schema(table_ident).get();
    ASSERT_TRUE(schema.has_value());

    // Remove a subfield from the set that we want to fill.
    auto type = std::get<struct_type>(test_nested_schema_type());
    reset_field_ids(type);
    std::get<struct_type>(type.fields.back()->type).fields.pop_back();

    auto load_res = schema_mgr.get_table_info(table_ident).get();
    ASSERT_FALSE(load_res.has_error());
    ASSERT_TRUE(load_res.value().fill_registered_ids(type));

    std::get<struct_type>(schema.value().schema_struct.fields.back()->type)
      .fields.pop_back();
    EXPECT_EQ(type, schema.value().schema_struct);
}

TEST_F(CatalogSchemaManagerTest, TestFillSuperset) {
    create_nested_table();

    // Add a couple nested fields to the desired type.
    auto type = std::get<struct_type>(test_nested_schema_type());
    reset_field_ids(type);
    for (size_t i = 0; i < 2; ++i) {
        struct_type nested;
        for (size_t j = 0; j < 10; ++j) {
            nested.fields.emplace_back(
              nested_field::create(
                0,
                fmt::format("inner-{}", j),
                field_required::no,
                boolean_type{}));
        }
        type.fields.emplace_back(
          nested_field::create(
            0,
            fmt::format("nested-{}", i),
            field_required::no,
            std::move(nested)));
    }
    // Alter the table schema
    auto ensure_res
      = schema_mgr.ensure_table_schema(table_ident, type, empty_pspec).get();
    ASSERT_FALSE(ensure_res.has_error());

    // Fill the ids in `type`
    auto load_res = schema_mgr.get_table_info(table_ident).get();
    ASSERT_FALSE(load_res.has_error());
    ASSERT_TRUE(load_res.value().fill_registered_ids(type));

    // Check the resulting schema.
    schema s{
      .schema_struct = std::move(type),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {},
    };
    EXPECT_EQ(39, s.highest_field_id());

    // Sanity check: the field IDs should match what is in the catalog.
    auto loaded_table = load_table_schema(table_ident).get();
    ASSERT_TRUE(loaded_table.has_value());
    ASSERT_EQ(loaded_table.value().schema_struct, s.schema_struct);
}

TEST_F(CatalogSchemaManagerTest, TestFillSupersetSubtype) {
    create_nested_table();

    // Add a couple fields to a subfield of the desired type.
    auto type = std::get<struct_type>(test_nested_schema_type());
    reset_field_ids(type);
    for (size_t i = 0; i < 2; ++i) {
        std::get<struct_type>(type.fields.back()->type)
          .fields.emplace_back(
            nested_field::create(
              0,
              fmt::format("extra-nested-{}", i),
              field_required::no,
              int_type{}));
    }
    // Alter the table schema
    auto ensure_res
      = schema_mgr.ensure_table_schema(table_ident, type, empty_pspec).get();
    ASSERT_FALSE(ensure_res.has_error());

    // Fill the ids
    auto load_res = schema_mgr.get_table_info(table_ident).get();
    ASSERT_FALSE(load_res.has_error());
    ASSERT_TRUE(load_res.value().fill_registered_ids(type));

    // Check the resulting schema.
    schema s{
      .schema_struct = std::move(type),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {},
    };
    EXPECT_EQ(19, s.highest_field_id());

    // Sanity check: the field IDs should match what is in the catalog.
    auto loaded_table = load_table_schema(table_ident).get();
    ASSERT_TRUE(loaded_table.has_value());
    ASSERT_EQ(loaded_table.value().schema_struct, s.schema_struct);
}

TEST_F(CatalogSchemaManagerTest, TestOptionalMismatch) {
    struct_type type;
    type.fields.emplace_back(
      nested_field::create(1, "required", field_required::yes, int_type{}));
    type.fields.emplace_back(
      nested_field::create(2, "optional", field_required::no, int_type{}));
    create_table(type);

    // Make the destinations both optional. This is fine.
    type.fields[0]->required = field_required::no;
    auto res
      = schema_mgr.ensure_table_schema(table_ident, type, empty_pspec).get();
    ASSERT_FALSE(res.has_error());

    // Make the destinations both required. This is a no-op under schema merging
    // rules.
    {
        auto all_req = type.copy();

        all_req.fields[0]->required = field_required::yes;
        all_req.fields[1]->required = field_required::yes;
        res = schema_mgr.ensure_table_schema(table_ident, all_req, empty_pspec)
                .get();
        ASSERT_FALSE(res.has_error());
    }

    auto info = schema_mgr.get_table_info(table_ident).get();
    ASSERT_TRUE(info.has_value());
    ASSERT_EQ(info.value().schema.schema_struct, type);
}

TEST_F(CatalogSchemaManagerTest, TestTypeMismatch) {
    create_nested_table();

    auto type = std::get<struct_type>(test_nested_schema_type());
    reset_field_ids(type);
    std::swap(type.fields.front()->type, type.fields.back()->type);

    auto res
      = schema_mgr.ensure_table_schema(table_ident, type, empty_pspec).get();
    ASSERT_TRUE(res.has_error());
    EXPECT_EQ(res.error(), schema_manager::errc::not_supported);
}

TEST_F(CatalogSchemaManagerTest, TestReorderFields) {
    create_nested_table();

    auto type = std::get<struct_type>(test_nested_schema_type());
    reset_field_ids(type);
    std::swap(type.fields.front(), type.fields.back());

    auto res
      = schema_mgr.ensure_table_schema(table_ident, type, empty_pspec).get();
    ASSERT_FALSE(res.has_error());
}

TEST_F(CatalogSchemaManagerTest, AcceptsValidTypePromotion) {
    auto original_type = std::get<struct_type>(test_nested_schema_type());
    original_type.fields.emplace_back(
      nested_field::create(18, "some_field", field_required::yes, int_type{}));
    create_table(original_type);

    auto type = original_type.copy();
    // int->long is a valid primitive type promotion
    type.fields.back()->type = long_type{};
    reset_field_ids(type);

    // so schema_mgr should accept the new schema
    auto ensure_res
      = schema_mgr.ensure_table_schema(table_ident, type, empty_pspec).get();
    ASSERT_FALSE(ensure_res.has_error()) << ensure_res.error();

    auto load_res = schema_mgr.get_table_info(table_ident).get();
    ASSERT_FALSE(load_res.has_error());
    ASSERT_TRUE(load_res.value().fill_registered_ids(type));

    auto loaded_table = load_table_schema(table_ident).get();
    ASSERT_TRUE(loaded_table.has_value());
    ASSERT_EQ(loaded_table.value().schema_struct, type);

    // test that ensuring schema with original types is a no-op because the
    // current schema can host pre-promotion data types without a problem
    {
        auto ensure_orig_res_later
          = schema_mgr
              .ensure_table_schema(table_ident, original_type, empty_pspec)
              .get();
        ASSERT_FALSE(ensure_orig_res_later.has_error())
          << ensure_orig_res_later.error();

        auto latest_schema = load_table_schema(table_ident).get();
        ASSERT_TRUE(latest_schema.has_value());
        ASSERT_EQ(
          latest_schema.value().schema_struct,
          loaded_table.value().schema_struct);
    }
}

TEST_F(CatalogSchemaManagerTest, RejectsInvalidTypePromotion) {
    auto original_type = std::get<struct_type>(test_nested_schema_type());
    original_type.fields.emplace_back(
      nested_field::create(18, "some_field", field_required::yes, int_type{}));
    create_table(original_type);

    auto type = original_type.copy();
    // int->string is not a valid primitive type promotion
    type.fields.back()->type = string_type{};
    reset_field_ids(type);

    // so schema_mgr should reject the new schema
    auto ensure_res
      = schema_mgr.ensure_table_schema(table_ident, type, empty_pspec).get();
    ASSERT_TRUE(ensure_res.has_error());
    EXPECT_EQ(ensure_res.error(), schema_manager::errc::not_supported)
      << ensure_res.error();

    auto load_res = schema_mgr.get_table_info(table_ident).get();
    ASSERT_FALSE(load_res.has_error());

    ASSERT_FALSE(load_res.value().fill_registered_ids(type));

    // check that the table still holds the original schema
    load_res = schema_mgr.get_table_info(table_ident).get();
    reset_field_ids(original_type);
    ASSERT_TRUE(load_res.value().fill_registered_ids(original_type));

    auto loaded_table = load_table_schema(table_ident).get();
    ASSERT_TRUE(loaded_table.has_value());
    ASSERT_EQ(loaded_table.value().schema_struct, original_type);
}

TEST_F(CatalogSchemaManagerTest, CustomPartitionSpec) {
    auto schema_type = std::get<struct_type>(test_nested_schema_type());

    auto pspec_fields
      = chunked_vector<unresolved_partition_spec::field>::single(
        unresolved_partition_spec::field{
          .source_name = {"bar"},
          .transform = identity_transform{},
          .name = "field1"});

    auto ensure_res = schema_mgr
                        .ensure_table_schema(
                          table_ident,
                          schema_type,
                          unresolved_partition_spec{
                            .fields = std::move(pspec_fields)})
                        .get();
    ASSERT_FALSE(ensure_res.has_error());

    auto load_res = catalog.load_table(table_ident).get();
    ASSERT_TRUE(load_res.has_value());

    auto pspec = load_res.value().get_partition_spec(
      load_res.value().default_spec_id);
    ASSERT_TRUE(pspec);

    chunked_vector<partition_field> expected_fields{partition_field{
      .source_id = nested_field::id_t{2},
      .field_id = partition_field::id_t{1000},
      .name = "field1",
      .transform = identity_transform{},
    }};
    auto expected = partition_spec{
      .spec_id = partition_spec::id_t{0},
      .fields = std::move(expected_fields),
    };
    ASSERT_EQ(*pspec, expected);
}

TEST_F(CatalogSchemaManagerTest, GetTableInfo) {
    struct_type first{};
    first.fields.emplace_back(
      nested_field::create(1, "foo", field_required::no, int_type{}));

    auto second = first.copy();
    second.fields.emplace_back(
      nested_field::create(2, "bar", field_required::no, string_type{}));

    auto third = first.copy();
    third.fields.emplace_back(
      nested_field::create(2, "baz", field_required::no, float_type{}));

    // set schema to 'first'
    auto ensure_res
      = schema_mgr.ensure_table_schema(table_ident, first, empty_pspec).get();
    ASSERT_FALSE(ensure_res.has_error());

    // get_table_info returns current schema by default
    auto info_res = schema_mgr.get_table_info(table_ident).get();
    ASSERT_FALSE(info_res.has_error());
    ASSERT_EQ(info_res.value().schema.schema_struct, first);

    // set schema to 'second'
    ensure_res
      = schema_mgr.ensure_table_schema(table_ident, second, empty_pspec).get();
    ASSERT_FALSE(ensure_res.has_error());

    // 'second' is current, so get_table_info returns this by default
    info_res = schema_mgr.get_table_info(table_ident).get();
    ASSERT_FALSE(info_res.has_error());
    ASSERT_EQ(info_res.value().schema.schema_struct, second);

    // set schema to 'third'
    ensure_res
      = schema_mgr.ensure_table_schema(table_ident, third, empty_pspec).get();
    ASSERT_FALSE(ensure_res.has_error());

    // 'third' is current, so get_table_info returns this by default
    info_res = schema_mgr.get_table_info(table_ident).get();
    ASSERT_FALSE(info_res.has_error());
    ASSERT_EQ(
      info_res.value().schema.schema_struct,
      [] {
          struct_type s{};
          s.fields.emplace_back(
            nested_field::create(1, "foo", field_required::no, int_type{}));
          s.fields.emplace_back(
            nested_field::create(2, "bar", field_required::no, string_type{}));
          s.fields.emplace_back(
            nested_field::create(3, "baz", field_required::no, float_type{}));
          return s;
      }())
      << "Expect merged schema";
}
