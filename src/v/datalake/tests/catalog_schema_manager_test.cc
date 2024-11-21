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
#include "cloud_io/tests/scoped_remote.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "datalake/catalog_schema_manager.h"
#include "iceberg/field_collecting_visitor.h"
#include "iceberg/filesystem_catalog.h"
#include "iceberg/table_identifier.h"
#include "iceberg/tests/test_schemas.h"

#include <gtest/gtest.h>

using namespace datalake;
using namespace iceberg;

namespace {
const auto table_ident = table_identifier{.ns = {"redpanda"}, .table = "foo"};
} // namespace

class CatalogSchemaManagerTest
  : public s3_imposter_fixture
  , public ::testing::Test {
public:
    static constexpr std::string_view base_location{"test"};
    CatalogSchemaManagerTest()
      : sr(cloud_io::scoped_remote::create(10, conf))
      , catalog(remote(), bucket_name, ss::sstring(base_location))
      , schema_mgr(catalog) {
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
        EXPECT_NE(table.current_schema_id, schema::unassigned_id);
        auto schema_it = std::ranges::find(
          table.schemas, table.current_schema_id, &schema::schema_id);
        if (schema_it == table.schemas.end()) {
            co_return std::nullopt;
        }
        co_return std::move(*schema_it);
    }

    std::unique_ptr<cloud_io::scoped_remote> sr;
    filesystem_catalog catalog;
    catalog_schema_manager schema_mgr;
};

TEST_F(CatalogSchemaManagerTest, TestCreateTable) {
    auto type = std::get<struct_type>(test_nested_schema_type());
    reset_field_ids(type);

    // Create the table
    auto create_res
      = schema_mgr.ensure_table_schema(model::topic{"foo"}, type).get();
    ASSERT_FALSE(create_res.has_error());

    // Fill the field IDs in `type`.
    auto fill_res
      = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    ASSERT_FALSE(fill_res.has_error());

    auto table_ident = table_identifier{.ns = {"redpanda"}, .table = "foo"};
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
    auto type = std::get<struct_type>(test_nested_schema_type());
    reset_field_ids(type);
    auto res = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    ASSERT_FALSE(res.has_error());
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

    auto res = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    ASSERT_FALSE(res.has_error());

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

    auto res = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    ASSERT_FALSE(res.has_error());

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
            nested.fields.emplace_back(nested_field::create(
              0,
              fmt::format("inner-{}", j),
              field_required::no,
              boolean_type{}));
        }
        type.fields.emplace_back(nested_field::create(
          0,
          fmt::format("nested-{}", i),
          field_required::no,
          std::move(nested)));
    }
    // Alter the table schema
    auto ensure_res
      = schema_mgr.ensure_table_schema(model::topic{"foo"}, type).get();
    ASSERT_FALSE(ensure_res.has_error());

    // Fill the ids in `type`
    auto fill_res
      = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    ASSERT_FALSE(fill_res.has_error());

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
          .fields.emplace_back(nested_field::create(
            0,
            fmt::format("extra-nested-{}", i),
            field_required::no,
            int_type{}));
    }
    // Alter the table schema
    auto ensure_res
      = schema_mgr.ensure_table_schema(model::topic{"foo"}, type).get();
    ASSERT_FALSE(ensure_res.has_error());

    // Fill the ids
    auto fill_res
      = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    ASSERT_FALSE(fill_res.has_error());

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
      nested_field::create(0, "required", field_required::yes, int_type{}));
    type.fields.emplace_back(
      nested_field::create(0, "optional", field_required::no, int_type{}));
    create_table(type);

    // Make the destinations both optional.
    type.fields[0]->required = field_required::no;
    auto res = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    ASSERT_TRUE(res.has_error());
    EXPECT_EQ(res.error(), schema_manager::errc::not_supported);

    // Make the destinations both required.
    type.fields[0]->required = field_required::yes;
    type.fields[1]->required = field_required::yes;
    res = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    ASSERT_TRUE(res.has_error());
    EXPECT_EQ(res.error(), schema_manager::errc::not_supported);
}

TEST_F(CatalogSchemaManagerTest, TestTypeMismatch) {
    create_nested_table();

    auto type = std::get<struct_type>(test_nested_schema_type());
    reset_field_ids(type);
    std::swap(type.fields.front(), type.fields.back());

    auto res = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    ASSERT_TRUE(res.has_error());
    EXPECT_EQ(res.error(), schema_manager::errc::not_supported);
}
