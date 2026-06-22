/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/coordinator/catalog_config.h"
#include "iceberg/field_name_comparison.h"
#include "model/metadata.h"
#include "test_utils/scoped_config.h"

#include <seastar/core/sstring.hh>

#include <gtest/gtest.h>

using namespace datalake::coordinator;
using fnn = iceberg::field_name_comparison;
using sci = model::iceberg_schema_case_insensitive;

namespace {

void set_glue_catalog(scoped_config& cfg) {
    cfg.get("iceberg_catalog_type")
      .set_value(config::datalake_catalog_type::rest);
    cfg.get("iceberg_rest_catalog_authentication_mode")
      .set_value(config::datalake_catalog_auth_mode::aws_sigv4);
    cfg.get("iceberg_rest_catalog_aws_service_name")
      .set_value(ss::sstring{"glue"});
}

} // namespace

// --- using_glue_catalog() ---

TEST(UsingGlueCatalog, TrueWhenAllThreeConditionsMet) {
    scoped_config cfg;
    set_glue_catalog(cfg);
    EXPECT_TRUE(using_glue_catalog());
}

TEST(UsingGlueCatalog, FalseWhenCatalogTypeIsNotRest) {
    scoped_config cfg;
    set_glue_catalog(cfg);
    cfg.get("iceberg_catalog_type")
      .set_value(config::datalake_catalog_type::object_storage);
    EXPECT_FALSE(using_glue_catalog());
}

TEST(UsingGlueCatalog, FalseWhenAuthModeIsNotSigv4) {
    scoped_config cfg;
    set_glue_catalog(cfg);
    cfg.get("iceberg_rest_catalog_authentication_mode")
      .set_value(config::datalake_catalog_auth_mode::none);
    EXPECT_FALSE(using_glue_catalog());
}

TEST(UsingGlueCatalog, FalseWhenServiceNameIsNotGlue) {
    scoped_config cfg;
    set_glue_catalog(cfg);
    cfg.get("iceberg_rest_catalog_aws_service_name")
      .set_value(ss::sstring{"s3"});
    EXPECT_FALSE(using_glue_catalog());
}

// --- resolve_field_name_comparison() ---

TEST(ResolveFieldNameComparison, AutoWithGlueCatalogReturnsLowerCase) {
    scoped_config cfg;
    set_glue_catalog(cfg);
    cfg.get("iceberg_schema_case_insensitive").set_value(sci::auto_);
    EXPECT_EQ(resolve_field_name_comparison(), fnn::lower_case);
}

TEST(ResolveFieldNameComparison, AutoWithoutGlueCatalogReturnsVerbatim) {
    scoped_config cfg;
    cfg.get("iceberg_schema_case_insensitive").set_value(sci::auto_);
    EXPECT_EQ(resolve_field_name_comparison(), fnn::verbatim);
}

TEST(ResolveFieldNameComparison, YesAlwaysReturnsLowerCase) {
    scoped_config cfg;
    cfg.get("iceberg_schema_case_insensitive").set_value(sci::yes);
    EXPECT_EQ(resolve_field_name_comparison(), fnn::lower_case);
}

TEST(ResolveFieldNameComparison, NoAlwaysReturnsVerbatim) {
    scoped_config cfg;
    set_glue_catalog(cfg);
    cfg.get("iceberg_schema_case_insensitive").set_value(sci::no);
    EXPECT_EQ(resolve_field_name_comparison(), fnn::verbatim);
}
