/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/configuration.h"
#include "config/configuration.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(test_backend_from_url) {
    auto cfg = cloud_storage_clients::s3_configuration{};
    cfg.uri = cloud_storage_clients::access_point_uri{"storage.googleapis.com"};
    auto inferred = cloud_storage_clients::infer_backend_from_configuration(
      cfg, model::cloud_credentials_source::config_file);
    BOOST_REQUIRE_EQUAL(
      inferred, model::cloud_storage_backend::google_s3_compat);

    cfg.uri = cloud_storage_clients::access_point_uri{"minio-s3"};
    inferred = cloud_storage_clients::infer_backend_from_configuration(
      cfg, model::cloud_credentials_source::config_file);
    BOOST_REQUIRE_EQUAL(inferred, model::cloud_storage_backend::minio);
}

BOOST_AUTO_TEST_CASE(test_backend_from_cred_src) {
    auto cfg = cloud_storage_clients::s3_configuration{};
    auto inferred = cloud_storage_clients::infer_backend_from_configuration(
      cfg, model::cloud_credentials_source::aws_instance_metadata);
    BOOST_REQUIRE_EQUAL(inferred, model::cloud_storage_backend::aws);

    inferred = cloud_storage_clients::infer_backend_from_configuration(
      cfg, model::cloud_credentials_source::sts);
    BOOST_REQUIRE_EQUAL(inferred, model::cloud_storage_backend::aws);

    inferred = cloud_storage_clients::infer_backend_from_configuration(
      cfg, model::cloud_credentials_source::gcp_instance_metadata);
    BOOST_REQUIRE_EQUAL(
      inferred, model::cloud_storage_backend::google_s3_compat);

    inferred = cloud_storage_clients::infer_backend_from_configuration(
      cfg, model::cloud_credentials_source::config_file);
    BOOST_REQUIRE_EQUAL(inferred, model::cloud_storage_backend::unknown);
}

BOOST_AUTO_TEST_CASE(test_backend_when_using_azure) {
    auto cfg = cloud_storage_clients::abs_configuration{};
    auto inferred = cloud_storage_clients::infer_backend_from_configuration(
      cfg, model::cloud_credentials_source::aws_instance_metadata);
    BOOST_REQUIRE_EQUAL(inferred, model::cloud_storage_backend::azure);
}

BOOST_AUTO_TEST_CASE(test_backend_override) {
    config::shard_local_cfg().cloud_storage_backend.set_value(
      model::cloud_storage_backend{
        model::cloud_storage_backend::google_s3_compat});
    auto cfg = cloud_storage_clients::abs_configuration{};
    auto inferred = cloud_storage_clients::infer_backend_from_configuration(
      cfg, model::cloud_credentials_source::aws_instance_metadata);
    BOOST_REQUIRE_EQUAL(
      inferred, model::cloud_storage_backend::google_s3_compat);
}
