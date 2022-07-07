/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "config/configuration.h"

#include <seastar/testing/thread_test_case.hh>

#include <yaml-cpp/yaml.h>

static constexpr auto source_mapping
  = std::to_array<std::pair<model::cloud_credentials_source, std::string_view>>(
    {
      {model::cloud_credentials_source::aws_instance_metadata,
       "aws_instance_metadata"},
      {model::cloud_credentials_source::sts, "sts"},
      {model::cloud_credentials_source::config_file, "config_file"},
      {model::cloud_credentials_source::gcp_instance_metadata,
       "gcp_instance_metadata"},
    });

SEASTAR_THREAD_TEST_CASE(test_decode_cloud_credentials_source) {
    for (const auto& [value, string_repr] : source_mapping) {
        auto node = YAML::Load(fmt::format("ccs: {}", string_repr))["ccs"];
        BOOST_REQUIRE_EQUAL(node.as<model::cloud_credentials_source>(), value);
    }
}

SEASTAR_THREAD_TEST_CASE(test_encode_cloud_credentials_source) {
    for (const auto& [value, string_repr] : source_mapping) {
        json::StringBuffer buf;
        json::Writer<json::StringBuffer> writer(buf);
        json::rjson_serialize(writer, value);
        BOOST_REQUIRE_EQUAL(
          YAML::Dump(YAML::Load(buf.GetString())), string_repr);
    }
}
