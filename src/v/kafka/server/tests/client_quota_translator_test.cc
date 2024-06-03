// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "config/configuration.h"
#include "kafka/server/client_quota_translator.h"

#include <boost/test/auto_unit_test.hpp>
#include <boost/test/test_tools.hpp>
#include <boost/test/unit_test.hpp>

#include <variant>

using namespace kafka;

const ss::sstring test_client_id = "franz-go";
const tracker_key test_client_id_key = k_client_id{test_client_id};

constexpr std::string_view raw_basic_produce_config = R"([
  {
    "group_name": "not-franz-go-produce-group",
    "clients_prefix": "not-franz-go",
    "quota": 2048
  },
  {
    "group_name": "franz-go-produce-group",
    "clients_prefix": "franz-go",
    "quota": 4096
  }
])";

constexpr std::string_view raw_basic_fetch_config = R"([
  {
    "group_name": "not-franz-go-fetch-group",
    "clients_prefix": "not-franz-go",
    "quota": 2049
  },
  {
    "group_name": "franz-go-fetch-group",
    "clients_prefix": "franz-go",
    "quota": 4097
  }
])";

// Helper for checking std::variant types for equality
const auto CHECK_VARIANT_EQ = [](auto expected, auto got) {
    BOOST_CHECK_EQUAL(expected, get<decltype(expected)>(got));
};

void reset_configs() {
    config::shard_local_cfg().target_quota_byte_rate.reset();
    config::shard_local_cfg().target_fetch_quota_byte_rate.reset();
    config::shard_local_cfg().kafka_admin_topic_api_rate.reset();
    config::shard_local_cfg().kafka_client_group_byte_rate_quota.reset();
    config::shard_local_cfg().kafka_client_group_fetch_byte_rate_quota.reset();
}

BOOST_AUTO_TEST_CASE(quota_translator_default_test) {
    reset_configs();

    auto default_limits = client_quota_limits{
      .produce_limit = 2147483648,
      .fetch_limit = std::nullopt,
      .partition_mutation_limit = std::nullopt,
    };
    auto [key, limits] = tr.find_quota(
      {client_quota_type::produce_quota, test_client_id});
    BOOST_CHECK_EQUAL(test_client_id_key, key);
    BOOST_CHECK_EQUAL(default_limits, limits);
}

BOOST_AUTO_TEST_CASE(quota_translator_modified_default_test) {
    reset_configs();
    config::shard_local_cfg().target_quota_byte_rate.set_value(1111);
    config::shard_local_cfg().target_fetch_quota_byte_rate.set_value(2222);
    config::shard_local_cfg().kafka_admin_topic_api_rate.set_value(3333);

    client_quota_translator tr;

    auto expected_limits = client_quota_limits{
      .produce_limit = 1111,
      .fetch_limit = 2222,
      .partition_mutation_limit = 3333,
    };
    auto [key, limits] = tr.find_quota(
      {client_quota_type::produce_quota, test_client_id});
    BOOST_CHECK_EQUAL(test_client_id_key, key);
    BOOST_CHECK_EQUAL(expected_limits, limits);
}

BOOST_AUTO_TEST_CASE(quota_translator_client_group_test) {
    reset_configs();
    constexpr auto P_DEF = 1111;
    constexpr auto F_DEF = 2222;
    constexpr auto PM_DEF = 3333;

    config::shard_local_cfg().target_quota_byte_rate.set_value(P_DEF);
    config::shard_local_cfg().target_fetch_quota_byte_rate.set_value(F_DEF);
    config::shard_local_cfg().kafka_admin_topic_api_rate.set_value(PM_DEF);

    config::shard_local_cfg().kafka_client_group_byte_rate_quota.set_value(
      YAML::Load(std::string(raw_basic_produce_config)));
    config::shard_local_cfg()
      .kafka_client_group_fetch_byte_rate_quota.set_value(
        YAML::Load(std::string(raw_basic_fetch_config)));

    client_quota_translator tr;

    // Stage 1 - Start by checking that tracker_key's are correctly detected
    // for various client ids
    auto get_produce_key = [&tr](auto client_id) {
        return tr.find_quota_key({client_quota_type::produce_quota, client_id});
    };
    auto get_fetch_key = [&tr](auto client_id) {
        return tr.find_quota_key({client_quota_type::fetch_quota, client_id});
    };
    auto get_mutation_key = [&tr](auto client_id) {
        return tr.find_quota_key(
          {client_quota_type::partition_mutation_quota, client_id});
    };

    // Check keys for produce
    CHECK_VARIANT_EQ(
      k_group_name{"franz-go-produce-group"}, get_produce_key("franz-go"));
    CHECK_VARIANT_EQ(
      k_group_name{"franz-go-produce-group"}, get_produce_key("franz-go"));
    CHECK_VARIANT_EQ(
      k_group_name{"not-franz-go-produce-group"},
      get_produce_key("not-franz-go"));
    CHECK_VARIANT_EQ(k_client_id{"unknown"}, get_produce_key("unknown"));
    CHECK_VARIANT_EQ(k_client_id{""}, get_produce_key(std::nullopt));

    // Check keys for fetch
    CHECK_VARIANT_EQ(
      k_group_name{"franz-go-fetch-group"}, get_fetch_key("franz-go"));
    CHECK_VARIANT_EQ(
      k_group_name{"not-franz-go-fetch-group"}, get_fetch_key("not-franz-go"));
    CHECK_VARIANT_EQ(k_client_id{"unknown"}, get_fetch_key("unknown"));
    CHECK_VARIANT_EQ(k_client_id{""}, get_fetch_key(std::nullopt));

    // Check keys for partition mutations
    CHECK_VARIANT_EQ(k_client_id{"franz-go"}, get_mutation_key("franz-go"));
    CHECK_VARIANT_EQ(
      k_client_id{"not-franz-go"}, get_mutation_key("not-franz-go"));
    CHECK_VARIANT_EQ(k_client_id{"unknown"}, get_mutation_key("unknown"));
    CHECK_VARIANT_EQ(k_client_id{""}, get_mutation_key(std::nullopt));

    // Stage 2 - Next verify that the correct quota limits apply to the
    // various tracker_key's being tested
    // Check limits for the franz-go groups
    auto franz_go_produce_limits = client_quota_limits{
      .produce_limit = 4096,
      .fetch_limit = F_DEF,
      .partition_mutation_limit = PM_DEF,
    };
    BOOST_CHECK_EQUAL(
      franz_go_produce_limits,
      tr.find_quota_value(k_group_name{"franz-go-produce-group"}));
    auto franz_go_fetch_limits = client_quota_limits{
      .produce_limit = P_DEF,
      .fetch_limit = 4097,
      .partition_mutation_limit = PM_DEF,
    };
    BOOST_CHECK_EQUAL(
      franz_go_fetch_limits,
      tr.find_quota_value(k_group_name{"franz-go-fetch-group"}));

    // Check limits for the not-franz-go groups
    auto not_franz_go_produce_limits = client_quota_limits{
      .produce_limit = 2048,
      .fetch_limit = F_DEF,
      .partition_mutation_limit = PM_DEF,
    };
    BOOST_CHECK_EQUAL(
      not_franz_go_produce_limits,
      tr.find_quota_value(k_group_name{"not-franz-go-produce-group"}));
    auto not_franz_go_fetch_limits = client_quota_limits{
      .produce_limit = P_DEF,
      .fetch_limit = 2049,
      .partition_mutation_limit = PM_DEF,
    };
    BOOST_CHECK_EQUAL(
      not_franz_go_fetch_limits,
      tr.find_quota_value(k_group_name{"not-franz-go-fetch-group"}));

    // Check limits for the non-client-group keys
    auto default_limits = client_quota_limits{
      .produce_limit = P_DEF,
      .fetch_limit = F_DEF,
      .partition_mutation_limit = PM_DEF,
    };
    BOOST_CHECK_EQUAL(
      default_limits, tr.find_quota_value(k_client_id{"unknown"}));
    BOOST_CHECK_EQUAL(default_limits, tr.find_quota_value(k_client_id{""}));
    BOOST_CHECK_EQUAL(
      default_limits, tr.find_quota_value(k_client_id{"franz-go"}));
    BOOST_CHECK_EQUAL(
      default_limits, tr.find_quota_value(k_client_id{"not-franz-go"}));
}
