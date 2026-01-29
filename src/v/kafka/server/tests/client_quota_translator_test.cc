// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "cluster/client_quota_serde.h"
#include "cluster/client_quota_store.h"
#include "config/configuration.h"
#include "kafka/server/client_quota_translator.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/auto_unit_test.hpp>
#include <boost/test/test_tools.hpp>
#include <boost/test/unit_test.hpp>

#include <optional>
#include <ranges>
#include <variant>

using namespace kafka;

using cluster::client_quota::entity_key;
using cluster::client_quota::entity_value;

static const auto default_key = entity_key(
  entity_key::client_id_default_match{});

const ss::sstring test_client_id = "franz-go";
const tracker_key test_client_id_key = k_client_id{test_client_id};
const tracker_key test_empty_key = k_not_applicable{};

constexpr auto P_DEF = 1111;
constexpr auto F_DEF = 2222;
constexpr auto PM_DEF = 3333;

// Helper for checking std::variant types for equality
const auto CHECK_VARIANT_EQ =
  [](const tracker_key& expected, const tracker_key& got) {
      BOOST_CHECK_EQUAL(expected, got);
  };

struct fixture {
    ss::sharded<cluster::client_quota::store> quota_store;
    kafka::client_quota_translator tr;

    fixture()
      : tr(std::ref(quota_store)) {
        quota_store.start().get();
    }

    ~fixture() { quota_store.stop().get(); }
};

SEASTAR_THREAD_TEST_CASE(quota_translator_default_test) {
    fixture f;

    auto default_limits = client_quota_limits{
      .produce_limit = std::nullopt,
      .fetch_limit = std::nullopt,
      .partition_mutation_limit = std::nullopt,
    };
    auto key = f.tr.find_quota_key(
      {.q_type = client_quota_type::produce_quota,
       .client_id = test_client_id});
    auto limits = f.tr.find_quota_value(key);
    BOOST_CHECK_EQUAL(test_empty_key, key);
    BOOST_CHECK_EQUAL(default_limits, limits);
    BOOST_CHECK(f.tr.is_empty());
}

SEASTAR_THREAD_TEST_CASE(quota_translator_modified_default_test) {
    fixture f;

    auto default_values = entity_value{
      .producer_byte_rate = 1111,
      .consumer_byte_rate = 2222,
      .controller_mutation_rate = 3333,
    };
    f.quota_store.local().set_quota(default_key, default_values);

    auto expected_limits = client_quota_limits{
      .produce_limit = 1111,
      .fetch_limit = 2222,
      .partition_mutation_limit = 3333,
    };
    auto key = f.tr.find_quota_key(
      {.q_type = client_quota_type::produce_quota,
       .client_id = test_client_id});
    auto limits = f.tr.find_quota_value(key);
    BOOST_CHECK_EQUAL(test_client_id_key, key);
    BOOST_CHECK_EQUAL(expected_limits, limits);
    BOOST_CHECK(!f.tr.is_empty());
}

void run_quota_translator_client_group_test(fixture& f) {
    // Stage 1 - Start by checking that tracker_key's are correctly detected
    // for various client ids
    auto get_produce_key = [&f](auto client_id) {
        return f.tr.find_quota_key(
          {.q_type = client_quota_type::produce_quota, .client_id = client_id});
    };
    auto get_fetch_key = [&f](auto client_id) {
        return f.tr.find_quota_key(
          {.q_type = client_quota_type::fetch_quota, .client_id = client_id});
    };
    auto get_mutation_key = [&f](auto client_id) {
        return f.tr.find_quota_key(
          {.q_type = client_quota_type::partition_mutation_quota,
           .client_id = client_id});
    };

    // Check keys for produce
    CHECK_VARIANT_EQ(k_group_name{"franz-go"}, get_produce_key("franz-go"));
    CHECK_VARIANT_EQ(
      k_group_name{"not-franz-go"}, get_produce_key("not-franz-go"));
    CHECK_VARIANT_EQ(k_client_id{"unknown"}, get_produce_key("unknown"));
    CHECK_VARIANT_EQ(k_client_id{""}, get_produce_key(std::nullopt));

    // Check keys for fetch
    CHECK_VARIANT_EQ(k_group_name{"franz-go"}, get_fetch_key("franz-go"));
    CHECK_VARIANT_EQ(
      k_group_name{"not-franz-go"}, get_fetch_key("not-franz-go"));
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
    auto franz_go_limits = client_quota_limits{
      .produce_limit = 4096,
      .fetch_limit = 4097,
      .partition_mutation_limit = {},
    };
    BOOST_CHECK_EQUAL(
      franz_go_limits, f.tr.find_quota_value(k_group_name{"franz-go"}));

    // Check limits for the not-franz-go groups
    auto not_franz_go_limits = client_quota_limits{
      .produce_limit = 2048,
      .fetch_limit = 2049,
      .partition_mutation_limit = {},
    };
    BOOST_CHECK_EQUAL(
      not_franz_go_limits, f.tr.find_quota_value(k_group_name{"not-franz-go"}));

    // Check limits for the non-client-group keys
    auto default_limits = client_quota_limits{
      .produce_limit = P_DEF,
      .fetch_limit = F_DEF,
      .partition_mutation_limit = PM_DEF,
    };
    BOOST_CHECK_EQUAL(
      default_limits, f.tr.find_quota_value(k_client_id{"unknown"}));
    BOOST_CHECK_EQUAL(default_limits, f.tr.find_quota_value(k_client_id{""}));
    BOOST_CHECK_EQUAL(
      default_limits, f.tr.find_quota_value(k_client_id{"franz-go"}));
    BOOST_CHECK_EQUAL(
      default_limits, f.tr.find_quota_value(k_client_id{"not-franz-go"}));
}

SEASTAR_THREAD_TEST_CASE(quota_translator_store_client_group_test) {
    fixture f;

    auto default_values = entity_value{
      .producer_byte_rate = P_DEF,
      .consumer_byte_rate = F_DEF,
      .controller_mutation_rate = PM_DEF,
    };

    auto franz_go_key = entity_key{
      entity_key::client_id_prefix_match{"franz-go"}};
    auto franz_go_values = entity_value{
      .producer_byte_rate = 4096,
      .consumer_byte_rate = 4097,
    };

    auto not_franz_go_key = entity_key{
      entity_key::client_id_prefix_match{"not-franz-go"}};
    auto not_franz_go_values = entity_value{
      .producer_byte_rate = 2048,
      .consumer_byte_rate = 2049,
    };

    f.quota_store.local().set_quota(default_key, default_values);
    f.quota_store.local().set_quota(franz_go_key, franz_go_values);
    f.quota_store.local().set_quota(not_franz_go_key, not_franz_go_values);

    run_quota_translator_client_group_test(f);
}

SEASTAR_THREAD_TEST_CASE(quota_translator_priority_order) {
    fixture f;

    using cluster::client_quota::entity_key;
    using cluster::client_quota::entity_value;

    auto check_quota = [&f](
                         const kafka::client_quota_type& q_type,
                         const tracker_key& expected_key,
                         const std::optional<uint64_t> expected_value,
                         const client_quota_rule expected_rule) {
        auto [k, value] = f.tr.find_quota(
          {.q_type = q_type, .user = "alice", .client_id = "franz-go"});
        CHECK_VARIANT_EQ(expected_key, k);
        BOOST_CHECK_EQUAL(expected_value, value.limit);
        BOOST_CHECK_EQUAL(expected_rule, value.rule);
    };
    auto check_quotas = [&check_quota](
                          const tracker_key& expected_key,
                          const client_quota_limits expected_limits,
                          const client_quota_rule expected_rule) {
        check_quota(
          kafka::client_quota_type::produce_quota,
          expected_key,
          expected_limits.produce_limit,
          expected_rule);
        check_quota(
          kafka::client_quota_type::fetch_quota,
          expected_key,
          expected_limits.fetch_limit,
          expected_rule);
        check_quota(
          kafka::client_quota_type::partition_mutation_quota,
          expected_key,
          expected_limits.partition_mutation_limit,
          expected_rule);
    };

    // This test walks through the priority levels of the various ways of
    // configuring quotas in increasing order and asserts that each successive
    // priority level overwrites the previous one. The quota values XY mean
    // priority level X and Y = {1, 2, 3} for produce/fetch/partition mutation
    // quotas respectively to check that their values are independent.

    // 0. First: no client quotas
    check_quotas(
      k_not_applicable{},
      {.produce_limit = std::nullopt,
       .fetch_limit = std::nullopt,
       .partition_mutation_limit = std::nullopt},
      client_quota_rule::not_applicable);

    // 1. Next: default client quota
    {
        auto ekey = entity_key{entity_key::client_id_default_match{}};
        auto evalues = entity_value{
          .producer_byte_rate = 11,
          .consumer_byte_rate = 12,
          .controller_mutation_rate = 13,
        };
        f.quota_store.local().set_quota(ekey, evalues);
    }

    check_quotas(
      k_client_id{"franz-go"},
      {.produce_limit = 11, .fetch_limit = 12, .partition_mutation_limit = 13},
      client_quota_rule::kafka_client_default);

    // 2. Next: client id prefix quota
    {
        auto ekey = entity_key{entity_key::client_id_prefix_match{"franz-go"}};
        auto evalues = entity_value{
          .producer_byte_rate = 21,
          .consumer_byte_rate = 22,
          .controller_mutation_rate = 23,
        };
        f.quota_store.local().set_quota(ekey, evalues);
    }

    check_quotas(
      k_group_name{"franz-go"},
      {.produce_limit = 21, .fetch_limit = 22, .partition_mutation_limit = 23},
      client_quota_rule::kafka_client_prefix);

    // 3. Next: client id exact match quota
    {
        auto ekey = entity_key{entity_key::client_id_match{"franz-go"}};
        auto evalues = entity_value{
          .producer_byte_rate = 31,
          .consumer_byte_rate = 32,
          .controller_mutation_rate = 33,
        };
        f.quota_store.local().set_quota(ekey, evalues);
    }

    check_quotas(
      k_client_id{"franz-go"},
      {.produce_limit = 31, .fetch_limit = 32, .partition_mutation_limit = 33},
      client_quota_rule::kafka_client_id);

    // 4. Next: user default match quota
    {
        auto ekey = entity_key{entity_key::user_default_match{}};
        auto evalues = entity_value{
          .producer_byte_rate = 41,
          .consumer_byte_rate = 42,
          .controller_mutation_rate = 43,
        };
        f.quota_store.local().set_quota(ekey, evalues);
    }

    check_quotas(
      k_user{"alice"},
      {.produce_limit = 41, .fetch_limit = 42, .partition_mutation_limit = 43},
      client_quota_rule::kafka_user_default);

    // 5. Next: user default client id default match quota
    {
        auto ekey = entity_key{
          entity_key::user_default_match{},
          entity_key::client_id_default_match{}};
        auto evalues = entity_value{
          .producer_byte_rate = 51,
          .consumer_byte_rate = 52,
          .controller_mutation_rate = 53,
        };
        f.quota_store.local().set_quota(ekey, evalues);
    }

    check_quotas(
      std::make_pair(k_user{"alice"}, k_client_id{"franz-go"}),
      {.produce_limit = 51, .fetch_limit = 52, .partition_mutation_limit = 53},
      client_quota_rule::kafka_user_default_client_default);

    // 6. Next: user default client id prefix match quota
    {
        auto ekey = entity_key{
          entity_key::user_default_match{},
          entity_key::client_id_prefix_match{"franz-go"}};
        auto evalues = entity_value{
          .producer_byte_rate = 61,
          .consumer_byte_rate = 62,
          .controller_mutation_rate = 63,
        };
        f.quota_store.local().set_quota(ekey, evalues);
    }

    check_quotas(
      std::make_pair(k_user{"alice"}, k_group_name{"franz-go"}),
      {.produce_limit = 61, .fetch_limit = 62, .partition_mutation_limit = 63},
      client_quota_rule::kafka_user_default_client_prefix);

    // 7. Next: user default client id exact match quota
    {
        auto ekey = entity_key{
          entity_key::user_default_match{},
          entity_key::client_id_match{"franz-go"}};
        auto evalues = entity_value{
          .producer_byte_rate = 71,
          .consumer_byte_rate = 72,
          .controller_mutation_rate = 73,
        };
        f.quota_store.local().set_quota(ekey, evalues);
    }

    check_quotas(
      std::make_pair(k_user{"alice"}, k_client_id{"franz-go"}),
      {.produce_limit = 71, .fetch_limit = 72, .partition_mutation_limit = 73},
      client_quota_rule::kafka_user_default_client_id);

    // 8. Next: user exact match quota
    {
        auto ekey = entity_key{entity_key::user_match{"alice"}};
        auto evalues = entity_value{
          .producer_byte_rate = 81,
          .consumer_byte_rate = 82,
          .controller_mutation_rate = 83,
        };
        f.quota_store.local().set_quota(ekey, evalues);
    }

    check_quotas(
      k_user{"alice"},
      {.produce_limit = 81, .fetch_limit = 82, .partition_mutation_limit = 83},
      client_quota_rule::kafka_user);

    // 9. Next: user exact client id default match quota
    {
        auto ekey = entity_key{
          entity_key::user_match{"alice"},
          entity_key::client_id_default_match{}};
        auto evalues = entity_value{
          .producer_byte_rate = 91,
          .consumer_byte_rate = 92,
          .controller_mutation_rate = 93,
        };
        f.quota_store.local().set_quota(ekey, evalues);
    }

    check_quotas(
      std::make_pair(k_user{"alice"}, k_client_id{"franz-go"}),
      {.produce_limit = 91, .fetch_limit = 92, .partition_mutation_limit = 93},
      client_quota_rule::kafka_user_client_default);

    // 10. Next: user exact client id prefix match quota
    {
        auto ekey = entity_key{
          entity_key::user_match{"alice"},
          entity_key::client_id_prefix_match{"franz-go"}};
        auto evalues = entity_value{
          .producer_byte_rate = 101,
          .consumer_byte_rate = 102,
          .controller_mutation_rate = 103,
        };
        f.quota_store.local().set_quota(ekey, evalues);
    }

    check_quotas(
      std::make_pair(k_user{"alice"}, k_group_name{"franz-go"}),
      {.produce_limit = 101,
       .fetch_limit = 102,
       .partition_mutation_limit = 103},
      client_quota_rule::kafka_user_client_prefix);

    // 11. Next: user exact client id exact match quota
    {
        auto ekey = entity_key{
          entity_key::user_match{"alice"},
          entity_key::client_id_match{"franz-go"}};
        auto evalues = entity_value{
          .producer_byte_rate = 111,
          .consumer_byte_rate = 112,
          .controller_mutation_rate = 113,
        };
        f.quota_store.local().set_quota(ekey, evalues);
    }

    check_quotas(
      std::make_pair(k_user{"alice"}, k_client_id{"franz-go"}),
      {.produce_limit = 111,
       .fetch_limit = 112,
       .partition_mutation_limit = 113},
      client_quota_rule::kafka_user_client_id);
}

SEASTAR_THREAD_TEST_CASE(quota_translator_watch_test) {
    fixture f;

    bool first_called = false;
    bool second_called = false;

    f.tr.watch([&first_called]() mutable { first_called = true; });
    f.tr.watch([&second_called]() mutable { second_called = true; });

    auto val = entity_value{.producer_byte_rate = P_DEF};
    f.quota_store.local().set_quota(default_key, val);

    BOOST_CHECK(first_called);
    BOOST_CHECK(second_called);
}
