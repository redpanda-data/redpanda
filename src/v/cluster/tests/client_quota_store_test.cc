// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/client_quota_serde.h"
#include "cluster/client_quota_store.h"
#include "cluster/controller_snapshot.h"
#include "utils/to_string.h"

#include <boost/algorithm/string.hpp>
#include <boost/range/irange.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>
#include <fmt/ostream.h>

namespace cluster::client_quota {

const entity_key key0{entity_key::client_id_default_match{}};
const entity_key key1{entity_key::client_id_match{"producer-app-1"}};
const entity_key key2{entity_key::client_id_match{"consumer-app-1"}};
const entity_key key3{entity_key::client_id_prefix_match{"franz-go-prefix"}};

const entity_value val0{
  .consumer_byte_rate = 10240,
};
const entity_value val1{
  .producer_byte_rate = 51200,
};
const entity_value val2{
  .producer_byte_rate = 25600,
  .consumer_byte_rate = 20480,
};
const entity_value val3{
  .producer_byte_rate = 12345,
};

BOOST_AUTO_TEST_CASE(quota_store_set_get_remove) {
    store st;

    BOOST_CHECK(!st.get_quota(key0).has_value());
    BOOST_CHECK(!st.get_quota(key1).has_value());

    st.set_quota(key0, val0);
    BOOST_CHECK_EQUAL(st.get_quota(key0), val0);

    st.set_quota(key1, val1);
    BOOST_CHECK_EQUAL(st.get_quota(key1), val1);

    st.set_quota(key1, val2);
    BOOST_CHECK_EQUAL(st.get_quota(key1), val2);

    st.remove_quota(key0);
    BOOST_CHECK(!st.get_quota(key0).has_value());
}

BOOST_AUTO_TEST_CASE(quota_store_size_clear) {
    store st;

    BOOST_CHECK_EQUAL(st.size(), 0);

    st.set_quota(key0, val0);

    BOOST_CHECK_EQUAL(st.size(), 1);

    st.clear();

    BOOST_CHECK_EQUAL(st.size(), 0);
}

BOOST_AUTO_TEST_CASE(quota_store_range) {
    store st;
    st.set_quota(key0, val0);

    auto all_quotas = st.range([](const auto&) { return true; });
    BOOST_CHECK_EQUAL(all_quotas.size(), 1);
    BOOST_CHECK_EQUAL(all_quotas[0].first, key0);
    BOOST_CHECK_EQUAL(all_quotas[0].second, val0);

    st.set_quota(key1, val1);
    st.set_quota(key2, val2);
    st.set_quota(key3, val3);

    auto default_client_quotas = st.range(
      [](const std::pair<entity_key, entity_value>& kv) -> bool {
          return store::entity_part_filter(
            kv,
            entity_key::part{
              .part = entity_key::part::client_id_default_match{}});
      });
    BOOST_CHECK_EQUAL(default_client_quotas.size(), 1);
    BOOST_CHECK_EQUAL(default_client_quotas[0].first, key0);
    BOOST_CHECK_EQUAL(default_client_quotas[0].second, val0);

    auto specific_client_quota = st.range(
      [](const std::pair<entity_key, entity_value>& kv) -> bool {
          return store::entity_part_filter(kv, *key1.parts.begin());
      });
    BOOST_CHECK_EQUAL(specific_client_quota.size(), 1);
    BOOST_CHECK_EQUAL(specific_client_quota[0].first, key1);
    BOOST_CHECK_EQUAL(specific_client_quota[0].second, val1);

    auto group_quotas = st.range(
      store::prefix_group_filter("franz-go-prefix--and-some-more"));
    BOOST_CHECK_EQUAL(group_quotas.size(), 1);
    BOOST_CHECK_EQUAL(group_quotas[0].first, key3);
    BOOST_CHECK_EQUAL(group_quotas[0].second, val3);
}

BOOST_AUTO_TEST_CASE(quota_store_snapshot_delta) {
    auto snap = controller_snapshot_parts::client_quotas_t {
        .quotas = {
            {key0, val0},
            {key1, val1},
        },
    };
    store st{snap};

    BOOST_CHECK_EQUAL(st.size(), 2);
    BOOST_CHECK_EQUAL(st.get_quota(key0), val0);
    BOOST_CHECK_EQUAL(st.get_quota(key1), val1);
    BOOST_CHECK(!st.get_quota(key2).has_value());

    auto delta = alter_delta_cmd_data{
        .ops = {
        alter_delta_cmd_data::op{
            .key = key2,
            .diff = {
              .entries = {
                entity_value_diff::entry(
                  entity_value_diff::key::producer_byte_rate, 25600),
                entity_value_diff::entry(
                  entity_value_diff::key::consumer_byte_rate, 20480),
                },
              },
            },
        alter_delta_cmd_data::op{
            .key = key1,
            .diff = {
              .entries = {
                entity_value_diff::entry(entity_value_diff::operation::remove, entity_value_diff::key::producer_byte_rate, 0),
              },
            },
          },
        },
    };
    st.apply_delta(delta);

    BOOST_CHECK_EQUAL(st.size(), 2);
    BOOST_CHECK_EQUAL(st.get_quota(key0), val0);
    BOOST_CHECK(!st.get_quota(key1).has_value());
    BOOST_CHECK_EQUAL(st.get_quota(key2), val2);
}

} // namespace cluster::client_quota
