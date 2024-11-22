// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"
#include "features/enterprise_feature_messages.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/tests/topic_properties_helpers.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"

#include <seastar/core/loop.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/defer.hh>

#include <absl/container/flat_hash_map.h>
#include <boost/test/tools/context.hpp>

using namespace std::chrono_literals; // NOLINT

inline ss::logger test_log("test"); // NOLINT

FIXTURE_TEST(
  test_unlicensed_topic_prop_create_partition, topic_properties_test_fixture) {
    using props_t = absl::flat_hash_map<ss::sstring, ss::sstring>;
    using test_t = std::pair<std::string_view, props_t>;
    const auto with = [](const std::string_view prop, const auto value) {
        return std::make_pair(
          prop, props_t{{ss::sstring{prop}, ssx::sformat("{}", value)}});
    };

    update_cluster_config(lconf().enable_schema_id_validation.name(), "compat");

    std::initializer_list<test_t> enterprise_props{
      // si_props
      // Exclude these; setting up s3_imposter is too complex for this test
      //  * kafka::topic_property_recovery
      //  * kafka::topic_property_read_replica
      with(kafka::topic_property_remote_read, true),
      with(kafka::topic_property_remote_write, true),
      // schema id validation
      with(kafka::topic_property_record_key_schema_id_validation, true),
      with(kafka::topic_property_record_key_schema_id_validation_compat, true),
      with(kafka::topic_property_record_value_schema_id_validation, true),
      with(
        kafka::topic_property_record_value_schema_id_validation_compat, true),
      // pin_leadership_props
      with(
        kafka::topic_property_leaders_preference,
        config::leaders_preference{
          .type = config::leaders_preference::type_t::racks,
          .racks = {model::rack_id{"A"}}})};

    const int32_t partitions = 3;

    for (const auto& [prop, props] : enterprise_props) {
        BOOST_TEST_CONTEXT(fmt::format("property: {}", prop)) {
            auto tp = model::topic{ssx::sformat("{}", prop)};

            auto c_res = create_topic(tp, props, 3).data;
            BOOST_REQUIRE_EQUAL(c_res.topics.size(), 1);
            BOOST_REQUIRE_EQUAL(
              c_res.topics[0].error_code, kafka::error_code::none);

            revoke_license();

            auto res = create_partitions(tp, partitions + 1).data;
            BOOST_REQUIRE_EQUAL(res.results.size(), 1);
            BOOST_CHECK_EQUAL(
              res.results[0].error_code, kafka::error_code::invalid_config);
            BOOST_CHECK(res.results[0].error_message.value_or("").contains(
              features::enterprise_error_message::required));

            delete_topic(
              model::topic_namespace{model::kafka_namespace, std::move(tp)})
              .get();

            reinstall_license();
        }
    }
}
