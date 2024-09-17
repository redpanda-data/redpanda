// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/schemata/create_topics_request.h"
#include "kafka/server/handlers/topics/types.h"
#include "kafka/server/handlers/topics/validators.h"
#include "pandaproxy/schema_registry/subject_name_strategy.h"

#include <boost/test/data/test_case.hpp>
#include <boost/test/unit_test.hpp>

#include <array>

namespace bdata = boost::unit_test::data;
using namespace pandaproxy::schema_registry;
using namespace kafka;

struct test_validator_data {
    std::vector<createable_topic_config> configs;
    bool is_valid;
    friend std::ostream&
    operator<<(std::ostream& os, const test_validator_data& d) {
        fmt::print(os, "configs: {}, expect valid: {}", d.configs, d.is_valid);
        return os;
    }
};

std::ostream& operator<<(std::ostream& os, subject_name_strategy sns) {
    return os << to_string_view(sns);
}

static const auto sns_data = std::to_array(
  {// Correct keys and values: - expect success
   test_validator_data{
     {{.name = ss::sstring{topic_property_record_key_subject_name_strategy},
       .value = ss::sstring{to_string_view(
         subject_name_strategy::topic_name)}}},
     true},
   {{{.name
      = ss::sstring{topic_property_record_key_subject_name_strategy_compat},
      .value = ss::sstring{to_string_view_compat(
        subject_name_strategy::topic_name)}}},
    true},
   {{{.name = ss::sstring{topic_property_record_value_subject_name_strategy},
      .value = ss::sstring{to_string_view(subject_name_strategy::topic_name)}}},
    true},
   {{{.name
      = ss::sstring{topic_property_record_value_subject_name_strategy_compat},
      .value = ss::sstring{to_string_view_compat(
        subject_name_strategy::topic_name)}}},
    true}, // Correct key, null value: expect success
   {{{.name = ss::sstring{topic_property_record_value_subject_name_strategy},
      .value = std::nullopt}},
    true},
   // Incorrect key: - expect success
   {{{.name = ss::sstring{topic_property_remote_read},
      .value = ss::sstring{to_string_view_compat(
        subject_name_strategy::topic_name)}}},
    true},
   // Correct key, invalid sns: - expect failure
   {{{.name = ss::sstring{topic_property_record_value_subject_name_strategy},
      .value = ss::sstring{"InvalidNameStrategy"}}},
    false}});

BOOST_DATA_TEST_CASE(
  test_subject_name_strategy_validator, bdata::make(sns_data), data) {
    creatable_topic no_options = {
      .name = model::topic_view{"test_tp"},
      .num_partitions = 1,
      .replication_factor = 1,
      .configs = data.configs};
    BOOST_REQUIRE_EQUAL(
      subject_name_strategy_validator::is_valid(no_options), data.is_valid);
}
