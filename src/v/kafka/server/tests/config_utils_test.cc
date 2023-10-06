// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/types.h"
#include "kafka/server/handlers/configs/config_utils.h"
#include "kafka/types.h"

#include <boost/test/auto_unit_test.hpp>
#include <boost/test/test_tools.hpp>

BOOST_AUTO_TEST_CASE(parse_and_set_optional_bool_alpha_test_set) {
    using namespace kafka;
    cluster::property_update<std::optional<bool>> property;

    // Set from 'true'
    parse_and_set_optional_bool_alpha(
      property, "true", config_resource_operation::set);
    BOOST_REQUIRE_EQUAL(property.value, true);
    BOOST_REQUIRE_EQUAL(
      property.op, cluster::incremental_update_operation::set);

    // Set from 'false'
    parse_and_set_optional_bool_alpha(
      property, "false", config_resource_operation::set);
    BOOST_REQUIRE_EQUAL(property.value, false);
    BOOST_REQUIRE_EQUAL(
      property.op, cluster::incremental_update_operation::set);

    // If no value provided, do nothing
    property.value = true;
    property.op = cluster::incremental_update_operation::none;
    parse_and_set_optional_bool_alpha(
      property, std::nullopt, config_resource_operation::set);
    BOOST_REQUIRE_EQUAL(property.value, true);
    BOOST_REQUIRE_EQUAL(
      property.op, cluster::incremental_update_operation::none);
}

BOOST_AUTO_TEST_CASE(parse_and_set_optional_bool_alpha_test_set_invalid) {
    using namespace kafka;
    cluster::property_update<std::optional<bool>> property;

    // "0" is invalid (c.f. parse_and_set_bool)
    BOOST_REQUIRE_THROW(
      parse_and_set_optional_bool_alpha(
        property, "0", config_resource_operation::set),
      boost::bad_lexical_cast);

    // "1" is invalid (c.f. parse_and_set_bool)
    BOOST_REQUIRE_THROW(
      parse_and_set_optional_bool_alpha(
        property, "1", config_resource_operation::set),
      boost::bad_lexical_cast);

    // An arbitrary string is invalid (c.f. parse_and_set_bool)
    BOOST_REQUIRE_THROW(
      parse_and_set_optional_bool_alpha(
        property, "foo", config_resource_operation::set),
      boost::bad_lexical_cast);
}

BOOST_AUTO_TEST_CASE(parse_and_set_optional_bool_alpha_test_remove) {
    using namespace kafka;
    cluster::property_update<std::optional<bool>> property;

    // Removing a property results in a remove
    parse_and_set_optional_bool_alpha(
      property, std::nullopt, config_resource_operation::remove);
    BOOST_REQUIRE_EQUAL(
      property.op, cluster::incremental_update_operation::remove);

    // Removing a property results in a remove, the value is ignored
    parse_and_set_optional_bool_alpha(
      property, "ignored", config_resource_operation::remove);
    BOOST_REQUIRE_EQUAL(
      property.op, cluster::incremental_update_operation::remove);
}

BOOST_AUTO_TEST_CASE(parse_and_set_topic_replication_factor_test) {
    using namespace kafka;
    cluster::property_update<std::optional<cluster::replication_factor>>
      property;

    using rep_factor_validator = config_validator_list<
      cluster::replication_factor,
      replication_factor_must_be_positive,
      replication_factor_must_be_odd>;

    parse_and_set_topic_replication_factor(
      property,
      "3",
      kafka::config_resource_operation::set,
      rep_factor_validator{});
    BOOST_REQUIRE_EQUAL(property.value, 3);

    auto is_pos_error = [](const validation_error& ex) {
        return ss::sstring(ex.what()).contains("positive");
    };
    auto is_odd_error = [](const validation_error& ex) {
        return ss::sstring(ex.what()).contains("odd");
    };

    BOOST_CHECK_EXCEPTION(
      parse_and_set_topic_replication_factor(
        property,
        "0",
        kafka::config_resource_operation::set,
        rep_factor_validator{}),
      validation_error,
      is_pos_error);
    BOOST_REQUIRE_EQUAL(property.value.has_value(), false);

    BOOST_CHECK_EXCEPTION(
      parse_and_set_topic_replication_factor(
        property,
        "4",
        kafka::config_resource_operation::set,
        rep_factor_validator{}),
      validation_error,
      is_odd_error);
    BOOST_REQUIRE_EQUAL(property.value.has_value(), false);
}
