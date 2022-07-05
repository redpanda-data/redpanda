/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/server/handlers/handler_interface.h"
#include "kafka/server/handlers/handlers.h"

#include <boost/test/unit_test.hpp>

template<kafka::KafkaApiHandlerAny H>
void check_any_vs_static() {
    BOOST_TEST_INFO("Testing " << H::api::name);
    auto hopt = kafka::handler_for_key(H::api::key);
    BOOST_REQUIRE(hopt.has_value());
    auto h = *hopt;
    BOOST_CHECK_EQUAL(h->min_supported(), H::min_supported);
    BOOST_CHECK_EQUAL(h->max_supported(), H::max_supported);
    BOOST_CHECK_EQUAL(h->key(), H::api::key);
    BOOST_CHECK_EQUAL(h->name(), H::api::name);
}

template<typename... Ts>
void check_all_types(kafka::type_list<Ts...>) {
    (check_any_vs_static<Ts>(), ...);
}

BOOST_AUTO_TEST_CASE(handler_all_types) {
    check_all_types(kafka::request_types{});
}

BOOST_AUTO_TEST_CASE(handler_handler_for_key) {
    // key too low
    BOOST_CHECK(!kafka::handler_for_key(kafka::api_key(-1)).has_value());
    // key too high
    const auto max_key = kafka::max_api_key(kafka::request_types{});
    BOOST_CHECK(
      !kafka::handler_for_key(kafka::api_key(max_key + 1)).has_value());
    // last key should be present
    BOOST_CHECK(kafka::handler_for_key(kafka::api_key(max_key)).has_value());
    // 34 is AlterReplicaLogDirs which we don't currently support, use it as a
    // test case for handlers which fall in the valid range but we don't support
    BOOST_CHECK(!kafka::handler_for_key(kafka::api_key(34)).has_value());
}
