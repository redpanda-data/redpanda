// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/named_type.h"
#include "utils/uuid.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

using test_uuid = named_type<uuid_t, struct test_uuid_tag>;

SEASTAR_THREAD_TEST_CASE(test_uuid_create) {
    auto uuid1 = uuid_t::create();
    auto uuid2 = uuid_t::create();
    BOOST_REQUIRE_NE(uuid1, uuid2);
}

SEASTAR_THREAD_TEST_CASE(test_named_uuid_type) {
    auto uuid1 = test_uuid(uuid_t::create());
    auto uuid2 = test_uuid(uuid_t::create());
    BOOST_REQUIRE_NE(uuid1, uuid2);
}

SEASTAR_THREAD_TEST_CASE(test_to_from_vec) {
    auto uuid1 = test_uuid(uuid_t::create());
    auto uuid1_vec = uuid1().to_vector();
    auto uuid2 = test_uuid(uuid1_vec);
    BOOST_REQUIRE_EQUAL(uuid1, uuid2);
}
