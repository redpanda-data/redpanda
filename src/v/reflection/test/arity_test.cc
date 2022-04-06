// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#define BOOST_TEST_MODULE reflection

#include "reflection/arity.h"
#include "rpc/test/test_types.h"

#include <boost/test/unit_test.hpp>

struct inherit_complex_pod : complex_custom {
    int i;
    float j;
};

BOOST_AUTO_TEST_CASE(verify_airty) {
    BOOST_CHECK_EQUAL(reflection::arity<pod>(), 3);
    BOOST_CHECK_EQUAL(reflection::arity<complex_custom>(), 2);
    // BOOST_CHECK_EQUAL(reflection::arity<inherit_complex_pod>(), 4);
}
