/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "test_utils/global_test_hooks.h"

#include <boost/test/unit_test_suite.hpp>

using namespace boost::unit_test;

class boost_hooks : public global_configuration {
public:
    virtual void test_unit_start(const test_unit& test) override {
        test_hooks::before_test_case(test.full_name());
    }
};

BOOST_TEST_GLOBAL_CONFIGURATION(boost_hooks);
