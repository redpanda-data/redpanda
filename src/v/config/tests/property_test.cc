// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/base_property.h"
#include "config/config_store.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

namespace {

struct test_config : public config::config_store {
    static constexpr int default_value = 1;

    config::property<int> prop;

    test_config()
      : prop(
        *this,
        "prop",
        "An integer property",
        {.needs_restart = config::needs_restart::no},
        default_value) {}
};

SEASTAR_THREAD_TEST_CASE(test_binding) {
    auto cfg = test_config();
    auto& prop = cfg.prop;

    BOOST_CHECK_EQUAL(prop.value(), 1);

    auto binding = prop.bind();
    BOOST_CHECK_EQUAL(binding(), 1);

    int call_count = 0;

    binding.watch([&]() {
        BOOST_CHECK_EQUAL(binding(), 2);
        BOOST_CHECK_EQUAL(prop(), 1);
        call_count++;
    });

    prop.set_value(2);

    BOOST_CHECK_EQUAL(call_count, 1);
    BOOST_CHECK_EQUAL(binding(), 2);
    BOOST_CHECK_EQUAL(prop(), 2);
}

} // namespace
