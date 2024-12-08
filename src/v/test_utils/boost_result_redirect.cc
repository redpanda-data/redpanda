// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include <boost/test/detail/global_typedef.hpp>
#include <boost/test/unit_test_log.hpp>
#include <boost/test/unit_test_suite.hpp>

#include <fstream>
#include <iomanip>

/**
 * This test configuration object is used to redirect the JUNIT result output
 * (if enabled) to the location specified by the XML_OUTPUT_FILE environment
 * variable. This variable is set by the bazel test runner so that the test
 * writes the results in a local that bazel can subsequently consume.
 * Ref: CORE-8013.
 */
struct bazel_result_handler_for_boost {
    bazel_result_handler_for_boost() {
        using namespace boost::unit_test;
        if (auto xml_path = std::getenv("XML_OUTPUT_FILE")) {
            _out = std::ofstream(xml_path);
            // use fixed format to avoid scientific notation as a work-around
            // to https://github.com/bazelbuild/bazel/issues/24605
            _out << std::fixed << std::setprecision(6);
            unit_test_log.add_format(OF_JUNIT);
            unit_test_log.set_stream(OF_JUNIT, _out);
        }
    }
    std::ofstream _out;
};

BOOST_TEST_GLOBAL_CONFIGURATION(bazel_result_handler_for_boost);
