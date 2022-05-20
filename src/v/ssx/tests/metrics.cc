// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/metrics.h"

#include "prometheus/prometheus_sanitize.h"
#include "seastarx.h"

#include <seastar/core/metrics.hh>
#include <seastar/testing/thread_test_case.hh>

#include <ostream>

namespace sm = ss::metrics;
namespace ssmi = sm::impl;
namespace ssxm = ssx::metrics;
namespace ssxmf = ssxm::filter;
using agg_op = ssx::metrics::filter::config::agg_op;

SEASTAR_THREAD_TEST_CASE(test_fetch_metrics) {
    uint64_t counter_0{17};
    uint64_t counter_1{19};
    uint64_t counter_2{23};
    sm::metric_groups metrics;

    sm::label label_ag{"aggregate"};
    sm::label label_leave{"leave"};

    metrics.add_group(
      prometheus_sanitize::metrics_name("test_fetch_metrics"),
      {
        sm::make_counter(
          "counter",
          [&] { return counter_0; },
          sm::description("Test a counter"),
          {
            label_ag(0),
            label_leave(0),
          }),
        sm::make_counter(
          "counter",
          [&] { return counter_1; },
          sm::description("Test a counter"),
          {
            label_ag(1),
            label_leave(0),
          }),
        sm::make_counter(
          "counter",
          [&] { return counter_2; },
          sm::description("Test a counter"),
          {
            label_ag(1),
            label_leave(1),
          }),
      });

    auto collect_values = [](const ssmi::value_map& metrics) {
        std::map<ssmi::labels_type, uint64_t> values;
        for (const auto& [n, m] : metrics) {
            for (const auto& [n, o] : m) {
                values[n] = o->get_function()().i();
            }
        }
        return values;
    };

    // Check registred metrics
    auto values = collect_values(ss::metrics::impl::get_value_map());
    auto expected_0 = ssmi::labels_type{
      {label_ag.name(), "0"},
      {label_leave.name(), "0"},
      {sm::shard_label.name(), "0"}};

    auto expected_1 = ssmi::labels_type{
      {label_ag.name(), "1"},
      {label_leave.name(), "0"},
      {sm::shard_label.name(), "0"}};

    auto expected_2 = ssmi::labels_type{
      {label_ag.name(), "1"},
      {label_leave.name(), "1"},
      {sm::shard_label.name(), "0"}};

    BOOST_CHECK_EQUAL(values[expected_0], 17);
    BOOST_CHECK_EQUAL(values[expected_1], 19);
    BOOST_CHECK_EQUAL(values[expected_2], 23);

    // Check summed metrics
    ssxmf::config config{{{"test_fetch_metrics_counter", {label_leave}}}};
    values = collect_values(ssxm::get_filtered(config));

    BOOST_CHECK_EQUAL(values.size(), 2);
    auto leave_0 = ssmi::labels_type{{label_leave.name(), "0"}};
    auto leave_1 = ssmi::labels_type{{label_leave.name(), "1"}};
    BOOST_CHECK_EQUAL(values[leave_0], 36);
    BOOST_CHECK_EQUAL(values[leave_1], 23);

    // Check count metrics
    config = {{{"test_fetch_metrics_counter", {label_leave}, agg_op::count}}};
    values = collect_values(ssxm::get_filtered(config));

    BOOST_CHECK_EQUAL(values.size(), 2);
    BOOST_CHECK_EQUAL(values[leave_0], 2);
    BOOST_CHECK_EQUAL(values[leave_1], 1);

    // Check min metrics
    config = {{{"test_fetch_metrics_counter", {label_leave}, agg_op::min}}};
    values = collect_values(ssxm::get_filtered(config));

    BOOST_CHECK_EQUAL(values.size(), 2);
    BOOST_CHECK_EQUAL(values[leave_0], 17);
    BOOST_CHECK_EQUAL(values[leave_1], 23);

    // Check max metrics
    config = {{{"test_fetch_metrics_counter", {label_leave}, agg_op::max}}};
    values = collect_values(ssxm::get_filtered(config));

    BOOST_CHECK_EQUAL(values.size(), 2);
    BOOST_CHECK_EQUAL(values[leave_0], 19);
    BOOST_CHECK_EQUAL(values[leave_1], 23);

    // Check avg metrics
    config = {{{"test_fetch_metrics_counter", {label_leave}, agg_op::avg}}};
    values = collect_values(ssxm::get_filtered(config));

    BOOST_CHECK_EQUAL(values.size(), 2);
    BOOST_CHECK_EQUAL(values[leave_0], 18);
    BOOST_CHECK_EQUAL(values[leave_1], 23);
}
