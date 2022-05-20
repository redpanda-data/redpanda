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

#include "ssx/metrics.h"

#include <seastar/core/metrics_api.hh>
#include <seastar/core/metrics_registration.hh>

#include <absl/algorithm/container.h>

#include <numeric>
#include <optional>
#include <tuple>
#include <vector>

namespace ssx::metrics {

namespace ssm = ss::metrics;
namespace ssmi = ssm::impl;
using agg_op = ssx::metrics::filter::config::agg_op;
using registered_instances = std::vector<ssmi::register_ref>;
// Store a mapping of allowed labels to aggregated registered_metrics
using metric_multi_instance = std::map<ssmi::labels_type, registered_instances>;

ssmi::metric_function get_op(agg_op op, registered_instances refs) {
    switch (op) {
    case agg_op::sum:
        return [refs{std::move(refs)}]() {
            auto type = refs.front()->get_function()().type();
            return absl::c_accumulate(
              refs, ssmi::metric_value(0, type), [](auto lhs, auto rhs) {
                  return lhs + rhs->get_function()();
              });
        };
    case agg_op::count:
        return [s = static_cast<double>(refs.size())]() {
            return ssmi::metric_value(s, ssmi::data_type::ABSOLUTE);
        };
    case agg_op::min:
        return [refs{std::move(refs)}]() {
            auto it = absl::c_min_element(refs, [](auto lhs, auto rhs) {
                return lhs->get_function()().d() < rhs->get_function()().d();
            });
            return (*it)->get_function()();
        };
    case agg_op::max:
        return [refs{std::move(refs)}]() {
            auto it = absl::c_max_element(refs, [](auto lhs, auto rhs) {
                return lhs->get_function()().d() < rhs->get_function()().d();
            });
            return (*it)->get_function()();
        };
    case agg_op::avg:
        return [refs{std::move(refs)}]() {
            auto type = refs.front()->get_function()().type();
            auto sum = absl::c_accumulate(
              refs, ssmi::metric_value{0, type}, [](auto lhs, auto rhs) {
                  return lhs + rhs->get_function()();
              });
            return ssmi::metric_value{
              sum.d() / static_cast<double>(refs.size()), type};
        };
    };
}

ss::metrics::impl::value_map get_filtered(const filter::config& cfg) {
    // The structure looks like this:
    // ssmi::value_map exemplar{
    //   {"test_fetch_metrics_counter",
    //    ssmi::metric_family{
    //      ssmi::metric_instances{
    //        {ssmi::labels_type{
    //           {"aggregate", "0"}, {"leave", "0"}, {"shard", "0"}},
    //         ssmi::register_ref{}},
    //        {ssmi::labels_type{
    //           {"aggregate", "1"}, {"leave", "0"}, {"shard", "0"}},
    //         ssmi::register_ref{}}},
    //      ssmi::metric_family_info{
    //        ssmi::data_type::COUNTER,
    //        ssm::metric_type_def{""},
    //        ssm::description{"Test a counter"},
    //        ss::sstring{"test_fetch_metrics_counter"}}}}};

    // Collect metric families
    ssmi::value_map result;
    for (const auto& [name, family] : ssmi::get_value_map()) {
        auto m_it = absl::c_find_if(
          cfg.allow, [&n = name](const auto& m) { return m.name == n; });

        // Ignore metrics that are not allowed.
        if (m_it == cfg.allow.end()) {
            continue;
        }

        const auto& metric_cfg{*m_it};

        // Collect aggregated instances
        metric_multi_instance new_instances;
        for (const auto& [base_labels, base_metric] : family) {
            ssmi::labels_type new_labels;
            for (const auto& [l_name, l_value] : base_labels) {
                auto l_it = absl::c_find_if(
                  metric_cfg.allow, [lhs = l_name](const auto& rhs) {
                      return lhs == rhs.name();
                  });
                if (l_it != metric_cfg.allow.end()) {
                    new_labels.emplace(l_name, l_value);
                }
            }
            new_instances[new_labels].emplace_back(base_metric);
        }

        // Convert aggregated instances to registred metrics
        ssmi::metric_instances family_instances;
        for (auto& ni : new_instances) {
            auto id = ni.second.front()->get_id();
            auto op = metric_cfg.op.value_or(agg_op::sum);

            family_instances.emplace(
              ni.first,
              ss::make_shared<ssmi::registered_metric>(
                std::move(id), get_op(op, std::move(ni.second))));
        }
        auto family_info = family.info();
        result.emplace(
          name,
          ssmi::metric_family{
            std::move(family_instances), std::move(family_info)});
    }
    return result;
}

} // namespace ssx::metrics
