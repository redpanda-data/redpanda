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

#pragma once

#include "base/seastarx.h"
#include "base/type_traits.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/sstring.hh>

#include <initializer_list>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>

namespace test_utils {

// Find the value of a metric by name and optional labels.
// The shard label is automatically added to the label set.
template<typename T>
std::optional<T> find_metric_value(
  std::string_view metric_name,
  int handle = ss::metrics::default_handle(),
  std::initializer_list<std::pair<ss::sstring, ss::sstring>> labels = {}) {
    auto metrics = ss::metrics::impl::get_value_map(handle);
    auto metrics_it = metrics.find(ss::sstring(metric_name));
    if (metrics_it == metrics.end()) {
        return std::nullopt;
    }

    ss::metrics::impl::labels_type label_set{
      {ss::metrics::shard_label.name(), ss::metrics::impl::shard()}};
    for (const auto& [key, value] : labels) {
        label_set.emplace(key, value);
    }

    seastar::metrics::impl::metric_family family = metrics_it->second;
    auto family_it = family.find(label_set);
    if (family_it == family.end()) {
        return std::nullopt;
    }

    ss::metrics::impl::metric_function metric_fn
      = family_it->second->get_function();
    seastar::metrics::impl::metric_value sample = metric_fn();

    if constexpr (std::is_same_v<uint64_t, T>) {
        return sample.ui();
    } else if constexpr (std::is_same_v<double, T>) {
        return sample.d();
    } else {
        static_assert(base::unsupported_type<T>::value, "unsupported type");
    }
}

} // namespace test_utils
