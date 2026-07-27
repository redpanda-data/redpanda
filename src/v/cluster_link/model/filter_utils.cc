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

#include "cluster_link/model/filter_utils.h"

namespace cluster_link::model {
namespace {
bool matches_pattern(
  std::string_view resource, const resource_name_filter_pattern& pattern) {
    switch (pattern.pattern_type) {
    case filter_pattern_type::literal:
        return pattern.pattern == resource_name_filter_pattern::wildcard
               || resource == pattern.pattern;
    case filter_pattern_type::prefix:
        return !pattern.pattern.empty()
               && resource.starts_with(pattern.pattern);
    }
    return false;
}

bool select_using_filter(
  std::string_view resource,
  const chunked_vector<resource_name_filter_pattern>& patterns) {
    bool matched = false;
    for (const auto& pattern : patterns) {
        if (matches_pattern(resource, pattern)) {
            switch (pattern.filter) {
            case filter_type::include:
                matched = true;
                break;
            case filter_type::exclude:
                return false;
            }
        }
    }

    return matched;
}

} // namespace
bool select_topic(
  ::model::topic_view topic,
  const chunked_vector<resource_name_filter_pattern>& patterns) {
    return select_using_filter(topic(), patterns);
}

bool select_topic_default_include(
  ::model::topic_view topic,
  const chunked_vector<resource_name_filter_pattern>& patterns) {
    bool has_include = false;
    bool include_matched = false;
    for (const auto& p : patterns) {
        const bool sel = matches_pattern(topic(), p);
        if (p.filter == filter_type::exclude) {
            if (sel) {
                return false; // exclude wins
            }
        } else {
            has_include = true;
            include_matched = include_matched || sel;
        }
    }
    return !has_include || include_matched;
}

bool select_group(
  const kafka::group_id& group_id,
  const chunked_vector<resource_name_filter_pattern>& patterns) {
    return select_using_filter(group_id(), patterns);
}

bool select_role(
  std::string_view role_name,
  const chunked_vector<resource_name_filter_pattern>& patterns) {
    return select_using_filter(role_name, patterns);
}

} // namespace cluster_link::model
