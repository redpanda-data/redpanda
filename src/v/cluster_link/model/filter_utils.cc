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
bool select_topic(
  ::model::topic_view topic,
  const chunked_vector<resource_name_filter_pattern>& patterns) {
    bool matched = false;
    for (const auto& pattern : patterns) {
        bool filter_selected = false;
        switch (pattern.pattern_type) {
        case filter_pattern_type::literal:
            filter_selected = (pattern.pattern
                               == resource_name_filter_pattern::wildcard)
                              || (topic == pattern.pattern);
            break;
        case filter_pattern_type::prefix:
            if (!pattern.pattern.empty()) {
                filter_selected = topic().starts_with(pattern.pattern);
            }
            break;
        }

        if (filter_selected) {
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
} // namespace cluster_link::model
