/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/vlog_callsite.h"

#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace vlog {

/// A rule for gating vlog callsites. Every populated predicate must match
/// for the rule to apply: the file field is a fnmatch(3) glob tested
/// against the basename-preserved __FILE__ path, line is an inclusive
/// [lo, hi] range the callsite's __LINE__ must fall within (lo == hi for
/// an exact-match rule), and contains must appear as a substring of the
/// format-string literal. An empty rule (no predicates set) matches every
/// callsite. The rule's state field is the value written onto every site
/// it matches; a callsite matching no rule resolves to default_.
struct rule {
    std::optional<std::string> file;
    std::optional<std::pair<unsigned, unsigned>> line;
    std::optional<std::string> contains;
    detail::callsite_base::state state = detail::callsite_base::state::default_;
};

/// Replace the active rule set and re-evaluate every registered callsite
/// against it. Rules are applied in order; the last matching rule's
/// state value wins. A site that matches no rule resolves to default_.
void apply_rules(std::vector<rule> rules);

/// Snapshot of the currently active rule set, in application order.
std::vector<rule> get_rules();

/// Shortcut for apply_rules({}): resets every callsite to default_.
void reset_rules();

/// Visit every registered callsite. The registry is lock-free: this is a
/// plain traversal of a singly-linked list, safe to call concurrently with
/// new callsites being registered by logger threads (though newly-added
/// sites may or may not appear in this walk).
void for_each_callsite(std::function<void(detail::callsite_base&)> fn);

} // namespace vlog
