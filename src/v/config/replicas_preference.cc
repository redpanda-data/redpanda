// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/replicas_preference.h"

#include "absl/strings/str_split.h"
#include "absl/strings/strip.h"
#include "base/vassert.h"

#include <fmt/format.h>

#include <algorithm>
#include <concepts>
#include <sstream>
#include <stdexcept>
#include <unordered_set>
#include <utility>

namespace config {

namespace {

constexpr std::string_view parse_error_prefix
  = "couldn't parse replicas_preference: ";

template<std::integral T>
uint32_t checked_uint32_cast(T v, std::string_view what) {
    vassert(
      std::cmp_greater_equal(v, 0)
        && std::cmp_less_equal(v, std::numeric_limits<uint32_t>::max()),
      "{} value {} does not fit in uint32_t",
      what,
      v);
    return static_cast<uint32_t>(v);
}

/// Append a parsed rack token to `ret`, enforcing non-empty, no stray braces,
/// and uniqueness. Bumps `current_offset` on success.
void consume_rack(
  std::string_view rack_str,
  replicas_preference& ret,
  std::unordered_set<model::rack_id>& seen,
  uint32_t& current_offset) {
    rack_str = absl::StripAsciiWhitespace(rack_str);
    if (rack_str.empty()) {
        throw std::runtime_error(
          fmt::format("{}empty rack token", parse_error_prefix));
    }
    if (
      rack_str.find('{') != std::string_view::npos
      || rack_str.find('}') != std::string_view::npos) {
        throw std::runtime_error(
          fmt::format("{}nested braces", parse_error_prefix));
    }
    auto rack = model::rack_id(ss::sstring(rack_str));
    if (seen.contains(rack)) {
        throw std::runtime_error(
          fmt::format("{}duplicate rack", parse_error_prefix));
    }
    seen.insert(rack);
    ret.rack_ids.push_back(std::move(rack));
    vassert(
      current_offset < std::numeric_limits<uint32_t>::max(),
      "current_offset overflow");
    current_offset++;
}

} // namespace

std::optional<uint32_t>
replicas_preference::group_index_for(const model::rack_id& rack) const {
    // CSR lookup: rack_ids is a flat array; group_offsets[i] is the first
    // index in rack_ids belonging to group i (with a trailing sentinel so
    // group i spans [group_offsets[i], group_offsets[i+1])). To find the
    // owning group for a rack, locate the rack in rack_ids and take the
    // upper_bound of its index in group_offsets — the preceding entry is
    // the owning group.
    auto it = std::ranges::find(rack_ids, rack);
    if (it == rack_ids.end()) {
        return std::nullopt;
    }
    auto rack_index = checked_uint32_cast(
      std::distance(rack_ids.begin(), it), "rack index");
    auto group_iter = std::ranges::upper_bound(group_offsets, rack_index);
    vassert(
      group_iter != group_offsets.begin(),
      "group_offsets invariant broken: upper_bound at begin for rack index {}",
      rack_index);
    return checked_uint32_cast(
      std::distance(group_offsets.begin(), group_iter) - 1, "group index");
}

uint32_t replicas_preference::num_groups() const {
    if (group_offsets.empty()) {
        return 0;
    }
    // group_offsets has num_groups + 1 entries (includes sentinel)
    return checked_uint32_cast(group_offsets.size() - 1, "num_groups");
}

replicas_preference replicas_preference::parse(std::string_view s) {
    auto trimmed = absl::StripAsciiWhitespace(s);

    if (trimmed == none_prefix || trimmed.empty()) {
        return replicas_preference{};
    }

    if (!trimmed.starts_with(racks_prefix)) {
        throw std::runtime_error(
          fmt::format(
            "{}should be \"{}\" or start with \"{}\"",
            parse_error_prefix,
            none_prefix,
            racks_prefix));
    }

    auto body = trimmed.substr(racks_prefix.size());

    replicas_preference ret;
    ret.type = type_t::racks;

    std::unordered_set<model::rack_id> seen;
    bool in_group = false;
    uint32_t current_offset = 0;

    // Start the first group
    ret.group_offsets.push_back(0);

    for (std::string_view token_sv : absl::StrSplit(body, ',')) {
        auto tok = absl::StripAsciiWhitespace(token_sv);
        if (tok.empty()) {
            throw std::runtime_error(
              fmt::format("{}empty rack token", parse_error_prefix));
        }

        bool starts_group = tok.starts_with("{");
        bool ends_group = tok.ends_with("}");

        if (starts_group && ends_group) {
            // Single-element group like {A}: strip '{' prefix and '}' suffix
            // with substr(1, tok.size() - 2).
            if (in_group) {
                throw std::runtime_error(
                  fmt::format("{}nested braces", parse_error_prefix));
            }
            consume_rack(
              tok.substr(1, tok.size() - 2), ret, seen, current_offset);
            ret.group_offsets.push_back(current_offset);
        } else if (starts_group) {
            // Group-opening token: strip leading '{' with substr(1).
            if (in_group) {
                throw std::runtime_error(
                  fmt::format("{}nested braces", parse_error_prefix));
            }
            in_group = true;
            consume_rack(tok.substr(1), ret, seen, current_offset);
        } else if (ends_group) {
            // Group-closing token: strip trailing '}' with
            // substr(0, tok.size() - 1).
            if (!in_group) {
                throw std::runtime_error(
                  fmt::format(
                    "{}unexpected closing brace", parse_error_prefix));
            }
            in_group = false;
            consume_rack(
              tok.substr(0, tok.size() - 1), ret, seen, current_offset);
            ret.group_offsets.push_back(current_offset);
        } else {
            consume_rack(tok, ret, seen, current_offset);
            if (!in_group) {
                ret.group_offsets.push_back(current_offset);
            }
        }
    }

    if (in_group) {
        throw std::runtime_error(
          fmt::format("{}unclosed brace", parse_error_prefix));
    }

    if (ret.rack_ids.empty()) {
        throw std::runtime_error(
          fmt::format("{}no racks specified", parse_error_prefix));
    }

    // Canonicalize: sort rack_ids within each group so that permutations
    // like "racks: A, {C, B}" and "racks: A, {B, C}" compare equal and
    // serialize identically. Groups themselves remain in the user-specified
    // priority order.
    for (uint32_t group_idx = 0; group_idx + 1 < ret.group_offsets.size();
         ++group_idx) {
        auto group_start = ret.rack_ids.begin() + ret.group_offsets[group_idx];
        auto group_end = ret.rack_ids.begin()
                         + ret.group_offsets[group_idx + 1];
        std::ranges::sort(group_start, group_end);
    }

    return ret;
}

fmt::iterator replicas_preference::format_to(fmt::iterator it) const {
    if (type == type_t::none) {
        return fmt::format_to(it, "{}", none_str);
    }

    it = fmt::format_to(it, "{} ", racks_prefix);
    auto total_groups = num_groups();
    for (uint32_t group_idx = 0; group_idx < total_groups; ++group_idx) {
        if (group_idx > 0) {
            it = fmt::format_to(it, ", ");
        }
        auto group_start = group_offsets[group_idx];
        auto group_end = group_offsets[group_idx + 1];
        auto group_size = group_end - group_start;

        if (group_size > 1) {
            it = fmt::format_to(it, "{{");
            for (auto i = group_start; i < group_end; ++i) {
                if (i > group_start) {
                    it = fmt::format_to(it, ", ");
                }
                it = fmt::format_to(it, "{}", rack_ids[i]);
            }
            it = fmt::format_to(it, "}}");
        } else {
            it = fmt::format_to(it, "{}", rack_ids[group_start]);
        }
    }
    return it;
}

std::istream& operator>>(std::istream& is, replicas_preference& res) {
    std::stringstream ss;
    ss << is.rdbuf();
    res = replicas_preference::parse(ss.str());
    return is;
}

} // namespace config
