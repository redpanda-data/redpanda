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

#include "base/format_to.h"
#include "model/metadata.h"
#include "serde/envelope.h"

#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

namespace config {

/// Per-topic replica placement preference using priority-ordered rack groups.
///
/// Format: "racks: A, {B, C}, D" means prefer rack A (highest priority),
/// then racks B and C (equal priority), then rack D.
///
/// Uses CSR-style storage: a flat rack_ids array with group_offsets indexing
/// into it. For the example above:
///   rack_ids = [A, B, C, D], group_offsets = [0, 1, 3, 4]
struct replicas_preference
  : public serde::envelope<
      replicas_preference,
      serde::version<0>,
      serde::compat_version<0>> {
    enum class type_t {
        none,
        racks,
    };

    static constexpr std::string_view none_str = "none";
    static constexpr std::string_view racks_str = "racks";
    static constexpr std::string_view none_prefix = "none";
    static constexpr std::string_view racks_prefix = "racks:";

    type_t type = type_t::none;

    // CSR-style storage for priority-ordered rack groups.
    std::vector<model::rack_id> rack_ids;
    std::vector<uint32_t> group_offsets;

    /// Returns the priority group index for a rack, or nullopt if not found.
    std::optional<uint32_t> group_index_for(const model::rack_id&) const;

    /// Number of priority groups.
    uint32_t num_groups() const;

    static replicas_preference parse(std::string_view);

    fmt::iterator format_to(fmt::iterator it) const;
    friend std::istream& operator>>(std::istream&, replicas_preference&);

    friend bool operator==(
      const replicas_preference&, const replicas_preference&) = default;

    auto serde_fields() { return std::tie(type, rack_ids, group_offsets); }
};

} // namespace config
