/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/metadata.h"
#include "serde/envelope.h"

#include <stdexcept>
#include <string_view>
#include <utility>

namespace config {

struct leaders_preference
  : public serde::envelope<
      leaders_preference,
      serde::version<0>,
      serde::compat_version<0>> {
    enum class type_t {
        // no preferece
        none,
        // user specifies a set of desired racks with no preference between them
        racks,
        // user specifies an ordered list of racks where closer to the beginning
        // of the list is better
        ordered_racks,
    };

    // names for logging
    static constexpr std::string_view none_str = "none";
    static constexpr std::string_view racks_str = "racks";
    static constexpr std::string_view ordered_racks_str = "ordered_racks";

    // prefixes for parsing from config string
    static constexpr std::string_view none_prefix = "none";
    static constexpr std::string_view racks_prefix = "racks:";
    static constexpr std::string_view ordered_racks_prefix = "ordered_racks:";

    static constexpr std::string_view type_to_prefix(type_t t) {
        switch (t) {
        case type_t::none:
            return none_str;
        case type_t::racks:
            return racks_prefix;
        case type_t::ordered_racks:
            return ordered_racks_prefix;
        default:
            throw std::invalid_argument("unknown leaders_preference type");
        }
        std::unreachable();
    }

    static constexpr std::string_view type_to_string(type_t t) {
        switch (t) {
        case type_t::none:
            return none_str;
        case type_t::racks:
            return racks_str;
        case type_t::ordered_racks:
            return ordered_racks_str;
        default:
            throw std::invalid_argument("unknown leaders_preference type");
        }
        std::unreachable();
    }

    type_t type = type_t::none;
    std::vector<model::rack_id> racks;

    static leaders_preference parse(std::string_view);

    friend std::ostream& operator<<(std::ostream&, type_t);
    friend std::ostream& operator<<(std::ostream&, const leaders_preference&);
    friend std::istream& operator>>(std::istream&, leaders_preference&);

    friend bool
    operator==(const leaders_preference&, const leaders_preference&) = default;

    auto serde_fields() { return std::tie(type, racks); }
};

} // namespace config
