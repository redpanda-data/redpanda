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

namespace config {

struct leaders_preference
  : public serde::envelope<
      leaders_preference,
      serde::version<0>,
      serde::compat_version<0>> {
    enum class type_t {
        none,
        racks,
    };

    type_t type = type_t::none;
    std::vector<model::rack_id> racks;

    static leaders_preference parse(std::string_view);

    friend std::ostream& operator<<(std::ostream&, const leaders_preference&);

    friend bool operator==(const leaders_preference&, const leaders_preference&)
      = default;

    auto serde_fields() { return std::tie(type, racks); }
};

} // namespace config
