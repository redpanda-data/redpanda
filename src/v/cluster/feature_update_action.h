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

#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/sstring.h"

#include <seastar/core/sstring.hh>

#include <iosfwd>

namespace cluster {

struct feature_update_action
  : serde::envelope<
      feature_update_action,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 1;
    enum class action_t : std::uint16_t {
        // Notify when a feature is done with preparing phase
        complete_preparing = 1,
        // Notify when a feature is made available, either by an administrator
        // or via auto-activation policy
        activate = 2,
        // Notify when a feature is explicitly disabled by an administrator
        deactivate = 3
    };

    // Features have an internal bitflag representation, but it is not
    // meant to be stable for use on the wire, so we refer to features by name
    ss::sstring feature_name;
    action_t action;

    friend bool
    operator==(const feature_update_action&, const feature_update_action&)
      = default;

    auto serde_fields() { return std::tie(feature_name, action); }

    friend std::ostream&
    operator<<(std::ostream&, const feature_update_action&);
};

} // namespace cluster
