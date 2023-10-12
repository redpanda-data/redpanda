/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "utils/string_switch.h"

#include <string_view>

namespace security::audit {

enum class event_type : std::uint8_t {
    management = 0,
    produce,
    consume,
    describe,
    heartbeat,
    authenticate,
    unknown,
    num_elements
};

inline event_type string_to_event_type(const std::string_view s) {
    return string_switch<event_type>(s)
      .match("management", event_type::management)
      .match("produce", event_type::produce)
      .match("consume", event_type::consume)
      .match("describe", event_type::describe)
      .match("heartbeat", event_type::heartbeat)
      .match("authenticate", event_type::authenticate)
      .default_match(event_type::unknown);
}

} // namespace security::audit
