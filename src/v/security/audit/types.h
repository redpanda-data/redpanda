/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once
#include "strings/string_switch.h"

#include <string_view>

namespace security::audit {

enum class event_type : std::uint8_t {
    management = 0,
    produce,
    consume,
    describe,
    heartbeat,
    authenticate,
    admin,
    schema_registry,
    unknown,
    num_elements
};

std::ostream& operator<<(std::ostream&, event_type);

inline event_type string_to_event_type(const std::string_view s) {
    return string_switch<event_type>(s)
      .match("management", event_type::management)
      .match("produce", event_type::produce)
      .match("consume", event_type::consume)
      .match("describe", event_type::describe)
      .match("heartbeat", event_type::heartbeat)
      .match("authenticate", event_type::authenticate)
      .match("admin", event_type::admin)
      .match("schema_registry", event_type::schema_registry)
      .default_match(event_type::unknown);
}

} // namespace security::audit
