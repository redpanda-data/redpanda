/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/audit/audit_log_topic.h"

#include "kafka/protocol/topic_properties.h"
#include "kafka/server/handlers/configs/config_response_utils.h"
#include "model/fundamental.h"

#include <chrono>

namespace security::audit {

cluster::topic_properties audit_log_topic_properties() {
    using namespace std::chrono_literals;
    constexpr std::chrono::milliseconds seven_days = 604800000ms;

    cluster::topic_properties props;
    props.retention_bytes = tristate<size_t>{};
    props.retention_duration = tristate<std::chrono::milliseconds>{seven_days};
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion;
    return props;
}

std::vector<kafka::createable_topic_config> audit_log_topic_configs() {
    auto props = audit_log_topic_properties();
    return {
      {.name = ss::sstring(kafka::topic_property_retention_bytes),
       .value = kafka::maybe_print_tristate(props.retention_bytes)},
      {.name = ss::sstring(kafka::topic_property_retention_duration),
       .value = kafka::maybe_print_tristate(props.retention_duration)},
      {.name = ss::sstring(kafka::topic_property_cleanup_policy),
       .value = kafka::describe_as_string(
         props.cleanup_policy_bitflags.value())}};
}

} // namespace security::audit
