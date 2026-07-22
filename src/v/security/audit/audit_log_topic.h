/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster/topic_properties.h"
#include "kafka/protocol/schemata/create_topics_request.h"

namespace security::audit {

/// \brief The intended properties of the audit log topic
/// (_redpanda.audit_log): delete cleanup policy with a fixed retention.
cluster::topic_properties audit_log_topic_properties();

/// \brief The same properties expressed as Kafka CreateTopics config
/// entries, for the creation path that goes through the Kafka API.
std::vector<kafka::createable_topic_config> audit_log_topic_configs();

} // namespace security::audit
