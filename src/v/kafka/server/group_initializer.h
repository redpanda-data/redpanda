/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/topic_configuration.h"
#include "kafka/server/coordinator_ntp_mapper.h"
/*
 * This class creates consumer offsets topic
 */
namespace kafka {

/// \brief The intended configuration of the consumer group metadata topic:
/// compacted, with group_topic_partitions partitions. Creation is permitted
/// while a consumer group migration is in progress.
cluster::topic_configuration consumer_offsets_topic_configuration(
  model::topic_namespace tp_ns, int16_t replication_factor);

class group_initializer {
public:
    group_initializer(
      coordinator_ntp_mapper& coordinator_ntp_mapper,
      ss::sharded<cluster::topics_frontend>& topics_frontend,
      ss::sharded<cluster::members_table>& members_table,
      ss::sharded<cluster::controller_api>& controller_api);

    ss::future<bool> assure_topic_exists(
      bool wait_for_reconciliation = false,
      model::timeout_clock::time_point reconciliation_deadline
      = model::timeout_clock::time_point::max());

private:
    ss::future<bool> try_create_consumer_group_topic();

    coordinator_ntp_mapper& _coordinator_ntp_mapper;
    ss::sharded<cluster::topics_frontend>& _topics_frontend;
    ss::sharded<cluster::members_table>& _members_table;
    ss::sharded<cluster::controller_api>& _controller_api;
};
} // namespace kafka
