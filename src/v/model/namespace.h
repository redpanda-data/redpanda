/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "model/fundamental.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <seastar/core/smp.hh>

namespace model {

inline const model::ns redpanda_ns("redpanda");

inline const model::ntp controller_ntp(
  redpanda_ns, model::topic("controller"), model::partition_id(0));

/*
 * The kvstore is organized as an ntp with a partition per core.
 */
inline const model::topic kvstore_topic("kvstore");
inline model::ntp kvstore_ntp(ss::shard_id shard) {
    return {redpanda_ns, kvstore_topic, model::partition_id(shard)};
}

inline const model::ns kafka_namespace("kafka");

inline const model::ns kafka_internal_namespace("kafka_internal");

inline const model::topic kafka_consumer_offsets_topic("__consumer_offsets");

inline const model::topic_namespace kafka_consumer_offsets_nt(
  model::kafka_namespace, kafka_consumer_offsets_topic);

inline const model::topic
  coprocessor_internal_topic("coprocessor_internal_topic");

inline const model::topic_partition coprocessor_internal_tp{
  coprocessor_internal_topic, model::partition_id(0)};

inline const model::topic tx_manager_topic("tx");
inline const model::topic_namespace
  tx_manager_nt(model::kafka_internal_namespace, tx_manager_topic);
// Previously we had only one partition in tm.
// Now we support multiple partitions.
// legacy_tm_ntp exists to support previous behaviour
inline const model::ntp legacy_tm_ntp(
  model::kafka_internal_namespace,
  model::tx_manager_topic,
  model::partition_id(0));

inline const model::topic id_allocator_topic("id_allocator");
inline const model::topic_namespace
  id_allocator_nt(model::kafka_internal_namespace, id_allocator_topic);
inline const model::ntp id_allocator_ntp(
  model::kafka_internal_namespace,
  model::id_allocator_topic,
  model::partition_id(0));

inline const model::topic_partition schema_registry_internal_tp{
  model::topic{"_schemas"}, model::partition_id{0}};

} // namespace model
