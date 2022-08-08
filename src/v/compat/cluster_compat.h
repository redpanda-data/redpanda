/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/types.h"
#include "compat/check.h"
#include "compat/cluster_generator.h"
#include "compat/cluster_json.h"
#include "compat/json.h"

namespace compat {

GEN_COMPAT_CHECK(
  cluster::config_status,
  {
      json_write(node);
      json_write(version);
      json_write(restart);
      json_write(unknown);
      json_write(invalid);
  },
  {
      json_read(node);
      json_read(version);
      json_read(restart);
      json_read(unknown);
      json_read(invalid);
  });

GEN_COMPAT_CHECK(
  cluster::cluster_property_kv,
  {
      json_write(key);
      json_write(value);
  },
  {
      json_read(key);
      json_read(value);
  });

GEN_COMPAT_CHECK(
  cluster::config_update_request,
  {
      json_write(upsert);
      json_write(remove);
  },
  {
      json_read(upsert);
      json_read(remove);
  });

GEN_COMPAT_CHECK(
  cluster::config_update_reply,
  {
      json_write(error);
      json_write(latest_version);
  },
  {
      json_read(error);
      json_read(latest_version);
  });

GEN_COMPAT_CHECK(
  cluster::hello_request,
  {
      json_write(peer);
      json_write(start_time);
  },
  {
      json_read(peer);
      json_read(start_time);
  });

GEN_COMPAT_CHECK(
  cluster::hello_reply, { json_write(error); }, { json_read(error); });

GEN_COMPAT_CHECK(
  cluster::feature_update_action,
  {
      json_write(feature_name);
      json_write(action);
  },
  {
      json_read(feature_name);
      json_read(action);
  });

GEN_COMPAT_CHECK(
  cluster::feature_action_request,
  { json_write(action); },
  { json_read(action); });

GEN_COMPAT_CHECK(
  cluster::feature_action_response,
  { json_write(error); },
  { json_read(error); });

GEN_COMPAT_CHECK(
  cluster::feature_barrier_request,
  {
      json_write(tag);
      json_write(peer);
      json_write(entered);
  },
  {
      json_read(tag);
      json_read(peer);
      json_read(entered);
  });

GEN_COMPAT_CHECK(
  cluster::feature_barrier_response,
  {
      json_write(entered);
      json_write(complete);
  },
  {
      json_read(entered);
      json_read(complete);
  });

GEN_COMPAT_CHECK(
  cluster::join_request, { json_write(node); }, { json_read(node); });

GEN_COMPAT_CHECK(
  cluster::join_reply, { json_write(success); }, { json_read(success); });

GEN_COMPAT_CHECK(
  cluster::join_node_request,
  {
      json_write(logical_version);
      json_write(node_uuid);
      json_write(node);
  },
  {
      json_read(logical_version);
      json_read(node_uuid);
      json_read(node);
  });

GEN_COMPAT_CHECK(
  cluster::join_node_reply,
  {
      json_write(success);
      json_write(id);
  },
  {
      json_read(success);
      json_read(id);
  })

GEN_COMPAT_CHECK(
  cluster::decommission_node_request, { json_write(id); }, { json_read(id); })

GEN_COMPAT_CHECK(
  cluster::decommission_node_reply,
  { json_write(error); },
  { json_read(error); })

GEN_COMPAT_CHECK(
  cluster::recommission_node_request, { json_write(id); }, { json_read(id); })

GEN_COMPAT_CHECK(
  cluster::recommission_node_reply,
  { json_write(error); },
  { json_read(error); })

GEN_COMPAT_CHECK(
  cluster::finish_reallocation_request, { json_write(id); }, { json_read(id); })

GEN_COMPAT_CHECK(
  cluster::finish_reallocation_reply,
  { json_write(error); },
  { json_read(error); })

GEN_COMPAT_CHECK(
  cluster::set_maintenance_mode_request,
  {
      json_write(id);
      json_write(enabled);
  },
  {
      json_read(id);
      json_read(enabled);
  })

GEN_COMPAT_CHECK(
  cluster::set_maintenance_mode_reply,
  { json_write(error); },
  { json_read(error); })

GEN_COMPAT_CHECK(
  cluster::reconciliation_state_request,
  { json_write(ntps); },
  { json_read(ntps); });

GEN_COMPAT_CHECK(
  cluster::reconciliation_state_reply,
  { json_write(results); },
  { json_read(results); });

GEN_COMPAT_CHECK(
  cluster::create_non_replicable_topics_request,
  {
      json_write(topics);
      json_write(timeout);
  },
  {
      json_read(topics);
      json_read(timeout);
  })

GEN_COMPAT_CHECK(
  cluster::create_non_replicable_topics_reply,
  { json_write(results); },
  { json_read(results); });

GEN_COMPAT_CHECK(
  cluster::finish_partition_update_request,
  {
      json_write(ntp);
      json_write(new_replica_set);
  },
  {
      json_read(ntp);
      json_read(new_replica_set);
  })

GEN_COMPAT_CHECK(
  cluster::finish_partition_update_reply,
  { json_write(result); },
  { json_read(result); })

GEN_COMPAT_CHECK(
  cluster::cancel_partition_movements_reply,
  {
      json_write(general_error);
      json_write(partition_results);
  },
  {
      json_read(general_error);
      json_read(partition_results);
  });

GEN_COMPAT_CHECK(
  cluster::cancel_node_partition_movements_request,
  {
      json_write(node_id);
      json_write(direction);
  },
  {
      json_read(node_id);
      json_read(direction);
  });

EMPTY_COMPAT_CHECK(cluster::cancel_all_partition_movements_request);

GEN_COMPAT_CHECK(
  cluster::configuration_update_request,
  {
      json_write(node);
      json_write(target_node);
  },
  {
      json_read(node);
      json_read(target_node);
  })

GEN_COMPAT_CHECK(
  cluster::configuration_update_reply,
  { json_write(success); },
  { json_read(success); })

GEN_COMPAT_CHECK(
  cluster::remote_topic_properties,
  {
      json_write(remote_revision);
      json_write(remote_partition_count);
  },
  {
      json_read(remote_revision);
      json_read(remote_partition_count);
  })

GEN_COMPAT_CHECK(
  cluster::topic_properties,
  {
      json_write(compression);
      json_write(cleanup_policy_bitflags);
      json_write(compaction_strategy);
      json_write(timestamp_type);
      json_write(segment_size);
      json_write(retention_bytes);
      json_write(retention_duration);
      json_write(recovery);
      json_write(shadow_indexing);
      json_write(read_replica);
      json_write(read_replica_bucket);
      json_write(remote_topic_properties);
  },
  {
      json_read(compression);
      json_read(cleanup_policy_bitflags);
      json_read(compaction_strategy);
      json_read(timestamp_type);
      json_read(segment_size);
      json_read(retention_bytes);
      json_read(retention_duration);
      json_read(recovery);
      json_read(shadow_indexing);
      json_read(read_replica);
      json_read(read_replica_bucket);
      json_read(remote_topic_properties);
  })

GEN_COMPAT_CHECK(
  cluster::topic_configuration,
  {
      json_write(tp_ns);
      json_write(partition_count);
      json_write(replication_factor);
      json_write(properties);
  },
  {
      json_read(tp_ns);
      json_read(partition_count);
      json_read(replication_factor);
      json_read(properties);
  })

GEN_COMPAT_CHECK(
  cluster::create_topics_request,
  {
      json_write(topics);
      json_write(timeout);
  },
  {
      json_read(topics);
      json_read(timeout);
  })

GEN_COMPAT_CHECK(
  cluster::topic_result,
  {
      json_write(tp_ns);
      json_write(ec);
  },
  {
      json_read(tp_ns);
      json_read(ec);
  });

GEN_COMPAT_CHECK(
  cluster::create_topics_reply,
  {
      json_write(results);
      json_write(metadata);
      json_write(configs);
  },
  {
      json_read(results);
      json_read(metadata);
      json_read(configs);
  })

GEN_COMPAT_CHECK(
  v8_engine::data_policy,
  {
      json_write(fn_name);
      json_write(sct_name);
  },
  {
      json_read(fn_name);
      json_read(sct_name);
  })

GEN_COMPAT_CHECK(
  cluster::incremental_topic_custom_updates,
  { json_write(data_policy); },
  { json_read(data_policy); })

GEN_COMPAT_CHECK(
  cluster::incremental_topic_updates,
  {
      json_write(compression);
      json_write(cleanup_policy_bitflags);
      json_write(compaction_strategy);
      json_write(timestamp_type);
      json_write(segment_size);
      json_write(retention_bytes);
      json_write(retention_duration);
      json_write(shadow_indexing);
  },
  {
      json_read(compression);
      json_read(cleanup_policy_bitflags);
      json_read(compaction_strategy);
      json_read(timestamp_type);
      json_read(segment_size);
      json_read(retention_bytes);
      json_read(retention_duration);
      json_read(shadow_indexing);
  })

} // namespace compat
