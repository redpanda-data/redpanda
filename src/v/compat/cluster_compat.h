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
#include "compat/model_json.h"

#include <vector>

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

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::config_status_request,
  { json_write(status); },
  { json_read(status); })

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::config_status_reply, { json_write(error); }, { json_read(error); })

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::cluster_property_kv,
  {
      json_write(key);
      json_write(value);
  },
  {
      json_read(key);
      json_read(value);
  });

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::config_update_request,
  {
      json_write(upsert);
      json_write(remove);
  },
  {
      json_read(upsert);
      json_read(remove);
  });

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::config_update_reply,
  {
      json_write(error);
      json_write(latest_version);
  },
  {
      json_read(error);
      json_read(latest_version);
  });

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::hello_request,
  {
      json_write(peer);
      json_write(start_time);
  },
  {
      json_read(peer);
      json_read(start_time);
  });

GEN_COMPAT_CHECK_SERDE_ONLY(
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

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::feature_action_request,
  { json_write(action); },
  { json_read(action); });

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::feature_action_response,
  { json_write(error); },
  { json_read(error); });

GEN_COMPAT_CHECK_SERDE_ONLY(
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

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::feature_barrier_response,
  {
      json_write(entered);
      json_write(complete);
  },
  {
      json_read(entered);
      json_read(complete);
  });

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::join_node_request,
  {
      json_write(latest_logical_version);
      json_write(earliest_logical_version);
      json_write(node_uuid);
      json_write(node);
  },
  {
      json_read(latest_logical_version);
      json_read(earliest_logical_version);
      json_read(node_uuid);
      json_read(node);
  });

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::join_node_reply,
  {
      json_write(success);
      json_write(id);
      json_write(raw_status);
  },
  {
      json_read(success);
      json_read(id);
      json_read(raw_status);
  })

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::decommission_node_request, { json_write(id); }, { json_read(id); })

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::decommission_node_reply,
  { json_write(error); },
  { json_read(error); })

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::recommission_node_request, { json_write(id); }, { json_read(id); })

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::recommission_node_reply,
  { json_write(error); },
  { json_read(error); })

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::finish_reallocation_request, { json_write(id); }, { json_read(id); })

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::finish_reallocation_reply,
  { json_write(error); },
  { json_read(error); })

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::set_maintenance_mode_request,
  {
      json_write(id);
      json_write(enabled);
  },
  {
      json_read(id);
      json_read(enabled);
  })

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::set_maintenance_mode_reply,
  { json_write(error); },
  { json_read(error); })

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::reconciliation_state_request,
  { json_write(ntps); },
  { json_read(ntps); });

template<>
struct compat_check<cluster::reconciliation_state_reply> {
    static constexpr std::string_view name = "reconciliation_state_reply";

    static std::vector<cluster::reconciliation_state_reply>
    create_test_cases() {
        return generate_instances<cluster::reconciliation_state_reply>();
    }

    static void to_json(
      cluster::reconciliation_state_reply obj,
      json::Writer<json::StringBuffer>& wr) {
        json::write_member(wr, "results", obj.results);
    }

    static cluster::reconciliation_state_reply from_json(json::Value& rd) {
        cluster::reconciliation_state_reply obj;
        json::read_member(rd, "results", obj.results);
        return obj;
    }

    static std::vector<compat_binary>
    to_binary(cluster::reconciliation_state_reply obj) {
        return {compat_binary::serde(std::move(obj))};
    }

    static void
    check(cluster::reconciliation_state_reply obj, compat_binary test) {
        verify_serde_only(std::move(obj), std::move(test));
    }
};

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::finish_partition_update_request,
  {
      json_write(ntp);
      json_write(new_replica_set);
  },
  {
      json_read(ntp);
      json_read(new_replica_set);
  })

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::finish_partition_update_reply,
  { json_write(result); },
  { json_read(result); })

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::cancel_partition_movements_reply,
  {
      json_write(general_error);
      json_write(partition_results);
  },
  {
      json_read(general_error);
      json_read(partition_results);
  });

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::cancel_node_partition_movements_request,
  {
      json_write(node_id);
      json_write(direction);
  },
  {
      json_read(node_id);
      json_read(direction);
  });

EMPTY_COMPAT_CHECK_SERDE_ONLY(cluster::cancel_all_partition_movements_request);

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::configuration_update_request,
  {
      json_write(node);
      json_write(target_node);
  },
  {
      json_read(node);
      json_read(target_node);
  })

GEN_COMPAT_CHECK_SERDE_ONLY(
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
/**
 * Custom check for topic_properties as ADL version will not include the
 * batch_max_bytes property. It is cleared away when serializing properties
 */
template<>
struct compat_check<cluster::topic_properties> {
    static constexpr std::string_view name = "cluster::topic_properties";

    static std::vector<cluster::topic_properties> create_test_cases() {
        return generate_instances<cluster::topic_properties>();
    }

    static void to_json(
      cluster::topic_properties obj, json::Writer<json::StringBuffer>& wr) {
        json::write_exceptional_member_type(wr, "compression", obj.compression);
        json::write_exceptional_member_type(
          wr, "cleanup_policy_bitflags", obj.cleanup_policy_bitflags);
        json_write(compaction_strategy);
        json::write_exceptional_member_type(
          wr, "timestamp_type", obj.timestamp_type);
        json_write(segment_size);
        json_write(retention_bytes);
        json_write(retention_duration);
        json_write(recovery);
        json_write(shadow_indexing);
        json_write(read_replica);
        json_write(read_replica_bucket);
        json_write(remote_topic_properties);
        json_write(batch_max_bytes);
        json_write(retention_local_target_bytes);
        json_write(retention_local_target_ms);
        json_write(remote_delete);
        json_write(segment_ms);
        json_write(record_key_schema_id_validation);
        json_write(record_key_schema_id_validation_compat);
        json_write(record_key_subject_name_strategy);
        json_write(record_key_subject_name_strategy_compat);
        json_write(record_value_schema_id_validation);
        json_write(record_value_schema_id_validation_compat);
        json_write(record_value_subject_name_strategy);
        json_write(record_value_subject_name_strategy_compat);
        json_write(initial_retention_local_target_bytes);
        json_write(initial_retention_local_target_ms);
        json_write(mpx_virtual_cluster_id);
        json::write_exceptional_member_type(
          wr, "write_caching", obj.write_caching);
        json_write(flush_ms);
        json_write(flush_bytes);
        json_write(remote_topic_namespace_override);
    }

    static cluster::topic_properties from_json(json::Value& rd) {
        cluster::topic_properties obj;
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
        json_read(batch_max_bytes);
        json_read(retention_local_target_bytes);
        json_read(retention_local_target_ms);
        json_read(remote_delete);
        json_read(segment_ms);
        json_read(record_key_schema_id_validation);
        json_read(record_key_schema_id_validation_compat);
        json_read(record_key_subject_name_strategy);
        json_read(record_key_subject_name_strategy_compat);
        json_read(record_value_schema_id_validation);
        json_read(record_value_schema_id_validation_compat);
        json_read(record_value_subject_name_strategy);
        json_read(record_value_subject_name_strategy_compat);
        json_read(initial_retention_local_target_bytes);
        json_read(initial_retention_local_target_ms);
        json_read(mpx_virtual_cluster_id);
        json_read(write_caching);
        json_read(flush_ms);
        json_read(flush_bytes);
        json_read(remote_topic_namespace_override);
        return obj;
    }

    static std::vector<compat_binary> to_binary(cluster::topic_properties obj) {
        return compat_binary::serde_and_adl(obj);
    }

    static void check(cluster::topic_properties obj, compat_binary test) {
        if (test.name == "serde") {
            verify_serde_only(obj, test);
            return;
        }
        vassert(test.name == "adl", "Unknown compat_binary format encountered");
        iobuf_parser iobp(std::move(test.data));
        auto reply = reflection::adl<cluster::topic_properties>{}.from(iobp);

        obj.batch_max_bytes = std::nullopt;
        obj.retention_local_target_bytes = tristate<size_t>{std::nullopt};
        obj.retention_local_target_ms = tristate<std::chrono::milliseconds>{
          std::nullopt};

        obj.segment_ms = tristate<std::chrono::milliseconds>{std::nullopt};
        obj.initial_retention_local_target_bytes = tristate<size_t>{
          std::nullopt};
        obj.initial_retention_local_target_ms
          = tristate<std::chrono::milliseconds>{std::nullopt};
        obj.mpx_virtual_cluster_id = std::nullopt;
        obj.write_caching = std::nullopt;
        obj.flush_bytes = std::nullopt;
        obj.flush_ms = std::nullopt;
        obj.remote_topic_namespace_override = std::nullopt;

        if (reply != obj) {
            throw compat_error(fmt::format(
              "Verify of {{cluster::topic_properties}} ADL decoding "
              "failed:\n Expected: {}\nDecoded: {}",
              obj,
              reply));
        }
    }
};

/// adl deserializer will not interpret the `read_replica`,
/// `read_replica_bucket`, `remote_topic_properties` fields and any
/// future fields. 'obj' will contain these fields populated due to the
/// default implementation of its respective json fuzzers. Remove those
/// fields from comparisons when checking against adl compat.

template<>
struct compat_check<cluster::topic_configuration> {
    static constexpr std::string_view name = "cluster::topic_configuration";

    static cluster::topic_configuration_vector create_test_cases() {
        auto i = generate_instances<cluster::topic_configuration>();
        return {
          std::make_move_iterator(i.begin()), std::make_move_iterator(i.end())};
    }

    static void to_json(
      cluster::topic_configuration obj, json::Writer<json::StringBuffer>& wr) {
        json_write(tp_ns);
        json_write(partition_count);
        json_write(replication_factor);
        json_write(is_migrated);
        json_write(properties);
    }

    static cluster::topic_configuration from_json(json::Value& rd) {
        cluster::topic_configuration obj;
        json_read(tp_ns);
        json_read(partition_count);
        json_read(replication_factor);
        json_read(is_migrated);
        json_read(properties);
        return obj;
    }

    static std::vector<compat_binary>
    to_binary(cluster::topic_configuration obj) {
        return compat_binary::serde_and_adl(obj);
    }

    static void check(cluster::topic_configuration obj, compat_binary test) {
        if (test.name == "serde") {
            verify_serde_only(obj, test);
            return;
        }
        vassert(test.name == "adl", "Unknown compat_binary format encounterd");
        iobuf_parser iobp(std::move(test.data));
        auto cfg = reflection::adl<cluster::topic_configuration>{}.from(iobp);
        obj.properties.read_replica = std::nullopt;
        obj.properties.read_replica_bucket = std::nullopt;
        obj.properties.remote_topic_namespace_override = std::nullopt;
        obj.properties.remote_topic_properties = std::nullopt;
        obj.properties.batch_max_bytes = std::nullopt;
        obj.properties.retention_local_target_bytes = tristate<size_t>{
          std::nullopt};
        obj.properties.retention_local_target_ms
          = tristate<std::chrono::milliseconds>{std::nullopt};

        // ADL will always squash remote_delete to false
        obj.properties.remote_delete = false;

        obj.properties.segment_ms = tristate<std::chrono::milliseconds>{
          std::nullopt};

        obj.properties.initial_retention_local_target_bytes = tristate<size_t>{
          std::nullopt};
        obj.properties.initial_retention_local_target_ms
          = tristate<std::chrono::milliseconds>{std::nullopt};
        obj.properties.write_caching = std::nullopt;
        obj.properties.flush_bytes = std::nullopt;
        obj.properties.flush_ms = std::nullopt;

        obj.properties.mpx_virtual_cluster_id = std::nullopt;

        // ADL will always squash is_migrated to false
        obj.is_migrated = false;

        if (cfg != obj) {
            throw compat_error(fmt::format(
              "Verify of {{cluster::topic_property}} adl decoding "
              "failed:\n Expected: {}\nDecoded: {}",
              obj,
              cfg));
        }
    }
};

template<>
struct compat_check<cluster::create_topics_request> {
    static constexpr std::string_view name = "cluster::create_topics_request";

    static std::vector<cluster::create_topics_request> create_test_cases() {
        return generate_instances<cluster::create_topics_request>();
    }

    static void to_json(
      cluster::create_topics_request obj,
      json::Writer<json::StringBuffer>& wr) {
        json_write(topics);
        json_write(timeout);
    }

    static cluster::create_topics_request from_json(json::Value& rd) {
        cluster::create_topics_request obj;
        json_read(topics);
        json_read(timeout);
        return obj;
    }

    static std::vector<compat_binary>
    to_binary(cluster::create_topics_request obj) {
        return {compat_binary::serde(std::move(obj))};
    }

    static void check(cluster::create_topics_request obj, compat_binary test) {
        if (test.name == "serde") {
            verify_serde_only(obj.copy(), test);
            return;
        }
    }
};

template<>
struct compat_check<cluster::create_topics_reply> {
    static constexpr std::string_view name = "cluster::create_topics_reply";

    static std::vector<cluster::create_topics_reply> create_test_cases() {
        return generate_instances<cluster::create_topics_reply>();
    }

    static void to_json(
      cluster::create_topics_reply obj, json::Writer<json::StringBuffer>& wr) {
        json_write(results);
        json_write(metadata);
        json_write(configs);
    }

    static cluster::create_topics_reply from_json(json::Value& rd) {
        cluster::create_topics_reply obj;
        json_read(results);
        json_read(metadata);
        json_read(configs);
        return obj;
    }

    static std::vector<compat_binary>
    to_binary(cluster::create_topics_reply obj) {
        return {compat_binary::serde(std::move(obj))};
    }

    static void check(cluster::create_topics_reply obj, compat_binary test) {
        if (test.name == "serde") {
            verify_serde_only(obj.copy(), test);
            return;
        }
    }
};

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
      json_write(get_shadow_indexing());
      json_write(remote_delete);
  },
  {
      json_read(compression);
      json_read(cleanup_policy_bitflags);
      json_read(compaction_strategy);
      json_read(timestamp_type);
      json_read(segment_size);
      json_read(retention_bytes);
      json_read(retention_duration);
      json_read(get_shadow_indexing());
      json_read(remote_delete);
  })

GEN_COMPAT_CHECK(
  cluster::topic_properties_update,
  {
      json_write(tp_ns);
      json_write(properties);
      json_write(custom_properties);
  },
  {
      json_read(tp_ns);
      json_read(properties);
      json_read(custom_properties);
  })

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::update_topic_properties_request,
  { json_write(updates); },
  { json_read(updates); })
GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::update_topic_properties_reply,
  { json_write(results); },
  { json_read(results); })

} // namespace compat
