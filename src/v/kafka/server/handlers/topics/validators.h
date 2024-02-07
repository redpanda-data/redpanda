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
#include "cluster/types.h"
#include "config/configuration.h"
#include "kafka/protocol/schemata/create_topics_request.h"
#include "kafka/protocol/schemata/create_topics_response.h"
#include "kafka/server/handlers/topics/types.h"
#include "model/metadata.h"
#include "model/namespace.h"

namespace kafka {
template<typename Request, typename T>
concept RequestValidator = requires(T validator, const Request& request) {
    { T::is_valid(request) } -> std::same_as<bool>;
    { T::ec } -> std::convertible_to<const error_code&>;
    { T::error_message } -> std::convertible_to<const char*>;
};

template<typename Request, typename... Ts>
struct validator_type_list {};

template<typename Request, typename... Validators>
requires(RequestValidator<Request, Validators>, ...)
using make_validator_types = validator_type_list<Request, Validators...>;

struct custom_partition_assignment_negative_partition_count {
    static constexpr error_code ec = error_code::invalid_request;
    static constexpr const char* error_message
      = "For custom partition assignment, partition count and replication "
        "factor must be equal to -1";

    static bool is_valid(const creatable_topic& c) {
        if (!c.assignments.empty()) {
            return c.num_partitions == -1 && c.replication_factor == -1;
        }

        return true;
    };
};

struct replicas_diversity {
    static constexpr error_code ec = error_code::invalid_request;
    static constexpr const char* error_message
      = "Topic partition replica set must contain unique nodes";

    static bool is_valid(const creatable_topic& c) {
        if (c.assignments.empty()) {
            return true;
        }
        return std::all_of(
          c.assignments.begin(),
          c.assignments.end(),
          [](const creatable_replica_assignment& cra) {
              auto ids = cra.broker_ids;
              std::sort(ids.begin(), ids.end());
              auto last = std::unique(ids.begin(), ids.end());
              return ids.size() == (size_t)std::distance(ids.begin(), last);
          });
    }
};

struct all_replication_factors_are_the_same {
    static constexpr error_code ec = error_code::invalid_request;
    static constexpr const char* error_message
      = "All custom assigned partitions must have the same number of replicas";

    static bool is_valid(const creatable_topic& c) {
        if (c.assignments.empty()) {
            return true;
        }
        auto replication_factor = c.assignments.front().broker_ids.size();
        return std::all_of(
          c.assignments.begin(),
          c.assignments.end(),
          [replication_factor](const creatable_replica_assignment& cra) {
              return cra.broker_ids.size() == replication_factor;
          });
    };
};

struct partition_count_must_be_positive {
    static constexpr error_code ec = error_code::invalid_partitions;
    static constexpr const char* error_message
      = "Partitions count must be greater than 0";

    static bool is_valid(const creatable_topic& c) {
        if (!c.assignments.empty()) {
            return true;
        }

        return c.num_partitions > 0;
    }
};

struct replication_factor_must_be_odd {
    static constexpr error_code ec = error_code::invalid_replication_factor;
    static constexpr const char* error_message
      = "Replication factor must be an odd number - 1,3,5,7,9,11...";

    static bool is_valid(const creatable_topic& c) {
        if (!c.assignments.empty()) {
            return true;
        }

        return (c.replication_factor % 2) == 1;
    }
};

struct replication_factor_must_be_positive {
    static constexpr error_code ec = error_code::invalid_replication_factor;
    static constexpr const char* error_message
      = "Replication factor must be greater than 0";

    static bool is_valid(const creatable_topic& c) {
        if (!c.assignments.empty()) {
            return true;
        }

        return c.replication_factor > 0;
    }
};

struct replication_factor_must_be_greater_or_equal_to_minimum {
    static constexpr error_code ec = error_code::invalid_replication_factor;
    static constexpr const char* error_message
      = "Replication factor must be greater than or equal to specified minimum "
        "value";

    static bool is_valid(const creatable_topic& c) {
        // All topics being validated as this level will be created in the kafka
        // namespace
        if (model::is_user_topic(
              model::topic_namespace{model::kafka_namespace, c.name})) {
            if (!c.assignments.empty()) {
                return true;
            } else {
                return c.replication_factor
                       >= config::shard_local_cfg()
                            .minimum_topic_replication.value();
            }
        } else {
            // Do not apply this validation against internally created topics
            return true;
        }
    }
};

struct unsupported_configuration_entries {
    static constexpr error_code ec = error_code::invalid_config;
    static constexpr const char* error_message
      = "Not supported configuration entry ";

    static bool is_valid(const creatable_topic& c) {
        auto config_entries = config_map(c.configs);
        auto end = config_entries.end();
        return end == config_entries.find("min.insync.replicas")
               && end == config_entries.find("flush.messages")
               && end == config_entries.find("flush.ms");
    }
};

struct remote_read_and_write_are_not_supported_for_read_replica {
    static constexpr error_code ec = error_code::invalid_config;
    static constexpr const char* error_message
      = "remote read and write are not supported for read replicas";

    static bool is_valid(const creatable_topic& c) {
        auto config_entries = config_map(c.configs);
        auto end = config_entries.end();
        bool is_recovery
          = (config_entries.find(topic_property_recovery) != end);
        bool is_read_replica
          = (config_entries.find(topic_property_read_replica) != end);
        bool remote_read
          = (config_entries.find(topic_property_remote_read) != end);
        bool remote_write
          = (config_entries.find(topic_property_remote_write) != end);

        if (is_read_replica && (remote_read || remote_write || is_recovery)) {
            return false;
        }
        return true;
    }
};

struct batch_max_bytes_limits {
    static constexpr error_code ec = error_code::invalid_config;
    static constexpr const char* error_message
      = "Property max.message.bytes value must be positive";

    static bool is_valid(const creatable_topic& c) {
        auto it = std::find_if(
          c.configs.begin(),
          c.configs.end(),
          [](const createable_topic_config& cfg) {
              return cfg.name == topic_property_max_message_bytes;
          });
        if (it != c.configs.end() && it->value.has_value()) {
            return boost::lexical_cast<int32_t>(it->value.value()) > 0;
        }

        return true;
    }
};

struct compression_type_validator_details {
    using validated_type = model::compression;

    static constexpr const char* error_message
      = "Unsupported compression type ";
    static constexpr const auto config_name = topic_property_compression;
};

struct compaction_strategy_validator_details {
    using validated_type = model::compaction_strategy;

    static constexpr const char* error_message
      = "Unsupported compaction strategy ";
    static constexpr const auto config_name
      = topic_property_compaction_strategy;
};

struct timestamp_type_validator_details {
    using validated_type = model::timestamp_type;

    static constexpr const char* error_message = "Unsupported timestamp type ";
    static constexpr const auto config_name = topic_property_timestamp_type;
};

struct cleanup_policy_validator_details {
    using validated_type = model::cleanup_policy_bitflags;

    static constexpr const char* error_message = "Unsupported cleanup policy ";
    static constexpr const auto config_name = topic_property_cleanup_policy;
};

struct subject_name_strategy_validator {
    static constexpr const char* error_message
      = "Unsupported subject name strategy ";
    static constexpr error_code ec = error_code::invalid_config;

    static bool is_valid(const creatable_topic& c) {
        return std::all_of(
          c.configs.begin(),
          c.configs.end(),
          [](createable_topic_config const& v) {
              return !is_sns_config(v) || !v.value.has_value()
                     || is_valid_sns(v.value.value());
          });
    }

private:
    static bool is_sns_config(createable_topic_config const& c) {
        static constexpr const auto config_names = {
          topic_property_record_key_subject_name_strategy,
          topic_property_record_key_subject_name_strategy_compat,
          topic_property_record_value_subject_name_strategy,
          topic_property_record_value_subject_name_strategy_compat};
        return std::any_of(
          config_names.begin(), config_names.end(), [&c](auto const& v) {
              return c.name == v;
          });
    }

    static bool is_valid_sns(std::string_view sv) {
        try {
            boost::lexical_cast<
              pandaproxy::schema_registry::subject_name_strategy>(sv);
            return true;
        } catch (...) {
            return false;
        }
    }
};

template<typename T>
struct configuration_value_validator {
    static constexpr const char* error_message = T::error_message;
    static constexpr error_code ec = error_code::invalid_config;

    static bool is_valid(const creatable_topic& c) {
        auto config_entries = config_map(c.configs);
        auto end = config_entries.end();

        auto iter = config_entries.find(T::config_name);

        if (end == iter) {
            return true;
        }

        try {
            boost::lexical_cast<typename T::validated_type>(iter->second);
            return true;
        } catch (...) {
            return false;
        }
    }
};
struct vcluster_id_validator {
    static constexpr const char* error_message = "invalid virtual cluster id";
    static constexpr error_code ec = error_code::invalid_config;

    static bool is_valid(const creatable_topic& c) {
        if (!config::shard_local_cfg().enable_mpx_extensions()) {
            return true;
        }
        auto config_entries = config_map(c.configs);

        auto it = config_entries.find(topic_property_mpx_virtual_cluster_id);

        if (it == config_entries.end()) {
            return true;
        }

        try {
            cluster::vcluster_id::type::from_string(it->second);
            return true;
        } catch (...) {
            return false;
        }
    }
};

using compression_type_validator
  = configuration_value_validator<compression_type_validator_details>;
using compaction_strategy_validator
  = configuration_value_validator<compaction_strategy_validator_details>;
using timestamp_type_validator
  = configuration_value_validator<timestamp_type_validator_details>;
using cleanup_policy_validator
  = configuration_value_validator<cleanup_policy_validator_details>;

} // namespace kafka
