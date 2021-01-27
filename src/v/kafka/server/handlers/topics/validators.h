/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "kafka/protocol/schemata/create_topics_request.h"
#include "kafka/protocol/schemata/create_topics_response.h"
#include "kafka/server/handlers/topics/types.h"

namespace kafka {
// clang-format off
CONCEPT(
template<typename Request, typename T>
concept RequestValidator = requires (T validator, const Request& request) {
    { T::is_valid(request) } -> std::same_as<bool>;
    { T::ec } -> std::convertible_to<const error_code&>;
    { T::error_message } -> std::convertible_to<const char*>;
};)
// clang-format on

template<typename Request, typename... Ts>
struct validator_type_list {};

template<typename Request, typename... Validators>
CONCEPT(requires(RequestValidator<Request, Validators>, ...))
using make_validator_types = validator_type_list<Request, Validators...>;

struct no_custom_partition_assignment {
    static constexpr error_code ec = error_code::invalid_replica_assignment;
    static constexpr const char* error_message
      = "Replica assignment is not supported";

    static bool is_valid(const creatable_topic& c) {
        return c.assignments.empty();
    }
};

struct partition_count_must_be_positive {
    static constexpr error_code ec = error_code::invalid_partitions;
    static constexpr const char* error_message
      = "Partitions count must be greater than 0";

    static bool is_valid(const creatable_topic& c) {
        return c.num_partitions > 0;
    }
};

struct replication_factor_must_be_odd {
    static constexpr error_code ec = error_code::invalid_replication_factor;
    static constexpr const char* error_message
      = "Replication factor must be an odd number - 1,3,5,7,9,11...";

    static bool is_valid(const creatable_topic& c) {
        return (c.replication_factor % 2) == 1;
    }
};

struct replication_factor_must_be_positive {
    static constexpr error_code ec = error_code::invalid_replication_factor;
    static constexpr const char* error_message
      = "Replication factor must be greater than 0";

    static bool is_valid(const creatable_topic& c) {
        return c.replication_factor > 0;
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

} // namespace kafka
