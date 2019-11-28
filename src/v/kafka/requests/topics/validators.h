#pragma once
#include "kafka/requests/topics/types.h"

namespace kafka {
// clang-format off
CONCEPT(
template<typename Request, typename T>
concept RequestValidator = requires (T validator, const Request& request) {
    { T::is_valid(request) } -> bool;
    { T::ec } -> error_code;
    { T::error_message } -> const char*;
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

    static bool is_valid(const new_topic_configuration& c) {
        return c.assignments.empty();
    }
};

struct partition_count_must_be_positive {
    static constexpr error_code ec = error_code::invalid_partitions;
    static constexpr const char* error_message
      = "Partitions count must be greater than 0";

    static bool is_valid(const new_topic_configuration& c) {
        return c.partition_count > 0;
    }
};

struct replication_factor_must_be_positive {
    static constexpr error_code ec = error_code::invalid_replication_factor;
    static constexpr const char* error_message
      = "Replication factor must be greater than 0";

    static bool is_valid(const new_topic_configuration& c) {
        return c.replication_factor > 0;
    }
};

struct unsupported_configuration_entries {
    static constexpr error_code ec = error_code::invalid_config;
    static constexpr const char* error_message
      = "Not supported configuration entry ";

    static bool is_valid(const new_topic_configuration& c) {
        auto end = c.config_entries.end();
        return end == c.config_entries.find("min.insync.replicas")
               && end == c.config_entries.find("flush.messages")
               && end == c.config_entries.find("flush.ms");
    }
};

} // namespace kafka
