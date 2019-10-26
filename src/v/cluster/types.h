#pragma once

#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "raft/types.h"
#include "rpc/models.h"

namespace cluster {

struct log_record_key {
    enum class type : int8_t{
        partition_assignment,
        topic_configuration
    };

    type record_type;
};

struct partition_assignment {
    /// \brief this is the same as a seastar::shard_id
    /// however, seastar uses unsized-ints (unsigned)
    /// and for predictability we need fixed-sized ints
    uint32_t shard;
    raft::group_id group;
    model::ntp ntp;
    model::broker broker;
};

struct topic_configuration {
  topic_configuration(model::ns n, model::topic t, int32_t count, int16_t rf)
      : ns(std::move(n))
      , topic(std::move(t))
      , partition_count(count)
      , replication_factor(rf) {
    }
    model::ns ns;
    model::topic topic;
    // using signed integer because Kafka protocol defines it as signed int
    int32_t partition_count;
    // using signed integer because Kafka protocol defines it as signed int
    int16_t replication_factor;
    // topic configuration entries
    model::compression compression = model::compression::none;
    model::topic_partition::compaction compaction
      = model::topic_partition::compaction::no;
    uint64_t retention_bytes = 0;
    model::timeout_clock::duration retention = model::max_duration;
};

enum class topic_error_code : int16_t {
    no_error,
    unknown_error,
    time_out,
    invalid_partitions,
    invalid_replication_factor,
    invalid_config,
    topic_error_code_min = no_error,
    topic_error_code_max = invalid_config
};

constexpr std::string_view topic_error_code_names[] = {
    [(int16_t)topic_error_code::no_error] = "no_error",
    [(int16_t)topic_error_code::unknown_error] = "unknown_error",
    [(int16_t)topic_error_code::time_out] = "time_out",
    [(int16_t)topic_error_code::invalid_partitions] = "invalid_partitions",
    [(int16_t)topic_error_code::invalid_replication_factor] = "invalid_replication_factor",
    [(int16_t)topic_error_code::invalid_config] = "invalid_config"
};

std::ostream& operator<<(std::ostream&, topic_error_code);

struct topic_result {
    model::topic topic;
    topic_error_code ec;
};

} // namespace cluster

namespace rpc {
template<>
inline future<cluster::partition_assignment> deserialize(source& in) {
    struct _simple {
        uint32_t shard;
        raft::group_id group;
        model::ntp ntp;
    };
    return deserialize<_simple>(in).then([&in](_simple s) {
        return deserialize<model::broker>(in).then(
          [s = std::move(s)](model::broker b) {
              return cluster::partition_assignment{.shard = s.shard,
                                                   .group = s.group,
                                                   .ntp = s.ntp,
                                                   .broker = std::move(b)};
          });
    });
}
} // namespace rpc
