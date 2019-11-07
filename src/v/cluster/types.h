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

/// Partition assignment describes an assignment of all replicas for single NTP.
/// The replicas are hold in vector of broker_shard.
struct partition_assignment {
    raft::group_id group;
    model::ntp ntp;
    std::vector<model::broker_shard> replicas;

    model::partition_metadata
    create_partition_metadata() const {
        auto p_md = model::partition_metadata(ntp.tp.partition);
        p_md.replicas.reserve(replicas.size());
        std::transform(
            replicas.begin(),
            replicas.end(),
            std::back_inserter(p_md.replicas),
            [](const model::broker_shard& bs){
                return bs.node_id;
            }
        );
        return p_md;
    }
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
    topic_result(
      model::topic t, topic_error_code ec = topic_error_code::no_error)
      : topic(std::move(t))
      , ec(ec) {
    }
    model::topic topic;
    topic_error_code ec;
};

} // namespace cluster

namespace rpc {

// Topic configuration type requires custom ser/des as is has custom constructor
template<>
future<cluster::topic_configuration> deserialize(source&);

template<>
void serialize(iobuf& out, cluster::topic_configuration&& t);
} // namespace rpc
