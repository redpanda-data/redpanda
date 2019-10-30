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

struct partition_replica {
    model::replica_id id;
    model::node_id node;
};


struct partition_assignment {
    /// \brief this is the same as a seastar::shard_id
    /// however, seastar uses unsized-ints (unsigned)
    /// and for predictability we need fixed-sized ints
    uint32_t shard;
    raft::group_id group;
    model::ntp ntp;
    model::broker broker;
    model::replica_id replica;

    partition_replica get_partition_replica() const {
        return partition_replica{
            .id = replica, 
            .node = broker.id()
        };
    }
};

struct partition_metadata_entry {
    using replicas_t = std::vector<partition_replica>;
    model::partition_id id;
    replicas_t replicas;
    model::node_id leader_node;

    model::partition_metadata as_model_type() const;
    void update_replicas(partition_replica);
private:
    replicas_t::iterator find_replica(model::replica_id r_id);
};

struct topic_metadata_entry {
    std::vector<partition_metadata_entry> partitions;
    
    void update_partition(model::partition_id, partition_replica);
    model::topic_metadata as_model_type(model::topic_view) const;
    std::optional<std::reference_wrapper<partition_metadata_entry>>
    find_partition(model::partition_id);
private:
    void add_new_partition(model::partition_id, partition_replica);
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

template<>
void serialize(bytes_ostream& out, cluster::topic_configuration&& t);

template<>
future<cluster::topic_configuration> deserialize(source&);

template<>
future<cluster::partition_assignment> deserialize(source& in);

} // namespace rpc
