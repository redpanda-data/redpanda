#pragma once

#include "cluster/errc.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "raft/types.h"
#include "reflection/adl.h"
#include "rpc/models.h"

namespace raft {
class consensus;
}

namespace cluster {

using consensus_ptr = ss::lw_shared_ptr<raft::consensus>;
using broker_ptr = ss::lw_shared_ptr<model::broker>;
struct log_record_key {
    enum class type : int8_t { partition_assignment, topic_configuration };

    type record_type;
};

/// Join request sent by node to join raft-0
struct join_request {
    explicit join_request(model::broker b)
      : node(std::move(b)) {}
    model::broker node;
};

struct join_reply {
    bool success;
};

/// Partition assignment describes an assignment of all replicas for single NTP.
/// The replicas are hold in vector of broker_shard.
struct partition_assignment {
    raft::group_id group;
    model::ntp ntp;
    std::vector<model::broker_shard> replicas;

    model::partition_metadata create_partition_metadata() const {
        auto p_md = model::partition_metadata(ntp.tp.partition);
        p_md.replicas = replicas;
        return p_md;
    }
};

struct topic_configuration {
    topic_configuration(model::ns n, model::topic t, int32_t count, int16_t rf)
      : ns(std::move(n))
      , topic(std::move(t))
      , partition_count(count)
      , replication_factor(rf) {}
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

struct topic_result {
    explicit topic_result(model::topic t, errc ec = errc::success)
      : topic(std::move(t))
      , ec(ec) {}
    model::topic topic;
    errc ec;
};

/// Structure representing difference between two set of brokers.
/// It is used to represent changes that have to be applied to raft client cache
struct brokers_diff {
    std::vector<broker_ptr> updated;
    std::vector<broker_ptr> removed;
};

} // namespace cluster

namespace reflection {
template<>
struct adl<cluster::topic_configuration> {
    void to(iobuf&, cluster::topic_configuration&&);
    cluster::topic_configuration from(iobuf_parser&);
};

template<>
struct adl<cluster::join_request> {
    void to(iobuf&, cluster::join_request&&);
    cluster::join_request from(iobuf);
    cluster::join_request from(iobuf_parser&);
};
} // namespace reflection
