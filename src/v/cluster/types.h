#pragma once

#include "cluster/errc.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "raft/types.h"
#include "reflection/adl.h"
#include "storage/types.h"
#include "tristate.h"
#include "utils/to_string.h"

#include <fmt/ostream.h>
namespace raft {
class consensus;
}

namespace cluster {

static constexpr model::record_batch_type controller_record_batch_type{3};
using consensus_ptr = ss::lw_shared_ptr<raft::consensus>;
using broker_ptr = ss::lw_shared_ptr<model::broker>;

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
    model::partition_id id;
    std::vector<model::broker_shard> replicas;

    model::partition_metadata create_partition_metadata() const {
        auto p_md = model::partition_metadata(id);
        p_md.replicas = replicas;
        return p_md;
    }
};

// Structure holding topic configuration, optionals will be replaced by broker
// defaults
struct topic_configuration {
    topic_configuration(
      model::ns ns,
      model::topic topic,
      int32_t partition_count,
      int16_t replication_factor);

    storage::ntp_config
    make_ntp_config(const ss::sstring&, model::partition_id) const;

    model::topic_namespace tp_ns;
    // using signed integer because Kafka protocol defines it as signed int
    int32_t partition_count;
    // using signed integer because Kafka protocol defines it as signed int
    int16_t replication_factor;

    std::optional<model::compression> compression;
    std::optional<model::cleanup_policy_bitflags> cleanup_policy_bitflags;
    std::optional<model::compaction_strategy> compaction_strategy;
    std::optional<model::timestamp_type> timestamp_type;
    std::optional<size_t> segment_size;

    // Tristate fields
    // Mapped according to the following policy:
    //
    // Kafka topic configuration value: -1 -> tristate disabled
    // Kafka topic configuration value: preset -> tristate with value
    // Kafka topic configuration value: not set -> tristate with std::nullopt
    tristate<size_t> retention_bytes;
    tristate<std::chrono::milliseconds> retention_duration;

    friend std::ostream& operator<<(std::ostream&, const topic_configuration&);
};

struct topic_configuration_assignment {
    topic_configuration_assignment(
      topic_configuration cfg, std::vector<partition_assignment> pas)
      : cfg(std::move(cfg))
      , assignments(std::move(pas)) {}

    topic_configuration cfg;
    std::vector<partition_assignment> assignments;

    model::topic_metadata get_metadata() const;
};

struct topic_result {
    explicit topic_result(model::topic_namespace t, errc ec = errc::success)
      : tp_ns(std::move(t))
      , ec(ec) {}
    model::topic_namespace tp_ns;
    errc ec;
    friend std::ostream& operator<<(std::ostream& o, const topic_result& r);
};

struct create_topics_request {
    std::vector<topic_configuration> topics;
    model::timeout_clock::duration timeout;
};

struct create_topics_reply {
    std::vector<topic_result> results;
    std::vector<model::topic_metadata> metadata;
    std::vector<topic_configuration> configs;
};

template<typename T>
struct patch {
    std::vector<T> additions;
    std::vector<T> deletions;

    bool empty() const { return additions.empty() && deletions.empty(); }
};

// generic type used for various registration handles such as in ntp_callbacks.h
using notification_id_type = named_type<int32_t, struct notification_id>;

struct configuration_invariants {
    static constexpr uint8_t current_version = 0;
    // version 0: node_id, core_count
    explicit configuration_invariants(model::node_id nid, uint16_t core_count)
      : node_id(nid)
      , core_count(core_count) {}

    uint8_t version = current_version;

    model::node_id node_id;
    uint16_t core_count;

    friend std::ostream&
    operator<<(std::ostream&, const configuration_invariants&);
};

class configuration_invariants_changed final : public std::exception {
public:
    explicit configuration_invariants_changed(
      const configuration_invariants& expected,
      const configuration_invariants& current)
      : _msg(fmt::format(
        "Configuration invariants changed. Expected: {}, current: {}",
        expected,
        current)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

} // namespace cluster

namespace reflection {

template<>
struct adl<model::timeout_clock::duration> {
    using rep = model::timeout_clock::rep;
    using duration = model::timeout_clock::duration;

    void to(iobuf& out, duration dur);
    duration from(iobuf_parser& in);
};

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

template<>
struct adl<cluster::topic_result> {
    void to(iobuf&, cluster::topic_result&&);
    cluster::topic_result from(iobuf_parser&);
};

template<>
struct adl<cluster::create_topics_request> {
    void to(iobuf&, cluster::create_topics_request&&);
    cluster::create_topics_request from(iobuf);
    cluster::create_topics_request from(iobuf_parser&);
};

template<>
struct adl<cluster::create_topics_reply> {
    void to(iobuf&, cluster::create_topics_reply&&);
    cluster::create_topics_reply from(iobuf);
    cluster::create_topics_reply from(iobuf_parser&);
};
template<>
struct adl<cluster::topic_configuration_assignment> {
    void to(iobuf&, cluster::topic_configuration_assignment&&);
    cluster::topic_configuration_assignment from(iobuf_parser&);
};

template<>
struct adl<cluster::configuration_invariants> {
    void to(iobuf&, cluster::configuration_invariants&&);
    cluster::configuration_invariants from(iobuf_parser&);
};

} // namespace reflection
