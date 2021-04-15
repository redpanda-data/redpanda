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

#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/types.h"
#include "security/acl.h"
#include "storage/ntp_config.h"
#include "tristate.h"
#include "utils/to_string.h"

#include <fmt/format.h>

namespace cluster {

static constexpr model::record_batch_type controller_record_batch_type{3};
static constexpr model::record_batch_type id_allocator_stm_batch_type{8};
static constexpr model::record_batch_type tx_prepare_batch_type{9};
static constexpr model::record_batch_type tx_fence_batch_type{10};
static constexpr model::record_batch_type tm_update_batch_type{11};
static constexpr model::record_batch_type group_prepare_tx_batch_type{14};
static constexpr model::record_batch_type group_commit_tx_batch_type{15};

using consensus_ptr = ss::lw_shared_ptr<raft::consensus>;
using broker_ptr = ss::lw_shared_ptr<model::broker>;

struct allocate_id_request {
    model::timeout_clock::duration timeout;
};

struct allocate_id_reply {
    int64_t id;
    errc ec;
};

enum class tx_errc {
    none = 0,
    leader_not_found,
    shard_not_found,
    partition_not_found,
    stm_not_found,
    partition_not_exists,
    timeout,
    conflict,
    fenced,
    stale,
    not_coordinator,
    coordinator_not_available,
    preparing_rebalance,
    rebalance_in_progress,
    coordinator_load_in_progress
};
struct tx_errc_category final : public std::error_category {
    const char* name() const noexcept final { return "cluster::tx_errc"; }

    std::string message(int c) const final {
        switch (static_cast<tx_errc>(c)) {
        case tx_errc::stale:
            return "Stale";
        case tx_errc::fenced:
            return "Fenced";
        case tx_errc::conflict:
            return "Conflict";
        case tx_errc::none:
            return "None";
        case tx_errc::leader_not_found:
            return "Leader not found";
        case tx_errc::shard_not_found:
            return "Shard not found";
        case tx_errc::partition_not_found:
            return "Partition not found";
        case tx_errc::stm_not_found:
            return "Stm not found";
        case tx_errc::partition_not_exists:
            return "Partition not exists";
        case tx_errc::timeout:
            return "Timeout";
        default:
            return "cluster::tx_errc::unknown";
        }
    }
};
inline const std::error_category& tx_error_category() noexcept {
    static tx_errc_category e;
    return e;
}
inline std::error_code make_error_code(tx_errc e) noexcept {
    return std::error_code(static_cast<int>(e), tx_error_category());
}

struct begin_group_tx_request {
    model::ntp ntp;
    kafka::group_id group_id;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout;
};
struct begin_group_tx_reply {
    model::term_id etag;
    tx_errc ec;
};
struct prepare_group_tx_request {
    model::ntp ntp;
    kafka::group_id group_id;
    model::term_id etag;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout;
};
struct prepare_group_tx_reply {
    tx_errc ec;
};
struct commit_group_tx_request {
    model::ntp ntp;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    kafka::group_id group_id;
    model::timeout_clock::duration timeout;
};
struct commit_group_tx_reply {
    tx_errc ec;
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

struct configuration_update_request {
    explicit configuration_update_request(model::broker b, model::node_id tid)
      : node(std::move(b))
      , target_node(tid) {}
    model::broker node;
    model::node_id target_node;
};

struct configuration_update_reply {
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

    friend std::ostream& operator<<(std::ostream&, const partition_assignment&);
};

/**
 * Structure holding topic properties overrides, empty values will be replaced
 * with defaults
 */
struct topic_properties {
    std::optional<model::compression> compression;
    std::optional<model::cleanup_policy_bitflags> cleanup_policy_bitflags;
    std::optional<model::compaction_strategy> compaction_strategy;
    std::optional<model::timestamp_type> timestamp_type;
    std::optional<size_t> segment_size;
    tristate<size_t> retention_bytes;
    tristate<std::chrono::milliseconds> retention_duration;

    bool is_compacted() const;
    bool has_overrides() const;

    storage::ntp_config::default_overrides get_ntp_cfg_overrides() const;

    friend std::ostream& operator<<(std::ostream&, const topic_properties&);
};

enum incremental_update_operation : int8_t { none, set, remove };
template<typename T>
struct property_update {
    T value;
    incremental_update_operation op = incremental_update_operation::none;
};

template<typename T>
struct property_update<tristate<T>> {
    tristate<T> value = tristate<T>(std::nullopt);
    incremental_update_operation op = incremental_update_operation::none;
};

struct incremental_topic_updates {
    property_update<std::optional<model::compression>> compression;
    property_update<std::optional<model::cleanup_policy_bitflags>>
      cleanup_policy_bitflags;
    property_update<std::optional<model::compaction_strategy>>
      compaction_strategy;
    property_update<std::optional<model::timestamp_type>> timestamp_type;
    property_update<std::optional<size_t>> segment_size;
    property_update<tristate<size_t>> retention_bytes;
    property_update<tristate<std::chrono::milliseconds>> retention_duration;
};

/**
 * Struct representing single topic properties update
 */
struct topic_properties_update {
    explicit topic_properties_update(model::topic_namespace tp_ns)
      : tp_ns(std::move(tp_ns)) {}

    model::topic_namespace tp_ns;
    incremental_topic_updates properties;
};

// Structure holding topic configuration, optionals will be replaced by broker
// defaults
struct topic_configuration {
    topic_configuration(
      model::ns ns,
      model::topic topic,
      int32_t partition_count,
      int16_t replication_factor);

    storage::ntp_config make_ntp_config(
      const ss::sstring&, model::partition_id, model::revision_id) const;

    bool is_internal() const {
        return tp_ns.ns == model::kafka_internal_namespace;
    }

    model::topic_namespace tp_ns;
    // using signed integer because Kafka protocol defines it as signed int
    int32_t partition_count;
    // using signed integer because Kafka protocol defines it as signed int
    int16_t replication_factor;

    topic_properties properties;

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

struct finish_partition_update_request {
    model::ntp ntp;
    std::vector<model::broker_shard> new_replica_set;
};

struct finish_partition_update_reply {
    cluster::errc result;
};

struct update_topic_properties_request {
    std::vector<topic_properties_update> updates;
};

struct update_topic_properties_reply {
    std::vector<topic_result> results;
};

template<typename T>
struct patch {
    std::vector<T> additions;
    std::vector<T> deletions;
    std::vector<T> updates;

    bool empty() const {
        return additions.empty() && deletions.empty() && updates.empty();
    }
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
      : _msg(ssx::sformat(
        "Configuration invariants changed. Expected: {}, current: {}",
        expected,
        current)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

struct create_acls_cmd_data {
    static constexpr int8_t current_version = 1;
    std::vector<security::acl_binding> bindings;
};

struct create_acls_request {
    create_acls_cmd_data data;
    model::timeout_clock::duration timeout;
};

struct create_acls_reply {
    std::vector<errc> results;
};

struct delete_acls_cmd_data {
    static constexpr int8_t current_version = 1;
    std::vector<security::acl_binding_filter> filters;
};

// result for a single filter
struct delete_acls_result {
    errc error;
    std::vector<security::acl_binding> bindings;
};

struct delete_acls_request {
    delete_acls_cmd_data data;
    model::timeout_clock::duration timeout;
};

struct delete_acls_reply {
    std::vector<delete_acls_result> results;
};

} // namespace cluster
namespace std {
template<>
struct is_error_code_enum<cluster::tx_errc> : true_type {};
} // namespace std

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
struct adl<cluster::configuration_update_request> {
    void to(iobuf&, cluster::configuration_update_request&&);
    cluster::configuration_update_request from(iobuf_parser&);
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

template<>
struct adl<cluster::topic_properties_update> {
    void to(iobuf&, cluster::topic_properties_update&&);
    cluster::topic_properties_update from(iobuf_parser&);
};

template<>
struct adl<cluster::create_acls_cmd_data> {
    void to(iobuf&, cluster::create_acls_cmd_data&&);
    cluster::create_acls_cmd_data from(iobuf_parser&);
};

template<>
struct adl<cluster::delete_acls_cmd_data> {
    void to(iobuf&, cluster::delete_acls_cmd_data&&);
    cluster::delete_acls_cmd_data from(iobuf_parser&);
};

template<>
struct adl<cluster::delete_acls_result> {
    void to(iobuf&, cluster::delete_acls_result&&);
    cluster::delete_acls_result from(iobuf_parser&);
};

} // namespace reflection
