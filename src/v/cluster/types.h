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
#include "kafka/types.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/types.h"
#include "security/acl.h"
#include "storage/ntp_config.h"
#include "tristate.h"
#include "utils/to_string.h"
#include "v8_engine/data_policy.h"

#include <fmt/format.h>

namespace cluster {
using consensus_ptr = ss::lw_shared_ptr<raft::consensus>;
using broker_ptr = ss::lw_shared_ptr<model::broker>;

// A cluster version is a logical protocol version describing the content
// of the raft0 on disk structures, and available features.  These are
// passed over the network via the health_manager, and persisted in
// the feature_manager
using cluster_version = named_type<int64_t, struct cluster_version_tag>;
constexpr cluster_version invalid_version = cluster_version{-1};

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
    pid_not_found,
    // when a request times out a client should not do any assumptions about its
    // effect. the request may time out before reaching the server, the request
    // may be successuly processed or may fail and the reply times out
    timeout,
    conflict,
    fenced,
    stale,
    not_coordinator,
    coordinator_not_available,
    preparing_rebalance,
    rebalance_in_progress,
    coordinator_load_in_progress,
    // an unspecified error happened, the effect of the request is unknown
    // the error code is very similar to timeout
    unknown_server_error,
    // an unspecified error happened, a client may assume it had zero effect on
    // the target node
    request_rejected,
    invalid_producer_id_mapping,
    invalid_txn_state
};
struct tx_errc_category final : public std::error_category {
    const char* name() const noexcept final { return "cluster::tx_errc"; }

    std::string message(int c) const final {
        switch (static_cast<tx_errc>(c)) {
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
        case tx_errc::pid_not_found:
            return "Pid not found";
        case tx_errc::timeout:
            return "Timeout";
        case tx_errc::conflict:
            return "Conflict";
        case tx_errc::fenced:
            return "Fenced";
        case tx_errc::stale:
            return "Stale";
        case tx_errc::not_coordinator:
            return "Not coordinator";
        case tx_errc::coordinator_not_available:
            return "Coordinator not available";
        case tx_errc::preparing_rebalance:
            return "Preparing rebalance";
        case tx_errc::rebalance_in_progress:
            return "Rebalance in progress";
        case tx_errc::coordinator_load_in_progress:
            return "Coordinator load in progress";
        case tx_errc::unknown_server_error:
            return "Unknown server error";
        case tx_errc::request_rejected:
            return "Request rejected";
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

struct try_abort_request {
    model::partition_id tm;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout;
};
struct try_abort_reply {
    bool commited{false};
    bool aborted{false};
    tx_errc ec;
};
struct init_tm_tx_request {
    kafka::transactional_id tx_id;
    std::chrono::milliseconds transaction_timeout_ms;
    model::timeout_clock::duration timeout;
};
struct init_tm_tx_reply {
    // partition_not_exists, not_leader, topic_not_exists
    model::producer_identity pid;
    tx_errc ec;
};
struct add_paritions_tx_request {
    struct topic {
        model::topic name{};
        std::vector<model::partition_id> partitions{};
    };
    kafka::transactional_id transactional_id{};
    kafka::producer_id producer_id{};
    int16_t producer_epoch{};
    std::vector<topic> topics{};
};
struct add_paritions_tx_reply {
    struct partition_result {
        model::partition_id partition_index{};
        tx_errc error_code{};
    };
    struct topic_result {
        model::topic name{};
        std::vector<add_paritions_tx_reply::partition_result> results{};
    };
    std::vector<add_paritions_tx_reply::topic_result> results{};
};
struct add_offsets_tx_request {
    kafka::transactional_id transactional_id{};
    kafka::producer_id producer_id{};
    int16_t producer_epoch{};
    kafka::group_id group_id{};
};
struct add_offsets_tx_reply {
    tx_errc error_code{};
};
struct end_tx_request {
    kafka::transactional_id transactional_id{};
    kafka::producer_id producer_id{};
    int16_t producer_epoch{};
    bool committed{};
};
struct end_tx_reply {
    tx_errc error_code{};
};
struct begin_tx_request {
    model::ntp ntp;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    std::chrono::milliseconds transaction_timeout_ms;
};
struct begin_tx_reply {
    model::ntp ntp;
    model::term_id etag;
    tx_errc ec;
};
struct prepare_tx_request {
    model::ntp ntp;
    model::term_id etag;
    model::partition_id tm;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout;
};
struct prepare_tx_reply {
    tx_errc ec;
};
struct commit_tx_request {
    model::ntp ntp;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout;
};
struct commit_tx_reply {
    tx_errc ec;
};
struct abort_tx_request {
    model::ntp ntp;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout;
};
struct abort_tx_reply {
    tx_errc ec;
};
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
struct abort_group_tx_request {
    model::ntp ntp;
    kafka::group_id group_id;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout;
};
struct abort_group_tx_reply {
    tx_errc ec;
};

/// Old-style request sent by node to join raft-0
/// - Does not specify logical version
/// - Always specifies node_id
/// (remove this RPC two versions after join_node_request was
///  added to replace it)
struct join_request {
    explicit join_request(model::broker b)
      : node(std::move(b)) {}

    model::broker node;
};

struct join_reply {
    bool success;
};

/// Successor to join_request:
/// - Include version metadata for joining node
/// - Has fields for implementing auto-selection of
///   node_id (https://github.com/vectorizedio/redpanda/issues/2793)
///   in future.
struct join_node_request {
    explicit join_node_request(
      cluster_version lv, std::vector<uint8_t> nuuid, model::broker b)
      : logical_version(lv)
      , node_uuid(nuuid)
      , node(std::move(b)) {}

    explicit join_node_request(cluster_version lv, model::broker b)
      : logical_version(lv)
      , node(std::move(b)) {}

    static constexpr int8_t current_version = 1;
    cluster_version logical_version;

    // node_uuid may be empty: this is for future use implementing auto
    // selection of node_id.  Convert to a more convenient type later:
    // the vector is just to reserve the on-disk layout.
    std::vector<uint8_t> node_uuid;
    model::broker node;
};

struct join_node_reply {
    bool success{false};
    model::node_id id{-1};
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
    tristate<size_t> retention_bytes{std::nullopt};
    tristate<std::chrono::milliseconds> retention_duration{std::nullopt};
    std::optional<bool> recovery;
    std::optional<model::shadow_indexing_mode> shadow_indexing;

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

    friend bool operator==(const property_update<T>&, const property_update<T>&)
      = default;
};

template<typename T>
struct property_update<tristate<T>> {
    tristate<T> value = tristate<T>(std::nullopt);
    incremental_update_operation op = incremental_update_operation::none;

    friend bool operator==(
      const property_update<tristate<T>>&, const property_update<tristate<T>>&)
      = default;
};

struct incremental_topic_updates {
    static constexpr int8_t version_with_data_policy = -1;
    static constexpr int8_t version_with_shadow_indexing = -3;
    // negative version indicating different format:
    // -1 - topic_updates with data_policy
    // -2 - topic_updates without data_policy
    // -3 - topic_updates with shadow_indexing
    static constexpr int8_t version = -3;
    property_update<std::optional<model::compression>> compression;
    property_update<std::optional<model::cleanup_policy_bitflags>>
      cleanup_policy_bitflags;
    property_update<std::optional<model::compaction_strategy>>
      compaction_strategy;
    property_update<std::optional<model::timestamp_type>> timestamp_type;
    property_update<std::optional<size_t>> segment_size;
    property_update<tristate<size_t>> retention_bytes;
    property_update<tristate<std::chrono::milliseconds>> retention_duration;
    property_update<std::optional<model::shadow_indexing_mode>> shadow_indexing;

    friend bool operator==(
      const incremental_topic_updates&, const incremental_topic_updates&)
      = default;
};

// This class contains updates for topic properties which are replicates not by
// topic_frontend
struct incremental_topic_custom_updates {
    // Data-policy property is replicated by data_policy_frontend and handled by
    // data_policy_manager.
    property_update<std::optional<v8_engine::data_policy>> data_policy;
};

/**
 * Struct representing single topic properties update
 */
struct topic_properties_update {
    // We need version to indetify request with custom_properties
    static constexpr int32_t version = -1;
    explicit topic_properties_update(model::topic_namespace tp_ns)
      : tp_ns(std::move(tp_ns)) {}

    model::topic_namespace tp_ns;

    // Tihs properties is serialized to update_topic_properties_cmd by
    // topic_frontend
    incremental_topic_updates properties;

    // This properties is not serialized to update_topic_properties_cmd, because
    // they have custom services for replication.
    incremental_topic_custom_updates custom_properties;
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
      const ss::sstring&,
      model::partition_id,
      model::revision_id,
      model::initial_revision_id) const;

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

struct custom_partition_assignment {
    model::partition_id id;
    std::vector<model::node_id> replicas;
    friend std::ostream&
    operator<<(std::ostream&, const custom_partition_assignment&);
};
/**
 * custom_assignable_topic_configuration type represents topic configuration
 * together with possible custom partition assignments. When assignments vector
 * is empty all the partitions will be assigned automatically.
 */
struct custom_assignable_topic_configuration {
    explicit custom_assignable_topic_configuration(topic_configuration cfg)
      : cfg(std::move(cfg)){};

    topic_configuration cfg;
    std::vector<custom_partition_assignment> custom_assignments;

    bool has_custom_assignment() const { return !custom_assignments.empty(); }

    friend std::ostream&
    operator<<(std::ostream&, const custom_assignable_topic_configuration&);
};

struct create_partititions_configuration {
    using custom_assignment = std::vector<model::node_id>;
    create_partititions_configuration(model::topic_namespace, int32_t);

    model::topic_namespace tp_ns;

    // This is new total number of partitions in topic.
    int32_t new_total_partition_count;

    // TODO: use when we will start supporting custom partitions assignment
    std::vector<custom_assignment> custom_assignments;

    friend std::ostream&
    operator<<(std::ostream&, const create_partititions_configuration&);
};

struct topic_configuration_assignment {
    topic_configuration_assignment() = delete;

    topic_configuration_assignment(
      topic_configuration cfg, std::vector<partition_assignment> pas)
      : cfg(std::move(cfg))
      , assignments(std::move(pas)) {}

    topic_configuration cfg;
    std::vector<partition_assignment> assignments;

    model::topic_metadata get_metadata() const;
};

struct create_partititions_configuration_assignment {
    create_partititions_configuration_assignment(
      create_partititions_configuration cfg,
      std::vector<partition_assignment> pas)
      : cfg(std::move(cfg))
      , assignments(std::move(pas)) {}

    create_partititions_configuration cfg;
    std::vector<partition_assignment> assignments;

    friend std::ostream& operator<<(
      std::ostream&, const create_partititions_configuration_assignment&);
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
constexpr notification_id_type notification_id_type_invalid{-1};

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
// delta propagated to backend
struct topic_table_delta {
    enum class op_type {
        add,
        del,
        update,
        update_finished,
        update_properties,
        add_non_replicable,
        del_non_replicable
    };

    topic_table_delta(
      model::ntp,
      cluster::partition_assignment,
      model::offset,
      op_type,
      std::optional<partition_assignment> = std::nullopt);

    model::ntp ntp;
    cluster::partition_assignment new_assignment;
    model::offset offset;
    op_type type;
    std::optional<partition_assignment> previous_assignment;

    model::topic_namespace_view tp_ns() const {
        return model::topic_namespace_view(ntp);
    }

    friend std::ostream& operator<<(std::ostream&, const topic_table_delta&);
    friend std::ostream& operator<<(std::ostream&, const op_type&);
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

struct backend_operation {
    ss::shard_id source_shard;
    partition_assignment p_as;
    topic_table_delta::op_type type;
    friend std::ostream& operator<<(std::ostream&, const backend_operation&);
};

struct create_data_policy_cmd_data {
    static constexpr int8_t current_version = 1; // In future dp will be vector
    v8_engine::data_policy dp;
};

struct non_replicable_topic {
    static constexpr int8_t current_version = 1;
    model::topic_namespace source;
    model::topic_namespace name;
};
std::ostream& operator<<(std::ostream&, const non_replicable_topic&);

using config_version = named_type<int64_t, struct config_version_type>;
constexpr config_version config_version_unset = config_version{-1};

struct config_status {
    model::node_id node;
    config_version version{config_version_unset};
    bool restart{false};
    std::vector<ss::sstring> unknown;
    std::vector<ss::sstring> invalid;

    bool operator==(const config_status&) const;
    friend std::ostream& operator<<(std::ostream&, const config_status&);
};

struct cluster_config_delta_cmd_data {
    static constexpr int8_t current_version = 0;
    std::vector<std::pair<ss::sstring, ss::sstring>> upsert;
    std::vector<ss::sstring> remove;
};
std::ostream& operator<<(std::ostream&, const cluster_config_delta_cmd_data&);

struct cluster_config_status_cmd_data {
    static constexpr int8_t current_version = 0;
    config_status status;
};

struct feature_update_action {
    static constexpr int8_t current_version = 1;
    enum class action_t : std::uint16_t {
        // Notify when a feature is done with preparing phase
        complete_preparing = 1,
        // Notify when a feature is explicitly made available by administrator
        administrative_activate = 2,
        // Notify when a feature is explicitly disabled by an administrator
        administrative_deactivate = 3
    };

    // Features have an internal bitflag representation, but it is not
    // meant to be stable for use on the wire, so we refer to features by name
    ss::sstring feature_name;
    action_t action;
};
std::ostream& operator<<(std::ostream&, const feature_update_action&);

struct feature_update_cmd_data {
    // To avoid ambiguity on 'versions' here: `current_version`
    // is the encoding version of the struct, subsequent version
    // fields are the payload.
    static constexpr int8_t current_version = 1;

    cluster_version logical_version;
    std::vector<feature_update_action> actions;
};
std::ostream& operator<<(std::ostream&, const feature_update_cmd_data&);

enum class reconciliation_status : int8_t {
    done,
    in_progress,
    error,
};
std::ostream& operator<<(std::ostream&, const reconciliation_status&);

class ntp_reconciliation_state {
public:
    // success case
    ntp_reconciliation_state(
      model::ntp, std::vector<backend_operation>, reconciliation_status);

    // error
    ntp_reconciliation_state(model::ntp, cluster::errc);

    ntp_reconciliation_state(
      model::ntp,
      std::vector<backend_operation>,
      reconciliation_status,
      cluster::errc);

    const model::ntp& ntp() const { return _ntp; }
    const std::vector<backend_operation>& pending_operations() const {
        return _backend_operations;
    }
    reconciliation_status status() const { return _status; }

    std::error_code error() const { return make_error_code(_error); }
    errc cluster_errc() const { return _error; }

    friend std::ostream&
    operator<<(std::ostream&, const ntp_reconciliation_state&);

private:
    model::ntp _ntp;
    std::vector<backend_operation> _backend_operations;
    reconciliation_status _status;
    errc _error;
};

struct reconciliation_state_request {
    std::vector<model::ntp> ntps;
};

struct reconciliation_state_reply {
    std::vector<ntp_reconciliation_state> results;
};

struct decommission_node_request {
    model::node_id id;
};

struct decommission_node_reply {
    errc error;
};
struct recommission_node_request {
    model::node_id id;
};

struct recommission_node_reply {
    errc error;
};

struct finish_reallocation_request {
    model::node_id id;
};

struct finish_reallocation_reply {
    errc error;
};

struct set_maintenance_mode_request {
    static constexpr int8_t current_version = 1;
    model::node_id id;
    bool enabled;
};

struct set_maintenance_mode_reply {
    static constexpr int8_t current_version = 1;
    errc error;
};

struct config_status_request {
    config_status status;
};

struct config_status_reply {
    errc error;
};

struct feature_action_request {
    feature_update_action action;
};

struct feature_action_response {
    errc error;
};

using feature_barrier_tag
  = named_type<ss::sstring, struct feature_barrier_tag_type>;

struct feature_barrier_request {
    static constexpr int8_t current_version = 1;
    feature_barrier_tag tag; // Each cooperative barrier must use a unique tag
    model::node_id peer;
    bool entered; // Has the requester entered?
};

struct feature_barrier_response {
    static constexpr int8_t current_version = 1;
    bool entered;  // Has the respondent entered?
    bool complete; // Has the respondent exited?
};

struct create_non_replicable_topics_request {
    static constexpr int8_t current_version = 1;
    std::vector<non_replicable_topic> topics;
    model::timeout_clock::duration timeout;
};

struct create_non_replicable_topics_reply {
    static constexpr int8_t current_version = 1;
    std::vector<topic_result> results;
};

struct config_update_request final {
    std::vector<std::pair<ss::sstring, ss::sstring>> upsert;
    std::vector<ss::sstring> remove;
};

struct config_update_reply {
    errc error;
    cluster::config_version latest_version{config_version_unset};
};

struct leader_term {
    leader_term(std::optional<model::node_id> leader, model::term_id term)
      : leader(leader)
      , term(term) {}

    std::optional<model::node_id> leader;
    model::term_id term;
    friend std::ostream& operator<<(std::ostream&, const leader_term&);
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
struct adl<cluster::join_node_request> {
    void to(iobuf&, cluster::join_node_request&&);
    cluster::join_node_request from(iobuf);
    cluster::join_node_request from(iobuf_parser&);
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
struct adl<cluster::create_non_replicable_topics_request> {
    void to(iobuf&, cluster::create_non_replicable_topics_request&&);
    cluster::create_non_replicable_topics_request from(iobuf_parser&);
};

template<>
struct adl<cluster::create_non_replicable_topics_reply> {
    void to(iobuf&, cluster::create_non_replicable_topics_reply&&);
    cluster::create_non_replicable_topics_reply from(iobuf_parser&);
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
struct adl<cluster::ntp_reconciliation_state> {
    void to(iobuf&, cluster::ntp_reconciliation_state&&);
    cluster::ntp_reconciliation_state from(iobuf_parser&);
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

template<>
struct adl<cluster::create_partititions_configuration> {
    void to(iobuf&, cluster::create_partititions_configuration&&);
    cluster::create_partititions_configuration from(iobuf_parser&);
};

template<>
struct adl<cluster::create_partititions_configuration_assignment> {
    void to(iobuf&, cluster::create_partititions_configuration_assignment&&);
    cluster::create_partititions_configuration_assignment from(iobuf_parser&);
};

template<>
struct adl<cluster::create_data_policy_cmd_data> {
    void to(iobuf&, cluster::create_data_policy_cmd_data&&);
    cluster::create_data_policy_cmd_data from(iobuf_parser&);
};

template<>
struct adl<cluster::non_replicable_topic> {
    void to(iobuf& out, cluster::non_replicable_topic&&);
    cluster::non_replicable_topic from(iobuf_parser&);
};

template<>
struct adl<cluster::incremental_topic_updates> {
    void to(iobuf& out, cluster::incremental_topic_updates&&);
    cluster::incremental_topic_updates from(iobuf_parser&);
};

template<>
struct adl<cluster::config_status> {
    void to(iobuf& out, cluster::config_status&&);
    cluster::config_status from(iobuf_parser&);
};

template<>
struct adl<cluster::cluster_config_delta_cmd_data> {
    void to(iobuf& out, cluster::cluster_config_delta_cmd_data&&);
    cluster::cluster_config_delta_cmd_data from(iobuf_parser&);
};

template<>
struct adl<cluster::cluster_config_status_cmd_data> {
    void to(iobuf& out, cluster::cluster_config_status_cmd_data&&);
    cluster::cluster_config_status_cmd_data from(iobuf_parser&);
};

template<>
struct adl<cluster::feature_update_action> {
    void to(iobuf& out, cluster::feature_update_action&&);
    cluster::feature_update_action from(iobuf_parser&);
};

template<>
struct adl<cluster::incremental_topic_custom_updates> {
    void to(iobuf& out, cluster::incremental_topic_custom_updates&&);
    cluster::incremental_topic_custom_updates from(iobuf_parser&);
};

template<>
struct adl<cluster::feature_update_cmd_data> {
    void to(iobuf&, cluster::feature_update_cmd_data&&);
    cluster::feature_update_cmd_data from(iobuf_parser&);
};

template<>
struct adl<cluster::feature_barrier_request> {
    void to(iobuf&, cluster::feature_barrier_request&&);
    cluster::feature_barrier_request from(iobuf_parser&);
};

template<>
struct adl<cluster::feature_barrier_response> {
    void to(iobuf&, cluster::feature_barrier_response&&);
    cluster::feature_barrier_response from(iobuf_parser&);
};

template<>
struct adl<cluster::set_maintenance_mode_request> {
    void to(iobuf&, cluster::set_maintenance_mode_request&&);
    cluster::set_maintenance_mode_request from(iobuf_parser&);
};

template<>
struct adl<cluster::set_maintenance_mode_reply> {
    void to(iobuf&, cluster::set_maintenance_mode_reply&&);
    cluster::set_maintenance_mode_reply from(iobuf_parser&);
};

} // namespace reflection
