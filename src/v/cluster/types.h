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

#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/errc.h"
#include "cluster/feature_update_action.h"
#include "cluster/fwd.h"
#include "cluster/nt_revision.h"
#include "cluster/remote_topic_properties.h"
#include "cluster/snapshot.h"
#include "cluster/topic_configuration.h"
#include "cluster/topic_properties.h"
#include "cluster/tx_errc.h"
#include "cluster/tx_hash_ranges.h"
#include "cluster/version.h"
#include "container/contiguous_range_map.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "model/transform.h"
#include "pandaproxy/schema_registry/subject_name_strategy.h"
#include "raft/errc.h"
#include "raft/fwd.h"
#include "raft/transfer_leadership.h"
#include "security/acl.h"
#include "security/license.h"
#include "security/role.h"
#include "security/scram_credential.h"
#include "security/types.h"
#include "serde/rw/bool_class.h"
#include "serde/rw/chrono.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/inet_address.h"
#include "serde/rw/map.h"
#include "serde/rw/named_type.h"
#include "serde/rw/sstring.h"
#include "serde/rw/vector.h"
#include "storage/ntp_config.h"
#include "utils/tristate.h"
#include "v8_engine/data_policy.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/variant_utils.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>
#include <absl/hash/hash.h>
#include <fmt/format.h>

#include <chrono>
#include <cstdint>
#include <optional>
#include <vector>

namespace cluster {
using consensus_ptr = ss::lw_shared_ptr<raft::consensus>;

using replicas_t = std::vector<model::broker_shard>;
struct allocate_id_request
  : serde::envelope<
      allocate_id_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::timeout_clock::duration timeout;

    allocate_id_request() noexcept = default;

    explicit allocate_id_request(model::timeout_clock::duration timeout)
      : timeout(timeout) {}

    friend bool
    operator==(const allocate_id_request&, const allocate_id_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const allocate_id_request& req) {
        fmt::print(o, "timeout: {}", req.timeout.count());
        return o;
    }

    auto serde_fields() { return std::tie(timeout); }
};

struct allocate_id_reply
  : serde::
      envelope<allocate_id_reply, serde::version<0>, serde::compat_version<0>> {
    int64_t id;
    errc ec;

    allocate_id_reply() noexcept = default;

    allocate_id_reply(int64_t id, errc ec)
      : id(id)
      , ec(ec) {}

    friend bool operator==(const allocate_id_reply&, const allocate_id_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const allocate_id_reply& rep) {
        fmt::print(o, "id: {}, ec: {}", rep.id, rep.ec);
        return o;
    }

    auto serde_fields() { return std::tie(id, ec); }
};

struct reset_id_allocator_request
  : serde::envelope<
      reset_id_allocator_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::timeout_clock::duration timeout;
    int64_t producer_id;

    reset_id_allocator_request() noexcept = default;

    explicit reset_id_allocator_request(
      model::timeout_clock::duration timeout, int64_t producer_id)
      : timeout(timeout)
      , producer_id(producer_id) {}

    friend bool operator==(
      const reset_id_allocator_request&, const reset_id_allocator_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const reset_id_allocator_request& req) {
        fmt::print(
          o,
          "timeout: {}, producer_id: {}",
          req.timeout.count(),
          req.producer_id);
        return o;
    }

    auto serde_fields() { return std::tie(timeout, producer_id); }
};

struct reset_id_allocator_reply
  : serde::envelope<
      reset_id_allocator_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    errc ec;

    reset_id_allocator_reply() noexcept = default;

    explicit reset_id_allocator_reply(errc ec)
      : ec(ec) {}

    friend bool
    operator==(const reset_id_allocator_reply&, const reset_id_allocator_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const reset_id_allocator_reply& rep) {
        fmt::print(o, "ec: {}", rep.ec);
        return o;
    }

    auto serde_fields() { return std::tie(ec); }
};

struct kafka_result {
    kafka::offset last_offset;
};
struct kafka_stages {
    kafka_stages(ss::future<>, ss::future<result<kafka_result>>);
    explicit kafka_stages(raft::errc);
    // after this future is ready, request in enqueued in raft and it will not
    // be reorderd
    ss::future<> request_enqueued;
    // after this future is ready, request was successfully replicated with
    // requested consistency level
    ss::future<result<kafka_result>> replicate_finished;
};

/**
 * When we remove a partition in the controller backend, we need to know
 * whether the action is just for this node, or whether the partition
 * is being deleted overall.
 */
enum class partition_removal_mode : uint8_t {
    // We are removing a partition from this node only: delete
    // local data but leave remote data alone.
    local_only = 0,
    // The partition is being permanently deleted from all nodes:
    // remove remote data as well as local data.
    global = 1
};

struct join_node_request
  : serde::
      envelope<join_node_request, serde::version<1>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    join_node_request() noexcept = default;

    explicit join_node_request(
      cluster_version lv,
      cluster_version ev,
      std::vector<uint8_t> nuuid,
      model::broker b)
      : latest_logical_version(lv)
      , node_uuid(nuuid)
      , node(std::move(b))
      , earliest_logical_version(ev) {}

    explicit join_node_request(
      cluster_version lv, cluster_version ev, model::broker b)
      : latest_logical_version(lv)
      , node(std::move(b))
      , earliest_logical_version(ev) {}

    static constexpr int8_t current_version = 1;

    // The highest version that the joining node supports
    cluster_version latest_logical_version{cluster::invalid_version};

    // node_uuid may be empty: this is for future use implementing auto
    // selection of node_id.  Convert to a more convenient type later:
    // the vector is just to reserve the on-disk layout.
    std::vector<uint8_t> node_uuid;
    model::broker node;

    // The lowest version that the joining node supports, it will already
    // have its feature table initialized to this version.
    cluster_version earliest_logical_version{cluster::invalid_version};

    friend bool operator==(const join_node_request&, const join_node_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const join_node_request& r) {
        fmt::print(
          o,
          "logical_version {}-{} node_uuid {} node {}",
          r.earliest_logical_version,
          r.latest_logical_version,
          r.node_uuid,
          r.node);
        return o;
    }

    auto serde_fields() {
        return std::tie(
          latest_logical_version, node_uuid, node, earliest_logical_version);
    }
};

struct join_node_reply
  : serde::
      envelope<join_node_reply, serde::version<1>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    enum class status_code : uint8_t {
        success = 0,
        // Non-specific error
        error = 1,
        // Newly-founded cluster is not yet ready to accept node joins
        not_ready = 2,
        // Cluster is busy adding another node, cannot add more until that is
        // done
        busy = 3,
        // Rejected because cluster version violates the bounds in the request
        incompatible = 4,
        // Rejected because node's address conflicted with an existing node
        conflict = 5,
        // Rejected because node claimed a node_id that differs from the
        // id it previously registered.
        id_changed = 6,
        // Rejected because a node is trying to claim the same node_id with
        // the same UUID as a node previously removed
        bad_rejoin = 7,
    };

    bool success{false};
    model::node_id id{model::unassigned_node_id};

    std::optional<iobuf> controller_snapshot;

    // Optional because old Redpandas just set the success boolean.
    // See status() for reading the status, do not read this directly.
    std::optional<status_code> raw_status;

    status_code status() const {
        return raw_status.value_or(
          success ? status_code::success : status_code::error);
    }

    const char* status_msg() const {
        switch (status()) {
        case status_code::success:
            return "success";
        case status_code::error:
            return "error";
        case status_code::not_ready:
            return "not_ready: cluster is still forming";
        case status_code::busy:
            return "busy: cluster currently adding another node";
        case status_code::incompatible:
            return "incompatible: cluster is too new or old for this Redpanda "
                   "version";
        case status_code::conflict:
            return "conflict: adding this node would conflict with another "
                   "node's address";
        case status_code::id_changed:
            return "id_changed: this node previously registered with a "
                   "different node ID";
        case status_code::bad_rejoin:
            return "bad_rejoin: trying to rejoin with same ID and UUID as a "
                   "decommissioned node";
        default:
            // Status codes are sent over the wire, so accomodate the
            // possibility that we were sent a status code from a newer version
            // that we don't understand.
            return "[unknown]";
        }
    }

    /**
     * Does the status code indicate that the caller should retry (i.e.
     * a non-permanent status like not_ready or busy?)
     */
    bool retryable() {
        return status() == status_code::not_ready
               || status() == status_code::busy;
    }

    join_node_reply() noexcept = default;

    join_node_reply(status_code status, model::node_id id)
      : success(status == status_code::success)
      , id(id)
      , raw_status(status) {}

    join_node_reply(join_node_reply&& rhs) noexcept = default;

    join_node_reply(
      status_code status, model::node_id id, std::optional<iobuf> snap)
      : success(status == status_code::success)
      , id(id)
      , controller_snapshot(std::move(snap))
      , raw_status(status) {}

    /// Copy constructor for use in encoding unit tests: general use should
    /// always move this object as the embedded controller offset may be large.
    join_node_reply(const join_node_reply& rhs)
      : success(rhs.success)
      , id(rhs.id)
      , raw_status(rhs.raw_status) {
        if (rhs.controller_snapshot.has_value()) {
            controller_snapshot = rhs.controller_snapshot.value().copy();
        }
    }

    join_node_reply& operator=(const join_node_reply& rhs) {
        success = rhs.success;
        id = rhs.id;
        raw_status = rhs.raw_status;
        if (rhs.controller_snapshot.has_value()) {
            controller_snapshot = rhs.controller_snapshot.value().copy();
        } else {
            controller_snapshot = std::nullopt;
        }

        return *this;
    }

    friend bool
    operator==(const join_node_reply& lhs, const join_node_reply& rhs)
      = default;

    friend std::ostream& operator<<(std::ostream& o, const join_node_reply& r) {
        fmt::print(
          o,
          "status {} ({:02x}, success={}) id {} snap {}",
          r.status_msg(),
          static_cast<uint8_t>(r.raw_status.value_or(status_code{0xff})),
          r.success,
          r.id,
          r.controller_snapshot.has_value()
            ? r.controller_snapshot.value().size_bytes()
            : 0);
        return o;
    }

    auto serde_fields() {
        return std::tie(success, id, controller_snapshot, raw_status);
    }
};

struct configuration_update_request
  : serde::envelope<
      configuration_update_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    configuration_update_request() noexcept = default;
    explicit configuration_update_request(model::broker b, model::node_id tid)
      : node(std::move(b))
      , target_node(tid) {}

    model::broker node;
    model::node_id target_node;

    friend bool operator==(
      const configuration_update_request&, const configuration_update_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const configuration_update_request&);

    auto serde_fields() { return std::tie(node, target_node); }
};

struct configuration_update_reply
  : serde::envelope<
      configuration_update_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    configuration_update_reply() noexcept = default;
    explicit configuration_update_reply(bool success)
      : success(success) {}

    bool success;

    friend bool operator==(
      const configuration_update_reply&, const configuration_update_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const configuration_update_reply&);

    auto serde_fields() { return std::tie(success); }
};

/// Partition assignment describes an assignment of all replicas for single NTP.
/// The replicas are hold in vector of broker_shard.
struct partition_assignment
  : serde::envelope<
      partition_assignment,
      serde::version<0>,
      serde::compat_version<0>> {
    partition_assignment() noexcept = default;
    partition_assignment(
      raft::group_id group, model::partition_id id, replicas_t replicas)
      : group(group)
      , id(id)
      , replicas(std::move(replicas)) {}

    raft::group_id group;
    model::partition_id id;
    replicas_t replicas;

    model::partition_metadata create_partition_metadata() const {
        auto p_md = model::partition_metadata(id);
        p_md.replicas = replicas;
        return p_md;
    }

    auto serde_fields() { return std::tie(group, id, replicas); }
    friend std::ostream& operator<<(std::ostream&, const partition_assignment&);

    friend bool
    operator==(const partition_assignment&, const partition_assignment&)
      = default;
};

enum incremental_update_operation : int8_t { none, set, remove };

inline std::string_view
incremental_update_operation_as_string(incremental_update_operation op) {
    switch (op) {
    case incremental_update_operation::none:
        return "none";
    case incremental_update_operation::set:
        return "set";
    case incremental_update_operation::remove:
        return "remove";
    default:
        vassert(false, "Unknown operation type passed: {}", int8_t(op));
    }
}

template<typename T>
struct property_update
  : serde::envelope<
      property_update<T>,
      serde::version<0>,
      serde::compat_version<0>> {
    property_update() = default;
    property_update(T v, incremental_update_operation op)
      : value(std::move(v))
      , op(op) {}

    T value;
    incremental_update_operation op = incremental_update_operation::none;

    auto serde_fields() { return std::tie(value, op); }

    friend std::ostream&
    operator<<(std::ostream& o, const property_update<T>& p) {
        fmt::print(
          o,
          "property_update: value: {} op: {}",
          p.value,
          incremental_update_operation_as_string(p.op));
        return o;
    }

    friend bool operator==(const property_update<T>&, const property_update<T>&)
      = default;
};

template<typename T>
struct property_update<tristate<T>>
  : serde::envelope<
      property_update<tristate<T>>,
      serde::version<0>,
      serde::compat_version<0>> {
    property_update()
      : value(std::nullopt) {};

    property_update(tristate<T> v, incremental_update_operation op)
      : value(std::move(v))
      , op(op) {}
    tristate<T> value;
    incremental_update_operation op = incremental_update_operation::none;

    auto serde_fields() { return std::tie(value, op); }

    friend std::ostream&
    operator<<(std::ostream& o, const property_update<tristate<T>>& p) {
        fmt::print(
          o,
          "property_update: value: {} op: {}",
          p.value,
          incremental_update_operation_as_string(p.op));
        return o;
    }

    friend bool operator==(
      const property_update<tristate<T>>&, const property_update<tristate<T>>&)
      = default;
};

struct incremental_topic_updates
  : serde::envelope<
      incremental_topic_updates,
      serde::version<7>,
      serde::compat_version<0>> {
    static constexpr int8_t version_with_data_policy = -1;
    static constexpr int8_t version_with_shadow_indexing = -3;
    static constexpr int8_t version_with_batch_max_bytes_and_local_retention
      = -4;
    static constexpr int8_t version_with_segment_ms = -5;
    static constexpr int8_t version_with_schema_id_validation = -6;
    static constexpr int8_t version_with_initial_retention = -7;
    static constexpr int8_t version_with_write_caching = -8;
    // negative version indicating different format:
    // -1 - topic_updates with data_policy
    // -2 - topic_updates without data_policy
    // -3 - topic_updates with shadow_indexing
    // -4 - topic update with batch_max_bytes and retention.local.target
    // -6 - topic updates with schema id validation
    // -7 - topic updates with initial retention
    // -8 - write caching properties
    // NOTE: As newly-serialized objects will always use serde format, we don't
    // need to support subsequently added fields in ADL routines - those should
    // just be able to deserialize fields that could be present in older
    // objects.
    static constexpr int8_t version = version_with_write_caching;
    property_update<std::optional<model::compression>> compression;
    property_update<std::optional<model::cleanup_policy_bitflags>>
      cleanup_policy_bitflags;
    property_update<std::optional<model::compaction_strategy>>
      compaction_strategy;
    property_update<std::optional<model::timestamp_type>> timestamp_type;
    property_update<std::optional<size_t>> segment_size;
    property_update<tristate<size_t>> retention_bytes;
    property_update<tristate<std::chrono::milliseconds>> retention_duration;
    property_update<std::optional<uint32_t>> batch_max_bytes;
    property_update<tristate<size_t>> retention_local_target_bytes;
    property_update<tristate<std::chrono::milliseconds>>
      retention_local_target_ms;
    property_update<bool> remote_read{
      false, incremental_update_operation::none};
    property_update<bool> remote_write{
      false, incremental_update_operation::none};
    property_update<bool> remote_delete{
      false, incremental_update_operation::none};
    property_update<tristate<std::chrono::milliseconds>> segment_ms;
    property_update<std::optional<bool>> record_key_schema_id_validation;
    property_update<std::optional<bool>> record_key_schema_id_validation_compat;
    property_update<
      std::optional<pandaproxy::schema_registry::subject_name_strategy>>
      record_key_subject_name_strategy;
    property_update<
      std::optional<pandaproxy::schema_registry::subject_name_strategy>>
      record_key_subject_name_strategy_compat;
    property_update<std::optional<bool>> record_value_schema_id_validation;
    property_update<std::optional<bool>>
      record_value_schema_id_validation_compat;
    property_update<
      std::optional<pandaproxy::schema_registry::subject_name_strategy>>
      record_value_subject_name_strategy;
    property_update<
      std::optional<pandaproxy::schema_registry::subject_name_strategy>>
      record_value_subject_name_strategy_compat;
    property_update<tristate<size_t>> initial_retention_local_target_bytes;
    property_update<tristate<std::chrono::milliseconds>>
      initial_retention_local_target_ms;
    property_update<std::optional<model::write_caching_mode>> write_caching;
    property_update<std::optional<std::chrono::milliseconds>> flush_ms;
    property_update<std::optional<size_t>> flush_bytes;
    property_update<bool> iceberg_enabled{
      storage::ntp_config::default_iceberg_enabled,
      incremental_update_operation::none};
    property_update<std::optional<config::leaders_preference>>
      leaders_preference;

    // To allow us to better control use of the deprecated shadow_indexing
    // field, use getters and setters instead.
    const auto& get_shadow_indexing() const { return shadow_indexing; }
    auto& get_shadow_indexing() { return shadow_indexing; }

    auto serde_fields() {
        return std::tie(
          compression,
          cleanup_policy_bitflags,
          compaction_strategy,
          timestamp_type,
          segment_size,
          retention_bytes,
          retention_duration,
          shadow_indexing,
          batch_max_bytes,
          retention_local_target_bytes,
          retention_local_target_ms,
          remote_delete,
          segment_ms,
          record_key_schema_id_validation,
          record_key_schema_id_validation_compat,
          record_key_subject_name_strategy,
          record_key_subject_name_strategy_compat,
          record_value_schema_id_validation,
          record_value_schema_id_validation_compat,
          record_value_subject_name_strategy,
          record_value_subject_name_strategy_compat,
          initial_retention_local_target_bytes,
          initial_retention_local_target_ms,
          write_caching,
          flush_ms,
          flush_bytes,
          iceberg_enabled,
          leaders_preference,
          remote_read,
          remote_write);
    }

    friend std::ostream&
    operator<<(std::ostream&, const incremental_topic_updates&);

    friend bool operator==(
      const incremental_topic_updates&, const incremental_topic_updates&)
      = default;

private:
    // This field is kept here for legacy purposes, but should be considered
    // deprecated in favour of remote_read and remote_write.
    property_update<std::optional<model::shadow_indexing_mode>> shadow_indexing;
};

using replication_factor
  = named_type<uint16_t, struct replication_factor_type_tag>;

std::istream& operator>>(std::istream& i, replication_factor& cs);

replication_factor parsing_replication_factor(const ss::sstring& value);

// This class contains updates for topic properties which are replicates not by
// topic_frontend
struct incremental_topic_custom_updates
  : serde::envelope<
      incremental_topic_custom_updates,
      serde::version<1>,
      serde::compat_version<0>> {
    // Data-policy property is replicated by data_policy_frontend and handled by
    // data_policy_manager.
    property_update<std::optional<v8_engine::data_policy>> data_policy;
    // Replication factor is custom handled.
    property_update<std::optional<replication_factor>> replication_factor;

    friend std::ostream&
    operator<<(std::ostream&, const incremental_topic_custom_updates&);

    friend bool operator==(
      const incremental_topic_custom_updates&,
      const incremental_topic_custom_updates&)
      = default;

    auto serde_fields() { return std::tie(data_policy, replication_factor); }
};

/**
 * Struct representing single topic properties update
 */
struct topic_properties_update
  : serde::envelope<
      topic_properties_update,
      serde::version<0>,
      serde::compat_version<0>> {
    // We need version to indetify request with custom_properties
    static constexpr int32_t version = -1;
    topic_properties_update() noexcept = default;
    explicit topic_properties_update(model::topic_namespace tp_ns)
      : tp_ns(std::move(tp_ns)) {}

    topic_properties_update(
      model::topic_namespace tp_ns,
      incremental_topic_updates properties,
      incremental_topic_custom_updates custom_properties)
      : tp_ns(std::move(tp_ns))
      , properties(properties)
      , custom_properties(std::move(custom_properties)) {}

    model::topic_namespace tp_ns;

    // Tihs properties is serialized to update_topic_properties_cmd by
    // topic_frontend
    incremental_topic_updates properties;

    // This properties is not serialized to update_topic_properties_cmd, because
    // they have custom services for replication.
    incremental_topic_custom_updates custom_properties;

    friend std::ostream&
    operator<<(std::ostream&, const topic_properties_update&);

    friend bool
    operator==(const topic_properties_update&, const topic_properties_update&)
      = default;

    auto serde_fields() {
        return std::tie(tp_ns, properties, custom_properties);
    }
};

using topic_properties_update_vector = chunked_vector<topic_properties_update>;

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
      : cfg(std::move(cfg)) {};

    topic_configuration cfg;
    std::vector<custom_partition_assignment> custom_assignments;

    bool has_custom_assignment() const { return !custom_assignments.empty(); }
    bool is_read_replica() const { return cfg.is_read_replica(); }
    bool is_recovery_enabled() const { return cfg.is_recovery_enabled(); }

    friend std::ostream&
    operator<<(std::ostream&, const custom_assignable_topic_configuration&);
};

using custom_assignable_topic_configuration_vector
  = chunked_vector<custom_assignable_topic_configuration>;

struct create_partitions_configuration
  : serde::envelope<
      create_partitions_configuration,
      serde::version<0>,
      serde::compat_version<0>> {
    using custom_assignment = std::vector<model::node_id>;

    create_partitions_configuration() = default;
    create_partitions_configuration(model::topic_namespace, int32_t);

    model::topic_namespace tp_ns;

    // This is new total number of partitions in topic.
    int32_t new_total_partition_count;

    // TODO: use when we start supporting custom partitions assignment
    std::vector<custom_assignment> custom_assignments;

    friend bool operator==(
      const create_partitions_configuration&,
      const create_partitions_configuration&)
      = default;

    auto serde_fields() {
        return std::tie(tp_ns, new_total_partition_count, custom_assignments);
    }

    friend std::ostream&
    operator<<(std::ostream&, const create_partitions_configuration&);
};

template<typename T>
struct configuration_with_assignment
  : serde::envelope<
      configuration_with_assignment<T>,
      serde::version<0>,
      serde::compat_version<0>> {
    configuration_with_assignment() = default;

    configuration_with_assignment(
      T value, ss::chunked_fifo<partition_assignment> pas)
      : cfg(std::move(value))
      , assignments(std::move(pas)) {}

    configuration_with_assignment(configuration_with_assignment&&) noexcept
      = default;
    configuration_with_assignment&
    operator=(configuration_with_assignment&&) noexcept
      = default;
    configuration_with_assignment&
    operator=(const configuration_with_assignment&)
      = delete;
    ~configuration_with_assignment() = default;
    // we need to make the type copyable as it is being copied when dispatched
    // to remote shards
    configuration_with_assignment(const configuration_with_assignment& src)
      : cfg(src.cfg) {
        ss::chunked_fifo<partition_assignment> assignments_cp;
        assignments_cp.reserve(assignments.size());
        std::copy(
          src.assignments.begin(),
          src.assignments.end(),
          std::back_inserter(assignments_cp));
        assignments = std::move(assignments_cp);
    }
    T cfg;
    ss::chunked_fifo<partition_assignment> assignments;

    auto serde_fields() { return std::tie(cfg, assignments); }

    friend bool operator==(
      const configuration_with_assignment<T>& lhs,
      const configuration_with_assignment<T>& rhs) {
        return lhs.cfg == rhs.cfg
               && std::equal(
                 lhs.assignments.begin(),
                 lhs.assignments.end(),
                 rhs.assignments.begin(),
                 rhs.assignments.end());
    };

    template<typename V>
    friend std::ostream&
    operator<<(std::ostream&, const configuration_with_assignment<V>&);
};

using create_partitions_configuration_assignment
  = configuration_with_assignment<create_partitions_configuration>;

/**
 * Soft-deleting a topic may put it into different modes: initially this is
 * just a two stage thing: create a marker that acts as a tombstone, later
 * drop the marker once deletion is complete.
 * In future this might include a "flushing" mode for writing out the last
 * of a topic's data to tiered storage before deleting it locally, or
 * an "offloaded" mode for topics that are parked in tiered storage with
 * no intention of deletion.
 */
enum class topic_lifecycle_transition_mode : uint8_t {
    // Drop the lifecycle marker: we do this after we're done with any
    // garbage collection.
    drop = 0,

    // Enter garbage collection phase: the topic appears deleted externally,
    // while internally we are garbage collecting any data that belonged
    // to it.
    pending_gc = 1,

    // Legacy-style deletion, where we attempt to delete local data and drop
    // the topic entirely from the topic table in one step.
    oneshot_delete = 2,

    // Local-only delete for migrated-from topics
    delete_migrated = 3
};

struct nt_lifecycle_marker
  : serde::envelope<
      nt_lifecycle_marker,
      serde::version<0>,
      serde::compat_version<0>> {
    topic_configuration config;

    model::initial_revision_id initial_revision_id;

    std::optional<ss::lowres_system_clock::time_point> timestamp;

    // Note that the serialisation of `timestamp` is explicitly avoided.
    auto serde_fields() { return std::tie(config, initial_revision_id); }
};

struct topic_lifecycle_transition
  : serde::envelope<
      topic_lifecycle_transition,
      serde::version<0>,
      serde::compat_version<0>> {
    nt_revision topic;

    topic_lifecycle_transition_mode mode;

    auto serde_fields() { return std::tie(topic, mode); }
};

using topic_configuration_assignment
  = configuration_with_assignment<topic_configuration>;

struct topic_result
  : serde::envelope<topic_result, serde::version<0>, serde::compat_version<0>> {
    topic_result() noexcept = default;
    explicit topic_result(model::topic_namespace t, errc ec = errc::success)
      : tp_ns(std::move(t))
      , ec(ec) {}
    model::topic_namespace tp_ns;
    errc ec;

    friend bool operator==(const topic_result&, const topic_result&) = default;

    friend std::ostream& operator<<(std::ostream& o, const topic_result& r);

    auto serde_fields() { return std::tie(tp_ns, ec); }
};

struct create_topics_request
  : serde::envelope<
      create_topics_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    topic_configuration_vector topics;
    model::timeout_clock::duration timeout;

    friend bool
    operator==(const create_topics_request&, const create_topics_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const create_topics_request&);

    auto serde_fields() { return std::tie(topics, timeout); }

    create_topics_request copy() const {
        return {.topics = topics.copy(), .timeout = timeout};
    }
};

struct create_topics_reply
  : serde::envelope<
      create_topics_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    std::vector<topic_result> results;
    std::vector<model::topic_metadata> metadata;
    topic_configuration_vector configs;

    create_topics_reply() noexcept = default;
    create_topics_reply(
      std::vector<topic_result> results,
      std::vector<model::topic_metadata> metadata,
      topic_configuration_vector configs)
      : results(std::move(results))
      , metadata(std::move(metadata))
      , configs(std::move(configs)) {}

    friend bool
    operator==(const create_topics_reply&, const create_topics_reply&)
      = default;

    friend std::ostream& operator<<(std::ostream&, const create_topics_reply&);

    auto serde_fields() { return std::tie(results, metadata, configs); }

    create_topics_reply copy() const {
        return {results, metadata, configs.copy()};
    }
};

struct purged_topic_request
  : serde::envelope<
      purged_topic_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    nt_revision topic;
    model::timeout_clock::duration timeout;

    friend bool
    operator==(const purged_topic_request&, const purged_topic_request&)
      = default;

    friend std::ostream& operator<<(std::ostream&, const purged_topic_request&);

    auto serde_fields() { return std::tie(topic, timeout); }
};

struct purged_topic_reply
  : serde::envelope<
      purged_topic_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    topic_result result;

    purged_topic_reply() noexcept = default;
    purged_topic_reply(topic_result r)
      : result(r) {}

    friend bool operator==(const purged_topic_reply&, const purged_topic_reply&)
      = default;

    friend std::ostream& operator<<(std::ostream&, const purged_topic_reply&);

    auto serde_fields() { return std::tie(result); }
};

struct finish_partition_update_request
  : serde::envelope<
      finish_partition_update_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    model::ntp ntp;
    replicas_t new_replica_set;

    friend bool operator==(
      const finish_partition_update_request&,
      const finish_partition_update_request&)
      = default;

    auto serde_fields() { return std::tie(ntp, new_replica_set); }

    friend std::ostream&
    operator<<(std::ostream& o, const finish_partition_update_request& r);
};

struct finish_partition_update_reply
  : serde::envelope<
      finish_partition_update_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    cluster::errc result;

    friend bool operator==(
      const finish_partition_update_reply&,
      const finish_partition_update_reply&)
      = default;

    auto serde_fields() { return std::tie(result); }

    friend std::ostream&
    operator<<(std::ostream& o, const finish_partition_update_reply& r);
};

struct update_topic_properties_request
  : serde::envelope<
      update_topic_properties_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    topic_properties_update_vector updates;

    friend std::ostream&
    operator<<(std::ostream&, const update_topic_properties_request&);

    friend bool operator==(
      const update_topic_properties_request&,
      const update_topic_properties_request&)
      = default;

    auto serde_fields() { return std::tie(updates); }

    update_topic_properties_request copy() const {
        return {.updates = updates.copy()};
    }
};

struct update_topic_properties_reply
  : serde::envelope<
      update_topic_properties_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    std::vector<topic_result> results;

    friend std::ostream&
    operator<<(std::ostream&, const update_topic_properties_reply&);

    friend bool operator==(
      const update_topic_properties_reply&,

      const update_topic_properties_reply&)
      = default;

    auto serde_fields() { return std::tie(results); }
};

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

enum class reconfiguration_policy {
    /**
     * Moving partition with full local retention policy will deliver all
     * partition data available locally on the leader to newly joining learners.
     * If tiered storage is disabled for a partition the move will always be
     * executed with full local retention.
     */
    full_local_retention = 0,
    /**
     * With target initial retention partition move policy controller backend
     * will calculate the learner start offset based on the configured learner
     * initial retention configuration (either a global cluster property or
     * topic override ) and partition max collectible offset. Max collectible
     * offset is advanced after all the previous offsets were successfully
     * uploaded to the cloud.
     */
    target_initial_retention = 1,
    /*
     * The min local retention policy, before requesting partition
     * reconfiguration in the Raft layer will request log to be uploaded to the
     * Object Store up to the active segment boundary. After all data are update
     * the controller backend will request partition move setting learner
     * initial offset to the active segment start offset, allowing all the data
     * from not active segments to be skiped while recovering learners.
     *
     * (NOTE: not yet implemented)
     */
    min_local_retention = 2
};

/**
 * Replicas revision map is used to track revision of brokers in a replica
 * set. When a node is added into replica set its gets the revision assigned
 */
using replicas_revision_map
  = absl::flat_hash_map<model::node_id, model::revision_id>;

/// Describes the shard placement target for a partition replica on this node.
/// Log revision is needed to distinguish different incarnations of the
/// partition.
struct shard_placement_target {
    shard_placement_target(
      raft::group_id g, model::revision_id lr, ss::shard_id s)
      : group(g)
      , log_revision(lr)
      , shard(s) {}

    raft::group_id group;
    model::revision_id log_revision;
    ss::shard_id shard;

    friend std::ostream&
    operator<<(std::ostream&, const shard_placement_target&);

    friend bool
    operator==(const shard_placement_target&, const shard_placement_target&)
      = default;
};

/// Type of controller backend operation
/// TODO: remove legacy types and bring more in line with what
/// controller_backend actually does.

enum class partition_operation_type {
    add,
    remove,
    update,
    force_update,
    finish_update,
    update_properties,
    add_non_replicable,
    del_non_replicable,
    cancel_update,
    force_cancel_update,
    reset,
};
std::ostream& operator<<(std::ostream&, const partition_operation_type&);

/// Notification of topic table state change related to a topic as a whole

enum class topic_table_topic_delta_type {
    added,
    removed,
    properties_updated,
};
std::ostream& operator<<(std::ostream&, const topic_table_topic_delta_type&);

struct topic_table_topic_delta {
    // revision of topic creation command (can be used to identify a topic
    // incarnation).
    model::revision_id creation_revision;
    model::topic_namespace ns_tp;
    // revision corresponding to the change itself.
    model::revision_id revision;
    topic_table_topic_delta_type type;

    topic_table_topic_delta(
      model::revision_id creation_rev,
      model::topic_namespace ns_tp,
      model::revision_id rev,
      topic_table_topic_delta_type type)
      : creation_revision(creation_rev)
      , ns_tp(std::move(ns_tp))
      , revision(rev)
      , type(type) {}

    friend std::ostream&
    operator<<(std::ostream&, const topic_table_topic_delta&);
};

/// Notification of topic table state change related to a single ntp

enum class topic_table_ntp_delta_type {
    added,
    removed,
    replicas_updated,
    properties_updated,
    disabled_flag_updated,
};
std::ostream& operator<<(std::ostream&, const topic_table_ntp_delta_type&);

struct topic_table_ntp_delta {
    model::ntp ntp;
    raft::group_id group;
    model::revision_id revision;
    topic_table_ntp_delta_type type;

    topic_table_ntp_delta(
      model::ntp ntp,
      raft::group_id gr,
      model::revision_id rev,
      topic_table_ntp_delta_type type)
      : ntp(std::move(ntp))
      , group(gr)
      , revision(rev)
      , type(type) {}

    friend std::ostream&
    operator<<(std::ostream&, const topic_table_ntp_delta&);
};

struct create_acls_cmd_data
  : serde::envelope<
      create_acls_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 1;
    std::vector<security::acl_binding> bindings;

    friend bool
    operator==(const create_acls_cmd_data&, const create_acls_cmd_data&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const create_acls_cmd_data& r) {
        fmt::print(o, "{{ bindings: {} }}", r.bindings);
        return o;
    }

    auto serde_fields() { return std::tie(bindings); }
};

struct create_acls_request
  : serde::envelope<
      create_acls_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    create_acls_cmd_data data;
    model::timeout_clock::duration timeout;

    create_acls_request() noexcept = default;
    create_acls_request(
      create_acls_cmd_data data, model::timeout_clock::duration timeout)
      : data(std::move(data))
      , timeout(timeout) {}

    friend bool
    operator==(const create_acls_request&, const create_acls_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const create_acls_request& r) {
        fmt::print(o, "{{ data: {}, timeout: {} }}", r.data, r.timeout.count());
        return o;
    }

    auto serde_fields() { return std::tie(data, timeout); }
};

struct create_acls_reply
  : serde::
      envelope<create_acls_reply, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    std::vector<errc> results;

    friend bool operator==(const create_acls_reply&, const create_acls_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const create_acls_reply& r) {
        fmt::print(o, "{{ results: {} }}", r.results);
        return o;
    }

    auto serde_fields() { return std::tie(results); }
};

struct delete_acls_cmd_data
  : serde::envelope<
      delete_acls_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 1;
    std::vector<security::acl_binding_filter> filters;

    friend bool
    operator==(const delete_acls_cmd_data&, const delete_acls_cmd_data&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const delete_acls_cmd_data& d) {
        fmt::print(o, "{{ filters: {} }}", d.filters);
        return o;
    }

    auto serde_fields() { return std::tie(filters); }
};

// result for a single filter
struct delete_acls_result
  : serde::envelope<
      delete_acls_result,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    errc error;
    std::vector<security::acl_binding> bindings;

    friend bool operator==(const delete_acls_result&, const delete_acls_result&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const delete_acls_result& r) {
        fmt::print(o, "{{ error: {} bindings: {} }}", r.error, r.bindings);
        return o;
    }

    auto serde_fields() { return std::tie(error, bindings); }
};

struct delete_acls_request
  : serde::envelope<
      delete_acls_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    delete_acls_cmd_data data;
    model::timeout_clock::duration timeout;

    delete_acls_request() noexcept = default;
    delete_acls_request(
      delete_acls_cmd_data data, model::timeout_clock::duration timeout)
      : data(std::move(data))
      , timeout(timeout) {}

    friend bool
    operator==(const delete_acls_request&, const delete_acls_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const delete_acls_request& r) {
        fmt::print(o, "{{ data: {} timeout: {} }}", r.data, r.timeout);
        return o;
    }

    auto serde_fields() { return std::tie(data, timeout); }
};

struct delete_acls_reply
  : serde::
      envelope<delete_acls_reply, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    std::vector<delete_acls_result> results;

    friend bool operator==(const delete_acls_reply&, const delete_acls_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const delete_acls_reply& r) {
        fmt::print(o, "{{ results: {} }}", r.results);
        return o;
    }

    auto serde_fields() { return std::tie(results); }
};

using transfer_leadership_request = raft::transfer_leadership_request;
using transfer_leadership_reply = raft::transfer_leadership_reply;

struct backend_operation
  : serde::
      envelope<backend_operation, serde::version<1>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    ss::shard_id source_shard;
    partition_assignment p_as;
    partition_operation_type type;

    uint64_t current_retry;
    cluster::errc last_operation_result;
    model::revision_id revision_of_operation;

    friend std::ostream& operator<<(std::ostream&, const backend_operation&);

    friend bool operator==(const backend_operation&, const backend_operation&)
      = default;

    auto serde_fields() {
        return std::tie(
          source_shard,
          p_as,
          type,
          current_retry,
          last_operation_result,
          revision_of_operation);
    }
};

using config_version = named_type<int64_t, struct config_version_type>;
constexpr config_version config_version_unset = config_version{-1};

struct config_status
  : serde::
      envelope<config_status, serde::version<0>, serde::compat_version<0>> {
    model::node_id node;
    config_version version{config_version_unset};
    bool restart{false};
    std::vector<ss::sstring> unknown;
    std::vector<ss::sstring> invalid;

    auto serde_fields() {
        return std::tie(node, version, restart, unknown, invalid);
    }

    bool operator==(const config_status&) const;
    friend std::ostream& operator<<(std::ostream&, const config_status&);
};

struct cluster_property_kv
  : serde::envelope<
      cluster_property_kv,
      serde::version<0>,
      serde::compat_version<0>> {
    cluster_property_kv() = default;
    cluster_property_kv(ss::sstring k, ss::sstring v)
      : key(std::move(k))
      , value(std::move(v)) {}

    ss::sstring key;
    ss::sstring value;

    auto serde_fields() { return std::tie(key, value); }

    friend bool
    operator==(const cluster_property_kv&, const cluster_property_kv&)
      = default;
    friend std::ostream& operator<<(std::ostream&, const cluster_property_kv&);
};

struct cluster_config_delta_cmd_data
  : serde::envelope<
      cluster_config_delta_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 0;
    std::vector<cluster_property_kv> upsert;
    std::vector<ss::sstring> remove;

    friend bool operator==(
      const cluster_config_delta_cmd_data&,
      const cluster_config_delta_cmd_data&)
      = default;

    auto serde_fields() { return std::tie(upsert, remove); }

    friend std::ostream&
    operator<<(std::ostream&, const cluster_config_delta_cmd_data&);
};

struct cluster_config_status_cmd_data
  : serde::envelope<
      cluster_config_status_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 0;

    friend bool operator==(
      const cluster_config_status_cmd_data&,
      const cluster_config_status_cmd_data&)
      = default;

    auto serde_fields() { return std::tie(status); }

    config_status status;
};

struct feature_update_cmd_data
  : serde::envelope<
      feature_update_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    // To avoid ambiguity on 'versions' here: `current_version`
    // is the encoding version of the struct, subsequent version
    // fields are the payload.
    static constexpr int8_t current_version = 1;

    cluster_version logical_version;
    std::vector<feature_update_action> actions;

    friend bool
    operator==(const feature_update_cmd_data&, const feature_update_cmd_data&)
      = default;

    auto serde_fields() { return std::tie(logical_version, actions); }

    friend std::ostream&
    operator<<(std::ostream&, const feature_update_cmd_data&);
};

using force_abort_update = ss::bool_class<struct force_abort_update_tag>;

struct cancel_moving_partition_replicas_cmd_data
  : serde::envelope<
      cancel_moving_partition_replicas_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    cancel_moving_partition_replicas_cmd_data() = default;
    explicit cancel_moving_partition_replicas_cmd_data(force_abort_update force)
      : force(force) {}
    force_abort_update force;

    auto serde_fields() { return std::tie(force); }
};

struct force_partition_reconfiguration_cmd_data
  : serde::envelope<
      force_partition_reconfiguration_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    force_partition_reconfiguration_cmd_data() noexcept = default;
    explicit force_partition_reconfiguration_cmd_data(replicas_t replicas)
      : replicas(std::move(replicas)) {}

    replicas_t replicas;

    auto serde_fields() { return std::tie(replicas); }

    friend bool operator==(
      const force_partition_reconfiguration_cmd_data&,
      const force_partition_reconfiguration_cmd_data&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const force_partition_reconfiguration_cmd_data&);
};

struct move_topic_replicas_data
  : serde::envelope<
      move_topic_replicas_data,
      serde::version<0>,
      serde::compat_version<0>> {
    move_topic_replicas_data() noexcept = default;
    explicit move_topic_replicas_data(
      model::partition_id partition, replicas_t replicas)
      : partition(partition)
      , replicas(std::move(replicas)) {}

    model::partition_id partition;
    replicas_t replicas;

    auto serde_fields() { return std::tie(partition, replicas); }

    friend std::ostream&
    operator<<(std::ostream&, const move_topic_replicas_data&);
};

struct set_topic_partitions_disabled_cmd_data
  : serde::envelope<
      set_topic_partitions_disabled_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    model::topic_namespace ns_tp;
    // if nullopt, applies to all partitions of the topic.
    std::optional<model::partition_id> partition_id;
    bool disabled = false;

    auto serde_fields() { return std::tie(ns_tp, partition_id, disabled); }

    friend bool operator==(
      const set_topic_partitions_disabled_cmd_data&,
      const set_topic_partitions_disabled_cmd_data&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const set_topic_partitions_disabled_cmd_data&);
};

struct feature_update_license_update_cmd_data
  : serde::envelope<
      feature_update_license_update_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    // Struct encoding version
    static constexpr int8_t current_version = 1;

    security::license redpanda_license;

    auto serde_fields() { return std::tie(redpanda_license); }

    friend std::ostream&
    operator<<(std::ostream&, const feature_update_license_update_cmd_data&);
};

struct user_and_credential
  : serde::envelope<
      user_and_credential,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    static constexpr int8_t current_version = 0;

    user_and_credential() = default;
    user_and_credential(
      security::credential_user&& username_,
      security::scram_credential&& credential_)
      : username(std::move(username_))
      , credential(std::move(credential_)) {}
    friend bool
    operator==(const user_and_credential&, const user_and_credential&)
      = default;
    auto serde_fields() { return std::tie(username, credential); }

    security::credential_user username;
    security::scram_credential credential;
};

struct cluster_recovery_init_state
  : serde::envelope<
      cluster_recovery_init_state,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    friend bool operator==(
      const cluster_recovery_init_state&, const cluster_recovery_init_state&)
      = default;

    auto serde_fields() { return std::tie(manifest, bucket); }

    // Cluster metadata manifest used to define the desired end state of the
    // recovery.
    cluster::cloud_metadata::cluster_metadata_manifest manifest;

    // Bucket from which to download the cluster recovery state.
    cloud_storage_clients::bucket_name bucket;
};

struct bootstrap_cluster_cmd_data
  : serde::envelope<
      bootstrap_cluster_cmd_data,
      serde::version<3>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    friend bool operator==(
      const bootstrap_cluster_cmd_data&, const bootstrap_cluster_cmd_data&)
      = default;

    auto serde_fields() {
        return std::tie(
          uuid,
          bootstrap_user_cred,
          node_ids_by_uuid,
          founding_version,
          initial_nodes,
          recovery_state);
    }

    model::cluster_uuid uuid;
    std::optional<user_and_credential> bootstrap_user_cred;
    absl::flat_hash_map<model::node_uuid, model::node_id> node_ids_by_uuid;

    // If this is set, fast-forward the feature_table to enable features
    // from this version. Indicates the version of Redpanda of
    // the node that generated the bootstrap record.
    cluster_version founding_version{invalid_version};
    std::vector<model::broker> initial_nodes;

    // If set, begins a cluster recovery using this state as the basis.
    std::optional<cluster_recovery_init_state> recovery_state;
};

struct cluster_recovery_init_cmd_data
  : serde::envelope<
      cluster_recovery_init_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    friend bool operator==(
      const cluster_recovery_init_cmd_data&,
      const cluster_recovery_init_cmd_data&)
      = default;

    auto serde_fields() { return std::tie(state); }

    cluster_recovery_init_state state;
};

enum class recovery_stage : int8_t {
    // A recovery has been initialized. We've already downloaded and serialized
    // the manifest. While in this state, a recovery manager may validate that
    // the recovery materials are downloadable.
    initialized = 0,

    // Recovery steps are beginning. We've already validated that the recovery
    // materials are downloadable, though these aren't persisted in the
    // controller beyond the manifest (it is expected that upon leadership
    // changes, they are redownloaded).
    starting = 1,

    recovered_license = 2,
    recovered_cluster_config = 3,
    recovered_users = 4,
    recovered_acls = 5,
    recovered_remote_topic_data = 6,
    recovered_topic_data = 7,

    // All state from the controller snapshot has been recovered.
    // Reconciliation attempts do not need to redownload the controller
    // snapshot to proceed.
    recovered_controller_snapshot = 8,

    recovered_offsets_topic = 9,
    recovered_tx_coordinator = 10,

    // Recovery has completed successfully. This is a terminal state.
    complete = 100,

    // Recovery has failed. This is a terminal state.
    failed = 101,
};
std::ostream& operator<<(std::ostream& o, const recovery_stage& s);

struct cluster_recovery_update_cmd_data
  : serde::envelope<
      cluster_recovery_update_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    friend bool operator==(
      const cluster_recovery_update_cmd_data&,
      const cluster_recovery_update_cmd_data&)
      = default;

    auto serde_fields() { return std::tie(stage, error_msg); }

    // The stage of the cluster recovery to be updated to.
    recovery_stage stage;

    // If set, the recovery is failed. Otherwise, it was a success.
    std::optional<ss::sstring> error_msg;
};

enum class reconciliation_status : int8_t {
    done,
    in_progress,
    error,
};
std::ostream& operator<<(std::ostream&, const reconciliation_status&);

class ntp_reconciliation_state
  : public serde::envelope<
      ntp_reconciliation_state,
      serde::version<0>,
      serde::compat_version<0>> {
public:
    using rpc_adl_exempt = std::true_type;
    ntp_reconciliation_state() noexcept = default;

    // success case
    ntp_reconciliation_state(
      model::ntp, ss::chunked_fifo<backend_operation>, reconciliation_status);

    // error
    ntp_reconciliation_state(model::ntp, cluster::errc);

    ntp_reconciliation_state(
      model::ntp,
      ss::chunked_fifo<backend_operation>,
      reconciliation_status,
      cluster::errc);

    const model::ntp& ntp() const { return _ntp; }
    const ss::chunked_fifo<backend_operation>& pending_operations() const {
        return _backend_operations;
    }

    ss::chunked_fifo<backend_operation>& pending_operations() {
        return _backend_operations;
    }

    reconciliation_status status() const { return _status; }

    std::error_code error() const { return make_error_code(_error); }
    errc cluster_errc() const { return _error; }

    friend bool operator==(
      const ntp_reconciliation_state& lhs,
      const ntp_reconciliation_state& rhs) {
        return lhs._ntp == rhs._ntp && lhs._status == rhs._status
               && lhs._error == rhs._error
               && lhs._backend_operations.size()
                    == rhs._backend_operations.size()
               && std::equal(
                 lhs._backend_operations.begin(),
                 lhs._backend_operations.end(),
                 rhs._backend_operations.begin());
    };

    ntp_reconciliation_state copy() const {
        ss::chunked_fifo<backend_operation> backend_operations;
        backend_operations.reserve(_backend_operations.size());
        std::copy(
          _backend_operations.begin(),
          _backend_operations.end(),
          std::back_inserter(backend_operations));
        return {_ntp, std::move(backend_operations), _status, _error};
    }

    friend std::ostream&
    operator<<(std::ostream&, const ntp_reconciliation_state&);

    auto serde_fields() {
        return std::tie(_ntp, _backend_operations, _status, _error);
    }

private:
    model::ntp _ntp;
    ss::chunked_fifo<backend_operation> _backend_operations;
    reconciliation_status _status;
    errc _error;
};

struct node_backend_operations {
    node_backend_operations(
      model::node_id id, ss::chunked_fifo<backend_operation> ops)
      : node_id(id)
      , backend_operations(std::move(ops)) {}

    model::node_id node_id;
    ss::chunked_fifo<backend_operation> backend_operations;

    node_backend_operations copy() const {
        ss::chunked_fifo<backend_operation> b_ops;
        b_ops.reserve(backend_operations.size());
        std::copy(
          backend_operations.begin(),
          backend_operations.end(),
          std::back_inserter(b_ops));
        return {node_id, std::move(b_ops)};
    }
};

struct node_error {
    node_error(model::node_id nid, std::error_code err_code)
      : node_id(nid)
      , ec(err_code) {}

    model::node_id node_id;
    std::error_code ec;
};

struct global_reconciliation_state {
    absl::node_hash_map<model::ntp, ss::chunked_fifo<node_backend_operations>>
      ntp_backend_operations;
    std::vector<node_error> node_errors;
};

struct reconciliation_state_request
  : serde::envelope<
      reconciliation_state_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    std::vector<model::ntp> ntps;

    friend bool operator==(
      const reconciliation_state_request&, const reconciliation_state_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const reconciliation_state_request& req) {
        fmt::print(o, "{{ ntps: {} }}", req.ntps);
        return o;
    }

    auto serde_fields() { return std::tie(ntps); }
};

struct ntp_with_majority_loss
  : serde::envelope<
      ntp_with_majority_loss,
      serde::version<0>,
      serde::compat_version<0>> {
    ntp_with_majority_loss() = default;
    explicit ntp_with_majority_loss(
      model::ntp n,
      model::revision_id r,
      std::vector<model::broker_shard> replicas,
      std::vector<model::node_id> dead_nodes)
      : ntp(std::move(n))
      , topic_revision(r)
      , assignment(std::move(replicas))
      , dead_nodes(std::move(dead_nodes)) {}
    model::ntp ntp;
    model::revision_id topic_revision;
    std::vector<model::broker_shard> assignment;
    std::vector<model::node_id> dead_nodes;

    template<typename H>
    friend H AbslHashValue(H h, const ntp_with_majority_loss& s) {
        return H::combine(
          std::move(h), s.ntp, s.topic_revision, s.assignment, s.dead_nodes);
    }

    friend std::ostream&
    operator<<(std::ostream& o, const ntp_with_majority_loss&);
    bool operator==(const ntp_with_majority_loss& other) const = default;
    auto serde_fields() {
        return std::tie(ntp, topic_revision, assignment, dead_nodes);
    }
};

struct bulk_force_reconfiguration_cmd_data
  : serde::envelope<
      bulk_force_reconfiguration_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    bulk_force_reconfiguration_cmd_data() = default;
    ~bulk_force_reconfiguration_cmd_data() noexcept = default;
    bulk_force_reconfiguration_cmd_data(bulk_force_reconfiguration_cmd_data&&)
      = default;
    bulk_force_reconfiguration_cmd_data(
      const bulk_force_reconfiguration_cmd_data&);
    bulk_force_reconfiguration_cmd_data&
    operator=(bulk_force_reconfiguration_cmd_data&&)
      = default;
    bulk_force_reconfiguration_cmd_data&
    operator=(const bulk_force_reconfiguration_cmd_data&);
    friend bool operator==(
      const bulk_force_reconfiguration_cmd_data&,
      const bulk_force_reconfiguration_cmd_data&)
      = default;

    std::vector<model::node_id> from_nodes;
    fragmented_vector<ntp_with_majority_loss>
      user_approved_force_recovery_partitions;

    auto serde_fields() {
        return std::tie(from_nodes, user_approved_force_recovery_partitions);
    }
};

using force_recoverable_partitions_t
  = absl::btree_map<model::ntp, std::vector<ntp_with_majority_loss>>;

struct reconciliation_state_reply
  : serde::envelope<
      reconciliation_state_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    std::vector<ntp_reconciliation_state> results;

    friend bool operator==(
      const reconciliation_state_reply&, const reconciliation_state_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const reconciliation_state_reply& rep) {
        fmt::print(o, "{{ results {} }}", rep.results);
        return o;
    }

    reconciliation_state_reply copy() const {
        std::vector<ntp_reconciliation_state> results_cp;
        results_cp.reserve(results.size());
        for (auto& r : results) {
            results_cp.push_back(r.copy());
        }

        return reconciliation_state_reply{.results = std::move(results_cp)};
    }

    auto serde_fields() { return std::tie(results); }
};

struct decommission_node_request
  : serde::envelope<
      decommission_node_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::node_id id;

    friend bool operator==(
      const decommission_node_request&, const decommission_node_request&)
      = default;

    auto serde_fields() { return std::tie(id); }

    friend std::ostream&
    operator<<(std::ostream& o, const decommission_node_request& r) {
        fmt::print(o, "id {}", r.id);
        return o;
    }
};

struct decommission_node_reply
  : serde::envelope<
      decommission_node_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    errc error;

    friend bool
    operator==(const decommission_node_reply&, const decommission_node_reply&)
      = default;

    auto serde_fields() { return std::tie(error); }

    friend std::ostream&
    operator<<(std::ostream& o, const decommission_node_reply& r) {
        fmt::print(o, "error {}", r.error);
        return o;
    }
};

struct recommission_node_request
  : serde::envelope<
      recommission_node_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::node_id id;

    friend bool operator==(
      const recommission_node_request&, const recommission_node_request&)
      = default;

    auto serde_fields() { return std::tie(id); }

    friend std::ostream&
    operator<<(std::ostream& o, const recommission_node_request& r) {
        fmt::print(o, "id {}", r.id);
        return o;
    }
};

struct recommission_node_reply
  : serde::envelope<
      recommission_node_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    errc error;

    friend bool
    operator==(const recommission_node_reply&, const recommission_node_reply&)
      = default;

    auto serde_fields() { return std::tie(error); }

    friend std::ostream&
    operator<<(std::ostream& o, const recommission_node_reply& r) {
        fmt::print(o, "error {}", r.error);
        return o;
    }
};

struct finish_reallocation_request
  : serde::envelope<
      finish_reallocation_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    model::node_id id;

    friend bool operator==(
      const finish_reallocation_request&, const finish_reallocation_request&)
      = default;

    auto serde_fields() { return std::tie(id); }

    friend std::ostream&
    operator<<(std::ostream& o, const finish_reallocation_request& r) {
        fmt::print(o, "id {}", r.id);
        return o;
    }
};

struct finish_reallocation_reply
  : serde::envelope<
      finish_reallocation_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    errc error;

    friend bool operator==(
      const finish_reallocation_reply&, const finish_reallocation_reply&)
      = default;

    auto serde_fields() { return std::tie(error); }

    friend std::ostream&
    operator<<(std::ostream& o, const finish_reallocation_reply& r) {
        fmt::print(o, "error {}", r.error);
        return o;
    }
};

struct set_maintenance_mode_request
  : serde::envelope<
      set_maintenance_mode_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    static constexpr int8_t current_version = 1;
    model::node_id id;
    bool enabled;

    friend bool operator==(
      const set_maintenance_mode_request&, const set_maintenance_mode_request&)
      = default;

    auto serde_fields() { return std::tie(id, enabled); }

    friend std::ostream&
    operator<<(std::ostream& o, const set_maintenance_mode_request& r) {
        fmt::print(o, "id {} enabled {}", r.id, r.enabled);
        return o;
    }
};

struct set_maintenance_mode_reply
  : serde::envelope<
      set_maintenance_mode_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    static constexpr int8_t current_version = 1;
    errc error;

    friend bool operator==(
      const set_maintenance_mode_reply&, const set_maintenance_mode_reply&)
      = default;

    auto serde_fields() { return std::tie(error); }

    friend std::ostream&
    operator<<(std::ostream& o, const set_maintenance_mode_reply& r) {
        fmt::print(o, "error {}", r.error);
        return o;
    }
};

struct config_status_request
  : serde::envelope<
      config_status_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    config_status status;

    friend std::ostream&
    operator<<(std::ostream&, const config_status_request&);

    friend bool
    operator==(const config_status_request&, const config_status_request&)
      = default;

    auto serde_fields() { return std::tie(status); }
};

struct config_status_reply
  : serde::envelope<
      config_status_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    errc error;

    friend std::ostream& operator<<(std::ostream&, const config_status_reply&);

    friend bool
    operator==(const config_status_reply&, const config_status_reply&)
      = default;

    auto serde_fields() { return std::tie(error); }
};

struct feature_action_request
  : serde::envelope<
      feature_action_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    feature_update_action action;

    friend bool
    operator==(const feature_action_request&, const feature_action_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const feature_action_request&);

    auto serde_fields() { return std::tie(action); }
};

struct feature_action_response
  : serde::envelope<
      feature_action_response,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    errc error;

    friend bool
    operator==(const feature_action_response&, const feature_action_response&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const feature_action_response&);

    auto serde_fields() { return std::tie(error); }
};

using feature_barrier_tag
  = named_type<ss::sstring, struct feature_barrier_tag_type>;

struct feature_barrier_request
  : serde::envelope<
      feature_barrier_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    static constexpr int8_t current_version = 1;
    feature_barrier_tag tag; // Each cooperative barrier must use a unique tag
    model::node_id peer;
    bool entered; // Has the requester entered?

    friend bool
    operator==(const feature_barrier_request&, const feature_barrier_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const feature_barrier_request&);

    auto serde_fields() { return std::tie(tag, peer, entered); }
};

struct feature_barrier_response
  : serde::envelope<
      feature_barrier_response,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    static constexpr int8_t current_version = 1;
    bool entered;  // Has the respondent entered?
    bool complete; // Has the respondent exited?

    friend bool
    operator==(const feature_barrier_response&, const feature_barrier_response&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const feature_barrier_response&);

    auto serde_fields() { return std::tie(entered, complete); }
};

struct config_update_request final
  : serde::envelope<
      config_update_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    std::vector<cluster_property_kv> upsert;
    std::vector<ss::sstring> remove;

    friend bool
    operator==(const config_update_request&, const config_update_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const config_update_request&);

    auto serde_fields() { return std::tie(upsert, remove); }
};

struct config_update_reply
  : serde::envelope<
      config_update_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    errc error;
    cluster::config_version latest_version{config_version_unset};

    friend bool
    operator==(const config_update_reply&, const config_update_reply&)
      = default;

    friend std::ostream& operator<<(std::ostream&, const config_update_reply&);

    auto serde_fields() { return std::tie(error, latest_version); }
};

struct hello_request final
  : serde::
      envelope<hello_request, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::node_id peer;

    // milliseconds since epoch
    std::chrono::milliseconds start_time;

    friend bool operator==(const hello_request&, const hello_request&)
      = default;

    auto serde_fields() { return std::tie(peer, start_time); }

    friend std::ostream& operator<<(std::ostream&, const hello_request&);
};

struct hello_reply
  : serde::envelope<hello_reply, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    errc error;

    friend bool operator==(const hello_reply&, const hello_reply&) = default;

    friend std::ostream& operator<<(std::ostream&, const hello_reply&);

    auto serde_fields() { return std::tie(error); }
};

struct leader_term {
    leader_term(std::optional<model::node_id> leader, model::term_id term)
      : leader(leader)
      , term(term) {}

    std::optional<model::node_id> leader;
    model::term_id term;
    friend std::ostream& operator<<(std::ostream&, const leader_term&);
};

using assignments_set
  = contiguous_range_map<model::partition_id::type, partition_assignment>;

struct topic_metadata_fields
  : serde::envelope<
      topic_metadata_fields,
      serde::version<0>,
      serde::compat_version<0>> {
    topic_configuration configuration;
    std::optional<model::topic> source_topic;
    model::revision_id revision;
    std::optional<model::initial_revision_id> remote_revision;

    topic_metadata_fields(
      topic_configuration cfg,
      std::optional<model::topic> st,
      model::revision_id rid,
      std::optional<model::initial_revision_id> remote_revision_id) noexcept;

    // for serde
    topic_metadata_fields() noexcept = default;

    friend bool
    operator==(const topic_metadata_fields&, const topic_metadata_fields&)
      = default;

    auto serde_fields() {
        return std::tie(configuration, source_topic, revision, remote_revision);
    }
};

class topic_metadata {
public:
    topic_metadata(
      topic_configuration_assignment,
      model::revision_id,
      std::optional<model::initial_revision_id> = std::nullopt) noexcept;

    topic_metadata(
      topic_configuration,
      assignments_set,
      model::revision_id,
      model::topic,
      std::optional<model::initial_revision_id> = std::nullopt) noexcept;

    topic_metadata(topic_metadata_fields, assignments_set) noexcept;

    model::revision_id get_revision() const;
    std::optional<model::initial_revision_id> get_remote_revision() const;

    const topic_metadata_fields& get_fields() const { return _fields; }
    topic_metadata_fields& get_fields() { return _fields; }

    const topic_configuration& get_configuration() const;
    topic_configuration& get_configuration();

    const assignments_set& get_assignments() const;
    assignments_set& get_assignments();

    replication_factor get_replication_factor() const;

    topic_metadata copy() const;

private:
    topic_metadata_fields _fields;
    assignments_set _assignments;
};

struct move_cancellation_result
  : serde::envelope<
      move_cancellation_result,
      serde::version<0>,
      serde::compat_version<0>> {
    move_cancellation_result() = default;

    move_cancellation_result(model::ntp ntp, cluster::errc ec)
      : ntp(std::move(ntp))
      , result(ec) {}

    auto serde_fields() { return std::tie(ntp, result); }

    friend bool
    operator==(const move_cancellation_result&, const move_cancellation_result&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const move_cancellation_result&);

    model::ntp ntp;
    cluster::errc result;
};

enum class partition_move_direction { to_node, from_node, all };
std::ostream& operator<<(std::ostream&, const partition_move_direction&);

struct cancel_all_partition_movements_request
  : serde::envelope<
      cancel_all_partition_movements_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    cancel_all_partition_movements_request() = default;

    auto serde_fields() { return std::tie(); }

    friend bool operator==(
      const cancel_all_partition_movements_request&,
      const cancel_all_partition_movements_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const cancel_all_partition_movements_request&) {
        fmt::print(o, "{{}}");
        return o;
    }
};
struct cancel_node_partition_movements_request
  : serde::envelope<
      cancel_node_partition_movements_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::node_id node_id;
    partition_move_direction direction;

    auto serde_fields() { return std::tie(node_id, direction); }

    friend bool operator==(
      const cancel_node_partition_movements_request&,
      const cancel_node_partition_movements_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const cancel_node_partition_movements_request&);
};

struct cancel_partition_movements_reply
  : serde::envelope<
      cancel_partition_movements_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    friend bool operator==(
      const cancel_partition_movements_reply&,
      const cancel_partition_movements_reply&)
      = default;

    auto serde_fields() { return std::tie(general_error, partition_results); }

    friend std::ostream&
    operator<<(std::ostream& o, const cancel_partition_movements_reply& r);

    errc general_error;
    std::vector<move_cancellation_result> partition_results;
};

struct cloud_storage_usage_request
  : serde::envelope<
      cloud_storage_usage_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    std::vector<model::ntp> partitions;

    friend bool operator==(
      const cloud_storage_usage_request&, const cloud_storage_usage_request&)
      = default;

    auto serde_fields() { return std::tie(partitions); }
};

struct cloud_storage_usage_reply
  : serde::envelope<
      cloud_storage_usage_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    uint64_t total_size_bytes{0};

    // When replies are handled in 'cloud_storage_size_reducer'
    // only the size of this list is currently used. However,
    // having the actual missing ntps allws for future optimisations:
    // the request can be retried only for the 'missing_partitions'.
    std::vector<model::ntp> missing_partitions;

    friend bool operator==(
      const cloud_storage_usage_reply&, const cloud_storage_usage_reply&)
      = default;

    auto serde_fields() {
        return std::tie(total_size_bytes, missing_partitions);
    }
};

struct producer_id_lookup_request
  : serde::envelope<
      producer_id_lookup_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    producer_id_lookup_request() noexcept = default;
    auto serde_fields() { return std::tie(); }
};

struct producer_id_lookup_reply
  : serde::envelope<
      producer_id_lookup_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    cluster::errc ec{};
    model::producer_id highest_producer_id{};

    producer_id_lookup_reply() noexcept = default;
    explicit producer_id_lookup_reply(cluster::errc e)
      : ec(e) {}
    explicit producer_id_lookup_reply(model::producer_id pid)
      : highest_producer_id(pid) {}

    auto serde_fields() { return std::tie(ec, highest_producer_id); }
};

struct partition_state_request
  : serde::envelope<
      partition_state_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::ntp ntp;
    friend bool
    operator==(const partition_state_request&, const partition_state_request&)
      = default;

    auto serde_fields() { return std::tie(ntp); }
};

struct partition_stm_state
  : serde::envelope<
      partition_stm_state,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    ss::sstring name;
    model::offset last_applied_offset;
    model::offset max_collectible_offset;

    auto serde_fields() {
        return std::tie(name, last_applied_offset, max_collectible_offset);
    }
};

struct partition_raft_state
  : serde::envelope<
      partition_raft_state,
      serde::version<5>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::node_id node;
    model::term_id term;
    ss::sstring offset_translator_state;
    ss::sstring group_configuration;
    model::term_id confirmed_term;
    model::offset flushed_offset;
    model::offset commit_index;
    model::offset majority_replicated_index;
    model::offset visibility_upper_bound_index;
    model::offset last_quorum_replicated_index;
    model::term_id last_snapshot_term;
    model::offset last_snapshot_index;
    model::offset received_snapshot_index;
    size_t received_snapshot_bytes;
    bool has_pending_flushes;
    bool is_leader;
    bool is_elected_leader;
    std::vector<partition_stm_state> stms;
    bool write_caching_enabled;
    size_t flush_bytes;
    std::chrono::milliseconds flush_ms;
    ss::sstring replication_monitor_state;
    std::chrono::milliseconds time_since_last_flush;

    struct follower_state
      : serde::envelope<
          follower_state,
          serde::version<0>,
          serde::compat_version<0>> {
        using rpc_adl_exempt = std::true_type;

        model::node_id node;
        model::offset last_flushed_log_index;
        model::offset last_dirty_log_index;
        model::offset match_index;
        model::offset next_index;
        model::offset expected_log_end_offset;
        size_t heartbeats_failed;
        bool is_learner;
        uint64_t ms_since_last_heartbeat;
        uint64_t last_sent_seq;
        uint64_t last_received_seq;
        uint64_t last_successful_received_seq;
        bool suppress_heartbeats;
        bool is_recovering;

        auto serde_fields() {
            return std::tie(
              node,
              last_flushed_log_index,
              last_dirty_log_index,
              match_index,
              next_index,
              expected_log_end_offset,
              heartbeats_failed,
              is_learner,
              ms_since_last_heartbeat,
              last_sent_seq,
              last_received_seq,
              last_successful_received_seq,
              suppress_heartbeats,
              is_recovering);
        }

        friend bool operator==(const follower_state&, const follower_state&)
          = default;
    };

    struct follower_recovery_state
      : serde::envelope<
          follower_recovery_state,
          serde::version<0>,
          serde::compat_version<0>> {
        bool is_active = false;
        int64_t pending_offset_count = 0;

        auto serde_fields() {
            return std::tie(is_active, pending_offset_count);
        }

        friend bool operator==(
          const follower_recovery_state&, const follower_recovery_state&)
          = default;
    };

    // Set only on leaders.
    std::optional<std::vector<follower_state>> followers;
    // Set only on recovering followers.
    std::optional<follower_recovery_state> recovery_state;

    auto serde_fields() {
        return std::tie(
          node,
          term,
          offset_translator_state,
          group_configuration,
          confirmed_term,
          flushed_offset,
          commit_index,
          majority_replicated_index,
          visibility_upper_bound_index,
          last_quorum_replicated_index,
          last_snapshot_term,
          last_snapshot_index,
          received_snapshot_index,
          received_snapshot_bytes,
          has_pending_flushes,
          is_leader,
          is_elected_leader,
          followers,
          stms,
          recovery_state,
          write_caching_enabled,
          flush_bytes,
          flush_ms,
          replication_monitor_state,
          time_since_last_flush);
    }

    friend bool
    operator==(const partition_raft_state&, const partition_raft_state&)
      = default;
};

struct partition_state
  : serde::
      envelope<partition_state, serde::version<1>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::offset start_offset;
    model::offset committed_offset;
    model::offset last_stable_offset;
    model::offset high_water_mark;
    model::offset dirty_offset;
    model::offset latest_configuration_offset;
    model::offset start_cloud_offset;
    model::offset next_cloud_offset;
    model::revision_id revision_id;
    size_t log_size_bytes;
    size_t non_log_disk_size_bytes;
    bool is_read_replica_mode_enabled;
    bool is_remote_fetch_enabled;
    bool is_cloud_data_available;
    ss::sstring read_replica_bucket;
    bool iceberg_enabled;
    partition_raft_state raft_state;

    auto serde_fields() {
        return std::tie(
          start_offset,
          committed_offset,
          last_stable_offset,
          high_water_mark,
          dirty_offset,
          latest_configuration_offset,
          start_cloud_offset,
          next_cloud_offset,
          revision_id,
          log_size_bytes,
          non_log_disk_size_bytes,
          is_read_replica_mode_enabled,
          is_remote_fetch_enabled,
          is_cloud_data_available,
          read_replica_bucket,
          raft_state,
          iceberg_enabled);
    }

    friend bool operator==(const partition_state&, const partition_state&)
      = default;
};

struct partition_state_reply
  : serde::envelope<
      partition_state_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::ntp ntp;
    std::optional<partition_state> state;
    errc error_code;

    friend bool
    operator==(const partition_state_reply&, const partition_state_reply&)
      = default;

    auto serde_fields() { return std::tie(ntp, state, error_code); }
};

struct revert_cancel_partition_move_cmd_data
  : serde::envelope<
      revert_cancel_partition_move_cmd_data,
      serde::version<0>,
      serde::version<0>> {
    model::ntp ntp;

    auto serde_fields() { return std::tie(ntp); }

    friend bool operator==(
      const revert_cancel_partition_move_cmd_data&,
      const revert_cancel_partition_move_cmd_data&)
      = default;
};

struct revert_cancel_partition_move_request
  : serde::envelope<
      revert_cancel_partition_move_request,
      serde::version<0>,
      serde::version<0>> {
    using rpc_adl_exempt = std::true_type;
    model::ntp ntp;

    auto serde_fields() { return std::tie(ntp); }

    friend bool operator==(
      const revert_cancel_partition_move_request&,
      const revert_cancel_partition_move_request&)
      = default;
};

struct revert_cancel_partition_move_reply
  : serde::envelope<
      revert_cancel_partition_move_reply,
      serde::version<0>,
      serde::version<0>> {
    using rpc_adl_exempt = std::true_type;
    errc result;

    auto serde_fields() { return std::tie(result); }

    friend bool operator==(
      const revert_cancel_partition_move_reply&,
      const revert_cancel_partition_move_reply&)
      = default;
};

struct upsert_role_cmd_data
  : serde::
      envelope<upsert_role_cmd_data, serde::version<0>, serde::version<0>> {
    using rpc_adl_exempt = std::true_type;
    security::role_name name;
    security::role role;

    auto serde_fields() { return std::tie(name, role); }

    friend bool
    operator==(const upsert_role_cmd_data&, const upsert_role_cmd_data&)
      = default;
};

struct delete_role_cmd_data
  : serde::
      envelope<delete_role_cmd_data, serde::version<0>, serde::version<0>> {
    using rpc_adl_exempt = std::true_type;
    security::role_name name;

    auto serde_fields() { return std::tie(name); }

    friend bool
    operator==(const delete_role_cmd_data&, const delete_role_cmd_data&)
      = default;
};

/**
 * Broker state transitions are coordinated centrally as opposite to
 * configuration which change is requested by the described node itself. Broker
 * state represents centrally managed node properties. The difference between
 * broker state and configuration is that the configuration change is made on
 * the node while state changes are managed by the cluster controller.
 */
class broker_state
  : public serde::
      envelope<broker_state, serde::version<1>, serde::compat_version<0>> {
public:
    model::membership_state get_membership_state() const {
        return _membership_state;
    }
    void set_membership_state(model::membership_state st) {
        _membership_state = st;
    }

    model::maintenance_state get_maintenance_state() const {
        return _maintenance_state;
    }
    void set_maintenance_state(model::maintenance_state st) {
        _maintenance_state = st;
    }

    friend bool operator==(const broker_state&, const broker_state&) = default;

    friend std::ostream& operator<<(std::ostream&, const broker_state&);

    auto serde_fields() {
        return std::tie(_membership_state, _maintenance_state);
    }

private:
    model::membership_state _membership_state = model::membership_state::active;
    model::maintenance_state _maintenance_state
      = model::maintenance_state::inactive;
};

/**
 * Node metadata describes a cluster node with its state and configuration
 */
struct node_metadata {
    model::broker broker;
    broker_state state;

    friend bool operator==(const node_metadata&, const node_metadata&)
      = default;
    friend std::ostream& operator<<(std::ostream&, const node_metadata&);
};

// Node update types, used for communication between members_manager and
// members_backend.
//
// NOTE: maintenance mode doesn't interact with the members_backend,
// instead interacting with each core via their respective drain_manager.
enum class node_update_type : int8_t {
    // A node has been added to the cluster.
    added,

    // A node has been decommissioned from the cluster.
    decommissioned,

    // A node has been recommissioned after an incomplete decommission.
    recommissioned,

    // All reallocations associated with a given node update have completed
    // (e.g. it's been fully decommissioned, indicating it can no longer be
    // recommissioned).
    reallocation_finished,

    // node has been removed from the cluster
    removed,

    // previous updates must be interrupted
    interrupted,
};

std::ostream& operator<<(std::ostream&, const node_update_type&);

/**
 * Reconfiguration state indicates if ongoing reconfiguration is a result of
 * partition movement, cancellation or forced cancellation
 */
enum class reconfiguration_state {
    in_progress = 0,
    force_update = 1,
    cancelled = 2,
    force_cancelled = 3
};

inline bool is_cancelled_state(reconfiguration_state rs) {
    switch (rs) {
    case reconfiguration_state::in_progress:
    case reconfiguration_state::force_update:
        return false;
    case reconfiguration_state::cancelled:
    case reconfiguration_state::force_cancelled:
        return true;
    default:
        __builtin_unreachable();
    }
}

std::ostream& operator<<(std::ostream&, reconfiguration_state);

struct replica_bytes {
    model::node_id node;
    size_t bytes{0};
};

struct partition_reconfiguration_state {
    model::ntp ntp;
    // assignments
    replicas_t previous_assignment;
    replicas_t current_assignment;
    // state indicating if reconfiguration was cancelled or requested
    reconfiguration_state state;
    // amount of bytes already transferred to new replicas
    std::vector<replica_bytes> already_transferred_bytes;
    // current size of partition
    size_t current_partition_size{0};
    // policy used to execute an update
    reconfiguration_policy policy;
};

struct node_decommission_progress {
    // indicate if node decommissioning finished
    bool finished = false;
    // number of replicas left on decommissioned node
    size_t replicas_left{0};
    // Replicas on the node with failures during reallocation.
    ss::chunked_fifo<model::ntp> allocation_failures;
    // list of currently ongoing partition reconfigurations
    std::vector<partition_reconfiguration_state> current_reconfigurations;
};

enum class cloud_storage_mode : uint8_t {
    disabled = 0,
    write_only = 1,
    read_only = 2,
    full = 3,
    read_replica = 4
};

std::ostream& operator<<(std::ostream&, const cloud_storage_mode&);

struct partition_cloud_storage_status {
    cloud_storage_mode mode;

    std::optional<std::chrono::milliseconds> since_last_manifest_upload;
    std::optional<std::chrono::milliseconds> since_last_segment_upload;
    std::optional<std::chrono::milliseconds> since_last_manifest_sync;

    size_t total_log_size_bytes{0};
    size_t cloud_log_size_bytes{0};
    size_t stm_region_size_bytes{0};
    size_t archive_size_bytes{0};
    size_t local_log_size_bytes{0};

    size_t stm_region_segment_count{0};
    size_t local_log_segment_count{0};

    // Friendlier name for archival_metadata_stm::get_dirty
    bool cloud_metadata_update_pending{false};

    std::optional<kafka::offset> cloud_log_start_offset;
    std::optional<kafka::offset> stm_region_start_offset;
    std::optional<kafka::offset> local_log_last_offset;

    std::optional<kafka::offset> cloud_log_last_offset;
    std::optional<kafka::offset> local_log_start_offset;
};

struct metrics_reporter_cluster_info
  : serde::envelope<
      metrics_reporter_cluster_info,
      serde::version<0>,
      serde::compat_version<0>> {
    ss::sstring uuid;
    model::timestamp creation_timestamp;

    bool is_initialized() const { return !uuid.empty(); }

    friend bool operator==(
      const metrics_reporter_cluster_info&,
      const metrics_reporter_cluster_info&)
      = default;

    auto serde_fields() { return std::tie(uuid, creation_timestamp); }
};

struct controller_committed_offset_request
  : serde::envelope<
      controller_committed_offset_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    auto serde_fields() { return std::tie(); }
};

struct controller_committed_offset_reply
  : serde::envelope<
      controller_committed_offset_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    model::offset last_committed;
    errc result;

    auto serde_fields() { return std::tie(last_committed, result); }
};

template<typename V>
std::ostream& operator<<(
  std::ostream& o, const configuration_with_assignment<V>& with_assignment) {
    fmt::print(
      o,
      "{{configuration: {}, assignments: {}}}",
      with_assignment.cfg,
      with_assignment.assignments);
    return o;
}

/**
 * Create/update a (Wasm) plugin.
 */
struct upsert_plugin_request
  : serde::envelope<
      upsert_plugin_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::transform_metadata transform;
    model::timeout_clock::duration timeout{};

    friend bool
    operator==(const upsert_plugin_request&, const upsert_plugin_request&)
      = default;

    auto serde_fields() { return std::tie(transform, timeout); }
};
struct upsert_plugin_response
  : serde::envelope<
      upsert_plugin_response,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    errc ec;

    friend bool
    operator==(const upsert_plugin_response&, const upsert_plugin_response&)
      = default;

    auto serde_fields() { return std::tie(ec); }
};

/**
 * Remove a (Wasm) plugin.
 */
struct remove_plugin_request
  : serde::envelope<
      remove_plugin_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    model::transform_name name;
    model::timeout_clock::duration timeout{};

    friend bool
    operator==(const remove_plugin_request&, const remove_plugin_request&)
      = default;

    auto serde_fields() { return std::tie(name, timeout); }
};
struct remove_plugin_response
  : serde::envelope<
      remove_plugin_response,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    uuid_t uuid;
    errc ec;

    friend bool
    operator==(const remove_plugin_response&, const remove_plugin_response&)
      = default;

    auto serde_fields() { return std::tie(uuid, ec); }
};

std::ostream& operator<<(std::ostream&, reconfiguration_policy);

struct update_partition_replicas_cmd_data
  : serde::envelope<
      update_partition_replicas_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::ntp ntp;
    replicas_t replicas;
    reconfiguration_policy policy;

    friend bool operator==(
      const update_partition_replicas_cmd_data&,
      const update_partition_replicas_cmd_data&)
      = default;

    auto serde_fields() { return std::tie(ntp, replicas, policy); }

    friend std::ostream&
    operator<<(std::ostream&, const update_partition_replicas_cmd_data&);
};

struct topic_disabled_partitions_set
  : serde::envelope<
      topic_disabled_partitions_set,
      serde::version<0>,
      serde::compat_version<0>> {
    // std::nullopt means that the topic is fully disabled.
    // This is different from the partitions set containing all partition ids,
    // as it will also affect partitions created later with create_partitions
    // request.
    std::optional<absl::node_hash_set<model::partition_id>> partitions;

    topic_disabled_partitions_set() noexcept
      : partitions(absl::node_hash_set<model::partition_id>{}) {}

    friend bool operator==(
      const topic_disabled_partitions_set&,
      const topic_disabled_partitions_set&)
      = default;

    auto serde_fields() { return std::tie(partitions); }

    friend std::ostream&
    operator<<(std::ostream&, const topic_disabled_partitions_set&);

    bool is_disabled(model::partition_id id) const {
        return !partitions || partitions->contains(id);
    }
    bool is_fully_disabled() const { return !partitions.has_value(); }
    bool is_fully_enabled() const { return partitions && partitions->empty(); }

    void add(model::partition_id id);
    void remove(model::partition_id id, const assignments_set& all_partitions);
    void set_fully_disabled() { partitions = std::nullopt; }
};

struct delete_topics_request
  : serde::envelope<
      delete_topics_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    std::vector<model::topic_namespace> topics_to_delete;
    std::chrono::milliseconds timeout;

    friend bool
    operator==(const delete_topics_request&, const delete_topics_request&)
      = default;

    auto serde_fields() { return std::tie(topics_to_delete, timeout); }
};

struct delete_topics_reply
  : serde::envelope<
      delete_topics_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    std::vector<topic_result> results;

    friend bool
    operator==(const delete_topics_reply&, const delete_topics_reply&)
      = default;

    auto serde_fields() { return std::tie(results); }
};

struct set_partition_shard_request
  : serde::envelope<
      set_partition_shard_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::ntp ntp;
    uint32_t shard = -1;

    friend bool operator==(
      const set_partition_shard_request&, const set_partition_shard_request&)
      = default;

    auto serde_fields() { return std::tie(ntp, shard); }
};

struct set_partition_shard_reply
  : serde::envelope<
      set_partition_shard_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    errc ec;

    friend bool operator==(
      const set_partition_shard_reply&, const set_partition_shard_reply&)
      = default;

    auto serde_fields() { return std::tie(ec); }
};

} // namespace cluster

namespace reflection {

template<>
struct adl<cluster::topic_result> {
    void to(iobuf&, cluster::topic_result&&);
    cluster::topic_result from(iobuf_parser&);
};

template<typename T>
struct adl<cluster::configuration_with_assignment<T>> {
    void
    to(iobuf& b, cluster::configuration_with_assignment<T>&& with_assignment) {
        reflection::serialize(
          b,
          std::move(with_assignment.cfg),
          std::move(with_assignment.assignments));
    }

    cluster::configuration_with_assignment<T> from(iobuf_parser& in) {
        auto cfg = adl<T>{}.from(in);
        auto assignments
          = adl<ss::chunked_fifo<cluster::partition_assignment>>{}.from(in);
        return {std::move(cfg), std::move(assignments)};
    }
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
struct adl<cluster::create_partitions_configuration> {
    void to(iobuf&, cluster::create_partitions_configuration&&);
    cluster::create_partitions_configuration from(iobuf_parser&);
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
struct adl<cluster::partition_assignment> {
    void to(iobuf&, cluster::partition_assignment&&);
    cluster::partition_assignment from(iobuf_parser&);
};

template<typename T>
struct adl<cluster::property_update<T>> {
    void to(iobuf& out, cluster::property_update<T>&& update) {
        reflection::serialize(out, std::move(update.value), update.op);
    }

    cluster::property_update<T> from(iobuf_parser& parser) {
        auto value = reflection::adl<T>{}.from(parser);
        auto op = reflection::adl<cluster::incremental_update_operation>{}.from(
          parser);
        return {std::move(value), op};
    }
};
template<>
struct adl<cluster::cluster_property_kv> {
    void to(iobuf&, cluster::cluster_property_kv&&);
    cluster::cluster_property_kv from(iobuf_parser&);
};

template<>
struct adl<cluster::cancel_moving_partition_replicas_cmd_data> {
    void
    to(iobuf& out, cluster::cancel_moving_partition_replicas_cmd_data&& d) {
        serialize(out, d.force);
    }
    cluster::cancel_moving_partition_replicas_cmd_data from(iobuf_parser& in) {
        auto force = adl<cluster::force_abort_update>{}.from(in);
        return cluster::cancel_moving_partition_replicas_cmd_data(force);
    }
};

template<>
struct adl<cluster::move_cancellation_result> {
    void to(iobuf& out, cluster::move_cancellation_result&& r) {
        serialize(out, std::move(r.ntp), r.result);
    }
    cluster::move_cancellation_result from(iobuf_parser& in) {
        auto ntp = adl<model::ntp>{}.from(in);
        auto ec = adl<cluster::errc>{}.from(in);
        return {std::move(ntp), ec};
    }
};
} // namespace reflection
