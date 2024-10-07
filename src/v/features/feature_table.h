/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "features/feature_state.h"
#include "security/license.h"
#include "storage/record_batch_builder.h"
#include "utils/waiter_queue.h"

#include <array>
#include <memory>
#include <string_view>
#include <unordered_set>

// cluster classes that we will make friends of the feature_table
namespace cluster {
class feature_backend;
class feature_manager;
class bootstrap_backend;
struct feature_update_action;
} // namespace cluster

namespace features {

struct feature_table_snapshot;

/// The integers in this enum must be unique wrt each other, but _not_ over
/// all time.  We always serialize features to string names, the integer is
/// only used at runtime.  Therefore it is safe to re-use an integer that
/// has been made available by another feature being retired.
enum class feature : std::uint64_t {
    serde_raft_0 = 1ULL << 5U,
    license = 1ULL << 6U,
    raft_improved_configuration = 1ULL << 7U,
    transaction_ga = 1ULL << 8U,
    raftless_node_status = 1ULL << 9U,
    rpc_v2_by_default = 1ULL << 10U,
    cloud_retention = 1ULL << 11U,
    node_id_assignment = 1ULL << 12U,
    replication_factor_change = 1ULL << 13U,
    ephemeral_secrets = 1ULL << 14U,
    seeds_driven_bootstrap_capable = 1ULL << 15U,
    tm_stm_cache = 1ULL << 16U,
    kafka_gssapi = 1ULL << 17U,
    partition_move_revert_cancel = 1ULL << 18U,
    node_isolation = 1ULL << 19U,
    group_offset_retention = 1ULL << 20U,
    rpc_transport_unknown_errc = 1ULL << 21U,
    membership_change_controller_cmds = 1ULL << 22U,
    controller_snapshots = 1ULL << 23U,
    cloud_storage_manifest_format_v2 = 1ULL << 24U,
    force_partition_reconfiguration = 1ULL << 26U,
    raft_append_entries_serde = 1ULL << 28U,
    delete_records = 1ULL << 29U,
    raft_coordinated_recovery = 1ULL << 31U,
    cloud_storage_scrubbing = 1ULL << 32U,
    enhanced_force_reconfiguration = 1ULL << 33U,
    broker_time_based_retention = 1ULL << 34U,
    wasm_transforms = 1ULL << 35U,
    raft_config_serde = 1ULL << 36U,
    fast_partition_reconfiguration = 1ULL << 38U,
    disabling_partitions = 1ULL << 39U,
    cloud_metadata_cluster_recovery = 1ULL << 40U,
    audit_logging = 1ULL << 41U,
    compaction_placeholder_batch = 1ULL << 42U,
    partition_shard_in_health_report = 1ULL << 43U,
    role_based_access_control = 1ULL << 44U,
    cluster_topic_manifest_format_v2 = 1ULL << 45U,
    node_local_core_assignment = 1ULL << 46U,
    unified_tx_state = 1ULL << 47U,
    data_migrations = 1ULL << 48U,
    group_tx_fence_dedicated_batch_type = 1ULL << 49U,
    transforms_specify_offset = 1ULL << 50U,
    remote_labels = 1ULL << 51U,
    partition_properties_stm = 1ULL << 52U,
    shadow_indexing_split_topic_property_update = 1ULL << 53U,

    // Dummy features for testing only
    test_alpha = 1ULL << 61U,
    test_bravo = 1ULL << 62U,
    test_charlie = 1ULL << 63U,
};

// Eventually, once a feature has been in use for a while, it is no longer
// behind a feature flag, and the flag itself is retired.  We remember a list
// of all retired features, because this enables us to distinguish between
// controller messages for unknown features (unexpected), and controller
// messages that refer to features that have been retired.
//
// retired does *not* mean the functionality is gone: it just means it
// is no longer guarded by a feature flag.
inline const std::unordered_set<std::string_view> retired_features = {
  "central_config",
  "consumer_offsets",
  "maintenance_mode",
  "mtls_authentication",
  "rm_stm_kafka_cache",
  "transaction_ga",
  "idempotency_v2",
  "transaction_partitioning",
  "lightweight_heartbeats",
};

// The latest_version associated with past releases. Increment this
// on protocol changes to raft0 structures, like adding new services and on each
// major version release.
//
// Although some previous stable branches have included feature version
// bumps, this is _not_ the intended usage, as stable branches are
// meant to be safely downgradable within the branch, and new features
// imply that new data formats may be written.
//
// The enum variants values should be dense between MIN and MAX inclusive.
enum class release_version : int64_t {
    MIN = 3,
    v22_1_1 = MIN,
    v22_1_5 = 4,
    v22_2_1 = 5,
    v22_2_6 = 6,
    v22_3_1 = 7,
    v22_3_6 = 8,
    v23_1_1 = 9,
    v23_2_1 = 10,
    v23_3_1 = 11,
    v24_1_1 = 12,
    v24_2_1 = 13,
    v24_3_1 = 14,
    MAX = v24_3_1, // affects the latest_version
};

constexpr cluster::cluster_version to_cluster_version(release_version rv) {
    switch (rv) {
    case release_version::v22_1_1:
    case release_version::v22_1_5:
    case release_version::v22_2_1:
    case release_version::v22_2_6:
    case release_version::v22_3_1:
    case release_version::v22_3_6:
    case release_version::v23_1_1:
    case release_version::v23_2_1:
    case release_version::v23_3_1:
    case release_version::v24_1_1:
    case release_version::v24_2_1:
    case release_version::v24_3_1:
        return cluster::cluster_version{static_cast<int64_t>(rv)};
    }
    vassert(false, "Invalid release_version");
}

bool is_major_version_upgrade(
  cluster::cluster_version from, cluster::cluster_version to);

/**
 * The definition of a feature specifies rules for when it should
 * be activated,
 */
struct feature_spec {
    // Policy defining how the feature behaves when in 'available' state.
    enum class available_policy {
        // The feature proceeds to activate as soon as it is available
        always,

        // The feature only becomes available once all cluster nodes
        // are recent enough *and* an administrator explicitly enables it.
        explicit_only,

        // The feature proceeds to activate only if the cluster's original
        // version is >= the feature's require_version
        new_clusters_only,
    };

    // Policy defining whether the feature passes through 'preparing'
    // state on the way to 'active' state.
    enum class prepare_policy {
        // The feature is activated as soon as it becomes available.
        always,

        // The feature only becomes active once a migration step has
        // completed and the feature manager has been notified of this.
        requires_migration
    };

    constexpr feature_spec(
      cluster::cluster_version require_version_,
      std::string_view name_,
      feature bits_,
      available_policy apol,
      prepare_policy ppol)
      : bits(bits_)
      , name(name_)
      , require_version(require_version_)
      , available_rule(apol)
      , prepare_rule(ppol) {}

    constexpr feature_spec(
      release_version require_version_,
      std::string_view name_,
      feature bits_,
      available_policy apol,
      prepare_policy ppol)
      : feature_spec(
          to_cluster_version(require_version_), name_, bits_, apol, ppol) {}

    feature bits{0};
    std::string_view name;
    cluster::cluster_version require_version;

    available_policy available_rule;
    prepare_policy prepare_rule;
};

inline constexpr std::array feature_schema{
  feature_spec{
    release_version::v22_2_1,
    "serde_raft_0",
    feature::serde_raft_0,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v22_2_1,
    "license",
    feature::license,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v22_2_1,
    "raft_improved_configuration",
    feature::raft_improved_configuration,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v22_2_6,
    "transaction_ga",
    feature::transaction_ga,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v22_3_1,
    "raftless_node_status",
    feature::raftless_node_status,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v22_3_1,
    "rpc_v2_by_default",
    feature::rpc_v2_by_default,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v22_3_1,
    "cloud_retention",
    feature::cloud_retention,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::requires_migration},
  feature_spec{
    release_version::v22_3_1,
    "node_id_assignment",
    feature::node_id_assignment,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v22_3_1,
    "replication_factor_change",
    feature::replication_factor_change,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v22_3_1,
    "ephemeral_secrets",
    feature::ephemeral_secrets,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v22_3_1,
    "seeds_driven_bootstrap_capable",
    feature::seeds_driven_bootstrap_capable,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v22_3_6,
    "tm_stm_cache",
    feature::tm_stm_cache,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_1_1,
    "kafka_gssapi",
    feature::kafka_gssapi,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_1_1,
    "partition_move_revert_cancel",
    feature::partition_move_revert_cancel,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_1_1,
    "node_isolation",
    feature::node_isolation,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_1_1,
    "group_offset_retention",
    feature::group_offset_retention,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_1_1,
    "rpc_transport_unknown_errc",
    feature::rpc_transport_unknown_errc,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_2_1,
    "membership_change_controller_cmds",
    feature::membership_change_controller_cmds,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_2_1,
    "controller_snapshots",
    feature::controller_snapshots,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_2_1,
    "cloud_storage_manifest_format_v2",
    feature::cloud_storage_manifest_format_v2,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_2_1,
    "force_partition_reconfiguration",
    feature::force_partition_reconfiguration,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_2_1,
    "raft_append_entries_serde",
    feature::raft_append_entries_serde,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_2_1,
    "delete_records",
    feature::delete_records,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_3_1,
    "raft_coordinated_recovery",
    feature::raft_coordinated_recovery,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_3_1,
    "cloud_storage_scrubbing",
    feature::cloud_storage_scrubbing,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_3_1,
    "enhanced_force_reconfiguration",
    feature::enhanced_force_reconfiguration,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_3_1,
    "broker_time_based_retention",
    feature::broker_time_based_retention,
    feature_spec::available_policy::new_clusters_only,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_3_1,
    "wasm_transforms",
    feature::wasm_transforms,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_3_1,
    "raft_config_serde",
    feature::raft_config_serde,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_3_1,
    "fast_partition_reconfiguration",
    feature::fast_partition_reconfiguration,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_3_1,
    "disabling_partitions",
    feature::disabling_partitions,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_3_1,
    "cloud_metadata_cluster_recovery",
    feature::cloud_metadata_cluster_recovery,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v23_3_1,
    "audit_logging",
    feature::audit_logging,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v24_1_1,
    "compaction_placeholder_batch",
    feature::compaction_placeholder_batch,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v24_1_1,
    "partition_shard_in_health_report",
    feature::partition_shard_in_health_report,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v24_1_1,
    "role_based_access_control",
    feature::role_based_access_control,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::requires_migration},
  feature_spec{
    release_version::v24_1_1,
    "cluster_topic_manifest_format_v2",
    feature::cluster_topic_manifest_format_v2,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v24_2_1,
    "node_local_core_assignment",
    feature::node_local_core_assignment,
    feature_spec::available_policy::explicit_only,
    feature_spec::prepare_policy::requires_migration},
  feature_spec{
    release_version::v24_2_1,
    "unified_tx_state",
    feature::unified_tx_state,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v24_2_1,
    "data_migrations",
    feature::data_migrations,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v24_2_1,
    "group_tx_fence_dedicated_batch_type",
    feature::group_tx_fence_dedicated_batch_type,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v24_2_1,
    "transforms_specify_offset",
    feature::transforms_specify_offset,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v24_2_1,
    "remote_labels",
    feature::remote_labels,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v24_3_1,
    "partition_properties_stm",
    feature::partition_properties_stm,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    release_version::v24_3_1,
    "shadow_indexing_split_topic_property_update",
    feature::shadow_indexing_split_topic_property_update,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
};

std::string_view to_string_view(feature);
std::string_view to_string_view(feature_state::state);

/**
 * To enable all shards to efficiently check enablement of features
 * in their hot paths, the cluster logical version and features
 * are copied onto each shard.
 *
 * Instances of this class are updated by feature_manager.
 */
class feature_table {
public:
    cluster::cluster_version get_active_version() const noexcept {
        return _active_version;
    }

    cluster::cluster_version get_original_version() const noexcept {
        return _original_version;
    }

    const std::vector<feature_state>& get_feature_state() const {
        return _feature_state;
    }

    /**
     * Query whether a feature is active, i.e. whether functionality
     * depending on this feature should be allowed to run.
     *
     * Keep this small and simple to be used in hot paths that need to check
     * for feature enablement.
     */
    bool is_active(feature f) const noexcept {
        return (uint64_t(f) & _active_features_mask) != 0;
    }

    /**
     * Query whether a feature has reached ::preparing state, i.e. it is
     * ready to go active, but will wait for preparatory work to be done
     * elsewhere in the system first.
     *
     * Once the preparatory work (like a data migration) is complete,
     * the feature may be advanced to ::active with a `feature_action`
     * RPC to the controller leader
     */
    bool is_preparing(feature f) const noexcept {
        return get_state(f).get_state() == feature_state::state::preparing;
    }

    ss::future<> await_feature(feature f, ss::abort_source& as);

    /**
     * Variant of await_feature that uses a built-in abort source rather
     * than requiring one to be passed in.
     *
     * You should almost always use an explicit abort_source version above:
     * using the built-in feature table abort source is only for locations
     * early in startup that otherwise don't have an abort source
     * handy.
     */
    ss::future<> await_feature(feature f) { return await_feature(f, _as); };

    /**
     * Like await_feature, but runs the given function once the feature is
     * successfully activated.
     *
     * If the feature never activates (i.e. if shutting down while waiting),
     * the given function is not run.
     */
    ss::future<> await_feature_then(feature f, std::function<void(void)> fn);

    ss::future<> await_feature_preparing(feature f, ss::abort_source& as);

    ss::future<> stop();

    static cluster::cluster_version get_latest_logical_version();

    static cluster::cluster_version get_earliest_logical_version();

    feature_table();
    feature_table(const feature_table&) = delete;
    feature_table& operator=(const feature_table&) = delete;
    feature_table(feature_table&&) = delete;
    feature_table& operator=(feature_table&&) = delete;
    ~feature_table() noexcept;

    feature_state& get_state(feature f_id);
    const feature_state& get_state(feature f_id) const {
        return const_cast<feature_table&>(*this).get_state(f_id);
    }

    std::optional<feature> resolve_name(std::string_view feature_name) const;

    void set_license(security::license license);

    void revoke_license();

    const std::optional<security::license>& get_license() const;

    /**
     * For use in unit tests: activate all features that would
     * be auto-activated after a normal cluster startup of the
     * latest version.
     */
    void testing_activate_all();

    model::offset get_applied_offset() const { return _applied_offset; }

    enum class version_durability : uint8_t {
        // An ephemeral update, such as generated on first startup: this updates
        // our active version but does not influence original_version
        ephemeral = 0,

        // A durable update, originating from a message in the controller log.
        durable = 1,
    };

    /** application and bootstrap_backend may use this to fast-forward a
     * feature table to the desired version synchronously, early in the
     * lifetime of a node.
     * @param ephemeral if true, this is a node-local in-memory fast-forward of
     * the feature table on initial node start.  Do not initialize original
     * version, that will come later when we get a true cluster version set.
     */
    void bootstrap_active_version(
      cluster::cluster_version,
      version_durability durability = version_durability::durable);

    // During upgrades from Redpanda <= 22.3 where the feature table snapshot
    // does not contain original_version, we infer it from a bootstrap event.
    void bootstrap_original_version(cluster::cluster_version);

    void abort_for_tests() { _as.request_abort(); }

    /*
     * The version fence is the structure encoded into a version fence batch
     * type and is intended to hold information that is useful for partitioning
     * the contents of a log with respect the time at which version and feature
     * metadata events occurred.
     */
    struct version_fence
      : serde::
          envelope<version_fence, serde::version<0>, serde::compat_version<0>> {
        cluster::cluster_version active_version;
        auto serde_fields() { return std::tie(active_version); }
    };

    static constexpr std::string_view version_fence_batch_key = "state";
    static version_fence decode_version_fence(model::record_batch batch);

    /*
     * Build a version fence that the caller should append to a log.
     *
     * This call will fail if the active version is not at least as new as the
     * specified minimum. It is expected that the caller wait until this
     * condition is guaranteed, such as waiting on a feature that becomes active
     * at desired cluster version.
     */
    model::record_batch
    encode_version_fence(cluster::cluster_version min_expected) const {
        /*
         * TODO: Right now the caller needs to provide linking against storage
         * and cluster for batch builder tooling. If it is decided that this is
         * the best place to build the version fence batch then this function
         * can be moved out of the header into feature_table.cc. But doing that
         * will require moving record_batch_builder from storage:: to a place
         * like model:: which is simple and mechanical but quite a large change
         * to commit to up front.
         */
        version_fence f;
        vassert(
          _active_version >= min_expected,
          "Cannot build version fence for active version {} < min version {}",
          _active_version,
          min_expected);
        f.active_version = _active_version;
        storage::record_batch_builder builder(
          model::record_batch_type::version_fence, model::offset(0));
        builder.add_raw_kv(
          serde::to_iobuf(ss::sstring(version_fence_batch_key)),
          serde::to_iobuf(f));
        return std::move(builder).build();
    }

    // Assert out on startup if we appear to have upgraded too far
    void assert_compatible_version(bool);

    // Visible for testing
    static long long calculate_expiry_metric(
      const std::optional<security::license>& license,
      security::license::clock::time_point now
      = security::license::clock::now());

private:
    class probe;

    // Only for use by our friends feature backend & manager
    void set_active_version(
      cluster::cluster_version,
      version_durability d = version_durability::durable);
    void apply_action(const cluster::feature_update_action& fua);

    // The controller log offset of last batch applied to this state machine
    void set_applied_offset(model::offset o) { _applied_offset = o; }
    void set_original_version(cluster::cluster_version v);

    void on_update();

    cluster::cluster_version _active_version{cluster::invalid_version};

    // The earliest version this cluster ever saw: guaranteed that no
    // on-disk structures were written with an encoding that predates
    // this.
    cluster::cluster_version _original_version{cluster::invalid_version};

    std::vector<feature_state> _feature_state;

    // Bitmask only used at runtime: if we run out of bits for features
    // just use a bigger one.  Do not serialize this as a bitmask anywhere.
    uint64_t _active_features_mask{0};

    // Waiting for a particular feature to be active
    waiter_queue<feature> _waiters_active;

    // Waiting for a particular feature to be preparing
    waiter_queue<feature> _waiters_preparing;

    // Currently loaded redpanda license details
    std::optional<security::license> _license;

    model::offset _applied_offset{};

    // feature_manager is a friend so that they can initialize
    // the active version on single-node first start.
    friend class cluster::feature_manager;

    // feature_backend is a friend for routine updates when
    // applying raft0 log events.
    friend class cluster::feature_backend;

    // for set_applied_offset when applying the bootstrap cmd
    friend class cluster::bootstrap_backend;

    // Unit testing hook.
    friend class feature_table_fixture;

    // Permit snapshot generation to read internals
    friend struct feature_table_snapshot;

    ss::gate _gate;
    ss::abort_source _as;
    std::unique_ptr<probe> _probe;
};

} // namespace features

template<>
struct fmt::formatter<features::feature_state::state> final
  : fmt::formatter<std::string_view> {
    template<typename FormatContext>
    auto
    format(const features::feature_state::state& s, FormatContext& ctx) const {
        return formatter<string_view>::format(features::to_string_view(s), ctx);
    }
};
