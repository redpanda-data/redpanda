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

#include "cluster/types.h"
#include "features/feature_state.h"
#include "security/license.h"
#include "storage/record_batch_builder.h"
#include "utils/waiter_queue.h"

#include <array>
#include <string_view>

// cluster classes that we will make friends of the feature_table
namespace cluster {
class feature_backend;
class feature_manager;
class bootstrap_backend;
} // namespace cluster

namespace features {

struct feature_table_snapshot;

enum class feature : std::uint64_t {
    central_config = 1ULL,
    consumer_offsets = 1ULL << 1U,
    maintenance_mode = 1ULL << 2U,
    mtls_authentication = 1ULL << 3U,
    rm_stm_kafka_cache = 1ULL << 4U,
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
    transaction_partitioning = 1ULL << 25U,

    // Dummy features for testing only
    test_alpha = 1ULL << 62U,
    test_bravo = 1ULL << 63U,
};

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

    feature bits{0};
    std::string_view name;
    cluster::cluster_version require_version;

    available_policy available_rule;
    prepare_policy prepare_rule;
};

constexpr static std::array feature_schema{
  feature_spec{
    cluster::cluster_version{1},
    "central_config",
    feature::central_config,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{2},
    "consumer_offsets",
    feature::consumer_offsets,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{3},
    "maintenance_mode",
    feature::maintenance_mode,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{3},
    "mtls_authentication",
    feature::mtls_authentication,
    feature_spec::available_policy::explicit_only,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{4},
    "rm_stm_kafka_cache",
    feature::rm_stm_kafka_cache,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{5},
    "serde_raft_0",
    feature::serde_raft_0,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{5},
    "license",
    feature::license,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{5},
    "raft_improved_configuration",
    feature::raft_improved_configuration,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{6},
    "transaction_ga",
    feature::transaction_ga,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{7},
    "raftless_node_status",
    feature::raftless_node_status,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{7},
    "rpc_v2_by_default",
    feature::rpc_v2_by_default,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{7},
    "cloud_retention",
    feature::cloud_retention,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::requires_migration},
  feature_spec{
    cluster::cluster_version{7},
    "node_id_assignment",
    feature::node_id_assignment,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{7},
    "replication_factor_change",
    feature::replication_factor_change,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{7},
    "ephemeral_secrets",
    feature::ephemeral_secrets,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{7},
    "seeds_driven_bootstrap_capable",
    feature::seeds_driven_bootstrap_capable,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{8},
    "tm_stm_cache",
    feature::tm_stm_cache,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{9},
    "kafka_gssapi",
    feature::kafka_gssapi,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{9},
    "partition_move_revert_cancel",
    feature::partition_move_revert_cancel,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{9},
    "node_isolation",
    feature::node_isolation,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{9},
    "group_offset_retention",
    feature::group_offset_retention,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{9},
    "rpc_transport_unknown_errc",
    feature::rpc_transport_unknown_errc,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{10},
    "membership_change_controller_cmds",
    feature::membership_change_controller_cmds,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{10},
    "controller_snapshots",
    feature::controller_snapshots,
    feature_spec::available_policy::explicit_only,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{10},
    "cloud_storage_manifest_format_v2",
    feature::cloud_storage_manifest_format_v2,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster::cluster_version{10},
    "transaction_partitioning",
    feature::transaction_partitioning,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},

  // For testing, a feature that does not auto-activate
  feature_spec{
    cluster::cluster_version{2001},
    "__test_alpha",
    feature::test_alpha,
    feature_spec::available_policy::explicit_only,
    feature_spec::prepare_policy::always},

  // For testing, a feature that auto-activates
  feature_spec{
    cluster::cluster_version{2001},
    "__test_bravo",
    feature::test_bravo,
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

private:
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

    // Unit testing hook.
    friend class feature_table_fixture;

    // Permit snapshot generation to read internals
    friend struct feature_table_snapshot;

    ss::gate _gate;
    ss::abort_source _as;
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
