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
#include "utils/waiter_queue.h"

#include <array>
#include <string_view>

// cluster classes that we will make friends of the feature_table
namespace cluster {
class feature_backend;
class feature_manager;
} // namespace cluster

namespace features {

struct feature_table_snapshot;

enum class feature : std::uint64_t {
    central_config = 0x1,
    consumer_offsets = 0x2,
    maintenance_mode = 0x4,
    mtls_authentication = 0x8,
    rm_stm_kafka_cache = 0x10,
    serde_raft_0 = 0x20,
    license = 0x40,
    raft_improved_configuration = 0x80,
    transaction_ga = 0x100,
    raftless_node_status = 0x200,
    rpc_v2_by_default = 0x400,
    cloud_retention = 0x800,
    node_id_assignment = 0x1000,
    replication_factor_change = 0x2000,
    ephemeral_secrets = 0x4000,
    seeds_driven_bootstrap_capable = 0x8000,
    tm_stm_cache = 0x10000,

    // Dummy features for testing only
    test_alpha = uint64_t(1) << 63,
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
    cluster::cluster_version{2001},
    "__test_alpha",
    feature::test_alpha,
    feature_spec::available_policy::explicit_only,
    feature_spec::prepare_policy::always}};

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

    ss::future<> await_feature_preparing(feature f, ss::abort_source& as);

    ss::future<> stop();

    static cluster::cluster_version get_latest_logical_version();

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

private:
    // Only for use by our friends feature backend & manager
    void set_active_version(cluster::cluster_version);
    void apply_action(const cluster::feature_update_action& fua);

    // The controller log offset of last batch applied to this state machine
    void set_applied_offset(model::offset o) { _applied_offset = o; }
    void set_original_version(cluster::cluster_version v) {
        _original_version = v;
    }

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
