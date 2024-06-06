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

#include "base/units.h"
#include "group_configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/consensus_utils.h"
#include "raft/fundamental.h"
#include "raft/logger.h"
#include "ssx/semaphore.h"
#include "storage/fwd.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>

#include <absl/container/btree_map.h>

#include <utility>
#include <vector>

namespace raft {

/**
 * The raft::configuration_manager is responsible for keeping all raft
 * configurations that are accessible in the log. The configuration manager is
 * used when creating the snapshots to access the configuration that corresponds
 * the snapshot last included offset. Additionally the configuration manager
 * stores all configuration in storage::kvstore, to speed up raft groups
 * recovery. Manager internal logic updates the last_highest knows offset every
 * 64MB of persisted data and when configuration is added to the manager. Thanks
 * to this when raft group is starting it only has to read up to 64MB of data to
 * find configurations that may not be included in the configuration manager.
 * The highest known offset is not group_configuration offset, it is an offset
 * up to which all configuration are guranted to be present in configuration
 * manager.
 */
class configuration_manager {
public:
    using configuration_idx = named_type<int64_t, struct configuration_idx_tag>;
    struct indexed_configuration {
        indexed_configuration(group_configuration c, configuration_idx i)
          : cfg(std::move(c))
          , idx(i) {}

        group_configuration cfg;
        configuration_idx idx;
    };

    // using ordered map in here to execute truncations and being able to
    // efficiently search for configurations with offset smaller or equal than
    // requested
    using underlying_t = absl::btree_map<model::offset, indexed_configuration>;
    using const_iterator = underlying_t::const_iterator;

    static constexpr size_t offset_update_treshold = 64_MiB;

    configuration_manager(
      group_configuration, raft::group_id, storage::api&, ctx_log&);

    ss::future<> start(bool reset, model::revision_id);

    ss::future<> stop();
    /**
     * Removes all configurations starting from the one at given offset
     */
    ss::future<> truncate(model::offset);

    /**
     * Removes all configurations up to given offset
     */
    ss::future<> prefix_truncate(model::offset);

    /**
     * Add configuration at given offset
     */
    ss::future<> add(model::offset, group_configuration);

    /**
     * Add all configurations
     */
    ss::future<> add(std::vector<offset_configuration>);

    /**
     * Get the configuration that is valid for given offset. This method return
     * configuration that was active for requested offset. That is the
     * configuration with biggest offset which is not greater than the requested
     * one.
     *
     * Example:
     *
     * [...][configuration@offset=100][...][configuration@offset=123][...]
     *
     * for offsets in range 100,122 (inclusive) valid configuration is at offset
     * 100
     */
    std::optional<group_configuration> get(model::offset) const;

    /**
     * Get latest configuration.
     */
    const group_configuration& get_latest() const;

    /**
     * Get latest configuration offset.
     */
    model::offset get_latest_offset() const;
    /**
     * Get latest configuration index.
     */
    configuration_idx get_latest_index() const;

    /**
     * Persist highest known offset to KV store when stored bytes threshold has
     * been reached. The first argument is an offset that was appended to the
     * log. The second method argument is number of bytes persisted to disk in
     * last append. Configuration manager tracks number of bytes that were
     * appended since last write of `highest_known_offset` to kv-store
     */
    void maybe_store_highest_known_offset_in_background(
      model::offset, size_t bytes, ss::gate&);

    /**
     * Returns the highest offset for which the configuration manager
     * contains valid configuration set. Beyond that offset log has to be
     * searched for configuration.
     */
    model::offset get_highest_known_offset() const {
        return _highest_known_offset;
    }
    /**
     * Waits for changes in configuration newer than requested offset, if latest
     * configuration has offset greater then the one requested it will return
     * immediately
     */
    ss::future<offset_configuration>
    wait_for_change(model::offset, ss::abort_source&);
    /**
     * Return revision of last known configuration
     */
    model::revision_id get_latest_revision() const;

    /**
     * Removes state that configuration manager stores in key value store
     */
    ss::future<> remove_persistent_state();

    const_iterator begin() const { return _configurations.begin(); }
    const_iterator end() const { return _configurations.end(); }
    const_iterator lower_bound(model::offset o) const {
        return _configurations.lower_bound(o);
    }

    int64_t offset_delta(model::offset) const;

    ss::future<> adjust_configuration_idx(configuration_idx);

    /**
     * Sets a forced override for current configuration. The override is active
     * and returned as latest configuration until it is cleared by adding a new
     * configuration to group configuration manage.
     *
     * @param cfg the configuration to override with
     */
    void set_override(group_configuration);

    /**
     * Checks if configuration override is active
     */
    bool has_configuration_override() const {
        return _configuration_force_override != nullptr;
    }

    friend std::ostream&
    operator<<(std::ostream&, const configuration_manager&);

private:
    void reset_override(model::revision_id);
    ss::future<> store_configurations();
    ss::future<> store_highest_known_offset();
    bytes configurations_map_key() const {
        return raft::details::serialize_group_key(
          _group, metadata_key::config_map);
    }

    bytes highest_known_offset_key() const {
        return raft::details::serialize_group_key(
          _group, metadata_key::config_latest_known_offset);
    }

    bytes next_configuration_idx_key() const {
        return raft::details::serialize_group_key(
          _group, metadata_key::config_next_cfg_idx);
    }

    void add_configuration(model::offset, group_configuration);

    ss::future<> do_maybe_store_highest_known_offset(size_t bytes);

    raft::group_id _group;
    underlying_t _configurations;
    /**
     * The highest know offset is latest offset for which configuration manager
     * has all configurations. In other words, some configuration may be in the
     * log at offsets higher than highest known offset
     */
    model::offset _highest_known_offset;
    storage::api& _storage;
    ss::condition_variable _config_changed;
    mutex _lock{"configuration_manager"};
    /**
     * We will persist highest known offset every 64MB, given this during
     * bootstrap redpanda will have to read up to 64MB per raft group.
     */
    size_t _bytes_since_last_offset_update = 0;

    // Units issued by the storage resource manager to track how many bytes
    // of data is currently pending checkpoint.
    ssx::semaphore_units _bytes_since_last_offset_update_units;

    // set to true when we checkpoint the highest known offset.
    bool _hko_checkpoint_in_progress = false;

    model::revision_id _initial_revision{};
    ctx_log& _ctxlog;
    configuration_idx _next_index{0};
    std::unique_ptr<group_configuration> _configuration_force_override
      = nullptr;
};
} // namespace raft
