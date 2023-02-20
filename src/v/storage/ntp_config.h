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
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "ssx/sformat.h"
#include "tristate.h"

#include <seastar/core/sstring.hh>

#include <optional>

namespace storage {
using with_cache = ss::bool_class<struct log_cache_tag>;
using topic_recovery_enabled
  = ss::bool_class<struct topic_recovery_enabled_tag>;

class ntp_config {
public:
    // Remote deletes are enabled by default in new tiered storage topics,
    // disabled by default in legacy topics during upgrade (the legacy path
    // is handled during adl/serde decode).
    static constexpr bool default_remote_delete{true};
    static constexpr bool legacy_remote_delete{false};

    struct default_overrides {
        // if not set use the log_manager's configuration
        std::optional<model::cleanup_policy_bitflags> cleanup_policy_bitflags;
        // if not set use the log_manager's configuration
        std::optional<model::compaction_strategy> compaction_strategy;
        // if not set, use the log_manager's configuration
        std::optional<size_t> segment_size;

        // partition retention settings. If tristate is disabled the feature
        // will be disabled if there is no value set the default will be used
        tristate<size_t> retention_bytes{std::nullopt};
        tristate<std::chrono::milliseconds> retention_time{std::nullopt};
        // if set, log will not use batch cache
        with_cache cache_enabled = with_cache::yes;
        // if set the value will be used during partition recovery
        topic_recovery_enabled recovery_enabled = topic_recovery_enabled::yes;
        // if set the value will control how data is uploaded and retrieved
        // to/from S3
        std::optional<model::shadow_indexing_mode> shadow_indexing_mode;

        std::optional<bool> read_replica;

        tristate<size_t> retention_local_target_bytes{std::nullopt};
        tristate<std::chrono::milliseconds> retention_local_target_ms{
          std::nullopt};

        // Controls whether topic deletion should imply deletion in S3
        std::optional<bool> remote_delete;

        // time before rolling a segment, from first write
        tristate<std::chrono::milliseconds> segment_ms{std::nullopt};

        friend std::ostream&
        operator<<(std::ostream&, const default_overrides&);
    };

    ntp_config(model::ntp n, ss::sstring base_dir) noexcept
      : _ntp(std::move(n))
      , _base_dir(std::move(base_dir)) {}

    ntp_config(
      model::ntp n,
      ss::sstring base_dir,
      std::unique_ptr<default_overrides> overrides) noexcept
      : ntp_config(
        std::move(n),
        std::move(base_dir),
        std::move(overrides),
        model::revision_id(0)) {}

    ntp_config(
      model::ntp n,
      ss::sstring base_dir,
      std::unique_ptr<default_overrides> overrides,
      model::revision_id id) noexcept
      : _ntp(std::move(n))
      , _base_dir(std::move(base_dir))
      , _overrides(std::move(overrides))
      , _revision_id(id) {}

    ntp_config(
      model::ntp n,
      ss::sstring base_dir,
      std::unique_ptr<default_overrides> overrides,
      model::revision_id id,
      model::initial_revision_id initial_id) noexcept
      : _ntp(std::move(n))
      , _base_dir(std::move(base_dir))
      , _overrides(std::move(overrides))
      , _revision_id(id)
      , _initial_rev(initial_id) {}

    const model::ntp& ntp() const { return _ntp; }
    model::ntp& ntp() { return _ntp; }

    model::revision_id get_revision() const { return _revision_id; }

    model::initial_revision_id get_initial_revision() const {
        return _initial_rev;
    }

    const ss::sstring& base_directory() const { return _base_dir; }
    ss::sstring& base_directory() { return _base_dir; }

    const default_overrides& get_overrides() const { return *_overrides; }
    default_overrides& get_overrides() { return *_overrides; }

    bool has_overrides() const { return _overrides != nullptr; }

    bool is_compacted() const {
        if (_overrides && _overrides->cleanup_policy_bitflags) {
            return (_overrides->cleanup_policy_bitflags.value()
                    & model::cleanup_policy_bitflags::compaction)
                   == model::cleanup_policy_bitflags::compaction;
        }
        return false;
    }

    bool is_collectable() const {
        // has no overrides
        if (!_overrides || !_overrides->cleanup_policy_bitflags) {
            return true;
        }
        // check if deletion bitflag is set
        return (_overrides->cleanup_policy_bitflags.value()
                & model::cleanup_policy_bitflags::deletion)
               == model::cleanup_policy_bitflags::deletion;
    }

    ss::sstring work_directory() const {
        return ssx::sformat("{}/{}_{}", _base_dir, _ntp.path(), _revision_id);
    }

    std::filesystem::path topic_directory() const {
        return std::filesystem::path(_base_dir) / _ntp.topic_path();
    }

    with_cache cache_enabled() const {
        return with_cache(!has_overrides() || _overrides->cache_enabled);
    }

    void set_overrides(default_overrides o) {
        _overrides = std::make_unique<default_overrides>(o);
    }

    std::optional<size_t> retention_bytes() const {
        if (_overrides) {
            // Handle the special "-1" case.
            if (_overrides->retention_bytes.is_disabled()) {
                return std::nullopt;
            }
            if (_overrides->retention_bytes.has_optional_value()) {
                return _overrides->retention_bytes.value();
            }
            // If no value set, fall through and use the cluster-wide default.
        }
        return config::shard_local_cfg().retention_bytes();
    }

    std::optional<std::chrono::milliseconds> retention_duration() const {
        if (_overrides) {
            // Handle the special "-1" case.
            if (_overrides->retention_time.is_disabled()) {
                return std::nullopt;
            }
            if (_overrides->retention_time.has_optional_value()) {
                return _overrides->retention_time.value();
            }
            // If no value set, fall through and use the cluster-wide default.
        }
        return config::shard_local_cfg().delete_retention_ms();
    }

    bool is_archival_enabled() const {
        return _overrides != nullptr && _overrides->shadow_indexing_mode
               && model::is_archival_enabled(
                 _overrides->shadow_indexing_mode.value());
    }

    bool is_remote_fetch_enabled() const {
        return _overrides != nullptr && _overrides->shadow_indexing_mode
               && model::is_fetch_enabled(
                 _overrides->shadow_indexing_mode.value());
    }

    bool is_read_replica_mode_enabled() const {
        return _overrides != nullptr && _overrides->read_replica
               && _overrides->read_replica.value();
    }

    /**
     * True if the topic is configured for "normal" tiered storage, i.e.
     * both reads and writes to S3, and is not a read replica.
     */
    bool is_tiered_storage() const {
        return _overrides != nullptr
               && !_overrides->read_replica.value_or(false)
               && _overrides->shadow_indexing_mode
                    == model::shadow_indexing_mode::full;
    }

    bool remote_delete() const {
        if (_overrides == nullptr) {
            return default_remote_delete;
        } else {
            return _overrides->remote_delete.value_or(default_remote_delete);
        }
    }

    auto segment_ms() const -> std::optional<std::chrono::milliseconds> {
        if (_overrides) {
            if (_overrides->segment_ms.is_disabled()) {
                return std::nullopt;
            }
            if (_overrides->segment_ms.has_optional_value()) {
                return _overrides->segment_ms.value();
            }
            // fall through to server config
        }
        return config::shard_local_cfg().log_segment_ms;
    }

private:
    model::ntp _ntp;
    /// \brief currently this is the basedir. In the future
    /// this will be used to load balance on devices so that there is no
    /// implicit hierarchy, simply directories with data
    ss::sstring _base_dir;

    std::unique_ptr<default_overrides> _overrides;

    /**
     * A number indicating an id of the NTP in case it was created more
     * than once (i.e. created, deleted and then created again)
     */
    model::revision_id _revision_id{0};

    /**
     * A number indicating an initial revision of the NTP. The revision
     * of the NTP might change when the partition is moved between the
     * nodes. The initial revision is the revision_id that was assigned
     * to the topic when it was created.
     */
    model::initial_revision_id _initial_rev{0};

    // in storage/types.cc
    friend std::ostream& operator<<(std::ostream&, const ntp_config&);
};

} // namespace storage
