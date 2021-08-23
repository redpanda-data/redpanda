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
        // if set the value will be used during parititon recovery
        topic_recovery_enabled recovery_enabled = topic_recovery_enabled::yes;

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

    const model::ntp& ntp() const { return _ntp; }
    model::ntp& ntp() { return _ntp; }

    model::revision_id get_revision() const { return _revision_id; }

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

    // in storage/types.cc
    friend std::ostream& operator<<(std::ostream&, const ntp_config&);
};

} // namespace storage
