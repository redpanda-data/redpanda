#pragma once
#include "model/fundamental.h"
#include "tristate.h"

#include <seastar/core/sstring.hh>

#include <optional>

namespace storage {
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
      : _ntp(std::move(n))
      , _base_dir(std::move(base_dir))
      , _overrides(std::move(overrides)) {}

    const model::ntp& ntp() const { return _ntp; }
    const ss::sstring& base_directory() const { return _base_dir; }
    const default_overrides& get_overrides() const { return *_overrides; }
    bool has_overrides() const { return _overrides != nullptr; }
    ss::sstring work_directory() const {
        return fmt::format("{}/{}", _base_dir, _ntp.path());
    }

private:
    model::ntp _ntp;
    /// \brief currently this is the basedir. In the future
    /// this will be used to load balance on devices so that there is no
    /// implicit hierarchy, simply directories with data
    ss::sstring _base_dir;

    std::unique_ptr<default_overrides> _overrides;

    // in storage/types.cc
    friend std::ostream& operator<<(std::ostream&, const ntp_config&);
};

} // namespace storage
