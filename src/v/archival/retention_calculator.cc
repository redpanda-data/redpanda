/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/retention_calculator.h"

namespace archival {
/*
 * Retention strategy for use with 'retention_calculator'.
 * Segments are accumulated until a segment with a new enough
 * timestamp is met. This is a naive approach that assumes
 * that timestamps are monotonically increasing. That's not always
 * the case, but treating them as such works well enough in most cases.
 */
class time_based_strategy final : public retention_strategy {
public:
    static constexpr auto strat_name = "time_based_retention";

    explicit time_based_strategy(model::timestamp);

    bool done(const cloud_storage::partition_manifest::segment_meta&) override;

    ss::sstring name() const override;

private:
    model::timestamp _oldest_allowed_timestamp;
};

/*
 * Retention strategy for use with 'retention_calculator'.
 * Segments are accumulated from the beggining of the "log" (read as manifest)
 * until enough has been reclaimed to cover for the provided overshoot.
 */
class size_based_strategy final : public retention_strategy {
public:
    static constexpr auto strat_name = "size_based_retention";

    explicit size_based_strategy(uint64_t);

    bool done(const cloud_storage::partition_manifest::segment_meta&) override;

    ss::sstring name() const override;

private:
    const uint64_t _overshot_by;
    size_t _reclaimed{0};
};

std::optional<retention_calculator> retention_calculator::factory(
  const cloud_storage::partition_manifest& manifest,
  const storage::ntp_config& ntp_config) {
    if (!ntp_config.is_collectable()) {
        return std::nullopt;
    }

    std::vector<std::unique_ptr<retention_strategy>> strats;
    strats.reserve(2);

    if (ntp_config.retention_bytes()) {
        auto total_retention_bytes = ntp_config.retention_bytes();

        auto cloud_log_size = manifest.cloud_log_size();
        if (cloud_log_size > *total_retention_bytes) {
            auto overshot_by = cloud_log_size - *total_retention_bytes;
            strats.push_back(
              std::make_unique<size_based_strategy>(overshot_by));
        }
    }

    if (ntp_config.retention_duration()) {
        model::timestamp oldest_allowed_timestamp{
          model::timestamp::now().value()
          - ntp_config.retention_duration()->count()};

        if (
          manifest.size() > 0
          && manifest.begin()->second.max_timestamp
               < oldest_allowed_timestamp) {
            strats.push_back(
              std::make_unique<time_based_strategy>(oldest_allowed_timestamp));
        }
    }

    if (strats.empty()) {
        return std::nullopt;
    }

    return retention_calculator{manifest, std::move(strats)};
}

retention_calculator::retention_calculator(
  const cloud_storage::partition_manifest& manifest,
  std::vector<std::unique_ptr<retention_strategy>> strategies)
  : _manifest(manifest)
  , _strategies(std::move(strategies)) {}

std::optional<model::offset> retention_calculator::next_start_offset() {
    auto begin = _manifest.first_addressable_segment();
    auto it = std::find_if(
      begin, _manifest.end(), [this](const auto& entry) -> bool {
          return std::all_of(
            _strategies.begin(), _strategies.end(), [&](auto& strat) {
                return strat->done(entry.second);
            });
      });

    // We made it to the end of our strategies and our policies are still not
    // satisfied. Return just past the end -- we will truncate all segments.
    if (it == _manifest.end()) {
        return model::next_offset(std::prev(it)->second.committed_offset);
    }

    return it->second.base_offset;
}

std::optional<ss::sstring> retention_calculator::strategy_name() const {
    if (_strategies.size() == 1) {
        return fmt::format("[{}]", _strategies[0]->name());
    }

    if (_strategies.size() == 2) {
        return fmt::format(
          "[{}]", _strategies[0]->name(), _strategies[1]->name());
    }

    return std::nullopt;
}

size_based_strategy::size_based_strategy(uint64_t overshot_by)
  : _overshot_by{overshot_by} {}

bool size_based_strategy::done(
  const cloud_storage::partition_manifest::segment_meta& current_segment_meta) {
    if (_reclaimed >= _overshot_by) {
        return true;
    } else {
        _reclaimed += current_segment_meta.size_bytes;
        return false;
    }
}

ss::sstring size_based_strategy::name() const { return strat_name; }

time_based_strategy::time_based_strategy(
  model::timestamp oldest_allowed_timestamp)
  : _oldest_allowed_timestamp(oldest_allowed_timestamp) {}

bool time_based_strategy::done(
  const cloud_storage::partition_manifest::segment_meta& current_segment_meta) {
    return current_segment_meta.max_timestamp >= _oldest_allowed_timestamp;
};

ss::sstring time_based_strategy::name() const { return strat_name; }

} // namespace archival
