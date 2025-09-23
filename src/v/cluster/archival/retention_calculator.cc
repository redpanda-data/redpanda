/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/archival/retention_calculator.h"

#include "base/vlog.h"
#include "cluster/archival/logger.h"
#include "model/timestamp.h"

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

/*
 * Retention strategy for use with 'retention_calculator'.
 * Segments are accumulated until a segment with a max offset higher than the
 * truncation point is met.
 */
class offset_based_strategy final : public retention_strategy {
public:
    static constexpr auto strat_name = "offset_based_retention";

    explicit offset_based_strategy(kafka::offset);

    bool done(const cloud_storage::partition_manifest::segment_meta&) override;

    ss::sstring name() const override;

private:
    kafka::offset _highest_offset_to_remove;
};

std::optional<retention_calculator> retention_calculator::factory(
  const cloud_storage::partition_manifest& manifest,
  const storage::ntp_config& ntp_config,
  std::optional<kafka::offset> pinned_offset) {
    if (!ntp_config.is_remotely_collectable()) {
        vlog(
          archival_log.trace, "{} Partition not collectible", ntp_config.ntp());
        return std::nullopt;
    }

    auto arch_so = manifest.get_archive_start_offset();
    auto last_so = manifest.get_start_offset();

    vlog(
      archival_log.debug,
      "{} Creating retention calculator, ntp_config: {}, archive start offset: "
      "{}, start offset: {}, pinned kafka offset: {}",
      ntp_config.ntp(),
      ntp_config,
      arch_so,
      last_so,
      pinned_offset);

    if (arch_so != model::offset{} && arch_so != last_so) {
        // Retention should be applied to the archive area of the log first
        // otherwise we may end up with a gap in the log. If we will apply
        // retention to the STM log there will be an offset gap between the
        // last spillover segment and the first STM segment.
        return std::nullopt;
    }

    std::vector<std::unique_ptr<retention_strategy>> strats;
    strats.reserve(3);

    if (ntp_config.retention_bytes()) {
        auto total_retention_bytes = ntp_config.retention_bytes();

        auto stm_region_size = manifest.stm_region_size_bytes();
        if (stm_region_size > *total_retention_bytes) {
            auto overshot_by = stm_region_size - *total_retention_bytes;
            strats.push_back(
              std::make_unique<size_based_strategy>(overshot_by));
            vlog(
              archival_log.trace,
              "{} size based retention strategy added, total retention bytes: "
              "{}, STM regions size: {}, overshot by: {}",
              ntp_config.ntp(),
              total_retention_bytes,
              stm_region_size,
              overshot_by);
        }
    }
    auto manifest_end = manifest.end();
    if (ntp_config.retention_duration()) {
        model::timestamp oldest_allowed_timestamp{
          model::timestamp::now().value()
          - ntp_config.retention_duration()->count()};

        if (manifest.size() > 0) {
            auto first_seg = manifest.first_addressable_segment();
            if (
              first_seg != manifest_end
              && first_seg->max_timestamp < oldest_allowed_timestamp) {
                strats.push_back(
                  std::make_unique<time_based_strategy>(
                    oldest_allowed_timestamp));
                vlog(
                  archival_log.trace,
                  "{} time based retention strategy added, oldest allowed "
                  "timestamp: {}, first segment: {}",
                  ntp_config.ntp(),
                  std::chrono::duration_cast<std::chrono::seconds>(
                    model::duration_since_epoch(oldest_allowed_timestamp)),
                  *first_seg);
            }
        }
    }
    auto start_kafka_override = manifest.get_start_kafka_offset_override();
    if (start_kafka_override > kafka::offset(0)) {
        auto first_seg = manifest.first_addressable_segment();
        if (
          first_seg != manifest_end
          && start_kafka_override > first_seg->last_kafka_offset()) {
            // The user has passed in a start override via DeleteRecords, and
            // there exists at least one segment below this offset. Remove up
            // to the desired start offset.
            auto highest_to_remove = start_kafka_override - kafka::offset(1);
            strats.emplace_back(
              std::make_unique<offset_based_strategy>(highest_to_remove));
            vlog(
              archival_log.trace,
              "{} offset based retention strategy added, "
              "first segment: {}, highest offset to remove: {}",
              ntp_config.ntp(),
              *first_seg,
              highest_to_remove);
        }
    }

    if (strats.empty()) {
        return std::nullopt;
    }

    return retention_calculator{manifest, std::move(strats), pinned_offset};
}

retention_calculator::retention_calculator(
  const cloud_storage::partition_manifest& manifest,
  std::vector<std::unique_ptr<retention_strategy>> strategies,
  std::optional<kafka::offset> pinned_offset)
  : _manifest(manifest)
  , _strategies(std::move(strategies))
  , _pinned_offset(pinned_offset) {}

std::optional<model::offset> retention_calculator::next_start_offset() {
    auto it = _manifest.first_addressable_segment();
    auto end_it = _manifest.end();
    for (; it != end_it; ++it) {
        const auto& entry = *it;
        if (_pinned_offset && entry.last_kafka_offset() >= *_pinned_offset) {
            // The pin is blocking us from removing this segment and beyond.
            vlog(
              archival_log.debug,
              "{} retention is blocked on segment [{}, {}] by pin at Kafka "
              "offset {}",
              _manifest.get_ntp(),
              entry.base_kafka_offset(),
              entry.last_kafka_offset(),
              *_pinned_offset);
            break;
        }
        const auto all_done = std::all_of(
          _strategies.begin(), _strategies.end(), [&](auto& strat) {
              return strat->done(entry);
          });
        if (all_done) {
            break;
        }
    }
    if (it == _manifest.first_addressable_segment()) {
        return std::nullopt;
    }

    // We made it to the end of our strategies and our policies are still not
    // satisfied. Return just past the end -- we will truncate all segments.
    if (it == end_it) {
        return model::next_offset(_manifest.get_last_offset());
    }

    return it->base_offset;
}

std::optional<ss::sstring> retention_calculator::strategy_name() const {
    if (_strategies.size() == 1) {
        return fmt::format("[{}]", _strategies[0]->name());
    }

    if (_strategies.size() == 2) {
        return fmt::format(
          "[{}, {}]", _strategies[0]->name(), _strategies[1]->name());
    }

    if (_strategies.size() == 3) {
        return fmt::format(
          "[{}, {}, {}]",
          _strategies[0]->name(),
          _strategies[1]->name(),
          _strategies[2]->name());
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

offset_based_strategy::offset_based_strategy(kafka::offset highest_to_remove)
  : _highest_offset_to_remove(highest_to_remove) {}

bool offset_based_strategy::done(
  const cloud_storage::partition_manifest::segment_meta& current_segment_meta) {
    // If the last offset in the segment is above the truncation point, we
    // can't remove it based on offset alone.
    return current_segment_meta.last_kafka_offset() > _highest_offset_to_remove;
};

ss::sstring offset_based_strategy::name() const { return strat_name; }

} // namespace archival
