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
#include "container/chunked_hash_map.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "utils/moving_average.h"

#include <seastar/core/lowres_clock.hh>

#include <optional>

namespace kafka {

struct partition_metadata {
    model::offset start_offset;
    model::offset high_watermark;
    model::offset last_stable_offset;
    size_t avg_bytes_per_offset;
    size_t avg_bytes_per_batch;
};

using namespace std::chrono_literals;

class fetch_metadata_cache {
public:
    explicit fetch_metadata_cache()
      : _eviction_timer([this] { evict(); }) {
        _eviction_timer.arm_periodic(eviction_timeout);
    }

    fetch_metadata_cache(const fetch_metadata_cache&) = delete;
    fetch_metadata_cache(fetch_metadata_cache&&) = delete;

    fetch_metadata_cache& operator=(const fetch_metadata_cache&) = delete;
    fetch_metadata_cache& operator=(fetch_metadata_cache&&) = delete;
    ~fetch_metadata_cache() = default;

    void insert_or_assign(
      const model::ktp_with_hash& ktp,
      model::offset start_offset,
      model::offset hw,
      model::offset lso,
      size_t offset_count,
      size_t batch_count,
      size_t total_size_bytes) {
        auto& e = _cache[ktp];
        e.reset_ts();

        auto rel_err = [](size_t sum, size_t count, auto& mov_avg) {
            if (mov_avg.has_samples()) {
                auto actual = static_cast<double>(sum)
                              / static_cast<double>(count);
                auto avg = static_cast<double>(mov_avg.get());
                auto abs_err = std::abs(actual - avg);
                const auto epsilon = 1e-6; // prevent division by zero
                auto re = abs_err / (actual + epsilon);
                return re;
            }
            return 0.0;
        };

        if (offset_count > 0) {
            auto bytes_per_offset_error = rel_err(
              total_size_bytes, offset_count, e.bytes_per_offset);
            _bytes_per_offset_error.update(bytes_per_offset_error, e.timestamp);
            e.bytes_per_offset.update(
              total_size_bytes, e.timestamp, offset_count);
        }

        if (batch_count > 0) {
            auto bytes_per_batch_error = rel_err(
              total_size_bytes, batch_count, e.bytes_per_batch);
            _bytes_per_batch_error.update(bytes_per_batch_error, e.timestamp);
            e.bytes_per_batch.update(
              total_size_bytes, e.timestamp, batch_count);
        }

        e.md = {
          .start_offset = start_offset,
          .high_watermark = hw,
          .last_stable_offset = lso,
          .avg_bytes_per_offset = e.bytes_per_offset.has_samples()
                                    ? e.bytes_per_offset.get()
                                    : 0,
          .avg_bytes_per_batch = e.bytes_per_batch.has_samples()
                                   ? e.bytes_per_batch.get()
                                   : 0,
        };
    }

    std::optional<partition_metadata> get(const model::ktp_with_hash& ktp) {
        auto it = _cache.find(ktp);
        return it != _cache.end()
                 ? std::make_optional<partition_metadata>(it->second.md)
                 : std::nullopt;
    }

    /**
     * @brief Return the number of items currently cached.
     */
    size_t size() const { return _cache.size(); }

    void setup_metrics() {
        if (config::shard_local_cfg().disable_metrics()) {
            return;
        }

        namespace sm = ss::metrics;
        _metrics.add_group(
          prometheus_sanitize::metrics_name("kafka:fetch_metadata_cache"),
          {sm::make_gauge(
             "bytes_per_offset_error",
             [this] {
                 return _bytes_per_offset_error.has_samples()
                          ? _bytes_per_offset_error.get()
                          : 0.0;
             },
             sm::description(
               "A moving average of the relative error for bytes per offset.")),
           sm::make_gauge(
             "bytes_per_batch_error",
             [this] {
                 return _bytes_per_batch_error.has_samples()
                          ? _bytes_per_batch_error.get()
                          : 0.0;
             },
             sm::description(
               "A moving average of the relative error for bytes per batch."))},
          {},
          {sm::shard_label});
    }

private:
    using entry_sliding_window_t
      = timed_moving_average<size_t, ss::lowres_clock>;
    using error_sliding_window_t
      = timed_moving_average<double, ss::lowres_clock>;

    constexpr static std::chrono::seconds eviction_timeout{60};
    constexpr static auto moving_avg_window{1h};
    constexpr static auto moving_avg_resolution{20min};

    struct entry {
        partition_metadata md;
        ss::lowres_clock::time_point timestamp;
        entry_sliding_window_t bytes_per_offset{
          moving_avg_window, moving_avg_resolution};
        entry_sliding_window_t bytes_per_batch{
          moving_avg_window, moving_avg_resolution};

        void reset_ts() { timestamp = ss::lowres_clock::now(); }
    };

    void evict() {
        const auto now = ss::lowres_clock::now();
        std::erase_if(_cache, [&now](const auto& e) {
            return (e.second.timestamp + eviction_timeout) < now;
        });
    }

    chunked_hash_map<model::ktp_with_hash, entry> _cache;
    ss::timer<> _eviction_timer;
    error_sliding_window_t _bytes_per_offset_error{
      moving_avg_window, moving_avg_resolution};
    error_sliding_window_t _bytes_per_batch_error{
      moving_avg_window, moving_avg_resolution};
    metrics::internal_metric_groups _metrics;
};
} // namespace kafka
