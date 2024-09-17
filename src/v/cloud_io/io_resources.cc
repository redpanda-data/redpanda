/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/io_resources.h"

#include "base/vlog.h"
#include "cloud_io/logger.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "resource_mgmt/io_priority.h"
#include "ssx/future-util.h"
#include "utils/token_bucket.h"

#include <seastar/core/io_queue.hh>
#include <seastar/core/reactor.hh>

namespace cloud_io {

namespace {

class throttled_dl_source : public ss::data_source_impl {
public:
    explicit throttled_dl_source(
      ss::input_stream<char> st,
      token_bucket<>& throughput_limit,
      std::function<void(size_t)> tick_dl_throttled_metric,
      ss::abort_source& as,
      ss::gate::holder h)
      : _src(std::move(st).detach())
      , _throughput_limit(throughput_limit)
      , _tick_dl_throttled_metric(std::move(tick_dl_throttled_metric))
      , _as(as)
      , _holder(std::move(h)) {}

    ss::future<ss::temporary_buffer<char>> get() override {
        auto h = _gate.hold();
        return _src.get().then(
          [this, h = std::move(h)](ss::temporary_buffer<char> buf) mutable {
              size_t buffer_size = buf.size();
              return maybe_throttle(buffer_size)
                .then([b = std::move(buf), h = std::move(h)]() mutable {
                    return std::move(b);
                });
          });
    }

    ss::future<ss::temporary_buffer<char>> skip(uint64_t n) override {
        auto holder = _gate.hold();
        return _src.skip(n).finally(
          [this, n, h = std::move(holder)] { return maybe_throttle(n); });
    }

    ss::future<> close() override {
        _holder.release();
        return _gate.close().then([this] { return _src.close(); });
    }

private:
    ss::future<> maybe_throttle(size_t buffer_size) {
        auto throttled = co_await _throughput_limit.maybe_throttle(
          buffer_size, _as);
        if (throttled) {
            // This path is taken only when the download is throttled. In case
            // of concurrency every `maybe_throttle` call will return the
            // precise value for deficiency and sleep time.
            auto duration
              = std::chrono::duration_cast<std::chrono::milliseconds>(
                throttled.value());
            _tick_dl_throttled_metric(duration.count());
            vlog(
              log.trace,
              "Download throttled: sleep time is {} ms",
              duration.count());
        }
    }
    ss::data_source _src;
    token_bucket<>& _throughput_limit;
    std::function<void(size_t)> _tick_dl_throttled_metric;
    ss::gate _gate;
    ss::abort_source& _as;
    // Gate holder of the `_parent`
    ss::gate::holder _holder;
};

struct device_throughput {
    size_t read = std::numeric_limits<size_t>::max();
    size_t write = std::numeric_limits<size_t>::max();
};
struct throughput_limit {
    size_t disk_node_throughput_limit = std::numeric_limits<size_t>::max();
    size_t download_shard_throughput_limit = std::numeric_limits<size_t>::max();
};

/// Compute integer multiplication and division without the integer overflow
/// \returns val * mul / div
size_t muldiv(size_t val, size_t mul, size_t div) {
    const auto dv = std::lldiv(
      static_cast<long long>(val), static_cast<long long>(div));
    return dv.quot * mul + dv.rem * mul / div;
}

throughput_limit get_hard_throughput_limit() {
    auto hard_limit = config::shard_local_cfg()
                        .cloud_storage_max_throughput_per_shard()
                        .value_or(0)
                      * ss::smp::count;

    if (hard_limit == 0) {
        // Run tiered-storage without throttling by setting
        // 'cloud_storage_max_throughput_per_shard' to nullopt
        return {};
    }

    return {
      .disk_node_throughput_limit = hard_limit,
      .download_shard_throughput_limit = hard_limit / ss::smp::count,
    };
}

throughput_limit get_throughput_limit(std::optional<size_t> device_throughput) {
    auto hard_limit = config::shard_local_cfg()
                        .cloud_storage_max_throughput_per_shard()
                        .value_or(0)
                      * ss::smp::count;

    if (
      config::shard_local_cfg()
          .cloud_storage_throughput_limit_percent()
          .value_or(0)
        == 0
      || hard_limit == 0) {
        // Run tiered-storage without throttling by setting
        // 'cloud_storage_throughput_limit_percent' to nullopt or
        // 'cloud_storage_max_throughput_per_shard' to nullopt
        return throughput_limit{};
    }

    if (!device_throughput.has_value()) {
        // The 'cloud_storage_throughput_limit_percent' is set
        // but but we couldn't read the actual device throughput. No need
        // to limit the disk bandwidth in this case. But since hard limit
        // is set we still need to limit network bandwidth even though
        // the limit is overly high.
        return throughput_limit{
          .download_shard_throughput_limit = hard_limit / ss::smp::count,
        };
    }

    auto tp = std::min(hard_limit, device_throughput.value());
    return {
      .disk_node_throughput_limit = tp,
      .download_shard_throughput_limit = tp / ss::smp::count,
    };
}

// Get device throughput for the mountpoint
ss::future<std::optional<device_throughput>> get_storage_device_throughput() {
    if (config::shard_local_cfg().cloud_storage_enabled() == false) {
        co_return std::nullopt;
    }
    try {
        auto cache_path = config::node().cloud_storage_cache_path().native();
        auto fs = co_await ss::file_stat(cache_path);
        auto& queue = ss::engine().get_io_queue(fs.device_id);
        auto cfg = queue.get_config();
        auto percent = config::shard_local_cfg()
                         .cloud_storage_throughput_limit_percent()
                         .value_or(0);
        if (percent > 0) {
            // percent == nullopt indicates that the throttling is disabled
            // intentionally
            co_return device_throughput{
              .read = muldiv(cfg.read_bytes_rate, percent, 100),
              .write = muldiv(cfg.write_bytes_rate, percent, 100),
            };
        }
    } catch (...) {
        vlog(
          log.info,
          "Can't get device throughput: {} for {} mountpoint",
          std::current_exception(),
          config::node().cloud_storage_cache_path().native());
    }
    co_return std::nullopt;
}

} // namespace

io_resources::io_resources()
  : _max_concurrent_hydrations_per_shard(
      config::shard_local_cfg()
        .cloud_storage_max_concurrent_hydrations_per_shard.bind())
  , _hydration_units(max_parallel_hydrations(), "cst_hydrations")
  , _throughput_limit(
      // apply shard limit to downloads
      get_hard_throughput_limit().download_shard_throughput_limit,
      "ts-segment-downloads")
  , _throughput_shard_limit_config(
      config::shard_local_cfg().cloud_storage_max_throughput_per_shard.bind())
  , _relative_throughput(
      config::shard_local_cfg().cloud_storage_throughput_limit_percent.bind()) {
    _max_concurrent_hydrations_per_shard.watch([this]() {
        // The 'max_connections' parameter can't be changed without restarting
        // redpanda.
        _hydration_units.set_capacity(max_parallel_hydrations());
    });

    auto reset_tp = [this] {
        ssx::spawn_with_gate(_gate, [this] { return update_throughput(); });
    };

    _throughput_shard_limit_config.watch(reset_tp);
    _relative_throughput.watch(reset_tp);

    reset_tp();
}

ss::future<> io_resources::stop() {
    log.debug("Stopping cloud_io::io_resources...");
    _throughput_limit.shutdown();
    _hydration_units.broken();

    co_await _gate.close();
    log.debug("Stopped cloud_io::io_resources...");
}

ss::future<> io_resources::start() {
    auto tput = co_await get_storage_device_throughput();
    if (tput.has_value()) {
        _device_throughput = tput->write;
    }
    co_await update_throughput();

    co_return;
}

size_t io_resources::max_parallel_hydrations() const {
    auto max_connections
      = config::shard_local_cfg().cloud_storage_max_connections();
    auto n_readers = config::shard_local_cfg()
                       .cloud_storage_max_partition_readers_per_shard();
    if (n_readers) {
        return std::min(
          static_cast<unsigned>(max_connections), n_readers.value());
    }
    return max_connections / 2;
}

size_t io_resources::current_ongoing_hydrations() const {
    return _hydration_units.outstanding();
}

ss::future<> io_resources::set_disk_max_bandwidth(size_t tput) {
    try {
        if (tput == 0 || tput == std::numeric_limits<size_t>::max()) {
            _throttling_disabled = true;
            vlog(
              log.info,
              "Scheduling group's {} bandwidth is not limited",
              priority_manager::local().shadow_indexing_priority().get_name());
            co_return;
        }
        _throttling_disabled = false;
        vlog(
          log.info,
          "Setting scheduling group {} bandwidth to {}",
          priority_manager::local().shadow_indexing_priority().get_name(),
          tput);

        co_await priority_manager::local()
          .shadow_indexing_priority()
          .update_bandwidth(tput);
    } catch (...) {
        vlog(
          log.error,
          "Failed to set tiered-storage disk throughput: {}",
          std::current_exception());
    }
}

void io_resources::set_net_max_bandwidth(size_t tput) {
    if (tput != 0 && tput != std::numeric_limits<size_t>::max()) {
        vlog(
          log.info,
          "Setting cloud storage download bandwidth to {} on this shard",
          tput);
        _throughput_limit.update_capacity(tput);
        _throughput_limit.update_rate(tput);
        _throttling_disabled = false;
    } else {
        vlog(
          log.info,
          "Disabling cloud storage download throttling on this shard");
        _throttling_disabled = true;
    }
}

ss::future<ssx::semaphore_units> io_resources::get_hydration_units(size_t n) {
    auto u = co_await _hydration_units.get_units(n);
    co_return std::move(u);
}

ss::input_stream<char> io_resources::throttle_download(
  ss::input_stream<char> underlying,
  ss::abort_source& as,
  std::function<void(size_t)> throttle_metric_ms_cb) {
    if (_throttling_disabled) {
        return underlying;
    }
    auto src = std::make_unique<throttled_dl_source>(
      std::move(underlying),
      _throughput_limit,
      std::move(throttle_metric_ms_cb),
      as,
      _gate.hold());
    ss::data_source ds(std::move(src));
    return ss::input_stream<char>(std::move(ds));
}

ss::future<> io_resources::update_throughput() {
    auto tp = get_throughput_limit(_device_throughput);
    if (ss::this_shard_id() == 0) {
        co_await set_disk_max_bandwidth(tp.disk_node_throughput_limit);
    }
    set_net_max_bandwidth(tp.download_shard_throughput_limit);
}

} // namespace cloud_io
