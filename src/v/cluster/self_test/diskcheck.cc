/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/self_test/diskcheck.h"

#include "cluster/logger.h"
#include "random/generators.h"
#include "ssx/sformat.h"
#include "utils/gate_guard.h"
#include "utils/uuid.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>

namespace cluster::self_test {

void diskcheck::validate_options(const diskcheck_opts& opts) {
    using namespace std::chrono_literals;
    if (opts.skip_write == true && opts.skip_read == true) {
        throw diskcheck_option_out_of_range(
          "Both skip_write and skip_read are true");
    }
    const auto duration = std::chrono::duration_cast<std::chrono::seconds>(
      opts.duration);
    if (duration < 1s || duration > (5 * 60s)) {
        throw diskcheck_option_out_of_range(
          "Duration out of range, min is 1s max is 5 minutes");
    }
    if (opts.parallelism < 1 || opts.parallelism > 256) {
        throw diskcheck_option_out_of_range(
          "IO Queue depth (parallelism) out of range, min is 1, max 256");
    }
}

diskcheck::diskcheck(ss::sharded<node::local_monitor>& nlm)
  : _nlm(nlm) {}

ss::future<> diskcheck::start() { return ss::now(); }

ss::future<> diskcheck::stop() {
    /// If test is currently running, expect `benchmark_aborted_exception`
    auto f = _gate.close();
    _as.request_abort();
    _intent.cancel();
    return f;
}

void diskcheck::cancel() {
    _cancelled = true;
    _intent.cancel();
}

ss::future<> diskcheck::verify_remaining_space(size_t dataset_size) {
    co_await _nlm.invoke_on(
      node::local_monitor::shard,
      [](node::local_monitor& lm) { return lm.update_state(); });
    const auto disk_state = co_await _nlm.invoke_on(
      node::local_monitor::shard,
      [](node::local_monitor& lm) { return lm.get_state_cached(); });
    if (disk_state.data_disk.free <= dataset_size) {
        throw diskcheck_option_out_of_range(fmt::format(
          "Not enough disk space to run benchmark, requested: {}, existing: {}",
          dataset_size,
          disk_state.data_disk.free));
    }
}

ss::future<std::vector<self_test_result>> diskcheck::run(diskcheck_opts opts) {
    if (_gate.is_closed()) {
        vlog(clusterlog.debug, "diskcheck - gate already closed");
        co_return std::vector<self_test_result>();
    }
    gate_guard g{_gate};
    co_await ss::futurize_invoke(validate_options, opts);
    co_await verify_remaining_space(opts.data_size);
    vlog(
      clusterlog.info,
      "Starting redpanda self-test disk benchmark, with options: {}",
      opts);
    _cancelled = false;
    _opts = opts;
    _last_pos = 0;
    if (std::filesystem::exists(_opts.dir)) {
        /// Ensure no leftover large files in the event there was a
        /// crash mid run and cleanup didn't get a chance to occur
        std::filesystem::remove_all(_opts.dir);
    }
    std::filesystem::create_directory(_opts.dir);
    const auto fname = ssx::sformat(
      "{}/rp-self-test-{}-{}",
      _opts.dir.string(),
      uuid_t::create(),
      ss::this_shard_id());
    co_return co_await initialize_benchmark(fname).finally([fname] {
        vlog(
          clusterlog.debug,
          "redpanda self-test disk benchmark completed gracefully");
        return ss::remove_file(fname).handle_exception_type(
          [fname](const std::filesystem::filesystem_error& fs_ex) {
              vlog(
                clusterlog.error,
                "Couldn't delete {}, reason {}",
                fname,
                fs_ex);
          });
    });
}

ss::future<std::vector<self_test_result>>
diskcheck::initialize_benchmark(ss::sstring fname) {
    auto flags = ss::open_flags::create | ss::open_flags::rw;
    if (_opts.dsync) {
        flags |= ss::open_flags::dsync;
    }
    ss::file_open_options file_opts{
      .extent_allocation_size_hint = _opts.file_size(),
      .append_is_unlikely = true};
    try {
        vlog(clusterlog.debug, "Creating file: {}", fname);
        auto file = co_await ss::open_file_dma(fname, flags, file_opts);
        co_await file.truncate(_opts.file_size());
        co_await file.flush();
        co_return co_await ss::with_scheduling_group(
          _opts.sg,
          [this, &file]() mutable { return run_configured_benchmarks(file); });
    } catch (const diskcheck_aborted_exception& ex) {
        vlog(clusterlog.debug, "diskcheck stopped due to call to stop()");
    }
    co_return std::vector<self_test_result>{};
}

ss::future<std::vector<self_test_result>>
diskcheck::run_configured_benchmarks(ss::file& file) {
    std::vector<self_test_result> r;
    auto write_metrics = co_await do_run_benchmark<read_or_write::write>(file);
    auto result = write_metrics.to_st_result();
    result.name = _opts.name;
    result.info = "write run";
    result.test_type = "disk";
    if (_cancelled) {
        result.warning = "Run was manually cancelled";
    }
    r.push_back(std::move(result));
    if (!_opts.skip_read) {
        auto read_metrics = co_await do_run_benchmark<read_or_write::read>(
          file);
        auto result = read_metrics.to_st_result();
        result.name = _opts.name;
        result.info = "read run";
        result.test_type = "disk";
        if (_cancelled) {
            result.warning = "Run was manually cancelled";
        }
        r.push_back(std::move(result));
    }
    co_return r;
}

template<diskcheck::read_or_write mode>
ss::future<metrics> diskcheck::do_run_benchmark(ss::file& file) {
    auto irange = boost::irange<uint16_t>(0, _opts.parallelism);
    auto start = ss::lowres_clock::now();
    static const auto five_seconds_us = 500000;
    metrics m{five_seconds_us};
    ss::timer<ss::lowres_clock> timer;
    timer.set_callback([this] { _intent.cancel(); });
    timer.rearm(start + _opts.duration);
    try {
        co_await ss::parallel_for_each(irange, [this, &start, &file, &m](auto) {
            return run_benchmark_fiber<mode>(start, file, m);
        });
    } catch (const ss::cancelled_error&) {
        /// Expect this to be thrown from cancelled futures via io calls, due to
        /// _intent.cancel() having been called
        vlog(clusterlog.debug, "Benchmark completed (duration reached)");
    }
    timer.cancel();
    m.set_total_time(ss::lowres_clock::now() - start);
    _last_pos = 0;
    co_return m;
}

template<diskcheck::read_or_write mode>
ss::future<> diskcheck::run_benchmark_fiber(
  ss::lowres_clock::time_point start, ss::file& file, metrics& m) {
    auto buf = ss::allocate_aligned_buffer<char>(
      _opts.request_size, _opts.alignment());
    random_generators::fill_buffer_randomchars(buf.get(), _opts.request_size);
    auto stop = start + _opts.duration;
    while (stop > ss::lowres_clock::now() && !_cancelled) {
        if (unlikely(_as.abort_requested())) {
            throw diskcheck_aborted_exception();
        }
        co_await m.measure([this, &buf, &file] {
            if constexpr (mode == read_or_write::write) {
                return file.dma_write(
                  get_pos(),
                  buf.get(),
                  _opts.request_size,
                  ss::default_priority_class(),
                  &_intent);
            } else {
                return file.dma_read(
                  get_pos(),
                  buf.get(),
                  _opts.request_size,
                  ss::default_priority_class(),
                  &_intent);
            }
        });
    }
}

/// Gets the next offset in the file to write. All fibers will be accessing
/// _last_pos without explicit synchronization, within one shard. The order of
/// what is written is not important it is just random data. Functionally this
/// will boil down to different fibers calling this method, obtaining
/// monotonically increasing offsets, making io calls that will be queued in
/// order of increasing offset.
uint64_t diskcheck::get_pos() {
    uint64_t pos = _last_pos;
    if (pos + _opts.request_size >= _opts.file_size()) {
        _last_pos = 0;
        return 0;
    }
    _last_pos += _opts.request_size;
    return pos;
}

} // namespace cluster::self_test
