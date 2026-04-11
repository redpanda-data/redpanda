// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "absl/strings/str_split.h"
#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "lsm/core/compression.h"
#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/impl.h"
#include "lsm/db/tests/immutable_tree.h"
#include "lsm/io/disk_persistence.h"
#include "lsm/io/memory_persistence.h"
#include "random/generators.h"
#include "ssx/future-util.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/signal.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <boost/program_options.hpp>

#include <chrono>
#include <exception>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace po = boost::program_options;

namespace {

static ss::logger bench_log("db_bench");

struct benchmark_config {
    size_t num = 1000000;
    std::string benchmarks = "fillseq";
    size_t value_size = 1024;
    bool verify = false;
    bool verify_async = true;
    size_t verify_interval = 1000;
    size_t report_interval = 5; // Report progress every N seconds (0 = disable)
    std::string db_path = "/tmp/lsm_bench";
    bool use_existing_db = false;
    size_t write_buffer_size = 16_MiB;
    bool compression = false; // Enable zstd compression
};

struct benchmark_stats {
    size_t ops_done = 0;
    size_t bytes_written = 0;
    size_t bytes_read = 0;
    size_t verification_failures = 0;
    size_t verification_checks = 0;
    std::chrono::steady_clock::time_point start;
    std::chrono::steady_clock::time_point finish;

    void report(std::string_view name) const {
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
          finish - start);
        double seconds = elapsed.count() / 1000.0;
        double ops_per_sec = seconds > 0 ? ops_done / seconds : 0;
        double mb_per_sec = seconds > 0 ? (bytes_written + bytes_read)
                                            / (1024.0 * 1024.0 * seconds)
                                        : 0;

        if (verification_checks > 0) {
            double error_rate = verification_failures
                                / static_cast<double>(verification_checks);
            bench_log.info(
              "{}: {:.0f} ops/sec, {:.2f} MB/sec, {} ops in {:.3f}s, verified "
              "{} ops, {} failures ({:.4f}%)",
              name,
              ops_per_sec,
              mb_per_sec,
              ops_done,
              seconds,
              verification_checks,
              verification_failures,
              error_rate * 100.0);
        } else {
            bench_log.info(
              "{}: {:.0f} ops/sec, {:.2f} MB/sec, {} ops in {:.3f}s",
              name,
              ops_per_sec,
              mb_per_sec,
              ops_done,
              seconds);
        }
    }

    void report_progress(std::string_view name) const {
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
          now - start);
        double seconds = elapsed.count() / 1000.0;
        double ops_per_sec = seconds > 0 ? ops_done / seconds : 0;
        double mb_per_sec = seconds > 0 ? (bytes_written + bytes_read)
                                            / (1024.0 * 1024.0 * seconds)
                                        : 0;

        if (verification_checks > 0) {
            bench_log.info(
              "{} [progress]: {:.0f} ops/sec, {:.2f} MB/sec, {} ops, {:.1f}s "
              "elapsed, {} verified, {} failures",
              name,
              ops_per_sec,
              mb_per_sec,
              ops_done,
              seconds,
              verification_checks,
              verification_failures);
        } else {
            bench_log.info(
              "{} [progress]: {:.0f} ops/sec, {:.2f} MB/sec, {} ops, {:.1f}s "
              "elapsed",
              name,
              ops_per_sec,
              mb_per_sec,
              ops_done,
              seconds);
        }
    }
};

// Shadow map to track expected database state for verification
struct shadow_state {
    using tree_type
      = lsm::db::immutable_tree<ss::sstring, std::optional<iobuf>>;
    tree_type data;

    void put(ss::sstring key, iobuf value) {
        data = data.insert(
          std::move(key), std::optional<iobuf>(std::move(value)));
    }

    void remove(ss::sstring key) {
        data = data.insert(std::move(key), std::optional<iobuf>{});
    }
};

class benchmark {
public:
    explicit benchmark(benchmark_config cfg)
      : _cfg(std::move(cfg)) {}

    ss::future<> setup() {
        auto opts = ss::make_lw_shared<lsm::internal::options>(
          lsm::internal::options{
            .levels = lsm::internal::options::make_levels(
              {
                .max_total_bytes = _cfg.write_buffer_size
                                   * lsm::internal::options::
                                     default_level_zero_stop_writes_trigger,
                .max_file_size = _cfg.write_buffer_size,
              },
              lsm::internal::options::default_level_multipler,
              lsm::internal::options::default_max_level),
            .write_buffer_size = _cfg.write_buffer_size,
          });
        if (_cfg.compression) {
            for (auto& level : opts->levels) {
                level.compression = lsm::compression_type::zstd;
            }
        }

        std::unique_ptr<lsm::io::data_persistence> data;
        std::unique_ptr<lsm::io::metadata_persistence> metadata;

        if (_cfg.db_path == ":memory:") {
            // Use in-memory persistence
            data = lsm::io::make_memory_data_persistence();
            metadata = lsm::io::make_memory_metadata_persistence();
            bench_log.info(
              "Using in-memory persistence on shard {} with "
              "write_buffer_size={}, compression={}",
              ss::this_shard_id(),
              _cfg.write_buffer_size,
              _cfg.compression ? "zstd" : "none");
        } else {
            // Create per-shard database directory
            auto shard_db_path = fmt::format(
              "{}/shard{}", _cfg.db_path, ss::this_shard_id());

            if (!_cfg.use_existing_db) {
                // Clean up existing database files
                try {
                    std::filesystem::remove_all(shard_db_path);
                    bench_log.info(
                      "Removed existing database at {}", shard_db_path);
                } catch (const std::exception& e) {
                    std::ignore = e;
                }
            }

            data = co_await lsm::io::open_disk_data_persistence(shard_db_path);
            metadata = co_await lsm::io::open_disk_metadata_persistence(
              shard_db_path);
            bench_log.info(
              "Database opened at {} with write_buffer_size={}, "
              "compression={}",
              shard_db_path,
              _cfg.write_buffer_size,
              _cfg.compression ? "zstd" : "none");
        }

        _db = co_await lsm::db::impl::open(
          opts,
          {
            .data = std::move(data),
            .metadata = std::move(metadata),
          });
    }

    ss::future<> interrupt() {
        _as.request_abort();
        co_return;
    }

    ss::future<> teardown() {
        co_await await_verification();
        if (_db) {
            co_await _db->close();
        }
    }

    ss::future<> run(std::vector<std::string> benches) {
        bench_log.info("running benchmarks: {}", benches);
        for (const auto& bench : benches) {
            if (_as.abort_requested()) {
                co_return;
            }
            co_await do_run(bench);
        }
    }

    ss::future<> do_run(std::string_view bench_name) {
        bench_log.info(
          "Starting benchmark: {} with {} operations", bench_name, _cfg.num);

        benchmark_stats stats;
        stats.start = std::chrono::steady_clock::now();

        // Start background progress reporter timer
        ss::timer<> progress_timer;
        if (_cfg.report_interval > 0) {
            progress_timer.set_callback(
              [&stats, bench_name] { stats.report_progress(bench_name); });
            progress_timer.arm_periodic(
              std::chrono::seconds(_cfg.report_interval));
        }

        if (bench_name == "fillseq") {
            co_await fillseq(stats);
        } else if (bench_name == "fillrandom") {
            co_await fillrandom(stats);
        } else if (bench_name == "overwrite") {
            co_await overwrite(stats);
        } else if (bench_name == "deleteseq") {
            co_await deleteseq(stats);
        } else if (bench_name == "deleterandom") {
            co_await deleterandom(stats);
        } else if (bench_name == "readrandom") {
            co_await readrandom(stats);
        } else if (bench_name == "readseq") {
            co_await readseq(stats);
        } else if (bench_name == "readmissing") {
            co_await readmissing(stats);
        } else if (bench_name == "mixedworkload") {
            co_await mixedworkload(stats);
        } else {
            bench_log.error("Unknown benchmark: {}", bench_name);
        }

        // Stop progress reporter
        progress_timer.cancel();

        stats.finish = std::chrono::steady_clock::now();
        stats.report(bench_name);
    }

private:
    ss::future<> fillseq(benchmark_stats& stats) {
        for (size_t i = 0; i < _cfg.num; ++i) {
            if (_as.abort_requested()) {
                co_return;
            }
            auto key = ss::sstring(fmt::format("key{:016d}", i));
            auto value = generate_value();

            co_await write_key(key, value.copy(), stats);

            if (_cfg.verify && (i % _cfg.verify_interval == 0)) {
                co_await maybe_verify(stats);
            }
        }

        if (_cfg.verify) {
            co_await await_verification();
            auto iter = co_await _db->create_iterator({});
            co_await verify_all(std::move(iter), _shadow.data, stats);
        }
    }

    ss::future<> fillrandom(benchmark_stats& stats) {
        auto& rng = random_generators::global();

        for (size_t i = 0; i < _cfg.num; ++i) {
            if (_as.abort_requested()) {
                co_return;
            }
            auto key_num = rng.get_int<size_t>(0, _cfg.num - 1);
            auto key = ss::sstring(fmt::format("key{:016d}", key_num));
            auto value = generate_value();

            co_await write_key(key, value.copy(), stats);

            if (_cfg.verify && (i % _cfg.verify_interval == 0)) {
                co_await maybe_verify(stats);
            }
        }

        if (_cfg.verify) {
            co_await await_verification();
            auto iter = co_await _db->create_iterator({});
            co_await verify_all(std::move(iter), _shadow.data, stats);
        }
    }

    ss::future<> overwrite(benchmark_stats& stats) {
        // First, ensure we have keys to overwrite
        for (size_t i = 0; i < _cfg.num; ++i) {
            if (_as.abort_requested()) {
                co_return;
            }
            auto key = ss::sstring(fmt::format("key{:016d}", i));
            auto value = generate_value();
            co_await write_key(key, std::move(value), stats);
        }

        // Now overwrite them
        for (size_t i = 0; i < _cfg.num; ++i) {
            if (_as.abort_requested()) {
                co_return;
            }
            auto key = ss::sstring(fmt::format("key{:016d}", i));
            auto value = generate_value();

            co_await write_key(key, std::move(value), stats);

            if (_cfg.verify && (i % _cfg.verify_interval == 0)) {
                co_await maybe_verify(stats);
            }
        }

        if (_cfg.verify) {
            co_await await_verification();
            auto iter = co_await _db->create_iterator({});
            co_await verify_all(std::move(iter), _shadow.data, stats);
        }
    }

    ss::future<> deleteseq(benchmark_stats& stats) {
        // First, ensure we have keys to delete
        for (size_t i = 0; i < _cfg.num; ++i) {
            if (_as.abort_requested()) {
                co_return;
            }
            auto key = ss::sstring(fmt::format("key{:016d}", i));
            auto value = generate_value();
            co_await write_key(key, value.copy(), stats);
        }

        // Now delete them
        for (size_t i = 0; i < _cfg.num; ++i) {
            if (_as.abort_requested()) {
                co_return;
            }
            auto key = ss::sstring(fmt::format("key{:016d}", i));
            co_await delete_key(key, stats);

            if (_cfg.verify && (i % _cfg.verify_interval == 0)) {
                co_await maybe_verify(stats);
            }
        }

        if (_cfg.verify) {
            co_await await_verification();
            auto iter = co_await _db->create_iterator({});
            co_await verify_all(std::move(iter), _shadow.data, stats);
        }
    }

    ss::future<> deleterandom(benchmark_stats& stats) {
        auto& rng = random_generators::global();

        // First, ensure we have keys to delete
        for (size_t i = 0; i < _cfg.num; ++i) {
            if (_as.abort_requested()) {
                co_return;
            }
            auto key = ss::sstring(fmt::format("key{:016d}", i));
            auto value = generate_value();
            co_await write_key(key, value.copy(), stats);
        }

        // Now delete them randomly
        for (size_t i = 0; i < _cfg.num; ++i) {
            if (_as.abort_requested()) {
                co_return;
            }
            auto key_num = rng.get_int<size_t>(0, _cfg.num - 1);
            auto key = ss::sstring(fmt::format("key{:016d}", key_num));
            co_await delete_key(key, stats);

            if (_cfg.verify && (i % _cfg.verify_interval == 0)) {
                co_await maybe_verify(stats);
            }
        }

        if (_cfg.verify) {
            co_await await_verification();
            auto iter = co_await _db->create_iterator({});
            co_await verify_all(std::move(iter), _shadow.data, stats);
        }
    }

    ss::future<> readrandom(benchmark_stats& stats) {
        auto& rng = random_generators::global();

        // First populate with data
        for (size_t i = 0; i < _cfg.num; ++i) {
            if (_as.abort_requested()) {
                co_return;
            }
            auto key = ss::sstring(fmt::format("key{:016d}", i));
            auto value = generate_value();
            co_await write_key(key, value.copy(), stats);
        }

        // Reset stats for read phase
        stats.ops_done = 0;
        stats.bytes_read = 0;

        // Now perform random reads
        for (size_t i = 0; i < _cfg.num; ++i) {
            if (_as.abort_requested()) {
                co_return;
            }
            auto key_num = rng.get_int<size_t>(0, _cfg.num - 1);
            auto key = ss::sstring(fmt::format("key{:016d}", key_num));

            co_await read_key(key, stats);

            if (_cfg.verify && (i % _cfg.verify_interval == 0)) {
                co_await maybe_verify(stats);
            }
        }

        if (_cfg.verify) {
            co_await await_verification();
            auto iter = co_await _db->create_iterator({});
            co_await verify_all(std::move(iter), _shadow.data, stats);
        }
    }

    ss::future<> readseq(benchmark_stats& stats) {
        // First populate with data
        for (size_t i = 0; i < _cfg.num; ++i) {
            if (_as.abort_requested()) {
                co_return;
            }
            auto key = ss::sstring(fmt::format("key{:016d}", i));
            auto value = generate_value();
            co_await write_key(key, value.copy(), stats);
        }

        // Reset stats for read phase
        stats.ops_done = 0;
        stats.bytes_read = 0;

        // Perform sequential iteration
        auto iter = co_await _db->create_iterator({});
        co_await iter->seek_to_first();

        size_t count = 0;
        while (iter->valid() && count < _cfg.num) {
            if (_as.abort_requested()) {
                co_return;
            }
            auto key = ss::sstring(iter->key().user_key());
            auto value = iter->value();

            stats.ops_done++;
            stats.bytes_read += key.size() + value.size_bytes();

            if (_cfg.verify) {
                stats.verification_checks++;
                auto* shadow_val = _shadow.data.get(key);
                if (!shadow_val) {
                    bench_log.error(
                      "Verification failed for key {}: not in shadow map", key);
                    stats.verification_failures++;
                } else if (!shadow_val->has_value()) {
                    bench_log.error(
                      "Verification failed for key {}: expected deleted", key);
                    stats.verification_failures++;
                } else if (shadow_val->value() != value) {
                    bench_log.error(
                      "Verification failed for key {}: value mismatch", key);
                    stats.verification_failures++;
                }
            }

            co_await iter->next();
            count++;
        }

        if (_cfg.verify) {
            co_await await_verification();
            auto iter = co_await _db->create_iterator({});
            co_await verify_all(std::move(iter), _shadow.data, stats);
        }
    }

    ss::future<> readmissing(benchmark_stats& stats) {
        // Try to read keys that don't exist
        for (size_t i = 0; i < _cfg.num; ++i) {
            if (_as.abort_requested()) {
                co_return;
            }
            auto key = ss::sstring(fmt::format("missing{:016d}", i));

            auto key_encoded = lsm::internal::key::encode(
              {.key = lsm::user_key_view(key),
               .seqno = _seqno,
               .type = lsm::internal::value_type::value});

            auto result = co_await _db->get(key_encoded);

            stats.ops_done++;
            stats.verification_checks++;

            if (!result.is_missing()) {
                bench_log.error(
                  "Verification failed: key {} should not exist", key);
                stats.verification_failures++;
            }
        }
    }

    ss::future<> mixedworkload(benchmark_stats& stats) {
        auto& rng = random_generators::global();

        for (size_t i = 0; i < _cfg.num; ++i) {
            if (_as.abort_requested()) {
                co_return;
            }
            auto op = rng.get_int<int>(0, 99);
            auto key_num = rng.get_int<size_t>(0, _cfg.num - 1);
            auto key = ss::sstring(fmt::format("key{:016d}", key_num));

            if (op < 50) {
                // 50% writes
                co_await write_key(key, generate_value(), stats);
            } else if (op < 90) {
                // 40% reads
                co_await read_key(key, stats);
            } else {
                // 10% deletes
                co_await delete_key(key, stats);
            }

            if (_cfg.verify && (i % _cfg.verify_interval == 0)) {
                co_await maybe_verify(stats);
            }
        }

        if (_cfg.verify) {
            co_await await_verification();
            auto iter = co_await _db->create_iterator({});
            co_await verify_all(std::move(iter), _shadow.data, stats);
        }
    }

    ss::future<>
    write_key(ss::sstring key, iobuf value, benchmark_stats& stats) {
        auto batch = ss::make_lw_shared<lsm::db::memtable>();
        auto key_encoded = lsm::internal::key::encode(
          {.key = lsm::user_key_view(key),
           .seqno = ++_seqno,
           .type = lsm::internal::value_type::value});

        stats.bytes_written += key.size() + value.size_bytes();
        batch->put(std::move(key_encoded), value.copy());

        if (_cfg.verify) {
            _shadow.put(key, value.copy());
        }

        co_await _db->apply(std::move(batch));
        stats.ops_done++;
    }

    ss::future<> delete_key(const ss::sstring& key, benchmark_stats& stats) {
        auto batch = ss::make_lw_shared<lsm::db::memtable>();
        auto key_encoded = lsm::internal::key::encode(
          {.key = lsm::user_key_view(key),
           .seqno = ++_seqno,
           .type = lsm::internal::value_type::tombstone});

        batch->remove(std::move(key_encoded));

        if (_cfg.verify) {
            _shadow.remove(key);
        }

        co_await _db->apply(std::move(batch));
        stats.ops_done++;
    }

    ss::future<> read_key(const ss::sstring& key, benchmark_stats& stats) {
        auto key_encoded = lsm::internal::key::encode(
          {.key = lsm::user_key_view(key),
           .seqno = _seqno,
           .type = lsm::internal::value_type::value});

        auto result = co_await _db->get(key_encoded);

        stats.ops_done++;
        stats.bytes_read += key.size();
        auto result_value = result.value();
        if (result_value) {
            stats.bytes_read += result_value->size_bytes();
        }

        if (_cfg.verify) {
            stats.verification_checks++;
            auto* shadow_val = _shadow.data.get(key);

            if (!shadow_val) {
                // Key not in shadow map
                if (result_value) {
                    bench_log.error(
                      "Verification failed: key {} exists in DB but not in "
                      "shadow map",
                      key);
                    stats.verification_failures++;
                }
            } else if (!shadow_val->has_value()) {
                // Key was deleted
                if (!result.is_missing() && !result.is_tombstone()) {
                    bench_log.error(
                      "Verification failed: deleted key {} still exists in DB",
                      key);
                    stats.verification_failures++;
                }
            } else {
                // Key should exist with specific value
                if (!result_value) {
                    bench_log.error(
                      "Verification failed: key {} missing from DB", key);
                    stats.verification_failures++;
                } else if (shadow_val->value() != result_value.value()) {
                    bench_log.error(
                      "Verification failed: key {} value mismatch", key);
                    stats.verification_failures++;
                }
            }
        }
    }

    ss::future<> maybe_verify(benchmark_stats& stats) {
        if (!_verify_fut.available()) {
            co_return;
        }
        auto iter = co_await _db->create_iterator({});
        auto snapshot = _shadow.data;
        auto fut = verify_all(std::move(iter), std::move(snapshot), stats)
                     .then_wrapped([](ss::future<> f) {
                         if (f.failed()) {
                             auto ep = f.get_exception();
                             bench_log.error(
                               "Background verification failed: {}", ep);
                         }
                     });
        if (_cfg.verify_async) {
            _verify_fut = std::move(fut);
        } else {
            co_await std::move(fut);
        }
    }

    ss::future<> await_verification() {
        return std::exchange(_verify_fut, ss::make_ready_future<>());
    }

    ss::future<> verify_all(
      std::unique_ptr<lsm::internal::iterator> iter,
      shadow_state::tree_type shadow,
      benchmark_stats& stats) {
        bench_log.debug("Performing full verification scan");

        co_await iter->seek_to_first();

        // Both the shadow tree and DB iterator produce entries in sorted
        // order. Walk the shadow tree and advance the DB iterator in
        // lockstep so we never need to copy the database into a map.
        co_await shadow.for_each(
          [&](
            this auto,
            const ss::sstring& shadow_key,
            const std::optional<iobuf>& shadow_val) -> ss::future<> {
              if (_as.abort_requested()) {
                  co_return;
              }
              // Drain DB entries that sort before this shadow key.
              while (iter->valid()) {
                  auto db_key = ss::sstring(iter->key().user_key());
                  if (db_key >= shadow_key) {
                      break;
                  }
                  stats.verification_checks++;
                  bench_log.error(
                    "Verification failed: key {} exists in DB but not in "
                    "shadow map",
                    db_key);
                  stats.verification_failures++;
                  co_await iter->next();
              }

              stats.verification_checks++;
              if (!iter->valid()) {
                  // DB exhausted; live shadow entries are missing.
                  if (shadow_val.has_value()) {
                      bench_log.error(
                        "Verification failed: key {} missing from DB",
                        shadow_key);
                      stats.verification_failures++;
                  }
                  co_return;
              }

              auto db_key = ss::sstring(iter->key().user_key());
              if (db_key == shadow_key) {
                  if (!shadow_val.has_value()) {
                      bench_log.error(
                        "Verification failed: deleted key {} still exists "
                        "in DB",
                        shadow_key);
                      stats.verification_failures++;
                  } else if (shadow_val.value() != iter->value()) {
                      bench_log.error(
                        "Verification failed: key {} value mismatch",
                        shadow_key);
                      stats.verification_failures++;
                  }
                  co_await iter->next();
              } else {
                  // db_key > shadow_key: shadow entry missing from DB.
                  if (shadow_val.has_value()) {
                      bench_log.error(
                        "Verification failed: key {} missing from DB",
                        shadow_key);
                      stats.verification_failures++;
                  }
              }
          });

        // Remaining DB entries not tracked by shadow.
        while (iter->valid()) {
            if (_as.abort_requested()) {
                co_return;
            }
            stats.verification_checks++;
            bench_log.error(
              "Verification failed: key {} exists in DB but not in "
              "shadow map",
              ss::sstring(iter->key().user_key()));
            stats.verification_failures++;
            co_await iter->next();
        }

        bench_log.debug("Verification scan complete");
    }

    iobuf generate_value() {
        auto str = random_generators::gen_alphanum_string(_cfg.value_size);
        return iobuf::from(std::move(str));
    }

    benchmark_config _cfg;
    std::unique_ptr<lsm::db::impl> _db;
    ss::abort_source _as;
    shadow_state _shadow;
    ss::future<> _verify_fut = ss::make_ready_future<>();
    lsm::internal::sequence_number _seqno;
};

ss::future<> run_benchmarks(benchmark_config cfg) {
    ss::sharded<benchmark> bench;
    ss::gate gate;
    ss::handle_signal(SIGINT, [&bench, &gate] {
        ssx::spawn_with_gate(gate, [&bench] {
            return bench.invoke_on_all(&benchmark::interrupt);
        });
    });
    std::vector<std::string> bench_names = absl::StrSplit(cfg.benchmarks, ",");
    std::exception_ptr ep;
    try {
        co_await bench.start(cfg);
        co_await bench.invoke_on_all(&benchmark::setup);
        co_await bench.invoke_on_all(&benchmark::run, bench_names);
    } catch (...) {
        ep = std::current_exception();
    }
    co_await bench.invoke_on_all(&benchmark::teardown);
    co_await gate.close();
    co_await bench.stop();
    if (ep) {
        std::rethrow_exception(ep);
    }
}
} // namespace

// clang-format off
// LSM Database Benchmark
//
// Run examples (note: Seastar flags like --smp go before the second --):
//
//   # Simple sequential write test
//   bazel run //src/v/lsm/db/tests:db_bench -- --smp 1 -- --num 100000 --benchmarks fillseq
//
//   # Multi-core with verification
//   bazel run //src/v/lsm/db/tests:db_bench -- --smp 8 -- --num 1000000 --benchmarks fillseq,readrandom --verify
//
//   # Overnight stress test
//   bazel run //src/v/lsm/db/tests:db_bench -- --smp 16 -- --num 10000000 --benchmarks mixedworkload --verify
//
//   # Mixed workload with deletes
//   bazel run //src/v/lsm/db/tests:db_bench -- --smp 4 -- --num 1000000 --benchmarks fillrandom,deleterandom --verify
//
//   # In-memory benchmark
//   bazel run //src/v/lsm/db/tests:db_bench -- --smp 4 -- --num 1000000 --benchmarks mixedworkload --db :memory:
//
// clang-format on
int main(int ac, char* av[]) {
    ss::app_template::config seastar_cfg;
    seastar_cfg.auto_handle_sigint_sigterm = false;
    ss::app_template app(seastar_cfg);

    benchmark_config cfg;

    app.add_options()(
      "num",
      po::value<size_t>(&cfg.num)->default_value(cfg.num),
      "Number of operations per benchmark")(
      "benchmarks",
      po::value<std::string>(&cfg.benchmarks)->default_value(cfg.benchmarks),
      "Comma-separated list: fillseq, fillrandom, overwrite, deleteseq, "
      "deleterandom, readrandom, readseq, readmissing, mixedworkload")(
      "value_size",
      po::value<size_t>(&cfg.value_size)->default_value(cfg.value_size),
      "Size of each value in bytes")(
      "verify",
      po::bool_switch(&cfg.verify)->default_value(cfg.verify),
      "Enable verification mode")(
      "verify_async",
      po::bool_switch(&cfg.verify_async)->default_value(cfg.verify_async),
      "Make verification run asynchronously to the other operations in the db")(
      "verify_interval",
      po::value<size_t>(&cfg.verify_interval)
        ->default_value(cfg.verify_interval),
      "Number of operations between full verification scans")(
      "report_interval",
      po::value<size_t>(&cfg.report_interval)
        ->default_value(cfg.report_interval),
      "Number of seconds between progress reports (0 = disable)")(
      "db",
      po::value<std::string>(&cfg.db_path)->default_value(cfg.db_path),
      "Database directory path (use :memory: for in-memory persistence)")(
      "use_existing_db",
      po::bool_switch(&cfg.use_existing_db)->default_value(cfg.use_existing_db),
      "Use existing database instead of creating new")(
      "write_buffer_size",
      po::value<size_t>(&cfg.write_buffer_size)
        ->default_value(cfg.write_buffer_size),
      "Write buffer size in bytes")(
      "compression",
      po::bool_switch(&cfg.compression)->default_value(cfg.compression),
      "Enable zstd compression for SST blocks");

    return app.run(ac, av, [&cfg]() mutable {
        return run_benchmarks(cfg).then([] { return 0; });
    });
}
