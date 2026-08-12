// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "model/record_utils.h"
#include "model/tests/random_batch.h"
#include "storage/api.h"
#include "storage/directories.h"
#include "storage/fs_utils.h"
#include "storage/segment.h"
#include "storage/segment_appender.h"
#include "storage/segment_reader.h"
#include "storage/segment_set.h"
#include "test_utils/metrics.h"
#include "test_utils/random_bytes.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/defer.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace std::chrono_literals; // NOLINT
using namespace storage;              // NOLINT

void write_garbage(segment_appender& ptr) {
    auto b = tests::random_bytes(100);
    // NOLINTNEXTLINE
    ptr.append(reinterpret_cast<const char*>(b.data()), b.size()).get();
    ptr.flush().get();
}

void write_batches(ss::lw_shared_ptr<segment> seg) {
    auto batches = model::test::make_random_batches(
                     seg->offsets().get_base_offset() + model::offset(1), 1)
                     .get();
    for (auto& b : batches) {
        b.header().header_crc = model::internal_header_only_crc(b.header());
        (void)seg->append(std::move(b)).get();
    }
    seg->flush().get();
}

/// Writes an empty file with the name a previous recovery would have given a
/// segment it dropped.
ss::sstring
stage_quarantined_file(const ntp_config& ntp, const ss::sstring& name) {
    auto path = ssx::sformat("{}/{}", ntp.work_directory(), name);
    ss::open_file_dma(path, ss::open_flags::create | ss::open_flags::rw)
      .get()
      .close()
      .get();
    return path;
}

inline ss::sstring test_directory() {
    char* tmpdir = std::getenv("TEST_TMPDIR");
    if (!tmpdir) {
        return "test.dir";
    }
    return {std::filesystem::path(tmpdir) / std::string("test.dir")};
}

log_config make_config() {
    return log_config{
      test_directory(), 1024, storage::make_sanitized_file_config()};
}

ntp_config config_from_ntp(const model::ntp& ntp) {
    return ntp_config(ntp, test_directory());
}

constexpr size_t default_segment_readahead_size = 128 * 1024;
constexpr unsigned default_segment_readahead_count = 10;

class LogManagerTest : public ::testing::Test {
public:
    LogManagerTest() {
        _feature_table.start().get();
        _feature_table
          .invoke_on_all(
            [](features::feature_table& f) { f.testing_activate_all(); })
          .get();
        auto conf = make_config();
        _store = std::make_unique<storage::api>(
          [conf]() {
              return storage::kvstore_config(
                1_MiB,
                config::mock_binding(10ms),
                conf.base_dir,
                storage::make_sanitized_file_config());
          },
          [conf]() { return conf; },
          _feature_table);
        _store->start().get();
    }

    ~LogManagerTest() override {
        _store->stop().get();
        _feature_table.stop().get();
    }

    log_manager& log_mgr() { return _store->log_mgr(); }
    ss::sharded<features::feature_table>& feature_table() {
        return _feature_table;
    }

    /// Reads the gauge back out, so the assertions cover the label as well as
    /// the count.
    static std::optional<uint64_t> quarantined(segment_position p) {
        return test_utils::find_metric_value<uint64_t>(
          "storage_manager_recovery_segments_quarantined",
          ss::metrics::default_handle(),
          {{"position", ss::sstring(to_string_view(p))}});
    }

private:
    ss::sharded<features::feature_table> _feature_table;
    std::unique_ptr<storage::api> _store;
};

TEST_F(LogManagerTest, test_can_load_logs) {
    auto& m = log_mgr();
    std::vector<storage::ntp_config> ntps;
    ntps.reserve(4);
    for (size_t i = 0; i < 4; ++i) {
        ntps.push_back(
          config_from_ntp(model::ntp(ssx::sformat("ns{}", i), "topic-1", i)));
        directories::initialize(ntps[i].work_directory()).get();
    }
    auto seg = m.make_log_segment(
                  ntps[0],
                  model::offset(10),
                  model::term_id(1),
                  default_segment_readahead_size,
                  default_segment_readahead_count,
                  0)
                 .get();
    seg->close().get();

    // auto ntp2 = empty

    auto seg3 = m.make_log_segment(
                   ntps[2],
                   model::offset(20),
                   model::term_id(1),
                   default_segment_readahead_size,
                   default_segment_readahead_count,
                   1_MiB)
                  .get();
    write_batches(seg3);
    seg3->close().get();

    auto seg4 = m.make_log_segment(
                   ntps[3],
                   model::offset(2),
                   model::term_id(1),
                   default_segment_readahead_size,
                   default_segment_readahead_count,
                   1_MiB)
                  .get();
    write_garbage(seg4->appender());
    seg4->close().get();

    std::vector<ss::shared_ptr<storage::log>> logs;
    for (size_t i = 0; i < 4; ++i) {
        auto log = m.manage(config_from_ntp(ntps[i].ntp())).get();
        log->stm_hookset()->start();
        logs.push_back(std::move(log));
    }
    auto stop_stms = ss::defer([&logs] {
        for (auto& log : logs) {
            log->stm_hookset()->stop();
        }
    });
    EXPECT_EQ(4, m.size());
    EXPECT_EQ(m.get(ntps[0].ntp())->segment_count(), 0);
    EXPECT_EQ(m.get(ntps[1].ntp())->segment_count(), 0);
    EXPECT_EQ(m.get(ntps[2].ntp())->segment_count(), 1);
    EXPECT_EQ(m.get(ntps[3].ntp())->segment_count(), 0);
    EXPECT_FALSE(file_exists(seg->reader().filename()).get());
    EXPECT_TRUE(file_exists(seg3->reader().filename()).get());
    EXPECT_FALSE(file_exists(seg4->reader().filename()).get());
    EXPECT_TRUE(
      file_exists(
        seg4->reader().filename() + ".header_crc_mismatch.tail.cannotrecover")
        .get());
}

TEST_F(
  LogManagerTest, test_unrecoverable_segment_name_carries_reason_and_position) {
    auto& m = log_mgr();

    auto ntp = config_from_ntp(model::ntp("ns-zeroed", "topic-1", 0));
    directories::initialize(ntp.work_directory()).get();

    auto seg = m.make_log_segment(
                  ntp,
                  model::offset(2),
                  model::term_id(1),
                  default_segment_readahead_size,
                  default_segment_readahead_count,
                  0)
                 .get();
    const ss::sstring log_path = seg->reader().filename();
    seg->close().get();

    // Simulate a situation where a segment was fallocated but never written
    // to before a crash. That is, a segment with non-zero file size that is
    // all zeros. A clean segment close would have trimmed the preallocated
    // tail.
    {
        auto f = ss::open_file_dma(log_path, ss::open_flags::rw).get();
        f.truncate(4096).get();
        f.close().get();
    }

    auto log = m.manage(config_from_ntp(ntp.ntp())).get();
    log->stm_hookset()->start();
    auto stop_stm = ss::defer([&log] { log->stm_hookset()->stop(); });

    // Replay stops at the first batch header, and the only segment in the log
    // is also the highest, so the name reports both.
    EXPECT_EQ(log->segment_count(), 0);
    EXPECT_FALSE(file_exists(log_path).get());
    EXPECT_TRUE(
      file_exists(log_path + ".zeroed_batch_header.tail.cannotrecover").get());
}

TEST_F(
  LogManagerTest, test_recovery_counts_a_mid_log_drop_apart_from_the_last) {
    auto& m = log_mgr();

    // Builds a two-segment log with garbage in one of them and recovers it.
    // manage() keeps the recovery_report to itself, so this calls
    // recover_segments directly.
    auto recover = [&](const ss::sstring& ns, model::offset garbage_at) {
        auto ntp = config_from_ntp(model::ntp(ns, "topic-1", 0));
        directories::initialize(ntp.work_directory()).get();
        for (auto base : {model::offset(0), model::offset(100)}) {
            auto seg = m.make_log_segment(
                          ntp,
                          base,
                          model::term_id(1),
                          default_segment_readahead_size,
                          default_segment_readahead_count,
                          1_MiB)
                         .get();
            if (base == garbage_at) {
                write_garbage(seg->appender());
            } else {
                write_batches(seg);
            }
            seg->close().get();
        }
        ss::abort_source as;
        recovery_report report;
        auto segments = recover_segments(
                          partition_path(ntp),
                          /*is_compaction_enabled=*/false,
                          [] { return std::nullopt; },
                          as,
                          default_segment_readahead_size,
                          default_segment_readahead_count,
                          std::nullopt,
                          m.resources(),
                          feature_table(),
                          std::nullopt,
                          &report)
                          .get();
        for (auto& s : segments) {
            s->close().get();
        }
        return report;
    };

    {
        // Garbage in the older segment. The newer segment has a higher base
        // offset, so the offsets the dropped segment claimed sit inside the
        // log.
        auto report = recover("ns-mid-log", model::offset(0));
        EXPECT_EQ(report.dropped_mid_log, 1);
        EXPECT_EQ(report.dropped_at_tail, 0);
    }
    {
        // Garbage in the newer segment, which is the shape an unclean stop
        // leaves on the segment that was open at the time.
        auto report = recover("ns-last", model::offset(100));
        EXPECT_EQ(report.dropped_mid_log, 0);
        EXPECT_EQ(report.dropped_at_tail, 1);
    }
}

TEST_F(LogManagerTest, test_recovery_counts_the_files_an_earlier_run_dropped) {
    auto& m = log_mgr();

    auto ntp = config_from_ntp(model::ntp("ns-quarantined", "topic-1", 0));
    directories::initialize(ntp.work_directory()).get();

    // Three names that recovery writes, and one from a version that recorded
    // no position.
    for (const auto* name :
         {"0-1-v1.log.zeroed_batch_header.tail.cannotrecover",
          "100-1-v1.log.header_crc_mismatch.mid_log.cannotrecover",
          "200-1-v1.log.record_crc_mismatch.mid_log.cannotrecover",
          "300-1-v1.log.cannotrecover"}) {
        stage_quarantined_file(ntp, name);
    }

    ss::abort_source as;
    recovery_report report;
    auto segments = recover_segments(
                      partition_path(ntp),
                      /*is_compaction_enabled=*/false,
                      [] { return std::nullopt; },
                      as,
                      default_segment_readahead_size,
                      default_segment_readahead_count,
                      std::nullopt,
                      m.resources(),
                      feature_table(),
                      std::nullopt,
                      &report)
                      .get();

    // None of them is a segment of the log any more, and the position comes
    // back out of the name.
    EXPECT_EQ(segments.size(), 0);
    EXPECT_EQ(report.dropped_at_tail, 1);
    EXPECT_EQ(report.dropped_mid_log, 2);
    EXPECT_EQ(report.dropped_position_unknown, 1);
}

TEST_F(LogManagerTest, test_removing_a_log_stops_reporting_its_files) {
    auto& m = log_mgr();

    auto ntp = config_from_ntp(model::ntp("ns-forget-remove", "topic-1", 0));
    directories::initialize(ntp.work_directory()).get();
    const auto path = stage_quarantined_file(
      ntp, "0-1-v1.log.header_crc_mismatch.mid_log.cannotrecover");

    // Other tests in this binary leave files of their own behind, so read the
    // gauge as a delta.
    const auto before = quarantined(segment_position::mid_log);
    ASSERT_TRUE(before.has_value());

    auto log = m.manage(config_from_ntp(ntp.ntp())).get();
    log->stm_hookset()->start();
    log->stm_hookset()->stop();
    ASSERT_EQ(quarantined(segment_position::mid_log), *before + 1);

    // remove() deletes the directory, so the file goes with it.
    m.remove(ntp.ntp()).get();
    EXPECT_EQ(quarantined(segment_position::mid_log), before);
    EXPECT_FALSE(ss::file_exists(path).get());
}

TEST_F(LogManagerTest, test_shutting_down_a_log_stops_reporting_its_files) {
    auto& m = log_mgr();

    auto ntp = config_from_ntp(model::ntp("ns-forget-shutdown", "topic-1", 0));
    directories::initialize(ntp.work_directory()).get();
    const auto path = stage_quarantined_file(
      ntp, "0-1-v1.log.header_crc_mismatch.mid_log.cannotrecover");

    const auto before = quarantined(segment_position::mid_log);
    ASSERT_TRUE(before.has_value());

    auto log = m.manage(config_from_ntp(ntp.ntp())).get();
    log->stm_hookset()->start();
    log->stm_hookset()->stop();
    ASSERT_EQ(quarantined(segment_position::mid_log), *before + 1);

    // shutdown() hands the log off to another shard and leaves the file where
    // it is, so this shard stops reporting a file that is still on disk. The
    // shard that takes the log walks the directory and reports it instead.
    m.shutdown(ntp.ntp()).get();
    EXPECT_EQ(quarantined(segment_position::mid_log), before);
    EXPECT_TRUE(ss::file_exists(path).get());
}
