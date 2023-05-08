/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/adjacent_segment_merger.h"
#include "archival/archival_policy.h"
#include "archival/ntp_archiver_service.h"
#include "archival/tests/service_fixture.h"
#include "bytes/iobuf.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/client_pool.h"
#include "model/metadata.h"
#include "net/types.h"
#include "net/unresolved_address.h"
#include "raft/offset_translator.h"
#include "ssx/sformat.h"
#include "storage/disk_log_impl.h"
#include "storage/parser.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/fixture.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

using namespace std::chrono_literals;
using namespace archival;

inline ss::logger test_log("test"); // NOLINT

static ss::abort_source never_abort;

static const auto manifest_namespace = model::ns("kafka");      // NOLINT
static const auto manifest_topic = model::topic("test-topic");  // NOLINT
static const auto manifest_partition = model::partition_id(42); // NOLINT
static const auto manifest_ntp = model::ntp(                    // NOLINT
  manifest_namespace,
  manifest_topic,
  manifest_partition);
static const auto manifest_revision = model::initial_revision_id(0); // NOLINT
static const ss::sstring manifest_url = ssx::sformat(                // NOLINT
  "/10000000/meta/{}_{}/manifest.json",
  manifest_ntp.path(),
  manifest_revision());

static constexpr ss::lowres_clock::duration segment_read_lock_timeout{60s};

static storage::ntp_config get_ntp_conf() {
    return storage::ntp_config(manifest_ntp, "base-dir");
}

static void log_segment(const storage::segment& s) {
    vlog(
      test_log.info,
      "Log segment {}. Offsets: {} {}. Is compacted: {}. Is sealed: {}.",
      s.filename(),
      s.offsets().base_offset,
      s.offsets().dirty_offset,
      s.is_compacted_segment(),
      !s.has_appender());
}

static void log_segment_set(storage::log_manager& lm) {
    auto log = lm.get(manifest_ntp);
    auto plog = dynamic_cast<const storage::disk_log_impl*>(log->get_impl());
    BOOST_REQUIRE(plog != nullptr);
    const auto& sset = plog->segments();
    for (const auto& s : sset) {
        log_segment(*s);
    }
}

void log_upload_candidate(const archival::upload_candidate& up) {
    auto first_source = up.sources.front();
    vlog(
      test_log.info,
      "Upload candidate, exposed name: {} "
      "real offsets: {} {}",
      up.exposed_name,
      first_source->offsets().base_offset,
      first_source->offsets().dirty_offset);
}

// NOLINTNEXTLINE
FIXTURE_TEST(test_upload_segments, archiver_fixture) {
    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(0), model::term_id(1)},
      {manifest_ntp, model::offset(1000), model::term_id(4)},
    };
    init_storage_api_local(segments);
    vlog(test_log.info, "Initialized, start waiting for partition leadership");

    wait_for_partition_leadership(manifest_ntp);
    auto part = app.partition_manager.local().get(manifest_ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [part]() mutable {
        return part->last_stable_offset() >= model::offset(1000);
    }).get();

    vlog(
      test_log.info,
      "Partition is a leader, HW {}, CO {}, partition: {}",
      part->high_watermark(),
      part->committed_offset(),
      *part);

    listen();
    auto [arch_conf, remote_conf] = get_configurations();
    archival::ntp_archiver archiver(
      get_ntp_conf(), arch_conf, remote.local(), *part);
    auto action = ss::defer([&archiver] { archiver.stop().get(); });

    retry_chain_node fib(never_abort);
    auto res = upload_next_with_retries(archiver).get0();

    auto non_compacted_result = res.non_compacted_upload_result;
    auto compacted_result = res.compacted_upload_result;

    BOOST_REQUIRE_EQUAL(non_compacted_result.num_succeeded, 2);
    BOOST_REQUIRE_EQUAL(non_compacted_result.num_failed, 0);

    BOOST_REQUIRE_EQUAL(compacted_result.num_succeeded, 0);
    BOOST_REQUIRE_EQUAL(compacted_result.num_failed, 0);

    for (auto [url, req] : get_targets()) {
        vlog(test_log.info, "{} {}", req.method, req.url);
    }
    BOOST_REQUIRE_EQUAL(get_requests().size(), 3);

    cloud_storage::partition_manifest manifest;
    {
        BOOST_REQUIRE(get_targets().count(manifest_url)); // NOLINT
        auto req_opt = get_latest_request(manifest_url);
        BOOST_REQUIRE(req_opt.has_value());
        auto req = req_opt.value().get();
        BOOST_REQUIRE_EQUAL(req.method, "PUT"); // NOLINT
        verify_manifest_content(req.content);
        manifest = load_manifest(req.content);
        BOOST_REQUIRE(manifest == part->archival_meta_stm()->manifest());
    }

    {
        segment_name segment1_name{"0-1-v1.log"};
        auto segment1_url = get_segment_path(manifest, segment1_name);
        auto req_opt = get_latest_request("/" + segment1_url().string());
        BOOST_REQUIRE(req_opt.has_value());
        auto req = req_opt.value().get();
        BOOST_REQUIRE_EQUAL(req.method, "PUT"); // NOLINT
        verify_segment(manifest_ntp, segment1_name, req.content);
    }

    {
        segment_name segment2_name{"1000-4-v1.log"};
        auto segment2_url = get_segment_path(manifest, segment2_name);
        auto it = get_targets().find("/" + segment2_url().string());
        BOOST_REQUIRE(it != get_targets().end());
        const auto& [url, req] = *it;
        BOOST_REQUIRE_EQUAL(req.method, "PUT"); // NOLINT
        verify_segment(manifest_ntp, segment2_name, req.content);
    }

    BOOST_REQUIRE(part->archival_meta_stm());
    const auto& stm_manifest = part->archival_meta_stm()->manifest();
    BOOST_REQUIRE_EQUAL(stm_manifest.size(), segments.size());
    for (size_t i = 0; i < segments.size(); ++i) {
        const auto& segment = segments[i];
        auto it = stm_manifest.begin();
        std::advance(it, i);

        BOOST_CHECK_EQUAL(segment.base_offset, it->second.base_offset);
    }
}

// NOLINTNEXTLINE
FIXTURE_TEST(test_retention, archiver_fixture) {
    /*
     * Test that segments are removed from cloud storage as indicated
     * by the retention policy. Four segments are created, two of which
     * are older than the retention policy set by the test. After,
     * retention was applied and garbage collection has run, we should
     * see DELETE requests for the old segments being made.
     */

    auto old_stamp = model::timestamp{
      model::timestamp::now().value()
      - std::chrono::milliseconds{10min}.count()};

    std::vector<segment_desc> segments = {
      {.ntp = manifest_ntp,
       .base_offset = model::offset(0),
       .term = model::term_id(1),
       .timestamp = old_stamp},
      {.ntp = manifest_ntp,
       .base_offset = model::offset(1000),
       .term = model::term_id(2),
       .timestamp = old_stamp},
      {.ntp = manifest_ntp,
       .base_offset = model::offset(2000),
       .term = model::term_id(3)},
      {.ntp = manifest_ntp,
       .base_offset = model::offset(3000),
       .term = model::term_id(4)}};

    init_storage_api_local(segments);
    vlog(test_log.info, "Initialized, start waiting for partition leadership");

    wait_for_partition_leadership(manifest_ntp);
    auto part = app.partition_manager.local().get(manifest_ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [part]() mutable {
        return part->last_stable_offset() >= model::offset(1);
    }).get();

    vlog(
      test_log.info,
      "Partition is a leader, HW {}, CO {}, partition: {}",
      part->high_watermark(),
      part->committed_offset(),
      *part);

    listen();

    auto [arch_conf, remote_conf] = get_configurations();
    archival::ntp_archiver archiver(
      get_ntp_conf(), arch_conf, remote.local(), *part);
    auto action = ss::defer([&archiver] { archiver.stop().get(); });

    retry_chain_node fib(never_abort);
    auto res = upload_next_with_retries(archiver).get0();
    BOOST_REQUIRE_EQUAL(res.non_compacted_upload_result.num_succeeded, 4);
    BOOST_REQUIRE_EQUAL(res.non_compacted_upload_result.num_failed, 0);

    // We generate the path here as we need the segment to be in the
    // manifest for this. After retention is applied (i.e.
    // apply_retention has run) that's not the case anymore.
    std::vector<std::pair<remote_segment_path, bool>> segment_urls;
    for (const auto& seg : segments) {
        auto name = cloud_storage::generate_local_segment_name(
          seg.base_offset, seg.term);
        auto path = get_segment_path(
          part->archival_meta_stm()->manifest(), name);

        bool deletion_expected = seg.timestamp == old_stamp;
        segment_urls.emplace_back(path, deletion_expected);
    }

    config::shard_local_cfg().delete_retention_ms.set_value(
      std::chrono::milliseconds{1min});
    archiver.apply_retention().get();
    archiver.garbage_collect().get();
    config::shard_local_cfg().delete_retention_ms.reset();

    for (auto [url, req] : get_targets()) {
        vlog(test_log.info, "{} {}", req.method, req.url);
    }

    for (const auto& [url, deletion_expected] : segment_urls) {
        auto [req_begin, req_end] = get_targets().equal_range(
          "/" + url().string());
        auto segment_deleted = std::find_if(
                                 req_begin,
                                 req_end,
                                 [](auto entry) {
                                     return entry.second.method == "DELETE";
                                 })
                               != req_end;

        BOOST_REQUIRE(segment_deleted == deletion_expected);
    }
}

FIXTURE_TEST(test_segments_pending_deletion_limit, archiver_fixture) {
    /*
     * This test verifies that the limit imposed by
     * cloud_storage_max_segments_pending_deletion_per_partition on the garbage
     * collection deletion backlog is respected. See
     * ntp_archiver_service::garbage_collect for more details.
     *
     * It works as follows:
     * 1. Create 4 segments; the first 3 have an old time stamp
     * 2. Upload all segments
     * 3. Set cloud_storage_max_segments_pending_deletion_per_partition to 2
     * 4. Set up the HTTP imposter to fail the DELETE request
     * for one of the "old" segments.
     * 4. Trigger retention and garbage collection. DELETE requests
     * will be sent out for the 3 "old" segments, but one of them will
     * fail.
     * 5. Check that the start offset was updated (i.e. manifest was
     * updated) despite the failure to delete. This should have happened
     * as the backlog size was breached (3 > 2).
     */

    auto old_stamp = model::timestamp{
      model::timestamp::now().value()
      - std::chrono::milliseconds{10min}.count()};
    std::vector<segment_desc> segments = {
      {.ntp = manifest_ntp,
       .base_offset = model::offset(0),
       .term = model::term_id(1),
       .timestamp = old_stamp},
      {.ntp = manifest_ntp,
       .base_offset = model::offset(1000),
       .term = model::term_id(2),
       .timestamp = old_stamp},
      {.ntp = manifest_ntp,
       .base_offset = model::offset(2000),
       .term = model::term_id(3),
       .timestamp = old_stamp},
      {.ntp = manifest_ntp,
       .base_offset = model::offset(3000),
       .term = model::term_id(4)}};

    init_storage_api_local(segments);

    wait_for_partition_leadership(manifest_ntp);

    auto part = app.partition_manager.local().get(manifest_ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [part]() mutable {
        return part->last_stable_offset() >= model::offset(3000);
    }).get();

    listen();
    config::shard_local_cfg().delete_retention_ms.set_value(
      std::chrono::milliseconds{1min});
    config::shard_local_cfg()
      .cloud_storage_max_segments_pending_deletion_per_partition(2);

    auto [arch_conf, remote_conf] = get_configurations();
    archival::ntp_archiver archiver(
      get_ntp_conf(), arch_conf, remote.local(), *part);
    auto action = ss::defer([&archiver] { archiver.stop().get(); });

    auto res = upload_next_with_retries(archiver).get0();
    BOOST_REQUIRE_EQUAL(res.non_compacted_upload_result.num_succeeded, 4);
    BOOST_REQUIRE_EQUAL(res.non_compacted_upload_result.num_failed, 0);

    // Fail the second deletion request received.
    fail_request_if(
      [delete_request_idx = 0](const ss::http::request& req) mutable {
          if (req._method == "DELETE") {
              return 2 == ++delete_request_idx;
          }

          return false;
      },
      {.body
       = {archival_tests::forbidden_payload.data(), archival_tests::forbidden_payload.size()},
       .status = ss::http::reply::status_type::bad_request});

    archiver.apply_retention().get();
    archiver.garbage_collect().get();

    for (auto [url, req] : get_targets()) {
        vlog(test_log.info, "{} {}", req.method, req.url);
    }

    const auto& manifest_after_retention
      = part->archival_meta_stm()->manifest();
    vlog(
      test_log.info,
      "Start offset after garbage collection is {}",
      manifest_after_retention.get_start_offset());
    BOOST_REQUIRE(
      manifest_after_retention.get_start_offset() == model::offset(3000));
}

// NOLINTNEXTLINE
FIXTURE_TEST(test_archiver_policy, archiver_fixture) {
    model::offset lso{9999};
    const auto offset1 = model::offset(0000);
    const auto offset2 = model::offset(1000);
    const auto offset3 = model::offset(2000);
    const auto offset4 = model::offset(10000);
    std::vector<segment_desc> segments = {
      {manifest_ntp, offset1, model::term_id(1)},
      {manifest_ntp, offset2, model::term_id(1)},
      {manifest_ntp, offset3, model::term_id(1)},
      {manifest_ntp, offset4, model::term_id(1)},
    };
    init_storage_api_local(segments);
    auto& lm = get_local_storage_api().log_mgr();
    archival::archival_policy policy(manifest_ntp);

    log_segment_set(lm);

    auto log = lm.get(manifest_ntp);
    BOOST_REQUIRE(log);

    auto partition = app.partition_manager.local().get(manifest_ntp);
    BOOST_REQUIRE(partition);
    const storage::offset_translator_state& tr
      = *partition->get_offset_translator_state();

    // Starting offset is lower than offset1
    auto upload1
      = policy
          .get_next_candidate(
            model::offset(0), lso, *log, tr, segment_read_lock_timeout)
          .get()
          .candidate;
    log_upload_candidate(upload1);
    BOOST_REQUIRE(!upload1.sources.empty());
    BOOST_REQUIRE(upload1.starting_offset == offset1);

    model::offset start_offset;

    start_offset = upload1.sources.front()->offsets().dirty_offset
                   + model::offset(1);
    auto upload2 = policy
                     .get_next_candidate(
                       start_offset, lso, *log, tr, segment_read_lock_timeout)
                     .get()
                     .candidate;
    log_upload_candidate(upload2);
    BOOST_REQUIRE(!upload2.sources.empty());
    BOOST_REQUIRE(upload2.starting_offset() == offset2);
    BOOST_REQUIRE(upload2.exposed_name != upload1.exposed_name);
    BOOST_REQUIRE(upload2.sources.front() != upload1.sources.front());
    BOOST_REQUIRE(upload2.sources.front()->offsets().base_offset == offset2);

    start_offset = upload2.sources.front()->offsets().dirty_offset
                   + model::offset(1);
    auto upload3 = policy
                     .get_next_candidate(
                       start_offset, lso, *log, tr, segment_read_lock_timeout)
                     .get()
                     .candidate;
    log_upload_candidate(upload3);
    BOOST_REQUIRE(!upload3.sources.empty());
    BOOST_REQUIRE(upload3.starting_offset() == offset3);
    BOOST_REQUIRE(upload3.exposed_name != upload2.exposed_name);
    BOOST_REQUIRE(upload3.sources.front() != upload2.sources.front());
    BOOST_REQUIRE(upload3.sources.front()->offsets().base_offset == offset3);

    start_offset = upload3.sources.front()->offsets().dirty_offset
                   + model::offset(1);
    auto upload4 = policy
                     .get_next_candidate(
                       start_offset, lso, *log, tr, segment_read_lock_timeout)
                     .get()
                     .candidate;
    BOOST_REQUIRE(upload4.sources.empty());

    start_offset = lso + model::offset(1);
    auto upload5 = policy
                     .get_next_candidate(
                       start_offset, lso, *log, tr, segment_read_lock_timeout)
                     .get()
                     .candidate;
    BOOST_REQUIRE(upload5.sources.empty());
}

FIXTURE_TEST(
  test_archival_policy_search_when_a_segment_is_compacted, archiver_fixture) {
    model::offset lso{9999};

    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset{0}, model::term_id(1)},
      {manifest_ntp, model::offset{1000}, model::term_id(1)},
    };

    storage::ntp_config::default_overrides o;
    o.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;

    init_storage_api_local(segments, o);
    auto& lm = get_local_storage_api().log_mgr();

    log_segment_set(lm);

    auto log = lm.get(manifest_ntp);
    BOOST_REQUIRE(log);

    ss::abort_source as{};
    log
      ->compact(storage::compaction_config(
        model::timestamp::now(),
        std::nullopt,
        model::offset::max(),
        ss::default_priority_class(),
        as))
      .get0();

    auto plog = dynamic_cast<storage::disk_log_impl*>(log->get_impl());

    auto seg = plog->segments().begin();

    BOOST_REQUIRE((*seg)->finished_self_compaction());

    auto partition = app.partition_manager.local().get(manifest_ntp);
    BOOST_REQUIRE(partition);

    auto candidate = archival::archival_policy{manifest_ntp}
                       .get_next_candidate(
                         model::offset(0),
                         lso,
                         *log,
                         *partition->get_offset_translator_state(),
                         segment_read_lock_timeout)
                       .get()
                       .candidate;

    // The search is expected to find the next segment after the compacted
    // segment, skipping the compacted one.
    BOOST_REQUIRE(!candidate.sources.empty());
    BOOST_REQUIRE_GT(candidate.starting_offset(), 0);
    BOOST_REQUIRE_GT(
      candidate.sources.front()->offsets().base_offset, model::offset{0});
}

// NOLINTNEXTLINE
SEASTAR_THREAD_TEST_CASE(test_archival_policy_timeboxed_uploads) {
    storage::disk_log_builder b;
    b | storage::start(manifest_ntp);

    archival::archival_policy policy(manifest_ntp, segment_time_limit{0s});

    auto log = b.get_log();

    raft::offset_translator tr(
      {model::record_batch_type::raft_configuration,
       model::record_batch_type::archival_metadata},
      raft::group_id{0},
      manifest_ntp,
      b.storage());
    tr.start(raft::offset_translator::must_reset::yes, {}).get();
    const auto& tr_state = *tr.state();

    // first offset that is not yet uploaded
    auto start_offset = model::offset{0};

    auto get_next_upload = [&]() {
        auto last_stable_offset = log.offsets().dirty_offset + model::offset{1};
        auto ret = policy
                     .get_next_candidate(
                       start_offset,
                       last_stable_offset,
                       log,
                       tr_state,
                       segment_read_lock_timeout)
                     .get()
                     .candidate;
        if (!ret.sources.empty()) {
            start_offset = ret.final_offset + model::offset{1};
        }
        return ret;
    };

    // configuration[0-0] + data[1-10] + archival_metadata[11-13]
    b | storage::add_segment(model::offset{0})
      | storage::add_random_batch(
        model::offset{0},
        1,
        storage::maybe_compress_batches::no,
        model::record_batch_type::raft_configuration)
      | storage::add_random_batch(model::offset{1}, 10)
      | storage::add_random_batch(
        model::offset{11},
        3,
        storage::maybe_compress_batches::no,
        model::record_batch_type::archival_metadata);
    BOOST_REQUIRE_EQUAL(log.offsets().dirty_offset, model::offset{13});
    tr.sync_with_log(log, std::nullopt).get();

    // should upload [0-13]
    {
        auto upload = get_next_upload();
        BOOST_REQUIRE(!upload.sources.empty());
        BOOST_REQUIRE_EQUAL(upload.exposed_name, "0-0-v1.log");
        BOOST_REQUIRE_EQUAL(upload.starting_offset, model::offset{0});
        BOOST_REQUIRE_EQUAL(upload.final_offset, model::offset{13});
    }

    // data[14-14]
    b | storage::add_random_batch(model::offset{14}, 1);
    BOOST_REQUIRE_EQUAL(log.offsets().dirty_offset, model::offset{14});
    tr.sync_with_log(log, std::nullopt).get();

    // should upload [14-14]
    {
        auto upload = get_next_upload();
        BOOST_REQUIRE(!upload.sources.empty());
        BOOST_REQUIRE_EQUAL(upload.exposed_name, "14-0-v1.log");
        BOOST_REQUIRE_EQUAL(upload.starting_offset, model::offset{14});
        BOOST_REQUIRE_EQUAL(upload.final_offset, model::offset{14});
    }

    // archival_metadata[15-16]
    b
      | storage::add_random_batch(
        model::offset{15},
        2,
        storage::maybe_compress_batches::no,
        model::record_batch_type::archival_metadata);
    BOOST_REQUIRE_EQUAL(log.offsets().dirty_offset, model::offset{16});
    tr.sync_with_log(log, std::nullopt).get();

    // should skip uploading because there are no data batches to upload
    {
        auto upload = get_next_upload();
        BOOST_REQUIRE(upload.sources.empty());
    }

    // data[17-17]
    b | storage::add_random_batch(model::offset{17}, 1);
    BOOST_REQUIRE_EQUAL(log.offsets().dirty_offset, model::offset{17});
    tr.sync_with_log(log, std::nullopt).get();

    // should upload [15-17]
    {
        auto upload = get_next_upload();
        BOOST_REQUIRE(!upload.sources.empty());
        BOOST_REQUIRE_EQUAL(upload.exposed_name, "15-0-v1.log");
        BOOST_REQUIRE_EQUAL(upload.starting_offset, model::offset{15});
        BOOST_REQUIRE_EQUAL(upload.final_offset, model::offset{17});
    }

    // archival_metadata[18-18]
    b
      | storage::add_random_batch(
        model::offset{18},
        1,
        storage::maybe_compress_batches::no,
        model::record_batch_type::archival_metadata);
    BOOST_REQUIRE_EQUAL(log.offsets().dirty_offset, model::offset{18});
    tr.sync_with_log(log, std::nullopt).get();

    // should skip uploading because there are no data batches to upload
    {
        auto upload6 = get_next_upload();
        BOOST_REQUIRE(upload6.sources.empty());
    }

    b.stop().get();
}

// NOLINTNEXTLINE
FIXTURE_TEST(test_upload_segments_leadership_transfer, archiver_fixture) {
    // This test simulates leadership transfer. In this situation the
    // manifest might contain misaligned segments. This triggers partial
    // segment upload which, in turn should guarantee that the progress is
    // made.
    // The manifest that this test generates contains a segment definition
    // that clashes with the partial upload.
    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(0), model::term_id(1), 10},
      {manifest_ntp, model::offset(1000), model::term_id(4), 10},
    };
    init_storage_api_local(segments);
    wait_for_partition_leadership(manifest_ntp);
    auto part = app.partition_manager.local().get(manifest_ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [part]() mutable {
        return part->last_stable_offset() >= model::offset(1);
    }).get();

    vlog(
      test_log.info,
      "Partition is a leader, HW {}, CO {}, partition: {}",
      part->high_watermark(),
      part->committed_offset(),
      *part);

    auto s1name = archival::segment_name("0-1-v1.log");
    auto s2name = archival::segment_name("1000-4-v1.log");
    auto segment1 = get_segment(manifest_ntp, s1name);
    BOOST_REQUIRE(static_cast<bool>(segment1));
    auto segment2 = get_segment(manifest_ntp, s2name);
    BOOST_REQUIRE(static_cast<bool>(segment2));

    cloud_storage::partition_manifest old_manifest(
      manifest_ntp, manifest_revision);
    cloud_storage::partition_manifest::segment_meta old_meta{
      .is_compacted = false,
      .size_bytes = 100,
      .base_offset = model::offset(2),
      .committed_offset = segment1->offsets().dirty_offset - model::offset(1),
      .segment_term = model::term_id{2},
    };
    auto oldname = archival::segment_name("2-2-v1.log");
    old_manifest.add(oldname, old_meta);
    ss::sstring segment3_url = "/dfee62b1/kafka/test-topic/42_0/2-2-v1.log";

    // Simulate pre-existing state in the snapshot
    std::vector<cloud_storage::segment_meta> old_segments;
    for (const auto& s : old_manifest) {
        old_segments.push_back(s.second);
    }
    part->archival_meta_stm()
      ->add_segments(old_segments, std::nullopt, ss::lowres_clock::now() + 1s)
      .get();

    listen();

    auto [arch_conf, remote_conf] = get_configurations();

    archival::ntp_archiver archiver(
      get_ntp_conf(), arch_conf, remote.local(), *part);
    auto action = ss::defer([&archiver] { archiver.stop().get(); });

    retry_chain_node fib(never_abort);

    auto res = upload_next_with_retries(archiver).get0();

    auto non_compacted_result = res.non_compacted_upload_result;
    auto compacted_result = res.compacted_upload_result;

    BOOST_REQUIRE_EQUAL(non_compacted_result.num_succeeded, 2);
    BOOST_REQUIRE_EQUAL(non_compacted_result.num_failed, 0);

    BOOST_REQUIRE_EQUAL(compacted_result.num_succeeded, 0);
    BOOST_REQUIRE_EQUAL(compacted_result.num_failed, 0);

    for (auto req : get_requests()) {
        vlog(test_log.info, "{} {}", req.method, req.url);
    }
    BOOST_REQUIRE_EQUAL(get_requests().size(), 3);

    cloud_storage::partition_manifest manifest;
    {
        BOOST_REQUIRE_EQUAL(1, get_request_count(manifest_url));
        auto req = get_latest_request(manifest_url).value().get();
        BOOST_REQUIRE(req.method == "PUT");
        manifest = load_manifest(req.content);
        BOOST_REQUIRE(manifest == part->archival_meta_stm()->manifest());
    }

    {
        // Check that we uploaded second segment
        auto url = "/" + get_segment_path(manifest, s2name)().string();
        BOOST_REQUIRE_EQUAL(1, get_request_count(url));
        auto req = get_latest_request(url).value().get();
        BOOST_REQUIRE(req.method == "PUT"); // NOLINT
    }

    BOOST_REQUIRE(part->archival_meta_stm());
    const auto& stm_manifest = part->archival_meta_stm()->manifest();
    // including the segment from the old manifest
    BOOST_REQUIRE_EQUAL(stm_manifest.size(), segments.size() + 1);

    for (const auto& [name, base_offset] :
         std::vector<std::pair<segment_name, model::offset>>{
           {s2name, segments[1].base_offset},
           {oldname, old_meta.base_offset}}) {
        BOOST_CHECK(stm_manifest.get(name));
        BOOST_CHECK_EQUAL(stm_manifest.get(name)->base_offset, base_offset);
    }
}

class counting_batch_consumer : public storage::batch_consumer {
public:
    struct stream_stats {
        model::offset min_offset{model::offset::max()};
        model::offset max_offset{model::offset::min()};
        std::vector<model::offset> base_offsets;
        std::vector<model::offset> last_offsets;
    };

    explicit counting_batch_consumer(stream_stats& s)
      : batch_consumer()
      , _stats(s) {}

    consume_result
    accept_batch_start(const model::record_batch_header&) const override {
        return consume_result::accept_batch;
    }
    void consume_batch_start(
      model::record_batch_header h,
      [[maybe_unused]] size_t physical_base_offset,
      [[maybe_unused]] size_t size_on_disk) override {
        _stats.min_offset = std::min(_stats.min_offset, h.base_offset);
        _stats.max_offset = std::max(_stats.max_offset, h.last_offset());
        _stats.base_offsets.push_back(h.base_offset);
        _stats.last_offsets.push_back(h.last_offset());
    }
    void skip_batch_start(model::record_batch_header, size_t, size_t) override {
    }
    void consume_records(iobuf&&) override {}
    stop_parser consume_batch_end() override { return stop_parser::no; }
    void print(std::ostream& o) const override {
        fmt::print(
          o,
          "counting_batch_consumer, min_offset: {}, max_offset: {}, {} batches "
          "consumed",
          _stats.min_offset,
          _stats.max_offset,
          _stats.base_offsets.size());
    }

    stream_stats& _stats;
};

static counting_batch_consumer::stream_stats
calculate_segment_stats(const http_test_utils::request_info& req) {
    iobuf stream_body;
    stream_body.append(req.content.data(), req.content_length);
    auto stream = make_iobuf_input_stream(std::move(stream_body));
    counting_batch_consumer::stream_stats stats{};
    auto consumer = std::make_unique<counting_batch_consumer>(std::ref(stats));
    storage::continuous_batch_parser parser(
      std::move(consumer), storage::segment_reader_handle(std::move(stream)));
    parser.consume().get();
    parser.close().get();
    return stats;
}

struct upload_range {
    size_t base;
    size_t last;
};

/// This test checks partial uploads. Partial upload can happen
/// if the idle time is set in config or when the leadership is
/// transferred to another node which has different data layout.
///
/// The test creates a segment and forces a partial upload of the
/// segment's middle part followed by the upload of the remaining
/// data.
static void test_partial_upload_impl(
  archiver_fixture& test, upload_range first, upload_range last) {
    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(0), model::term_id(1), 10},
    };

    test.init_storage_api_local(segments);
    test.wait_for_partition_leadership(manifest_ntp);
    auto part = test.app.partition_manager.local().get(manifest_ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [part]() mutable {
        return part->last_stable_offset() >= model::offset(1);
    }).get();

    auto s1name = archival::segment_name("0-1-v1.log");

    auto segment1 = test.get_segment(manifest_ntp, s1name);
    BOOST_REQUIRE(static_cast<bool>(segment1));

    // Generate new manifest
    cloud_storage::partition_manifest manifest(manifest_ntp, manifest_revision);
    const auto& layout = test.get_layouts(manifest_ntp);
    vlog(test_log.debug, "Layout size: {}", layout.size());
    for (const auto& s : layout) {
        vlog(test_log.debug, "- Segment {}", s.base_offset);
        for (const auto& r : s.ranges) {
            vlog(
              test_log.debug, "-- Batch {}-{}", r.base_offset, r.last_offset);
        }
    }

    auto last_uploaded_range = layout[0].ranges[first.base];
    auto last_uploaded_offset = last_uploaded_range.base_offset
                                - model::offset(1);

    model::offset lso = layout[0].ranges[last.base].base_offset;
    model::offset next_uploaded_offset
      = layout[0].ranges[first.last].last_offset;

    model::offset base_upl1 = layout[0].ranges[first.base].base_offset;
    model::offset last_upl1 = layout[0].ranges[first.last].last_offset;
    model::offset base_upl2 = layout[0].ranges[last.base].base_offset;
    model::offset last_upl2 = layout[0].ranges[last.last].last_offset;

    vlog(
      test_log.debug,
      "First range: {}-{}, second range: {}-{}",
      base_upl1,
      last_upl1,
      base_upl2,
      last_upl2);

    cloud_storage::partition_manifest::segment_meta segment_meta{
      .is_compacted = false,
      .size_bytes = 1, // doesn't matter
      .base_offset = model::offset(0),
      .committed_offset = last_uploaded_offset,
      .ntp_revision = manifest.get_revision_id()};

    manifest.add(s1name, segment_meta);
    std::vector<cloud_storage::segment_meta> all_segments;
    for (const auto& s : manifest) {
        all_segments.push_back(s.second);
    }
    part->archival_meta_stm()
      ->add_segments(all_segments, std::nullopt, ss::lowres_clock::now() + 1s)
      .get();

    segment_name s2name{
      ssx::sformat("{}-1-v1.log", last_uploaded_offset() + 1)};
    segment_name s3name{
      ssx::sformat("{}-1-v1.log", next_uploaded_offset() + 1)};

    vlog(
      test_log.debug,
      "Expected segment names {} and {}, last_uploaded_offset: {}, "
      "last_stable_offset: {}",
      s2name,
      s3name,
      last_uploaded_offset,
      lso);

    auto [aconf, cconf] = test.get_configurations();

    aconf->time_limit = segment_time_limit(0s);

    archival::ntp_archiver archiver(
      get_ntp_conf(), aconf, test.remote.local(), *part);

    retry_chain_node fib(never_abort);
    test.listen();
    auto res = test.upload_next_with_retries(archiver, lso).get0();

    auto non_compacted_result = res.non_compacted_upload_result;
    auto compacted_result = res.compacted_upload_result;

    BOOST_REQUIRE_EQUAL(non_compacted_result.num_succeeded, 1);
    BOOST_REQUIRE_EQUAL(non_compacted_result.num_failed, 0);

    BOOST_REQUIRE_EQUAL(compacted_result.num_succeeded, 0);
    BOOST_REQUIRE_EQUAL(compacted_result.num_failed, 0);

    test.log_requests();
    BOOST_REQUIRE_EQUAL(test.get_requests().size(), 2);

    {
        auto [begin, end] = test.get_targets().equal_range(manifest_url);
        size_t len = std::distance(begin, end);
        BOOST_REQUIRE_EQUAL(len, 1);
        BOOST_REQUIRE(begin->second.method == "PUT");
        manifest = load_manifest(begin->second.content);
        BOOST_REQUIRE(manifest == part->archival_meta_stm()->manifest());
    }

    ss::sstring url2 = "/" + get_segment_path(manifest, s2name)().string();

    {
        auto [begin, end] = test.get_targets().equal_range(url2);
        size_t len = std::distance(begin, end);
        BOOST_REQUIRE_EQUAL(len, 1);
        BOOST_REQUIRE(begin->second.method == "PUT"); // NOLINT

        // check that the uploaded log contains the right
        // offsets
        auto stats = calculate_segment_stats(begin->second);

        BOOST_REQUIRE_EQUAL(stats.min_offset, base_upl1);
        BOOST_REQUIRE_EQUAL(stats.max_offset, last_upl1);
    }

    lso = last_upl2 + model::offset(1);
    res = test.upload_next_with_retries(archiver, lso).get0();

    non_compacted_result = res.non_compacted_upload_result;
    compacted_result = res.compacted_upload_result;

    BOOST_REQUIRE_EQUAL(non_compacted_result.num_succeeded, 1);
    BOOST_REQUIRE_EQUAL(non_compacted_result.num_failed, 0);

    BOOST_REQUIRE_EQUAL(compacted_result.num_succeeded, 0);
    BOOST_REQUIRE_EQUAL(compacted_result.num_failed, 0);

    test.log_requests();
    BOOST_REQUIRE_EQUAL(test.get_requests().size(), 4);
    {
        auto [begin, end] = test.get_targets().equal_range(manifest_url);
        size_t len = std::distance(begin, end);
        BOOST_REQUIRE_EQUAL(len, 2);
        std::multiset<ss::sstring> expected = {"PUT", "PUT"};
        for (auto it = begin; it != end; it++) {
            auto key = it->second.method;
            BOOST_REQUIRE(expected.contains(key));
            auto i = expected.find(key);
            expected.erase(i);

            if (key == "PUT") {
                auto new_manifest = load_manifest(it->second.content);
                if (new_manifest.size() > manifest.size()) {
                    manifest = new_manifest;
                }
            }
        }
        BOOST_REQUIRE(expected.empty());
        BOOST_REQUIRE(part->archival_meta_stm());
        const auto& stm_manifest = part->archival_meta_stm()->manifest();
        BOOST_REQUIRE(stm_manifest == manifest);
    }

    {
        auto [begin, end] = test.get_targets().equal_range(url2);
        size_t len = std::distance(begin, end);
        BOOST_REQUIRE_EQUAL(len, 1);
        BOOST_REQUIRE(begin->second.method == "PUT"); // NOLINT
    }
    {
        ss::sstring url3 = "/" + get_segment_path(manifest, s3name)().string();
        auto [begin, end] = test.get_targets().equal_range(url3);
        size_t len = std::distance(begin, end);
        BOOST_REQUIRE_EQUAL(len, 1);
        BOOST_REQUIRE(begin->second.method == "PUT"); // NOLINT

        // check that the uploaded log contains the right offsets
        auto stats = calculate_segment_stats(begin->second);

        BOOST_REQUIRE_EQUAL(stats.min_offset, base_upl2);
        BOOST_REQUIRE_EQUAL(stats.max_offset, last_upl2);
    }
}

// NOLINTNEXTLINE
FIXTURE_TEST(test_partial_upload1, archiver_fixture) {
    test_partial_upload_impl(*this, {3, 7}, {8, 9});
}

// NOLINTNEXTLINE
FIXTURE_TEST(test_partial_upload2, archiver_fixture) {
    test_partial_upload_impl(*this, {3, 3}, {4, 9});
}

// NOLINTNEXTLINE
FIXTURE_TEST(test_partial_upload3, archiver_fixture) {
    test_partial_upload_impl(*this, {3, 8}, {9, 9});
}

// NOLINTNEXTLINE
FIXTURE_TEST(test_upload_segments_with_overlap, archiver_fixture) {
    // Test situation when the offset ranges of segments have some overlap.
    // This shouldn't normally happen with committed offset but might be
    // the case with dirty offset.
    // For instance if we have segments A with base offset 0 committed offset
    // 100 and dirty offset 101, and B with base offset 100 and committed offset
    // 200, the archival_policy should return A and then B. Before the fix this
    // is not the case and it always retuns A.
    const auto offset1 = model::offset(0);
    const auto offset2 = model::offset(1000);
    const auto offset3 = model::offset(2000);
    std::vector<segment_desc> segments = {
      {manifest_ntp, offset1, model::term_id(1)},
      {manifest_ntp, offset2, model::term_id(1)},
      {manifest_ntp, offset3, model::term_id(1)},
    };
    init_storage_api_local(segments);
    auto& lm = get_local_storage_api().log_mgr();
    archival::archival_policy policy(manifest_ntp);

    // Patch segment offsets to create overlaps for the archival_policy.
    // The archival_policy instance only touches the offsets, not the
    // actual data so having them a bit inconsistent for the sake of testing
    // is OK.
    auto segment1 = get_segment(
      manifest_ntp, archival::segment_name("0-1-v1.log"));
    auto& tracker1 = const_cast<storage::segment::offset_tracker&>(
      segment1->offsets());
    tracker1.dirty_offset = offset2 - model::offset(1);
    auto segment2 = get_segment(
      manifest_ntp, archival::segment_name("1000-1-v1.log"));
    auto& tracker2 = const_cast<storage::segment::offset_tracker&>(
      segment2->offsets());
    tracker2.dirty_offset = offset3 - model::offset(1);

    // Every segment should be returned once as we're calling the
    // policy to get next candidate.
    log_segment_set(lm);

    auto log = lm.get(manifest_ntp);
    BOOST_REQUIRE(log);

    auto partition = app.partition_manager.local().get(manifest_ntp);
    BOOST_REQUIRE(partition);
    const storage::offset_translator_state& tr
      = *partition->get_offset_translator_state();

    model::offset start_offset{0};
    model::offset lso{9999};
    // Starting offset is lower than offset1
    auto upload1 = policy
                     .get_next_candidate(
                       start_offset, lso, *log, tr, segment_read_lock_timeout)
                     .get()
                     .candidate;
    log_upload_candidate(upload1);
    BOOST_REQUIRE(!upload1.sources.empty());
    BOOST_REQUIRE(upload1.starting_offset == offset1);

    start_offset = upload1.sources.front()->offsets().dirty_offset
                   + model::offset(1);
    auto upload2 = policy
                     .get_next_candidate(
                       start_offset, lso, *log, tr, segment_read_lock_timeout)
                     .get()
                     .candidate;
    log_upload_candidate(upload2);
    BOOST_REQUIRE(!upload2.sources.empty());
    BOOST_REQUIRE(upload2.starting_offset == offset2);
    BOOST_REQUIRE(upload2.exposed_name != upload1.exposed_name);
    BOOST_REQUIRE(upload2.sources.front() != upload1.sources.front());
    BOOST_REQUIRE(upload2.sources.front()->offsets().base_offset == offset2);

    start_offset = upload2.sources.front()->offsets().dirty_offset
                   + model::offset(1);
    auto upload3 = policy
                     .get_next_candidate(
                       start_offset, lso, *log, tr, segment_read_lock_timeout)
                     .get()
                     .candidate;
    log_upload_candidate(upload3);
    BOOST_REQUIRE(!upload3.sources.empty());
    BOOST_REQUIRE(upload3.starting_offset == offset3);
    BOOST_REQUIRE(upload3.exposed_name != upload2.exposed_name);
    BOOST_REQUIRE(upload3.sources.front() != upload2.sources.front());
    BOOST_REQUIRE(upload3.sources.front()->offsets().base_offset == offset3);

    start_offset = upload3.sources.front()->offsets().dirty_offset
                   + model::offset(1);
    auto upload4 = policy
                     .get_next_candidate(
                       start_offset, lso, *log, tr, segment_read_lock_timeout)
                     .get()
                     .candidate;
    BOOST_REQUIRE(upload4.sources.empty());
}

SEASTAR_THREAD_TEST_CASE(small_segment_run_test) {
    size_t high_watermark = 100;
    std::vector<cloud_storage::segment_meta> segments = {
      {.size_bytes = 120,
       .base_offset = model::offset{0},
       .committed_offset = model::offset{10},
       .ntp_revision = model::initial_revision_id{20},
       .archiver_term = model::term_id{5},
       .segment_term = model::term_id{4},
       .sname_format = cloud_storage::segment_name_format::v2},
      {.size_bytes = 20,
       .base_offset = model::offset{11},
       .committed_offset = model::offset{12},
       .ntp_revision = model::initial_revision_id{20},
       .archiver_term = model::term_id{5},
       .segment_term = model::term_id{4},
       .sname_format = cloud_storage::segment_name_format::v2},
      {.size_bytes = 40,
       .base_offset = model::offset{13},
       .committed_offset = model::offset{17},
       .ntp_revision = model::initial_revision_id{20},
       .archiver_term = model::term_id{5},
       .segment_term = model::term_id{4},
       .sname_format = cloud_storage::segment_name_format::v2},
      {.size_bytes = 40,
       .base_offset = model::offset{18},
       .committed_offset = model::offset{21},
       .ntp_revision = model::initial_revision_id{20},
       .archiver_term = model::term_id{5},
       .segment_term = model::term_id{4},
       .sname_format = cloud_storage::segment_name_format::v2},
      {.size_bytes = 40,
       .base_offset = model::offset{22},
       .committed_offset = model::offset{25},
       .ntp_revision = model::initial_revision_id{20},
       .archiver_term = model::term_id{5},
       .segment_term = model::term_id{4},
       .sname_format = cloud_storage::segment_name_format::v2},
    };
    archival::adjacent_segment_run run(manifest_ntp);
    for (const auto& s : segments) {
        if (run.maybe_add_segment(s, high_watermark)) {
            break;
        }
    }
    BOOST_REQUIRE(run.meta.base_offset == model::offset{11});
    BOOST_REQUIRE(run.meta.committed_offset == model::offset{21});
    BOOST_REQUIRE(run.num_segments == 3);
    BOOST_REQUIRE(run.meta.size_bytes == 100);
}
