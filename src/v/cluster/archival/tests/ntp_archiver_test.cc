/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cloud_storage/async_manifest_view.h"
#include "cloud_storage/fwd.h"
#include "cloud_storage/read_path_probes.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/tests/manual_fixture.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/client_pool.h"
#include "cluster/archival/adjacent_segment_merger.h"
#include "cluster/archival/archival_metadata_stm.h"
#include "cluster/archival/archival_policy.h"
#include "cluster/archival/ntp_archiver_service.h"
#include "cluster/archival/tests/service_fixture.h"
#include "config/configuration.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record_batch_types.h"
#include "net/types.h"
#include "ssx/sformat.h"
#include "storage/disk_log_impl.h"
#include "storage/parser.h"
#include "storage/storage_resources.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/archival.h"
#include "test_utils/fixture.h"
#include "test_utils/scoped_config.h"
#include "utils/retry_chain_node.h"
#include "utils/unresolved_address.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

using namespace std::chrono_literals;
using namespace archival;

inline ss::logger test_log("test"); // NOLINT

namespace {
cloud_storage::remote_path_provider path_provider(std::nullopt, std::nullopt);
} // anonymous namespace

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
  "/10000000/meta/{}_{}/manifest.bin",
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
      s.offsets().get_base_offset(),
      s.offsets().get_dirty_offset(),
      s.is_compacted_segment(),
      !s.has_appender());
}

static void log_segment_set(storage::log_manager& lm) {
    auto plog = lm.get(manifest_ntp);
    BOOST_REQUIRE(plog != nullptr);
    const auto& sset = plog->segments();
    for (const auto& s : sset) {
        log_segment(*s);
    }
}

static remote_manifest_path generate_spill_manifest_path(
  const cloud_storage::partition_manifest& stm_manifest,
  const cloud_storage::segment_meta& spillover_manifest) {
    cloud_storage::spillover_manifest_path_components comp{
      .base = spillover_manifest.base_offset,
      .last = spillover_manifest.committed_offset,
      .base_kafka = spillover_manifest.base_kafka_offset(),
      .next_kafka = spillover_manifest.next_kafka_offset(),
      .base_ts = spillover_manifest.base_timestamp,
      .last_ts = spillover_manifest.max_timestamp,
    };
    return remote_manifest_path{
      path_provider.spillover_manifest_path(stm_manifest, comp)};
}

void log_upload_candidate(const archival::upload_candidate& up) {
    auto first_source = up.sources.front();
    vlog(
      test_log.info,
      "Upload candidate, exposed name: {} "
      "real offsets: {} {}",
      up.exposed_name,
      first_source->offsets().get_base_offset(),
      first_source->offsets().get_dirty_offset());
}

bool is_partition_manifest_upload_req(
  const http_test_utils::request_info& req) {
    return req.method == "PUT" && req.url == manifest_url;
}

// NOLINTNEXTLINE
FIXTURE_TEST(test_upload_segments, archiver_fixture) {
    std::vector<segment_desc> segments = {
      {.ntp = manifest_ntp,
       .base_offset = model::offset(0),
       .term = model::term_id(1),
       .num_records = 1000},
      {.ntp = manifest_ntp,
       .base_offset = model::offset(1000),
       .term = model::term_id(4),
       .num_records = 1000},
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
    auto amv = ss::make_shared<cloud_storage::async_manifest_view>(
      remote,
      app.shadow_index_cache,
      part->archival_meta_stm()->manifest(),
      arch_conf->bucket_name,
      path_provider);
    archival::ntp_archiver archiver(
      get_ntp_conf(),
      arch_conf,
      remote.local(),
      app.shadow_index_cache.local(),
      *part,
      amv);
    auto action = ss::defer([&archiver, &amv] {
        archiver.stop().get();
        amv->stop().get();
    });

    retry_chain_node fib(never_abort);
    auto res = upload_next_with_retries(archiver).get();

    auto non_compacted_result = res.non_compacted_upload_result;
    auto compacted_result = res.compacted_upload_result;

    BOOST_REQUIRE_EQUAL(non_compacted_result.num_succeeded, 2);
    BOOST_REQUIRE_EQUAL(non_compacted_result.num_failed, 0);

    BOOST_REQUIRE_EQUAL(compacted_result.num_succeeded, 0);
    BOOST_REQUIRE_EQUAL(compacted_result.num_failed, 0);

    for (auto [url, req] : get_targets()) {
        vlog(test_log.info, "{} {}", req.method, req.url);
    }
    BOOST_REQUIRE_EQUAL(get_requests().size(), 5);

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

        auto index_url = get_segment_index_path(manifest, segment1_name);
        auto index_req_maybe = get_latest_request("/" + index_url().string());
        BOOST_REQUIRE(index_req_maybe.has_value());
        auto index_req = index_req_maybe.value().get();
        BOOST_REQUIRE_EQUAL(index_req.method, "PUT");
        verify_index(manifest_ntp, segment1_name, manifest, index_req.content);
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

        BOOST_CHECK_EQUAL(segment.base_offset, it->base_offset);
    }
}

FIXTURE_TEST(test_upload_after_failure, archiver_fixture) {
    // During a segment upload, the stream used to read from the segment and
    // upload it can be created in two ways, depending on the failures that
    // occur. The first upload uses a stream created from a fanout. If that
    // upload fails, subsequent uploads use a simple stream read from file. This
    // test checks that the switch from the first type of stream to the second
    // works as expected, and also that the segment index is uploaded even if
    // the segment upload fails.

    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(0), model::term_id(1)},
    };
    init_storage_api_local(segments);
    wait_for_partition_leadership(manifest_ntp);
    auto part = app.partition_manager.local().get(manifest_ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [part]() mutable {
        return part->last_stable_offset() >= model::offset(1);
    }).get();

    auto fail_resp = http_test_utils::response{
      .body = R"xml(<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>SlowDown</Code>
    <Message>Slow Down</Message>
    <Resource>resource</Resource>
    <RequestId>requestid</RequestId>
</Error>)xml",
      .status = http_test_utils::response::status_type::service_unavailable};

    bool state{false};
    std::regex logexpr{".*/0-.*log\\.\\d+"};

    // Only fail the first segment put request
    fail_request_if(
      [&state, &logexpr](const ss::http::request& req) {
          auto should_fail = !state && req._method == "PUT"
                             && std::regex_match(
                               req._url.begin(), req._url.end(), logexpr);
          state = true;
          return should_fail;
      },
      std::move(fail_resp));

    listen();

    auto [arch_conf, remote_conf] = get_configurations();
    auto amv = ss::make_shared<cloud_storage::async_manifest_view>(
      remote,
      app.shadow_index_cache,
      part->archival_meta_stm()->manifest(),
      arch_conf->bucket_name,
      path_provider);
    archival::ntp_archiver archiver(
      get_ntp_conf(),
      arch_conf,
      remote.local(),
      app.shadow_index_cache.local(),
      *part,
      amv);

    auto action = ss::defer([&archiver, &amv] {
        archiver.stop().get();
        amv->stop().get();
    });

    retry_chain_node fib(never_abort);
    auto res = upload_next_with_retries(archiver).get();

    auto&& [non_compacted_result, compacted_result] = res;

    BOOST_REQUIRE_EQUAL(non_compacted_result.num_succeeded, 1);
    BOOST_REQUIRE_EQUAL(non_compacted_result.num_failed, 0);

    BOOST_REQUIRE_EQUAL(compacted_result.num_succeeded, 0);
    BOOST_REQUIRE_EQUAL(compacted_result.num_failed, 0);
    BOOST_REQUIRE_EQUAL(get_requests().size(), 4);

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

    segment_name segment1_name{"0-1-v1.log"};
    auto segment1_url = get_segment_path(manifest, segment1_name);
    auto req_opt = get_latest_request("/" + segment1_url().string());
    BOOST_REQUIRE(req_opt.has_value());
    auto req = req_opt.value().get();
    BOOST_REQUIRE_EQUAL(req.method, "PUT"); // NOLINT
    verify_segment(manifest_ntp, segment1_name, req.content);

    auto index_url = get_segment_index_path(manifest, segment1_name);
    const auto& index_req = get_targets().find("/" + index_url().native());
    BOOST_REQUIRE(index_req != get_targets().end());
    BOOST_REQUIRE_EQUAL(index_req->second.method, "PUT");
    verify_index(
      manifest_ntp, segment1_name, manifest, index_req->second.content);
}

FIXTURE_TEST(
  test_index_not_uploaded_if_segment_not_uploaded, archiver_fixture) {
    // This test asserts that uploading of segment index will only happen if the
    // segment upload is successful. Indices uploaded without segments are not
    // found during cleanup, thus leaving behind orphan items in the bucket.

    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(0), model::term_id(1)},
    };
    init_storage_api_local(segments);
    wait_for_partition_leadership(manifest_ntp);
    auto part = app.partition_manager.local().get(manifest_ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [part]() mutable {
        return part->last_stable_offset() >= model::offset(1);
    }).get();

    auto fail_resp = http_test_utils::response{
      .body = R"xml(<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>AccessDenied</Code>
    <Message>Access Denied</Message>
    <Resource>resource</Resource>
    <RequestId>requestid</RequestId>
</Error>)xml",
      .status = http_test_utils::response::status_type::forbidden};
    std::regex logexpr{".*/0-.*log\\.\\d+"};
    fail_request_if(
      [&logexpr](const ss::http::request& req) {
          return req._method == "PUT"
                 && std::regex_match(req._url.begin(), req._url.end(), logexpr);
      },
      std::move(fail_resp));

    listen();

    auto [arch_conf, remote_conf] = get_configurations();
    auto amv = ss::make_shared<cloud_storage::async_manifest_view>(
      remote,
      app.shadow_index_cache,
      part->archival_meta_stm()->manifest(),
      arch_conf->bucket_name,
      path_provider);
    archival::ntp_archiver archiver(
      get_ntp_conf(),
      arch_conf,
      remote.local(),
      app.shadow_index_cache.local(),
      *part,
      amv);

    auto action = ss::defer([&archiver, &amv] {
        archiver.stop().get();
        amv->stop().get();
    });

    retry_chain_node fib(never_abort);
    auto res = archiver.upload_next_candidates().get();

    auto&& [non_compacted_result, compacted_result] = res;
    BOOST_REQUIRE_EQUAL(non_compacted_result.num_succeeded, 0);
    BOOST_REQUIRE_EQUAL(non_compacted_result.num_failed, 1);
    BOOST_REQUIRE_EQUAL(get_requests().size(), 1);
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
       .num_records = 1000,
       .timestamp = old_stamp},
      {.ntp = manifest_ntp,
       .base_offset = model::offset(1000),
       .term = model::term_id(2),
       .num_records = 1000,
       .timestamp = old_stamp},
      {
        .ntp = manifest_ntp,
        .base_offset = model::offset(2000),
        .term = model::term_id(3),
        .num_records = 1000,
      },
      {
        .ntp = manifest_ntp,
        .base_offset = model::offset(3000),
        .term = model::term_id(4),
        .num_records = 1000,
      }};

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
    auto amv = ss::make_shared<cloud_storage::async_manifest_view>(
      remote,
      app.shadow_index_cache,
      part->archival_meta_stm()->manifest(),
      arch_conf->bucket_name,
      path_provider);
    archival::ntp_archiver archiver(
      get_ntp_conf(),
      arch_conf,
      remote.local(),
      app.shadow_index_cache.local(),
      *part,
      amv);
    auto action = ss::defer([&archiver, &amv] {
        archiver.stop().get();
        amv->stop().get();
    });

    retry_chain_node fib(never_abort);
    auto res = upload_next_with_retries(archiver).get();
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

    config::shard_local_cfg().log_retention_ms.set_value(
      std::chrono::milliseconds{1min});
    config::shard_local_cfg()
      .cloud_storage_garbage_collect_timeout_ms.set_value(
        std::chrono::milliseconds{1min});
    archiver.apply_retention().get();
    archiver.garbage_collect().get();
    config::shard_local_cfg().log_retention_ms.reset();
    config::shard_local_cfg().cloud_storage_garbage_collect_timeout_ms.reset();

    for (auto [url, req] : get_targets()) {
        vlog(test_log.info, "{} {}", req.method, req.url);
    }

    for (const auto& [url, deletion_expected] : segment_urls) {
        auto urlstr = url().string();
        auto expected_delete_paths = {
          urlstr, urlstr + ".index", urlstr + ".tx"};
        auto [req_begin, req_end] = get_targets().equal_range("/?delete");
        BOOST_REQUIRE_EQUAL(std::distance(req_begin, req_end), 1);
        for (const auto& p : expected_delete_paths) {
            const bool entity_deleted = req_begin->second.content.contains(p);
            BOOST_REQUIRE(entity_deleted == deletion_expected);
        }
    }
}

FIXTURE_TEST(test_archive_retention, archiver_fixture) {
    /*
     * Test retention within the archive focusing on the specific
     * case where archive garbage collection needs to remove the contents
     * of the entire spillover manifest.
     */

    auto cfg = scoped_config{};
    cfg.get("cloud_storage_spillover_manifest_max_segments")
      .set_value(std::optional<size_t>{2});
    cfg.get("cloud_storage_spillover_manifest_size")
      .set_value(std::optional<size_t>{std::nullopt});

    // Write segments to local log
    auto old_stamp = model::timestamp{
      model::timestamp::now().value()
      - std::chrono::milliseconds{10min}.count()};

    std::vector<segment_desc> segments = {
      {.ntp = manifest_ntp,
       .base_offset = model::offset(0),
       .term = model::term_id(1),
       .num_records = 1000,
       .timestamp = old_stamp},
      {.ntp = manifest_ntp,
       .base_offset = model::offset(1000),
       .term = model::term_id(2),
       .num_records = 1000,
       .timestamp = old_stamp},
      {.ntp = manifest_ntp,
       .base_offset = model::offset(2000),
       .term = model::term_id(3),
       .num_records = 1000},
      {.ntp = manifest_ntp,
       .base_offset = model::offset(3000),
       .term = model::term_id(4),
       .num_records = 1000}};

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

    // Instantiate archiver
    auto [arch_conf, remote_conf] = get_configurations();
    auto amv = ss::make_shared<cloud_storage::async_manifest_view>(
      remote,
      app.shadow_index_cache,
      part->archival_meta_stm()->manifest(),
      arch_conf->bucket_name,
      path_provider);
    archival::ntp_archiver archiver(
      get_ntp_conf(),
      arch_conf,
      remote.local(),
      app.shadow_index_cache.local(),
      *part,
      amv);
    amv->start().get();
    auto action = ss::defer([&archiver, &amv] {
        archiver.stop().get();
        amv->stop().get();
    });

    // Wait for uploads to complete
    retry_chain_node fib(never_abort);
    auto res = upload_next_with_retries(archiver).get();
    BOOST_REQUIRE_EQUAL(res.non_compacted_upload_result.num_succeeded, 4);
    BOOST_REQUIRE_EQUAL(res.non_compacted_upload_result.num_failed, 0);

    // We generate the path here as we need the segment to be in the
    // manifest for this. After retention and/or spillover are applied
    // that's not the case anymore.
    std::vector<std::pair<remote_segment_path, bool>> segment_urls;
    for (const auto& seg : segments) {
        auto name = cloud_storage::generate_local_segment_name(
          seg.base_offset, seg.term);
        auto path = get_segment_path(
          part->archival_meta_stm()->manifest(), name);

        bool deletion_expected = seg.timestamp == old_stamp;
        segment_urls.emplace_back(path, deletion_expected);
    }

    // Trigger spillover
    archiver.apply_spillover().get();

    const auto& spills
      = part->archival_meta_stm()->manifest().get_spillover_map();
    BOOST_REQUIRE_EQUAL(spills.size(), 1);
    BOOST_REQUIRE_EQUAL(spills.begin()->base_offset, model::offset{0});
    BOOST_REQUIRE_EQUAL(spills.begin()->committed_offset, model::offset{1999});
    auto spill_path = generate_spill_manifest_path(
      part->archival_meta_stm()->manifest(), *(spills.begin()));

    config::shard_local_cfg().log_retention_ms.set_value(
      std::chrono::milliseconds{5min});
    auto reset_cfg = ss::defer(
      [] { config::shard_local_cfg().log_retention_ms.reset(); });

    // Apply retention within the archive.
    archiver.apply_archive_retention().get();
    BOOST_REQUIRE_EQUAL(
      part->archival_meta_stm()->manifest().get_archive_start_offset(),
      model::offset{2000});
    BOOST_REQUIRE_EQUAL(
      part->archival_meta_stm()->manifest().get_archive_clean_offset(),
      model::offset{0});

    // Trigger garbage collection within the archive. This should
    // remove segments in the [0, 2000) offset interval.
    auto fut = archiver.garbage_collect_archive();
    tests::cooperative_spin_wait_with_timeout(5s, [this, part]() mutable {
        const auto& manifest = part->archival_meta_stm()->manifest();
        bool archive_clean_offset_reset = manifest.get_archive_clean_offset()
                                          == model::offset{};
        bool archive_start_offset_reset = manifest.get_archive_start_offset()
                                          == model::offset{};
        bool deletes_sent = std::count_if(
                              get_targets().begin(),
                              get_targets().end(),
                              [](auto it) { return it.second.has_q_delete; })
                            == 2;
        return archive_clean_offset_reset && archive_start_offset_reset
               && deletes_sent;
    }).get();
    fut.get();

    BOOST_REQUIRE_EQUAL(
      part->archival_meta_stm()->manifest().get_spillover_map().size(), 0);

    ss::sstring delete_payloads;
    for (const auto& [url, req] : get_targets()) {
        if (req.has_q_delete) {
            delete_payloads += req.content;
        }
    }

    for (auto [url, req] : get_targets()) {
        vlog(test_log.info, "{} {}", req.method, req.url);
    }

    // Validate the contents of plural delete requests
    vlog(test_log.info, "Delete payloads: {}", delete_payloads);
    for (const auto& [url, deletion_expected] : segment_urls) {
        auto urlstr = url().string();
        auto expected_delete_paths = {urlstr, urlstr + ".index"};
        for (const auto& p : expected_delete_paths) {
            bool deleted = delete_payloads.find(p) != ss::sstring::npos;
            vlog(test_log.info, "Object {} deleted={}", p, deleted);
            BOOST_REQUIRE_EQUAL(deleted, deletion_expected);
        }
    }

    bool spill_manifest_deleted = delete_payloads.find(spill_path().string())
                                  != ss::sstring::npos;
    BOOST_REQUIRE_EQUAL(true, spill_manifest_deleted);
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
       .num_records = 1000,
       .timestamp = old_stamp},
      {.ntp = manifest_ntp,
       .base_offset = model::offset(1000),
       .term = model::term_id(2),
       .num_records = 1000,
       .timestamp = old_stamp},
      {.ntp = manifest_ntp,
       .base_offset = model::offset(2000),
       .term = model::term_id(3),
       .num_records = 1000,
       .timestamp = old_stamp},
      {
        .ntp = manifest_ntp,
        .base_offset = model::offset(3000),
        .term = model::term_id(4),
        .num_records = 1000,
      }};

    init_storage_api_local(segments);

    wait_for_partition_leadership(manifest_ntp);

    auto part = app.partition_manager.local().get(manifest_ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [part]() mutable {
        return part->last_stable_offset() >= model::offset(3000);
    }).get();

    listen();
    config::shard_local_cfg().log_retention_ms.set_value(
      std::chrono::milliseconds{1min});
    auto reset_cfg = ss::defer(
      [] { config::shard_local_cfg().log_retention_ms.reset(); });

    config::shard_local_cfg()
      .cloud_storage_max_segments_pending_deletion_per_partition.set_value(2);

    auto [arch_conf, remote_conf] = get_configurations();
    auto amv = ss::make_shared<cloud_storage::async_manifest_view>(
      remote,
      app.shadow_index_cache,
      part->archival_meta_stm()->manifest(),
      arch_conf->bucket_name,
      path_provider);
    archival::ntp_archiver archiver(
      get_ntp_conf(),
      arch_conf,
      remote.local(),
      app.shadow_index_cache.local(),
      *part,
      amv);
    auto action = ss::defer([&archiver] { archiver.stop().get(); });

    auto res = upload_next_with_retries(archiver).get();
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

    // Starting offset is lower than offset1
    auto upload1 = require_upload_candidate(policy
                                              .get_next_candidate(
                                                model::offset(0),
                                                lso,
                                                std::nullopt,
                                                log,
                                                segment_read_lock_timeout)
                                              .get())
                     .candidate;
    log_upload_candidate(upload1);
    BOOST_REQUIRE(!upload1.sources.empty());
    BOOST_REQUIRE(upload1.starting_offset == offset1);

    model::offset start_offset;

    start_offset = upload1.sources.front()->offsets().get_dirty_offset()
                   + model::offset(1);
    auto upload2
      = require_upload_candidate(
          policy
            .get_next_candidate(
              start_offset, lso, std::nullopt, log, segment_read_lock_timeout)
            .get())
          .candidate;
    log_upload_candidate(upload2);
    BOOST_REQUIRE(!upload2.sources.empty());
    BOOST_REQUIRE(upload2.starting_offset() == offset2);
    BOOST_REQUIRE(upload2.exposed_name != upload1.exposed_name);
    BOOST_REQUIRE(upload2.sources.front() != upload1.sources.front());
    BOOST_REQUIRE(
      upload2.sources.front()->offsets().get_base_offset() == offset2);

    start_offset = upload2.sources.front()->offsets().get_dirty_offset()
                   + model::offset(1);
    auto upload3
      = require_upload_candidate(
          policy
            .get_next_candidate(
              start_offset, lso, std::nullopt, log, segment_read_lock_timeout)
            .get())
          .candidate;
    log_upload_candidate(upload3);
    BOOST_REQUIRE(!upload3.sources.empty());
    BOOST_REQUIRE(upload3.starting_offset() == offset3);
    BOOST_REQUIRE(upload3.exposed_name != upload2.exposed_name);
    BOOST_REQUIRE(upload3.sources.front() != upload2.sources.front());
    BOOST_REQUIRE(
      upload3.sources.front()->offsets().get_base_offset() == offset3);

    start_offset = upload3.sources.front()->offsets().get_dirty_offset()
                   + model::offset(1);
    require_candidate_creation_error(
      policy
        .get_next_candidate(
          start_offset, lso, std::nullopt, log, segment_read_lock_timeout)
        .get(),
      candidate_creation_error::no_segment_for_begin_offset);
    require_candidate_creation_error(
      policy
        .get_next_candidate(
          lso + model::offset(1),
          lso,
          std::nullopt,
          log,
          segment_read_lock_timeout)
        .get(),
      candidate_creation_error::no_segment_for_begin_offset);
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
      ->housekeeping(storage::housekeeping_config(
        model::timestamp::now(),
        std::nullopt,
        model::offset{999},
        std::nullopt,
        ss::default_priority_class(),
        as))
      .get();

    auto seg = log->segments().begin();

    BOOST_REQUIRE((*seg)->finished_self_compaction());

    auto partition = app.partition_manager.local().get(manifest_ntp);
    BOOST_REQUIRE(partition);

    auto candidate = require_upload_candidate(
                       archival::archival_policy{manifest_ntp}
                         .get_next_candidate(
                           model::offset(0),
                           lso,
                           std::nullopt,
                           log,
                           segment_read_lock_timeout)
                         .get())
                       .candidate;

    // The search is expected to find first compacted segment
    BOOST_REQUIRE(!candidate.sources.empty());
    BOOST_REQUIRE_EQUAL(candidate.starting_offset(), 0);
    BOOST_REQUIRE_EQUAL(
      candidate.sources.front()->offsets().get_base_offset(), model::offset{0});
}

// NOLINTNEXTLINE
SEASTAR_THREAD_TEST_CASE(test_archival_policy_timeboxed_uploads) {
    storage::disk_log_builder b(
      storage::log_builder_config(),
      model::offset_translator_batch_types(),
      raft::group_id{0});
    b | storage::start(manifest_ntp);

    archival::archival_policy policy(manifest_ntp, segment_time_limit{0s});

    auto log = b.get_log();

    // Must initialize translator state.
    log->start(std::nullopt).get();

    // first offset that is not yet uploaded
    auto start_offset = model::offset{0};

    auto get_next_upload = [&]() {
        auto last_stable_offset = log->offsets().dirty_offset
                                  + model::offset{1};
        auto ret = require_upload_candidate(policy
                                              .get_next_candidate(
                                                start_offset,
                                                last_stable_offset,
                                                std::nullopt,
                                                log,
                                                segment_read_lock_timeout)
                                              .get())
                     .candidate;
        start_offset = ret.final_offset + model::offset{1};
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
    BOOST_REQUIRE_EQUAL(log->offsets().dirty_offset, model::offset{13});

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
    BOOST_REQUIRE_EQUAL(log->offsets().dirty_offset, model::offset{14});

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
    BOOST_REQUIRE_EQUAL(log->offsets().dirty_offset, model::offset{16});

    // should skip uploading because there are no data batches to upload
    {
        require_candidate_creation_error(
          policy
            .get_next_candidate(
              start_offset,
              log->offsets().dirty_offset + model::offset{1},
              std::nullopt,
              log,
              segment_read_lock_timeout)
            .get(),
          candidate_creation_error::no_segment_for_begin_offset);
    }

    // data[17-17]
    b | storage::add_random_batch(model::offset{17}, 1);
    BOOST_REQUIRE_EQUAL(log->offsets().dirty_offset, model::offset{17});

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
    BOOST_REQUIRE_EQUAL(log->offsets().dirty_offset, model::offset{18});

    // should skip uploading because there are no data batches to upload
    {
        require_candidate_creation_error(
          policy
            .get_next_candidate(
              start_offset,
              log->offsets().dirty_offset + model::offset{1},
              std::nullopt,
              log,
              segment_read_lock_timeout)
            .get(),
          candidate_creation_error::no_segment_for_begin_offset);
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
      {.ntp = manifest_ntp,
       .base_offset = model::offset(0),
       .term = model::term_id(1),
       .num_records = 1000},
      {.ntp = manifest_ntp,
       .base_offset = model::offset(1000),
       .term = model::term_id(4),
       .num_records = 1000},
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
      .committed_offset = segment1->offsets().get_committed_offset()
                          - model::offset(10),
      .segment_term = model::term_id{2},
      .delta_offset_end = model::offset_delta(0),
    };
    auto oldname = archival::segment_name("2-2-v1.log");
    old_manifest.add(oldname, old_meta);
    ss::sstring segment3_url = "/dfee62b1/kafka/test-topic/42_0/2-2-v1.log";

    // Simulate pre-existing state in the snapshot
    std::vector<cloud_storage::segment_meta> old_segments;
    for (const auto& s : old_manifest) {
        old_segments.push_back(s);
    }
    part->archival_meta_stm()
      ->add_segments(
        old_segments,
        std::nullopt,
        model::producer_id{},
        ss::lowres_clock::now() + 1s,
        never_abort,
        cluster::segment_validated::yes)
      .get();

    listen();

    auto [arch_conf, remote_conf] = get_configurations();

    auto amv = ss::make_shared<cloud_storage::async_manifest_view>(
      remote,
      app.shadow_index_cache,
      part->archival_meta_stm()->manifest(),
      arch_conf->bucket_name,
      path_provider);
    archival::ntp_archiver archiver(
      get_ntp_conf(),
      arch_conf,
      remote.local(),
      app.shadow_index_cache.local(),
      *part,
      amv);

    auto action = ss::defer([&archiver] { archiver.stop().get(); });

    retry_chain_node fib(never_abort);

    auto res = upload_next_with_retries(archiver).get();

    auto non_compacted_result = res.non_compacted_upload_result;
    auto compacted_result = res.compacted_upload_result;

    BOOST_REQUIRE_EQUAL(non_compacted_result.num_succeeded, 2);
    BOOST_REQUIRE_EQUAL(non_compacted_result.num_failed, 0);

    BOOST_REQUIRE_EQUAL(compacted_result.num_succeeded, 0);
    BOOST_REQUIRE_EQUAL(compacted_result.num_failed, 0);

    for (auto req : get_requests()) {
        vlog(test_log.info, "{} {}", req.method, req.url);
    }
    BOOST_REQUIRE_EQUAL(get_requests().size(), 5);

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
    ss::future<stop_parser> consume_batch_end() override {
        co_return stop_parser::no;
    }
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
      .ntp_revision = manifest.get_revision_id(),
      .delta_offset_end = model::offset_delta{0}};

    manifest.add(s1name, segment_meta);
    std::vector<cloud_storage::segment_meta> all_segments;
    for (const auto& s : manifest) {
        all_segments.push_back(s);
    }
    part->archival_meta_stm()
      ->add_segments(
        all_segments,
        std::nullopt,
        model::producer_id{},
        ss::lowres_clock::now() + 1s,
        never_abort,
        cluster::segment_validated::yes)
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

    auto amv = ss::make_shared<cloud_storage::async_manifest_view>(
      test.remote,
      test.app.shadow_index_cache,
      part->archival_meta_stm()->manifest(),
      aconf->bucket_name,
      path_provider);

    archival::ntp_archiver archiver(
      get_ntp_conf(),
      aconf,
      test.remote.local(),
      test.app.shadow_index_cache.local(),
      *part,
      amv);

    retry_chain_node fib(never_abort);
    test.listen();
    auto res = test.upload_next_with_retries(archiver, lso).get();

    auto non_compacted_result = res.non_compacted_upload_result;
    auto compacted_result = res.compacted_upload_result;

    BOOST_REQUIRE_EQUAL(non_compacted_result.num_succeeded, 1);
    BOOST_REQUIRE_EQUAL(non_compacted_result.num_failed, 0);

    BOOST_REQUIRE_EQUAL(compacted_result.num_succeeded, 0);
    BOOST_REQUIRE_EQUAL(compacted_result.num_failed, 0);

    test.log_requests();
    BOOST_REQUIRE_EQUAL(test.get_requests().size(), 3);

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
    res = test.upload_next_with_retries(archiver, lso).get();

    non_compacted_result = res.non_compacted_upload_result;
    compacted_result = res.compacted_upload_result;

    BOOST_REQUIRE_EQUAL(non_compacted_result.num_succeeded, 1);
    BOOST_REQUIRE_EQUAL(non_compacted_result.num_failed, 0);

    BOOST_REQUIRE_EQUAL(compacted_result.num_succeeded, 0);
    BOOST_REQUIRE_EQUAL(compacted_result.num_failed, 0);

    test.log_requests();
    BOOST_REQUIRE_EQUAL(test.get_requests().size(), 6);
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
                    manifest = std::move(new_manifest);
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
      {manifest_ntp, offset1, model::term_id(1), 1000},
      {manifest_ntp, offset2, model::term_id(1), 1000},
      {manifest_ntp, offset3, model::term_id(1), 1000},
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
    tracker1.set_offset(storage::segment::offset_tracker::dirty_offset_t{
      offset2 - model::offset(1)});
    auto segment2 = get_segment(
      manifest_ntp, archival::segment_name("1000-1-v1.log"));
    auto& tracker2 = const_cast<storage::segment::offset_tracker&>(
      segment2->offsets());
    tracker2.set_offset(storage::segment::offset_tracker::dirty_offset_t{
      offset3 - model::offset(1)});

    // Every segment should be returned once as we're calling the
    // policy to get next candidate.
    log_segment_set(lm);

    auto log = lm.get(manifest_ntp);
    BOOST_REQUIRE(log);

    auto partition = app.partition_manager.local().get(manifest_ntp);
    BOOST_REQUIRE(partition);

    model::offset start_offset{0};
    model::offset lso{9999};
    // Starting offset is lower than offset1
    auto upload1
      = require_upload_candidate(
          policy
            .get_next_candidate(
              start_offset, lso, std::nullopt, log, segment_read_lock_timeout)
            .get())
          .candidate;
    log_upload_candidate(upload1);
    BOOST_REQUIRE(!upload1.sources.empty());
    BOOST_REQUIRE(upload1.starting_offset == offset1);

    start_offset = upload1.sources.front()->offsets().get_dirty_offset()
                   + model::offset(1);
    auto upload2
      = require_upload_candidate(
          policy
            .get_next_candidate(
              start_offset, lso, std::nullopt, log, segment_read_lock_timeout)
            .get())
          .candidate;
    log_upload_candidate(upload2);
    BOOST_REQUIRE(!upload2.sources.empty());
    BOOST_REQUIRE(upload2.starting_offset == offset2);
    BOOST_REQUIRE(upload2.exposed_name != upload1.exposed_name);
    BOOST_REQUIRE(upload2.sources.front() != upload1.sources.front());
    BOOST_REQUIRE(
      upload2.sources.front()->offsets().get_base_offset() == offset2);

    start_offset = upload2.sources.front()->offsets().get_dirty_offset()
                   + model::offset(1);
    auto upload3
      = require_upload_candidate(
          policy
            .get_next_candidate(
              start_offset, lso, std::nullopt, log, segment_read_lock_timeout)
            .get())
          .candidate;
    log_upload_candidate(upload3);
    BOOST_REQUIRE(!upload3.sources.empty());
    BOOST_REQUIRE(upload3.starting_offset == offset3);
    BOOST_REQUIRE(upload3.exposed_name != upload2.exposed_name);
    BOOST_REQUIRE(upload3.sources.front() != upload2.sources.front());
    BOOST_REQUIRE(
      upload3.sources.front()->offsets().get_base_offset() == offset3);

    start_offset = upload3.sources.front()->offsets().get_dirty_offset()
                   + model::offset(1);
    require_candidate_creation_error(
      policy
        .get_next_candidate(
          start_offset, lso, std::nullopt, log, segment_read_lock_timeout)
        .get(),
      candidate_creation_error::no_segment_for_begin_offset);
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
    cloud_storage::partition_manifest pm(manifest_ntp, manifest_revision);
    archival::adjacent_segment_run run(manifest_ntp);
    for (const auto& s : segments) {
        if (run.maybe_add_segment(pm, s, high_watermark, path_provider)) {
            break;
        }
    }
    BOOST_REQUIRE(run.meta.base_offset == model::offset{11});
    BOOST_REQUIRE(run.meta.committed_offset == model::offset{21});
    BOOST_REQUIRE(run.num_segments == 3);
    BOOST_REQUIRE(run.meta.size_bytes == 100);
}

static void test_manifest_spillover_impl(
  archiver_fixture& test,
  size_t spillover_manifest_size,
  size_t start_manifest_size) {
    // Add segments until spillover condition will be met and check that
    // spillover actually triggered.
    const int64_t rec_per_segment = 1;
    cloud_storage::partition_manifest manifest(manifest_ntp, manifest_revision);

    int i = 0;
    while (manifest.segments_metadata_bytes() < start_manifest_size) {
        auto bo = model::offset(i * rec_per_segment);
        auto co = model::offset(i * rec_per_segment + rec_per_segment - 1);
        auto delta = model::offset_delta(0);
        auto delta_end = model::offset_delta(0);
        cloud_storage::partition_manifest::segment_meta segment_meta{
          .is_compacted = false,
          .size_bytes = 1, // doesn't matter
          .base_offset = bo,
          .committed_offset = co,
          .base_timestamp = model::timestamp(i),
          .max_timestamp = model::timestamp(i),
          .delta_offset = delta,
          .ntp_revision = manifest.get_revision_id(),
          .archiver_term = model::term_id(1),
          .segment_term = model::term_id(1),
          .delta_offset_end = delta_end,
        };
        manifest.add(segment_meta);
        i++;
    }
    vlog(
      test_log.info,
      "Generated manifest with {} segments, manifest storage size: {}",
      manifest.size(),
      manifest.segments_metadata_bytes());
    test.init_storage_api_local({
      {manifest_ntp, model::offset(0), model::term_id(1)},
    });
    test.wait_for_partition_leadership(manifest_ntp);
    test.listen();

    auto part = test.app.partition_manager.local().get(manifest_ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [part]() mutable {
        return part->last_stable_offset() >= model::offset(0);
    }).get();

    // Generate new manifest based on data layout on disk
    vlog(test_log.debug, "stm add segments");
    auto add_segments =
      [&part](std::vector<cloud_storage::segment_meta> segments) {
          part->archival_meta_stm()
            ->add_segments(
              std::move(segments),
              std::nullopt,
              model::producer_id{},
              ss::lowres_clock::now() + 1s,
              never_abort,
              cluster::segment_validated::yes)
            .get();
      };

    // 10 at a time to avoid reactor stalls.
    const int max_batch_size = 10;
    std::vector<cloud_storage::segment_meta> batch;
    for (const auto& s : manifest) {
        if (batch.size() >= max_batch_size) {
            add_segments(batch);
            batch.clear();
        }
        batch.push_back(s);
    }
    add_segments(batch);

    vlog(test_log.debug, "stm add segments completed");

    vlog(
      test_log.debug,
      "archival metadata stm last offset: {}",
      part->archival_meta_stm()->get_last_offset());

    auto [aconf, cconf] = test.get_configurations();

    aconf->time_limit = segment_time_limit(0s);

    auto amv = ss::make_shared<cloud_storage::async_manifest_view>(
      test.remote,
      test.app.shadow_index_cache,
      part->archival_meta_stm()->manifest(),
      aconf->bucket_name,
      path_provider);

    archival::ntp_archiver archiver(
      get_ntp_conf(),
      aconf,
      test.remote.local(),
      test.app.shadow_index_cache.local(),
      *part,
      amv);

    auto stop_archiver = ss::defer([&archiver] { archiver.stop().get(); });

    // Do not start archiver and run spillover manually.
    vlog(
      test_log.debug,
      "apply spillover, spillover manifest size: {}",
      spillover_manifest_size);
    config::shard_local_cfg().cloud_storage_spillover_manifest_size.set_value(
      std::make_optional(static_cast<size_t>(spillover_manifest_size)));
    auto reset_cfg = ss::defer([] {
        config::shard_local_cfg().cloud_storage_spillover_manifest_size.reset();
    });

    archiver.apply_spillover().get();

    const auto& stm_manifest = part->archival_meta_stm()->manifest();
    auto new_so = stm_manifest.get_start_offset();
    auto new_kafka = stm_manifest.get_start_kafka_offset();
    auto archive_so = stm_manifest.get_archive_start_offset();
    auto archive_kafka = stm_manifest.get_archive_start_kafka_offset();
    auto archive_clean = stm_manifest.get_archive_clean_offset();

    vlog(
      test_log.info,
      "new_so: {}, new_kafka: {}, archive_so: {}, archive_kafka: {}, "
      "archive_clean: {}",
      new_so,
      new_kafka,
      archive_so,
      archive_kafka,
      archive_clean);

    // Validate uploaded spillover manifest
    vlog(test_log.info, "Reconciling storage bucket");
    std::map<model::offset, cloud_storage::partition_manifest> uploaded;
    for (const auto& [key, req] : test.get_targets()) {
        vlog(test_log.info, "Found {}", key);
        BOOST_REQUIRE_EQUAL(req.method, "PUT");
        cloud_storage::partition_manifest spm(manifest_ntp, manifest_revision);
        iobuf sbuf;
        sbuf.append(req.content.data(), req.content_length);
        auto sstr = make_iobuf_input_stream(std::move(sbuf));
        spm.update(std::move(sstr)).get();
        auto spm_so = spm.get_start_offset().value_or(model::offset{});
        uploaded.insert(std::make_pair(spm_so, std::move(spm)));
    }

    BOOST_REQUIRE(uploaded.size() != 0);
    const auto& last = uploaded.rbegin()->second;
    const auto& first = uploaded.begin()->second;

    BOOST_REQUIRE(model::next_offset(last.get_last_offset()) == new_so);
    BOOST_REQUIRE(first.get_start_offset().value() == archive_so);
    BOOST_REQUIRE(first.get_start_kafka_offset().value() == archive_kafka);

    model::offset expected_so = archive_so;
    for (const auto& [key, m] : uploaded) {
        std::ignore = key;
        BOOST_REQUIRE(m.get_start_offset().value() == expected_so);
        expected_so = model::next_offset(m.get_last_offset());
    }
}

// NOLINTNEXTLINE
FIXTURE_TEST(test_manifest_spillover, archiver_fixture) {
    test_manifest_spillover_impl(*this, 0x1000, 0x3000);
}

// NOLINTNEXTLINE
FIXTURE_TEST(test_upload_with_gap_blocked, archiver_fixture) {
    std::vector<segment_desc> segments = {
      {.ntp = manifest_ntp,
       .base_offset = model::offset(0),
       .term = model::term_id(1),
       .num_records = 900},
      {.ntp = manifest_ntp,
       .base_offset = model::offset(1000),
       .term = model::term_id(4),
       .num_records = 1000},
    };

    init_storage_api_local(segments);
    wait_for_partition_leadership(manifest_ntp);

    auto part = app.partition_manager.local().get(manifest_ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [part]() mutable {
        return part->last_stable_offset() >= model::offset(1000);
    }).get();

    vlog(
      test_log.info,
      "Partition is a leader, high-watermark: {}, partition: {}",
      part->high_watermark(),
      *part);

    listen();

    auto [arch_conf, remote_conf] = get_configurations();

    auto manifest_view = ss::make_shared<cloud_storage::async_manifest_view>(
      remote,
      app.shadow_index_cache,
      part->archival_meta_stm()->manifest(),
      arch_conf->bucket_name,
      path_provider);

    archival::ntp_archiver archiver(
      get_ntp_conf(),
      arch_conf,
      remote.local(),
      app.shadow_index_cache.local(),
      *part,
      manifest_view);

    auto action = ss::defer([&archiver, &manifest_view] {
        archiver.stop().get();
        manifest_view->stop().get();
    });

    retry_chain_node fib(never_abort);
    auto res = upload_next_with_retries(archiver).get();

    for (auto [url, req] : get_targets()) {
        vlog(test_log.info, "{} {}", req.method, req.url);
    }

    // The archiver will upload both segments successfully but will be
    // able to add to the manifest only the first one.
    BOOST_REQUIRE_EQUAL(res.non_compacted_upload_result.num_succeeded, 2);
    BOOST_REQUIRE_EQUAL(res.non_compacted_upload_result.num_failed, 0);
    BOOST_REQUIRE_EQUAL(res.non_compacted_upload_result.num_cancelled, 0);
    BOOST_REQUIRE_EQUAL(res.compacted_upload_result.num_succeeded, 0);
    BOOST_REQUIRE_EQUAL(res.compacted_upload_result.num_failed, 0);
    BOOST_REQUIRE_EQUAL(res.compacted_upload_result.num_cancelled, 0);

    BOOST_REQUIRE_EQUAL(get_requests().size(), 5);

    cloud_storage::partition_manifest manifest;
    {
        BOOST_REQUIRE(get_targets().count(manifest_url)); // NOLINT
        auto req_opt = get_latest_request(manifest_url);
        BOOST_REQUIRE(req_opt.has_value());
        auto req = req_opt.value().get();
        BOOST_REQUIRE_EQUAL(req.method, "PUT"); // NOLINT
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

        auto index_url = get_segment_index_path(manifest, segment1_name);
        auto index_req_maybe = get_latest_request("/" + index_url().string());
        BOOST_REQUIRE(index_req_maybe.has_value());
        auto index_req = index_req_maybe.value().get();
        BOOST_REQUIRE_EQUAL(index_req.method, "PUT");
        verify_index(manifest_ntp, segment1_name, manifest, index_req.content);
    }

    // The stm manifest should have only the first segment
    BOOST_REQUIRE(part->archival_meta_stm());
    const auto& stm_manifest = part->archival_meta_stm()->manifest();
    BOOST_REQUIRE_EQUAL(stm_manifest.size(), 1);
    BOOST_REQUIRE_EQUAL(
      stm_manifest.last_segment()->base_offset, segments[0].base_offset);
}

FIXTURE_TEST(test_flush_not_leader, cloud_storage_manual_multinode_test_base) {
    // start a second fixture and wait for stable setup
    auto fx2 = start_second_fixture();
    RPTEST_REQUIRE_EVENTUALLY(3s, [&] {
        return app.controller->get_members_table().local().node_ids().size()
               == 2;
    });

    // Create topic
    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);
    cluster::topic_properties props;
    props.shadow_indexing = model::shadow_indexing_mode::full;
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion;
    props.segment_size = 64_KiB;
    props.retention_local_target_bytes = tristate<size_t>(1);
    add_topic({model::kafka_namespace, topic_name}, 1, props, 2).get();

    // Figure out which fixture is the follower
    redpanda_thread_fixture* fx_follower = nullptr;
    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        cluster::partition* prt_a
          = app.partition_manager.local().get(ntp).get();
        cluster::partition* prt_b
          = fx2->app.partition_manager.local().get(ntp).get();
        if (!prt_a || !prt_b) {
            return false;
        }
        if (!prt_a->is_leader()) {
            fx_follower = this;
            return true;
        }
        if (!prt_b->is_leader()) {
            fx_follower = fx2.get();
            return true;
        }
        return false;
    });

    BOOST_REQUIRE(fx_follower != nullptr);

    // Get the follower partition
    cluster::partition* prt_follower
      = fx_follower->app.partition_manager.local().get(ntp).get();

    auto archiver_opt = prt_follower->archiver();
    BOOST_REQUIRE(archiver_opt.has_value());

    auto& archiver = archiver_opt.value().get();

    // Expect that a flush() call to the follower partition's archiver is
    // rejected
    auto flush_res = archiver.flush();
    BOOST_REQUIRE(!flush_res.offset.has_value());
    BOOST_REQUIRE_EQUAL(flush_res.response, flush_response::rejected);
}

FIXTURE_TEST(test_flush_leader, cloud_storage_manual_multinode_test_base) {
    // start a second fixture and wait for stable setup
    auto fx2 = start_second_fixture();
    RPTEST_REQUIRE_EVENTUALLY(3s, [&] {
        return app.controller->get_members_table().local().node_ids().size()
               == 2;
    });

    // Create topic
    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);
    cluster::topic_properties props;
    props.shadow_indexing = model::shadow_indexing_mode::full;
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion;
    props.segment_size = 64_KiB;
    props.retention_local_target_bytes = tristate<size_t>(1);
    add_topic({model::kafka_namespace, topic_name}, 1, props, 2).get();

    // Figure out which fixture is the leader
    redpanda_thread_fixture* fx_leader = nullptr;
    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        cluster::partition* prt_a
          = app.partition_manager.local().get(ntp).get();
        cluster::partition* prt_b
          = fx2->app.partition_manager.local().get(ntp).get();
        if (!prt_a || !prt_b) {
            return false;
        }
        if (prt_a->is_leader()) {
            fx_leader = this;
            return true;
        }
        if (prt_b->is_leader()) {
            fx_leader = fx2.get();
            return true;
        }
        return false;
    });

    BOOST_REQUIRE(fx_leader != nullptr);

    // Get the leader partition
    cluster::partition* prt_leader
      = fx_leader->app.partition_manager.local().get(ntp).get();

    auto archiver_opt = prt_leader->archiver();
    BOOST_REQUIRE(archiver_opt.has_value());

    auto& archiver = archiver_opt.value().get();

    // Expect that a flush() call to the leader partition's archiver is accepted
    auto flush_res = archiver.flush();
    BOOST_REQUIRE_EQUAL(flush_res.response, flush_response::accepted);
    BOOST_REQUIRE(flush_res.offset.has_value());
}

FIXTURE_TEST(test_flush_wait_out_of_bounds, archiver_fixture) {
    const auto num_segments = 2;
    const auto term_id = 1;
    const auto num_records = 100;
    std::vector<segment_desc> segments = {};
    segments.reserve(num_segments);
    for (int i = 0; i < num_segments; ++i) {
        segments.emplace_back(
          manifest_ntp,
          model::offset(i * num_records),
          model::term_id(term_id),
          num_records);
    }

    const auto very_clearly_out_of_bounds_offset = model::offset{
      num_segments * num_records * 100};

    init_storage_api_local(segments);
    vlog(test_log.info, "Initialized, start waiting for partition leadership");

    wait_for_partition_leadership(manifest_ntp);
    auto part = app.partition_manager.local().get(manifest_ntp);

    listen();
    auto [arch_conf, remote_conf] = get_configurations();
    auto amv = ss::make_shared<cloud_storage::async_manifest_view>(
      remote,
      app.shadow_index_cache,
      part->archival_meta_stm()->manifest(),
      arch_conf->bucket_name,
      path_provider);
    archival::ntp_archiver archiver(
      get_ntp_conf(),
      arch_conf,
      remote.local(),
      app.shadow_index_cache.local(),
      *part,
      amv);

    auto action = ss::defer([&archiver, &amv] {
        archiver.stop().get();
        amv->stop().get();
    });

    // Issue a flush request, syncing up with added segments doesn't matter for
    // this test.
    auto flush_res = archiver.flush();
    BOOST_REQUIRE_EQUAL(flush_res.response, flush_response::accepted);

    model::offset flush_offset = very_clearly_out_of_bounds_offset;
    auto wait_res = archiver.wait(flush_offset).get();

    // Out of bounds wait() request will return not_in_progress.
    BOOST_REQUIRE_EQUAL(wait_res, wait_result::not_in_progress);
}

FIXTURE_TEST(test_flush_wait_with_no_flush, archiver_fixture) {
    const auto num_segments = 2;
    const auto term_id = 1;
    const auto num_records = 100;
    std::vector<segment_desc> segments = {};
    segments.reserve(num_segments);
    for (int i = 0; i < num_segments; ++i) {
        segments.emplace_back(
          manifest_ntp,
          model::offset(i * num_records),
          model::term_id(term_id),
          num_records);
    }

    init_storage_api_local(segments);
    vlog(test_log.info, "Initialized, start waiting for partition leadership");

    wait_for_partition_leadership(manifest_ntp);
    auto part = app.partition_manager.local().get(manifest_ntp);

    listen();
    auto [arch_conf, remote_conf] = get_configurations();
    auto amv = ss::make_shared<cloud_storage::async_manifest_view>(
      remote,
      app.shadow_index_cache,
      part->archival_meta_stm()->manifest(),
      arch_conf->bucket_name,
      path_provider);
    archival::ntp_archiver archiver(
      get_ntp_conf(),
      arch_conf,
      remote.local(),
      app.shadow_index_cache.local(),
      *part,
      amv);

    auto action = ss::defer([&archiver, &amv] {
        archiver.stop().get();
        amv->stop().get();
    });

    model::offset flush_offset = model::offset{1};
    auto wait_res = archiver.wait(flush_offset).get();
    BOOST_REQUIRE_EQUAL(wait_res, wait_result::not_in_progress);
}

FIXTURE_TEST(test_flush_wait_with_flush, archiver_fixture) {
    scoped_config test_local_cfg;
    test_local_cfg.get("cloud_storage_disable_upload_loop_for_tests")
      .set_value(true);

    const auto num_segments = 10;
    const auto term_id = 1;
    const auto num_records = 100;
    const auto last_offset = model::offset{num_segments * num_records};
    std::vector<segment_desc> segments = {};
    segments.reserve(num_segments);
    for (int i = 0; i < num_segments; ++i) {
        segments.emplace_back(
          manifest_ntp,
          model::offset(i * num_records),
          model::term_id(term_id),
          num_records);
    }
    init_storage_api_local(segments);
    vlog(test_log.info, "Initialized, start waiting for partition leadership");

    wait_for_partition_leadership(manifest_ntp);
    auto part = app.partition_manager.local().get(manifest_ntp);

    listen();
    auto [arch_conf, remote_conf] = get_configurations();
    auto amv = ss::make_shared<cloud_storage::async_manifest_view>(
      remote,
      app.shadow_index_cache,
      part->archival_meta_stm()->manifest(),
      arch_conf->bucket_name,
      path_provider);
    archival::ntp_archiver archiver(
      get_ntp_conf(),
      arch_conf,
      remote.local(),
      app.shadow_index_cache.local(),
      *part,
      amv);

    auto action = ss::defer([&archiver, &amv] {
        archiver.stop().get();
        amv->stop().get();
    });

    // Flush once max_uploadable_offset_exclusive() reflects added segments,
    // and assert the request is accepted.
    model::offset flush_offset;
    RPTEST_REQUIRE_EVENTUALLY(3s, [&] {
        auto max_uploadable_offset = archiver.max_uploadable_offset_exclusive();
        if (max_uploadable_offset > model::offset{0}) {
            auto flush_res = archiver.flush();
            flush_offset = flush_res.offset.value();
            return flush_res.response == flush_response::accepted
                   && flush_offset == last_offset;
        }
        return false;
    });

    // Create a waiter.
    auto wait_res = archiver.wait(flush_offset);

    // Start the upload loop.
    auto upload_future = upload_until_term_change(archiver);

    // Assert the flush eventually completes.
    BOOST_REQUIRE(wait_res.get() == wait_result::complete);

    // Check that last offset of the manifest is equal to or past the last
    // segment offset.
    const auto& manifest = archiver.manifest();
    BOOST_REQUIRE(manifest.get_last_offset() >= last_offset);

    // Now we can force a step down to stop upload loop.
    part->raft()->step_down("forced stepdown").get();
    upload_future.get();

    auto http_requests = get_requests(is_partition_manifest_upload_req);
    BOOST_REQUIRE(!http_requests.empty());
}

FIXTURE_TEST(test_flush_wait_with_flush_multiple_waiters, archiver_fixture) {
    const auto num_segments = 10;
    const auto term_id = 1;
    const auto num_records = 100;
    const auto last_offset = model::offset{num_segments * num_records};
    std::vector<segment_desc> segments = {};
    segments.reserve(num_segments);
    for (int i = 0; i < num_segments; ++i) {
        segments.emplace_back(
          manifest_ntp,
          model::offset(i * num_records),
          model::term_id(term_id),
          num_records);
    }
    init_storage_api_local(segments);
    vlog(test_log.info, "Initialized, start waiting for partition leadership");

    wait_for_partition_leadership(manifest_ntp);
    auto part = app.partition_manager.local().get(manifest_ntp);

    listen();
    auto [arch_conf, remote_conf] = get_configurations();
    auto amv = ss::make_shared<cloud_storage::async_manifest_view>(
      remote,
      app.shadow_index_cache,
      part->archival_meta_stm()->manifest(),
      arch_conf->bucket_name,
      path_provider);
    archival::ntp_archiver archiver(
      get_ntp_conf(),
      arch_conf,
      remote.local(),
      app.shadow_index_cache.local(),
      *part,
      amv);

    auto action = ss::defer([&archiver, &amv] {
        archiver.stop().get();
        amv->stop().get();
    });

    // Flush once max_uploadable_offset_exclusive() reflects added segments,
    // and assert the request is accepted.
    RPTEST_REQUIRE_EVENTUALLY(3s, [&] {
        auto max_uploadable_offset = archiver.max_uploadable_offset_exclusive();
        if (max_uploadable_offset > model::offset{0}) {
            auto flush_res = archiver.flush();
            return flush_res.response == flush_response::accepted
                   && flush_res.offset.value() == last_offset;
        }
        return false;
    });

    const auto num_waiters = 10;
    std::vector<ss::future<wait_result>> waiters;
    waiters.reserve(num_waiters);
    for (int i = 0; i < num_waiters; ++i) {
        auto wait_offset = model::offset{(i + 1) * last_offset / num_waiters};
        waiters.push_back(archiver.wait(wait_offset));
    }

    auto upload_future = upload_until_term_change(archiver);

    // Assert the flush eventually completes.
    RPTEST_REQUIRE_EVENTUALLY(3s, [&] {
        return std::all_of(
          waiters.begin(), waiters.end(), [](auto& wait_future) {
              return wait_future.get() == wait_result::complete;
          });
    });

    // Check that last offset of the manifest is equal to or past the last
    // segment offset.
    const auto& manifest = archiver.manifest();
    BOOST_REQUIRE(manifest.get_last_offset() >= last_offset);

    // Now we can force a step down to stop upload loop.
    part->raft()->step_down("forced stepdown").get();
    upload_future.get();

    auto http_requests = get_requests(is_partition_manifest_upload_req);
    BOOST_REQUIRE(!http_requests.empty());
}

FIXTURE_TEST(test_flush_with_leadership_change, archiver_fixture) {
    const auto num_segments = 10;
    const auto term_id = 1;
    const auto num_records = 100;
    const auto last_offset = model::offset{num_segments * num_records};
    std::vector<segment_desc> segments = {};
    segments.reserve(num_segments);
    for (int i = 0; i < num_segments; ++i) {
        segments.emplace_back(
          manifest_ntp,
          model::offset(i * num_records),
          model::term_id(term_id),
          num_records);
    }
    init_storage_api_local(segments);
    vlog(test_log.info, "Initialized, start waiting for partition leadership");

    wait_for_partition_leadership(manifest_ntp);
    auto part = app.partition_manager.local().get(manifest_ntp);

    listen();
    auto [arch_conf, remote_conf] = get_configurations();
    auto amv = ss::make_shared<cloud_storage::async_manifest_view>(
      remote,
      app.shadow_index_cache,
      part->archival_meta_stm()->manifest(),
      arch_conf->bucket_name,
      path_provider);
    archival::ntp_archiver archiver(
      get_ntp_conf(),
      arch_conf,
      remote.local(),
      app.shadow_index_cache.local(),
      *part,
      amv);

    auto action = ss::defer([&archiver, &amv] {
        archiver.stop().get();
        amv->stop().get();
    });

    // Flush once max_uploadable_offset_exclusive() reflects added segments,
    // and assert the request is accepted.
    model::offset flush_offset;
    RPTEST_REQUIRE_EVENTUALLY(3s, [&] {
        auto max_uploadable_offset = archiver.max_uploadable_offset_exclusive();
        if (max_uploadable_offset > model::offset{0}) {
            auto flush_res = archiver.flush();
            flush_offset = flush_res.offset.value();
            return flush_res.response == flush_response::accepted
                   && flush_offset == last_offset;
        }
        return false;
    });

    // Create a waiter.
    auto wait_res = archiver.wait(flush_offset);

    // Start the upload loop.
    auto upload_future = upload_until_term_change(archiver);

    // Now we can force a step down
    part->raft()->step_down("forced stepdown").get();

    // Manually signal to the condition variable, so that waiters can wake up
    // and notice the loss of leadership.
    broadcast_flush_condition_variable(archiver);

    // Assert that we lost leadership while waiting for a flush() to complete.
    BOOST_REQUIRE(wait_res.get() == wait_result::lost_leadership);

    upload_future.get();
}
