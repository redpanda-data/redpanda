/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/archival_policy.h"
#include "archival/ntp_archiver_service.h"
#include "archival/tests/service_fixture.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "storage/disk_log_impl.h"
#include "test_utils/fixture.h"
#include "utils/retry_chain_node.h"
#include "utils/unresolved_address.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>

#include <boost/algorithm/string.hpp>

using namespace std::chrono_literals;
using namespace archival;

inline ss::logger test_log("test"); // NOLINT

static constexpr std::string_view manifest_payload = R"json({
    "version": 1,
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 0,
    "last_offset": 1004,
    "segments": {
        "1-2-v1.log": {
            "is_compacted": false,
            "size_bytes": 100,
            "committed_offset": 2,
            "base_offset": 1
        },
        "1000-4-v1.log": {
            "is_compacted": false,
            "size_bytes": 200,
            "committed_offset": 1004,
            "base_offset": 3
        }
    }
})json";
static constexpr std::string_view manifest_with_deleted_segment = R"json({
    "version": 1,
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 0,
    "last_offset": 4,
    "segments": {
        "1000-4-v1.log": {
            "is_compacted": false,
            "size_bytes": 200,
            "committed_offset": 1004,
            "base_offset": 3
        }
    }
})json";

static const auto manifest_namespace = model::ns("test-ns");    // NOLINT
static const auto manifest_topic = model::topic("test-topic");  // NOLINT
static const auto manifest_partition = model::partition_id(42); // NOLINT
static const auto manifest_ntp = model::ntp(                    // NOLINT
  manifest_namespace,
  manifest_topic,
  manifest_partition);
static const auto manifest_revision = model::revision_id(0); // NOLINT
static const ss::sstring manifest_url = ssx::sformat(        // NOLINT
  "/20000000/meta/{}_{}/manifest.json",
  manifest_ntp.path(),
  manifest_revision());

// NOLINTNEXTLINE
static const ss::sstring segment1_url
  = "/ce4fd1a3/test-ns/test-topic/42_0/1-2-v1.log";
// NOLINTNEXTLINE
static const ss::sstring segment2_url
  = "/e622410d/test-ns/test-topic/42_0/1000-4-v1.log";

static const std::vector<s3_imposter_fixture::expectation>
  default_expectations({
    s3_imposter_fixture::expectation{
      .url = manifest_url, .body = ss::sstring(manifest_payload)},
    s3_imposter_fixture::expectation{.url = segment1_url, .body = "segment1"},
    s3_imposter_fixture::expectation{.url = segment2_url, .body = "segment2"},
  });

static storage::ntp_config get_ntp_conf() {
    return storage::ntp_config(manifest_ntp, "base-dir");
}

static cloud_storage::manifest load_manifest(std::string_view v) {
    cloud_storage::manifest m;
    iobuf i;
    i.append(v.data(), v.size());
    auto s = make_iobuf_input_stream(std::move(i));
    m.update(std::move(s)).get();
    return std::move(m);
}

/// Compare two json objects logically by parsing them first and then going
/// through fields
static bool
compare_json_objects(const std::string_view& lhs, const std::string_view& rhs) {
    using namespace rapidjson;
    Document lhsd, rhsd;
    lhsd.Parse({lhs.data(), lhs.size()});
    rhsd.Parse({rhs.data(), rhs.size()});
    if (lhsd != rhsd) {
        vlog(
          test_log.trace, "Json objects are not equal:\n{}and\n{}", lhs, rhs);
    }
    return lhsd == rhsd;
}

static void log_segment(const storage::segment& s) {
    vlog(
      test_log.info,
      "Log segment {}. Offsets: {} {}. Is compacted: {}. Is sealed: {}.",
      s.reader().filename(),
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
    vlog(
      test_log.info,
      "Upload candidate, exposed name: {} "
      "real offsets: {} {}",
      up.exposed_name,
      up.source->offsets().base_offset,
      up.source->offsets().dirty_offset);
}

FIXTURE_TEST(test_download_manifest, s3_imposter_fixture) { // NOLINT
    set_expectations_and_listen(default_expectations);
    auto conf = get_configuration();
    service_probe probe(service_metrics_disabled::yes);
    cloud_storage::remote remote(
      conf.connection_limit, conf.client_config, probe);
    archival::ntp_archiver archiver(
      get_ntp_conf(), get_configuration(), remote, probe);
    auto action = ss::defer([&archiver] { archiver.stop().get(); });
    retry_chain_node fib;
    auto res = archiver.download_manifest(fib).get0();
    BOOST_REQUIRE(res == cloud_storage::download_result::success);
    auto expected = load_manifest(manifest_payload);
    BOOST_REQUIRE(expected == archiver.get_remote_manifest()); // NOLINT
}

FIXTURE_TEST(test_upload_manifest, s3_imposter_fixture) { // NOLINT
    set_expectations_and_listen(default_expectations);
    auto conf = get_configuration();
    service_probe probe(service_metrics_disabled::yes);
    cloud_storage::remote remote(
      conf.connection_limit, conf.client_config, probe);
    archival::ntp_archiver archiver(
      get_ntp_conf(), get_configuration(), remote, probe);
    auto action = ss::defer([&archiver] { archiver.stop().get(); });
    auto pm = const_cast<cloud_storage::manifest*>( // NOLINT
      &archiver.get_remote_manifest());
    pm->add(
      segment_name("1-2-v1.log"),
      {
        .is_compacted = false,
        .size_bytes = 100, // NOLINT
        .base_offset = model::offset(1),
        .committed_offset = model::offset(2),
      });
    pm->add(
      segment_name("1000-4-v1.log"),
      {
        .is_compacted = false,
        .size_bytes = 200, // NOLINT
        .base_offset = model::offset(3),
        .committed_offset = model::offset(1004),
      });
    retry_chain_node fib;
    auto res = archiver.upload_manifest(fib).get0();
    BOOST_REQUIRE(res == cloud_storage::upload_result::success);
    auto req = get_requests().front();
    // NOLINTNEXTLINE
    BOOST_REQUIRE(compare_json_objects(req.content, manifest_payload));
}

// NOLINTNEXTLINE
FIXTURE_TEST(test_upload_segments, archiver_fixture) {
    set_expectations_and_listen(default_expectations);
    auto conf = get_configuration();
    service_probe probe(service_metrics_disabled::yes);
    cloud_storage::remote remote(
      conf.connection_limit, conf.client_config, probe);
    archival::ntp_archiver archiver(
      get_ntp_conf(), get_configuration(), remote, probe);
    auto action = ss::defer([&archiver] { archiver.stop().get(); });

    model::offset high_watermark = model::offset::max();
    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(1), model::term_id(2)},
      {manifest_ntp, model::offset(1000), model::term_id(4)},
    };
    init_storage_api_local(segments);

    retry_chain_node fib;
    auto res = archiver
                 .upload_next_candidates(
                   get_local_storage_api().log_mgr(), high_watermark, fib)
                 .get0();
    BOOST_REQUIRE_EQUAL(res.num_succeded, 2);
    BOOST_REQUIRE_EQUAL(res.num_failed, 0);

    for (auto [url, req] : get_targets()) {
        vlog(test_log.error, "{}", url);
    }
    BOOST_REQUIRE_EQUAL(get_requests().size(), 3);
    BOOST_REQUIRE(get_targets().count(manifest_url)); // NOLINT
    {
        auto it = get_targets().find(manifest_url);
        const auto& [url, req] = *it;
        verify_manifest_content(req.content);
        BOOST_REQUIRE(req._method == "PUT"); // NOLINT
    }
    BOOST_REQUIRE(get_targets().count(segment1_url)); // NOLINT
    {
        auto it = get_targets().find(segment1_url);
        const auto& [url, req] = *it;
        auto name = url.substr(url.size() - std::strlen("#-#-v#.log"));
        verify_segment(manifest_ntp, archival::segment_name(name), req.content);
        BOOST_REQUIRE(req._method == "PUT"); // NOLINT
    }
    BOOST_REQUIRE(get_targets().count(segment2_url)); // NOLINT
    {
        auto it = get_targets().find(segment2_url);
        const auto& [url, req] = *it;
        auto name = url.substr(url.size() - std::strlen("#-#-v#.log"));
        verify_segment(manifest_ntp, archival::segment_name(name), req.content);
        BOOST_REQUIRE(req._method == "PUT"); // NOLINT
    }
}

// NOLINTNEXTLINE
FIXTURE_TEST(test_archiver_policy, archiver_fixture) {
    model::offset high_watermark{9999};
    const auto offset1 = model::offset(1000);
    const auto offset2 = model::offset(2000);
    const auto offset3 = model::offset(3000);
    const auto offset4 = model::offset(10000);
    std::vector<segment_desc> segments = {
      {manifest_ntp, offset1, model::term_id(1)},
      {manifest_ntp, offset2, model::term_id(1)},
      {manifest_ntp, offset3, model::term_id(1)},
      {manifest_ntp, offset4, model::term_id(1)},
    };
    init_storage_api_local(segments);
    auto& lm = get_local_storage_api().log_mgr();
    ntp_level_probe ntp_probe(per_ntp_metrics_disabled::yes, manifest_ntp);
    service_probe svc_probe(service_metrics_disabled::yes);
    archival::archival_policy policy(manifest_ntp, svc_probe, ntp_probe);

    log_segment_set(lm);
    // Starting offset is lower than offset1
    auto upload1 = policy.get_next_candidate(
      model::offset(0), high_watermark, lm);
    log_upload_candidate(upload1);
    BOOST_REQUIRE(upload1.source.get() != nullptr);
    BOOST_REQUIRE(upload1.starting_offset == offset1);

    auto upload2 = policy.get_next_candidate(
      upload1.source->offsets().dirty_offset + model::offset(1),
      high_watermark,
      lm);
    log_upload_candidate(upload2);
    BOOST_REQUIRE(upload2.source.get() != nullptr);
    BOOST_REQUIRE(upload2.starting_offset() == offset2);
    BOOST_REQUIRE(upload2.exposed_name != upload1.exposed_name);
    BOOST_REQUIRE(upload2.source != upload1.source);
    BOOST_REQUIRE(upload2.source->offsets().base_offset == offset2);

    auto upload3 = policy.get_next_candidate(
      upload2.source->offsets().dirty_offset + model::offset(1),
      high_watermark,
      lm);
    log_upload_candidate(upload3);
    BOOST_REQUIRE(upload3.source.get() != nullptr);
    BOOST_REQUIRE(upload3.starting_offset() == offset3);
    BOOST_REQUIRE(upload3.exposed_name != upload2.exposed_name);
    BOOST_REQUIRE(upload3.source != upload2.source);
    BOOST_REQUIRE(upload3.source->offsets().base_offset == offset3);

    auto upload4 = policy.get_next_candidate(
      upload3.source->offsets().dirty_offset + model::offset(1),
      high_watermark,
      lm);
    BOOST_REQUIRE(upload4.source.get() == nullptr);

    auto upload5 = policy.get_next_candidate(
      high_watermark + model::offset(1), high_watermark, lm);
    BOOST_REQUIRE(upload5.source.get() == nullptr);
}

// NOLINTNEXTLINE
FIXTURE_TEST(test_upload_segments_leadership_transfer, archiver_fixture) {
    // This test simulates leadership transfer. In this situation the
    // manifest might contain misaligned segments. This triggers partial
    // segment upload which, in turn should guarantee that the progress is
    // made.
    // The manifest that this test generates contains a segment definition
    // that clashes with the partial upload.
    model::offset high_watermark = model::offset::max();
    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(1), model::term_id(2)},
      {manifest_ntp, model::offset(1000), model::term_id(4)},
    };
    init_storage_api_local(segments);
    auto s1name = archival::segment_name("1-2-v1.log");
    auto s2name = archival::segment_name("1000-4-v1.log");
    auto segment1 = get_segment(manifest_ntp, s1name);
    BOOST_REQUIRE(static_cast<bool>(segment1));
    auto segment2 = get_segment(manifest_ntp, s2name);
    BOOST_REQUIRE(static_cast<bool>(segment2));

    //
    cloud_storage::manifest old_manifest(manifest_ntp, manifest_revision);
    cloud_storage::manifest::segment_meta old_meta{
      .is_compacted = false,
      .size_bytes = 100,
      .base_offset = model::offset(2),
      .committed_offset = segment1->offsets().dirty_offset - model::offset(1)};
    auto oldname = archival::segment_name("2-2-v1.log");
    old_manifest.add(oldname, old_meta);
    std::stringstream old_str;
    old_manifest.serialize(old_str);
    ss::sstring segment3_url = "/dfee62b1/test-ns/test-topic/42_0/2-2-v1.log";

    std::vector<s3_imposter_fixture::expectation> expectations({
      s3_imposter_fixture::expectation{
        .url = manifest_url, .body = ss::sstring(old_str.str())},
      s3_imposter_fixture::expectation{.url = segment1_url, .body = "segment1"},
      s3_imposter_fixture::expectation{.url = segment2_url, .body = "segment2"},
      s3_imposter_fixture::expectation{.url = segment3_url, .body = "segment3"},
    });

    set_expectations_and_listen(expectations);
    auto conf = get_configuration();
    service_probe probe(service_metrics_disabled::yes);
    cloud_storage::remote remote(
      conf.connection_limit, conf.client_config, probe);
    archival::ntp_archiver archiver(
      get_ntp_conf(), get_configuration(), remote, probe);
    auto action = ss::defer([&archiver] { archiver.stop().get(); });

    retry_chain_node fib;

    archiver.download_manifest(fib).get();

    auto res = archiver
                 .upload_next_candidates(
                   get_local_storage_api().log_mgr(), high_watermark, fib)
                 .get0();
    BOOST_REQUIRE_EQUAL(res.num_succeded, 2);
    BOOST_REQUIRE_EQUAL(res.num_failed, 0);

    for (auto req : get_requests()) {
        vlog(test_log.error, "{}", req._url);
    }
    BOOST_REQUIRE_EQUAL(get_requests().size(), 4);
    {
        auto [begin, end] = get_targets().equal_range(manifest_url);
        size_t len = std::distance(begin, end);
        BOOST_REQUIRE_EQUAL(len, 2);
        std::set<ss::sstring> expected = {"PUT", "GET"};
        for (auto it = begin; it != end; it++) {
            auto key = it->second._method;
            BOOST_REQUIRE(expected.contains(key));
            expected.erase(key);
        }
        BOOST_REQUIRE(expected.empty());
    }
    {
        auto [begin, end] = get_targets().equal_range(segment2_url);
        size_t len = std::distance(begin, end);
        BOOST_REQUIRE_EQUAL(len, 1);
        BOOST_REQUIRE(begin->second._method == "PUT"); // NOLINT
    }
    {
        auto [begin, end] = get_targets().equal_range(segment3_url);
        size_t len = std::distance(begin, end);
        BOOST_REQUIRE_EQUAL(len, 1);
        BOOST_REQUIRE(begin->second._method == "PUT"); // NOLINT
    }
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
    const auto offset1 = model::offset(1000);
    const auto offset2 = model::offset(2000);
    const auto offset3 = model::offset(3000);
    std::vector<segment_desc> segments = {
      {manifest_ntp, offset1, model::term_id(1)},
      {manifest_ntp, offset2, model::term_id(1)},
      {manifest_ntp, offset3, model::term_id(1)},
    };
    init_storage_api_local(segments);
    auto& lm = get_local_storage_api().log_mgr();
    ntp_level_probe ntp_probe(per_ntp_metrics_disabled::yes, manifest_ntp);
    service_probe svc_probe(service_metrics_disabled::yes);
    archival::archival_policy policy(manifest_ntp, svc_probe, ntp_probe);

    // Patch segment offsets to create overlaps for the archival_policy.
    // The archival_policy instance only touches the offsets, not the
    // actual data so having them a bit inconsistent for the sake of testing
    // is OK.
    auto segment1 = get_segment(
      manifest_ntp, archival::segment_name("1000-1-v1.log"));
    auto& tracker1 = const_cast<storage::segment::offset_tracker&>(
      segment1->offsets());
    tracker1.dirty_offset = offset2 - model::offset(1);
    auto segment2 = get_segment(
      manifest_ntp, archival::segment_name("2000-1-v1.log"));
    auto& tracker2 = const_cast<storage::segment::offset_tracker&>(
      segment2->offsets());
    tracker2.dirty_offset = offset3 - model::offset(1);

    // Every segment should be returned once as we're calling the
    // policy to get next candidate.
    log_segment_set(lm);
    model::offset high_watermark{9999};
    // Starting offset is lower than offset1
    auto upload1 = policy.get_next_candidate(
      model::offset(0), high_watermark, lm);
    log_upload_candidate(upload1);
    BOOST_REQUIRE(upload1.source.get() != nullptr);
    BOOST_REQUIRE(upload1.starting_offset == offset1);

    auto upload2 = policy.get_next_candidate(
      upload1.source->offsets().dirty_offset + model::offset(1),
      high_watermark,
      lm);
    log_upload_candidate(upload2);
    BOOST_REQUIRE(upload2.source.get() != nullptr);
    BOOST_REQUIRE(upload2.starting_offset == offset2);
    BOOST_REQUIRE(upload2.exposed_name != upload1.exposed_name);
    BOOST_REQUIRE(upload2.source != upload1.source);
    BOOST_REQUIRE(upload2.source->offsets().base_offset == offset2);

    auto upload3 = policy.get_next_candidate(
      upload2.source->offsets().dirty_offset + model::offset(1),
      high_watermark,
      lm);
    log_upload_candidate(upload3);
    BOOST_REQUIRE(upload3.source.get() != nullptr);
    BOOST_REQUIRE(upload3.starting_offset == offset3);
    BOOST_REQUIRE(upload3.exposed_name != upload2.exposed_name);
    BOOST_REQUIRE(upload3.source != upload2.source);
    BOOST_REQUIRE(upload3.source->offsets().base_offset == offset3);

    auto upload4 = policy.get_next_candidate(
      upload3.source->offsets().dirty_offset + model::offset(1),
      high_watermark,
      lm);
    BOOST_REQUIRE(upload4.source.get() == nullptr);
}
