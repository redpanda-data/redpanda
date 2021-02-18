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
#include "cluster/tests/utils.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "test_utils/fixture.h"
#include "utils/unresolved_address.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>

#include <boost/algorithm/string.hpp>

using namespace std::chrono_literals;
using namespace archival;

inline ss::logger test_log("test"); // NOLINT

static constexpr std::string_view manifest_payload = R"json({
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 0,
    "segments": {
        "1-2-v1.log": {
            "is_compacted": false,
            "size_bytes": 100,
            "committed_offset": 2,
            "base_offset": 1,
            "deleted": false
        },
        "3-4-v1.log": {
            "is_compacted": false,
            "size_bytes": 200,
            "committed_offset": 4,
            "base_offset": 3,
            "deleted": false
        }
    }
})json";
static constexpr std::string_view manifest_with_deleted_segment = R"json({
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 0,
    "segments": {
        "3-4-v1.log": {
            "is_compacted": false,
            "size_bytes": 200,
            "committed_offset": 4,
            "base_offset": 3,
            "deleted": false
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
static const ss::sstring manifest_url = fmt::format(         // NOLINT
  "/a0000000/meta/{}_{}/manifest.json",
  manifest_ntp.path(),
  manifest_revision());

// NOLINTNEXTLINE
static const ss::sstring segment1_url
  = "/3931f368/test-ns/test-topic/42_0/1-2-v1.log";
// NOLINTNEXTLINE
static const ss::sstring segment2_url
  = "/47bef4d3/test-ns/test-topic/42_0/3-4-v1.log";

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

static manifest load_manifest(std::string_view v) {
    manifest m;
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

FIXTURE_TEST(test_download_manifest, s3_imposter_fixture) { // NOLINT
    set_expectations_and_listen(default_expectations);
    archival::ntp_archiver archiver(get_ntp_conf(), get_configuration());
    auto action = ss::defer([&archiver] { archiver.stop().get(); });
    archiver.download_manifest().get();
    auto expected = load_manifest(manifest_payload);
    BOOST_REQUIRE(expected == archiver.get_remote_manifest()); // NOLINT
}

FIXTURE_TEST(test_upload_manifest, s3_imposter_fixture) { // NOLINT
    set_expectations_and_listen(default_expectations);
    archival::ntp_archiver archiver(get_ntp_conf(), get_configuration());
    auto action = ss::defer([&archiver] { archiver.stop().get(); });
    auto pm = const_cast<manifest*>( // NOLINT
      &archiver.get_remote_manifest());
    pm->add(
      segment_name("1-2-v1.log"),
      {.is_compacted = false,
       .size_bytes = 100, // NOLINT
       .base_offset = model::offset(1),
       .committed_offset = model::offset(2),
       .is_deleted_locally = false

      });
    pm->add(
      segment_name("3-4-v1.log"),
      {.is_compacted = false,
       .size_bytes = 200, // NOLINT
       .base_offset = model::offset(3),
       .committed_offset = model::offset(4),
       .is_deleted_locally = false

      });
    archiver.upload_manifest().get();
    auto req = get_requests().front();
    // NOLINTNEXTLINE
    BOOST_REQUIRE(compare_json_objects(req.content, manifest_payload));
}

// NOLINTNEXTLINE
FIXTURE_TEST(test_archival_non_compacted_selection_policy, archiver_fixture) {
    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(1), model::term_id(2)},
      {manifest_ntp, model::offset(3), model::term_id(4)},
    };
    init_storage_api_local(segments);
    auto policy = archival::make_archival_policy(
      archival::upload_policy_selector::archive_non_compacted,
      archival::delete_policy_selector::do_not_keep,
      manifest_ntp,
      manifest_revision);
    vlog(test_log.trace, "update local manifest");
    archival::manifest empty(manifest_ntp, manifest_revision);
    auto m = policy->generate_upload_set(
      empty, get_local_storage_api().log_mgr());
    BOOST_REQUIRE(m.has_value());
    std::stringstream json;
    m->serialize(json);
    vlog(test_log.trace, "local manifest: {}", json.str());
    verify_manifest(*m);
}

// NOLINTNEXTLINE
FIXTURE_TEST(test_upload_segments, archiver_fixture) {
    set_expectations_and_listen(default_expectations);
    archival::ntp_archiver archiver(get_ntp_conf(), get_configuration());
    auto action = ss::defer([&archiver] { archiver.stop().get(); });

    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(1), model::term_id(2)},
      {manifest_ntp, model::offset(3), model::term_id(4)},
    };
    init_storage_api_local(segments);

    ss::semaphore limit(2);
    auto res
      = archiver.upload_next_candidate(limit, get_local_storage_api().log_mgr())
          .get0();
    BOOST_REQUIRE_EQUAL(res.succeded, 2);
    BOOST_REQUIRE_EQUAL(res.failed, 0);

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
FIXTURE_TEST(test_delete_segments, archiver_fixture) {
    set_expectations_and_listen(default_expectations);
    archival::ntp_archiver archiver(get_ntp_conf(), get_configuration());
    auto action = ss::defer([&archiver] { archiver.stop().get(); });

    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(3), model::term_id(4)},
    };
    init_storage_api_local(segments);

    ss::semaphore limit(2);
    archiver.download_manifest().get();
    auto res
      = archiver.delete_next_candidate(limit, get_local_storage_api().log_mgr())
          .get0();
    BOOST_REQUIRE_EQUAL(res.succeded, 2);
    BOOST_REQUIRE_EQUAL(res.failed, 0);

    BOOST_REQUIRE_EQUAL(get_requests().size(), 3);
    BOOST_REQUIRE(get_targets().count(manifest_url)); // NOLINT
    auto erange = get_targets().equal_range(manifest_url);
    for (auto it = erange.first; it != erange.second; it++) {
        const auto& [url, req] = *it;
        if (req._method == "PUT") {
            // NOLINTNEXTLINE
            BOOST_REQUIRE(
              compare_json_objects(req.content, manifest_with_deleted_segment));
        } else {
            BOOST_REQUIRE(req._method == "GET"); // NOLINT
        }
    }
    BOOST_REQUIRE(get_targets().count(segment1_url)); // NOLINT
    {
        auto it = get_targets().find(segment1_url);
        const auto& [url, req] = *it;
        BOOST_REQUIRE(req._method == "DELETE"); // NOLINT
    }
}
