/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/async_manifest_view.h"
#include "cloud_storage/read_path_probes.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage_clients/client_pool.h"
#include "cluster/archival/archival_metadata_stm.h"
#include "cluster/archival/tests/service_fixture.h"
#include "config/configuration.h"
#include "storage/ntp_config.h"
#include "test_utils/fixture.h"

#include <seastar/core/sharded.hh>

using namespace std::chrono_literals;
using namespace archival;

inline ss::logger test_log("test");

namespace {
cloud_storage::remote_path_provider path_provider(std::nullopt, std::nullopt);
} // anonymous namespace

static const auto manifest_namespace = model::ns("kafka");
static const auto manifest_topic = model::topic("test-topic");
static const auto manifest_partition = model::partition_id(42);
static const auto manifest_ntp = model::ntp(
  manifest_namespace, manifest_topic, manifest_partition);
static const auto manifest_revision = model::initial_revision_id(0);
static const ss::sstring manifest_url = ssx::sformat(
  "/10000000/meta/{}_{}/manifest.bin",
  manifest_ntp.path(),
  manifest_revision());

static storage::ntp_config get_ntp_conf() { return {manifest_ntp, "base-dir"}; }

static void verify_stm_manifest(
  const cloud_storage::partition_manifest& manifest,
  const std::vector<segment_desc>& segment_spec) {
    BOOST_REQUIRE_EQUAL(manifest.size(), segment_spec.size());
    for (size_t i = 0; i < segment_spec.size(); ++i) {
        const auto& segment = segment_spec[i];
        auto it = manifest.begin();
        std::advance(it, i);
        BOOST_CHECK_EQUAL(segment.base_offset, it->base_offset);
    }
}

static constexpr std::string_view misaligned_lco = R"json({
"version": 1,
"namespace": "kafka",
"topic": "test-topic",
"partition": 42,
"revision": 0,
"last_offset": 1010,
"last_uploaded_compacted_offset": 250,
"segments": {
    "0-1-v1.log": {
        "is_compacted": false,
        "size_bytes": 1024,
        "base_offset": 0,
        "committed_offset": 499
    },
    "500-1-v1.log": {
        "is_compacted": false,
        "size_bytes": 1024,
        "base_offset": 500,
        "committed_offset": 999
    },
    "1000-4-v1.log": {
        "is_compacted": false,
        "size_bytes": 2048,
        "base_offset": 1000,
        "committed_offset": 1010,
        "max_timestamp": 1234567890
    }
}
})json";

static constexpr std::string_view gap_manifest = R"json({
"version": 1,
"namespace": "kafka",
"topic": "test-topic",
"partition": 42,
"revision": 0,
"last_offset": 1010,
"segments": {
    "0-1-v1.log": {
        "is_compacted": false,
        "size_bytes": 1024,
        "base_offset": 0,
        "committed_offset": 200
    },
    "500-1-v1.log": {
        "is_compacted": false,
        "size_bytes": 1024,
        "base_offset": 500,
        "committed_offset": 999
    },
    "1000-4-v1.log": {
        "is_compacted": false,
        "size_bytes": 2048,
        "base_offset": 1000,
        "committed_offset": 1010,
        "max_timestamp": 1234567890
    }
}
})json";

struct reupload_fixture : public archiver_fixture {
    void create_segment(segment_desc seg) {
        auto segment = get_local_storage_api()
                         .log_mgr()
                         .make_log_segment(
                           storage::ntp_config{seg.ntp, data_dir.string()},
                           seg.base_offset,
                           seg.term,
                           ss::default_priority_class(),
                           128_KiB,
                           10,
                           1_MiB)
                         .get();
        write_random_batches(segment, seg.num_records.value(), 2);
        disk_log_impl()->segments().add(segment);
    }

    void initialize(
      const std::vector<segment_desc>& segment_spec,
      bool enable_compaction = true) {
        if (enable_compaction) {
            storage::ntp_config::default_overrides o;
            o.cleanup_policy_bitflags
              = model::cleanup_policy_bitflags::compaction;
            init_storage_api_local(segment_spec, o, true);
        } else {
            init_storage_api_local(segment_spec, std::nullopt, true);
        }
        wait_for_partition_leadership(manifest_ntp);
        auto part = app.partition_manager.local().get(manifest_ntp);
        RPTEST_REQUIRE_EVENTUALLY(10s, [part]() mutable {
            return part->high_watermark() >= model::offset(1);
        });
        init_archiver();
    }

    ss::shared_ptr<storage::log> disk_log_impl() {
        return get_local_storage_api().log_mgr().get(manifest_ntp);
    }

    cloud_storage::partition_manifest
    verify_manifest_request(cluster::partition& partition) {
        BOOST_REQUIRE(get_targets().count(manifest_url));

        auto req_opt = get_latest_request(manifest_url);
        BOOST_REQUIRE(req_opt.has_value());
        auto req = req_opt.value().get();

        BOOST_REQUIRE_EQUAL(req.method, "PUT");
        verify_manifest_content(req.content);
        cloud_storage::partition_manifest manifest = load_manifest(req.content);
        BOOST_REQUIRE(manifest == partition.archival_meta_stm()->manifest());
        return manifest;
    }

    void verify_segment_request(
      std::string_view name,
      const cloud_storage::partition_manifest& m,
      std::string_view method = "PUT") {
        segment_name s_name{name};
        auto s_url = get_segment_path(m, s_name);
        vlog(test_log.debug, "search URL: {}", "/" + s_url().string());
        auto it = get_targets().find("/" + s_url().string());
        BOOST_REQUIRE(it != get_targets().end());
        const auto& [url, req] = *it;
        BOOST_REQUIRE_EQUAL(req.method, method);
        verify_segment(manifest_ntp, s_name, req.content);
    }

    void verify_concat_segment_request(
      std::vector<std::string_view> names,
      const cloud_storage::partition_manifest& m,
      std::string_view method = "PUT") {
        auto s_url = get_segment_path(
          m, cloud_storage::segment_name{names.front()});

        vlog(test_log.info, "searching for target: {}", s_url);
        auto it = get_targets().find("/" + s_url().string());
        BOOST_REQUIRE(it != get_targets().end());
        const auto& [url, req] = *it;
        BOOST_REQUIRE_EQUAL(req.method, method);
        std::vector<segment_name> segment_names;
        segment_names.reserve(names.size());
        std::transform(
          names.begin(),
          names.end(),
          std::back_inserter(segment_names),
          [](auto n) { return segment_name{n}; });
        verify_segments(manifest_ntp, segment_names, req.content);
    }

    void init_archiver() {
        auto [arch_conf, remote_conf] = get_configurations();
        auto part = app.partition_manager.local().get(manifest_ntp);
        part_probe.emplace(get_ntp_conf().ntp());
        manifest_view = ss::make_shared<cloud_storage::async_manifest_view>(
          remote,
          app.shadow_index_cache,
          part->archival_meta_stm()->manifest(),
          arch_conf->bucket_name,
          path_provider);

        archiver.emplace(
          get_ntp_conf(),
          arch_conf,
          remote.local(),
          app.shadow_index_cache.local(),
          *part,
          manifest_view);
    }

    ss::lw_shared_ptr<storage::segment> self_compact_next_segment(
      model::offset max_collectible = model::offset::max()) {
        auto& seg_set = disk_log_impl()->segments();
        auto size_before = seg_set.size();

        disk_log_impl()
          ->housekeeping(storage::housekeeping_config{
            model::timestamp::max(),
            std::nullopt,
            max_collectible,
            std::nullopt,
            ss::default_priority_class(),
            abort_source})
          .get();

        auto size_after = seg_set.size();

        // We are only looking to trigger self-compaction here.
        // If the segment count reduced, adjacent segment compaction must
        // have occurred.
        BOOST_REQUIRE_EQUAL(size_before, size_after);

        ss::lw_shared_ptr<storage::segment> last_compacted_segment;
        for (auto& i : seg_set) {
            if (i->finished_self_compaction()) {
                last_compacted_segment = i;
            }
        }
        return last_compacted_segment;
    }

    std::optional<cloud_storage::partition_probe> part_probe;
    ss::shared_ptr<cloud_storage::async_manifest_view> manifest_view;
    std::optional<archival::ntp_archiver> archiver;
    ss::abort_source abort_source;
};

namespace cluster::details {

class archival_metadata_stm_accessor {
public:
    explicit archival_metadata_stm_accessor(
      cluster::archival_metadata_stm& archival_metadata_stm)
      : stm(archival_metadata_stm) {}

    void replace_manifest(std::string_view json) {
        iobuf i;
        i.append(json.data(), json.size());
        cloud_storage::partition_manifest m;

        m.update(
           cloud_storage::manifest_format::json,
           make_iobuf_input_stream(std::move(i)))
          .get();

        stm._manifest = ss::make_shared<cloud_storage::partition_manifest>(
          std::move(m));
    }

private:
    cluster::archival_metadata_stm& stm;
};

} // namespace cluster::details

FIXTURE_TEST(test_upload_compacted_segments, reupload_fixture) {
    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(0), model::term_id(1), 1000, 2},
      {manifest_ntp, model::offset(1000), model::term_id(4), 10, 2},
    };

    initialize(segments);
    auto action = ss::defer([this] { archiver->stop().get(); });

    auto part = app.partition_manager.local().get(manifest_ntp);
    listen();

    // Upload two non compacted segments, no segment is compacted yet.
    auto expected = archival::ntp_archiver::batch_result{{2, 0, 0}, {0, 0, 0}};
    upload_and_verify(archiver.value(), expected);
    BOOST_REQUIRE_EQUAL(get_requests().size(), 5);

    auto manifest = verify_manifest_request(*part);
    verify_segment_request("0-1-v1.log", manifest);
    verify_segment_request("1000-4-v1.log", manifest);

    BOOST_REQUIRE(part->archival_meta_stm());
    const cloud_storage::partition_manifest& stm_manifest
      = part->archival_meta_stm()->manifest();
    verify_stm_manifest(stm_manifest, segments);

    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_last_uploaded_compacted_offset(), model::offset{});

    // Mark first segment compacted, and re-upload, now only one segment is
    // uploaded.
    reset_http_call_state();
    auto seg = self_compact_next_segment(
      stm_manifest.first_addressable_segment()->committed_offset);

    expected = archival::ntp_archiver::batch_result{{0, 0, 0}, {1, 0, 0}};
    upload_and_verify(archiver.value(), expected);
    BOOST_REQUIRE_EQUAL(get_requests().size(), 3);

    verify_segment_request("0-1-v1.log", part->archival_meta_stm()->manifest());

    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_last_uploaded_compacted_offset(),
      seg->offsets().get_committed_offset());

    auto replaced = stm_manifest.replaced_segments();
    BOOST_REQUIRE_EQUAL(replaced.size(), 1);
    BOOST_REQUIRE_EQUAL(replaced[0].base_offset, model::offset{0});

    // Mark second segment as compacted and re-upload.
    seg = self_compact_next_segment(
      std::next(stm_manifest.first_addressable_segment())->committed_offset);

    reset_http_call_state();

    expected = archival::ntp_archiver::batch_result{{0, 0, 0}, {1, 0, 0}};
    upload_and_verify(archiver.value(), expected);

    BOOST_REQUIRE_EQUAL(get_requests().size(), 3);

    verify_segment_request(
      "1000-4-v1.log", part->archival_meta_stm()->manifest());

    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_last_uploaded_compacted_offset(),
      seg->offsets().get_committed_offset());

    replaced = stm_manifest.replaced_segments();
    BOOST_REQUIRE_EQUAL(replaced.size(), 2);
    BOOST_REQUIRE_EQUAL(replaced[0].base_offset, model::offset{0});
    BOOST_REQUIRE_EQUAL(replaced[1].base_offset, model::offset{1000});
}

FIXTURE_TEST(test_upload_compacted_segments_concat, reupload_fixture) {
    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(0), model::term_id(1), 1000, 2},
      {manifest_ntp, model::offset(1000), model::term_id(4), 10, 2},
    };

    initialize(segments);
    auto action = ss::defer([this] { archiver->stop().get(); });

    auto part = app.partition_manager.local().get(manifest_ntp);
    listen();

    // Upload two non compacted segments, no segment is compacted yet.
    archival::ntp_archiver::batch_result expected{{2, 0, 0}, {0, 0, 0}};
    upload_and_verify(archiver.value(), expected);

    // Two segments, two indices, one manifest
    BOOST_REQUIRE_EQUAL(get_requests().size(), 5);

    auto manifest = verify_manifest_request(*part);
    verify_segment_request("0-1-v1.log", manifest);
    verify_segment_request("1000-4-v1.log", manifest);

    BOOST_REQUIRE(part->archival_meta_stm());
    const cloud_storage::partition_manifest& stm_manifest
      = part->archival_meta_stm()->manifest();
    verify_stm_manifest(stm_manifest, segments);

    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_last_uploaded_compacted_offset(), model::offset{});

    // Mark both segments compacted, and re-upload. One concatenated segment is
    // uploaded.
    reset_http_call_state();
    auto seg = self_compact_next_segment();

    expected = archival::ntp_archiver::batch_result{{0, 0, 0}, {1, 0, 0}};
    upload_and_verify(archiver.value(), expected);
    BOOST_REQUIRE_EQUAL(get_requests().size(), 3);

    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_last_uploaded_compacted_offset(),
      seg->offsets().get_committed_offset());

    auto replaced = stm_manifest.replaced_segments();
    BOOST_REQUIRE_EQUAL(replaced.size(), 2);
    BOOST_REQUIRE_EQUAL(replaced[0].base_offset, model::offset{0});
    BOOST_REQUIRE_EQUAL(replaced[1].base_offset, model::offset{1000});

    verify_concat_segment_request(
      {"0-1-v1.log", "1000-4-v1.log"}, part->archival_meta_stm()->manifest());
}

FIXTURE_TEST(
  test_upload_compacted_segments_manifest_alignment, reupload_fixture) {
    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(0), model::term_id(1), 500, 2},
      {manifest_ntp, model::offset(500), model::term_id(1), 500, 2},
      {manifest_ntp, model::offset(1000), model::term_id(4), 10},
    };

    initialize(segments);
    auto action = ss::defer([this] { archiver->stop().get(); });

    auto part = app.partition_manager.local().get(manifest_ntp);
    cluster::details::archival_metadata_stm_accessor stm_acc{
      *part->archival_meta_stm()};

    // Place lco at 250, first segment should be skipped to preserve alignment.
    stm_acc.replace_manifest(misaligned_lco);
    const auto& stm_manifest = part->archival_meta_stm()->manifest();

    listen();

    // Self-compact just the first couple segments.
    self_compact_next_segment(model::offset{999});

    archival::ntp_archiver::batch_result expected{{0, 0, 0}, {1, 0, 0}};
    upload_and_verify(archiver.value(), expected);
    BOOST_REQUIRE_EQUAL(get_requests().size(), 3);

    std::stringstream st;
    stm_manifest.serialize_json(st);
    vlog(test_log.debug, "manifest: {}", st.str());
    verify_segment_request("500-1-v1.log", stm_manifest);

    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_last_uploaded_compacted_offset(), model::offset{999});
    auto replaced = stm_manifest.replaced_segments();
    BOOST_REQUIRE_EQUAL(replaced[0].base_offset, model::offset{500});
}

FIXTURE_TEST(test_upload_compacted_segments_fill_gap, reupload_fixture) {
    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(0), model::term_id(1), 1000, 2},
      {manifest_ntp, model::offset(1000), model::term_id(4), 10, 2},
    };

    initialize(segments);
    auto action = ss::defer([this] { archiver->stop().get(); });

    auto part = app.partition_manager.local().get(manifest_ntp);
    cluster::details::archival_metadata_stm_accessor stm_acc{
      *part->archival_meta_stm()};

    // Manifest has gap: offset 199 to 500. Re-upload will fill the gap.
    stm_acc.replace_manifest(gap_manifest);

    const auto& stm_manifest = part->archival_meta_stm()->manifest();

    listen();

    self_compact_next_segment();

    archival::ntp_archiver::batch_result expected{{0, 0, 0}, {1, 0, 0}};
    upload_and_verify(archiver.value(), expected);

    BOOST_REQUIRE_EQUAL(get_requests().size(), 3);

    verify_segment_request("0-1-v1.log", stm_manifest);

    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_last_uploaded_compacted_offset(), model::offset{999});

    auto replaced = stm_manifest.replaced_segments();
    BOOST_REQUIRE_EQUAL(replaced[0].base_offset, model::offset{0});
    BOOST_REQUIRE_EQUAL(replaced[1].base_offset, model::offset{500});
}

FIXTURE_TEST(test_upload_both_compacted_and_non_compacted, reupload_fixture) {
    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(0), model::term_id(1), 20, 2},
      {manifest_ntp, model::offset(20), model::term_id(4), 10, 2},
    };

    initialize(segments);
    auto action = ss::defer([this] { archiver->stop().get(); });

    auto part = app.partition_manager.local().get(manifest_ntp);
    listen();

    // Upload two non compacted segments, no segment is compacted yet.
    archival::ntp_archiver::batch_result expected{{2, 0, 0}, {0, 0, 0}};
    upload_and_verify(archiver.value(), expected);

    BOOST_REQUIRE_EQUAL(get_requests().size(), 5);

    auto manifest = verify_manifest_request(*part);
    verify_segment_request("0-1-v1.log", manifest);
    verify_segment_request("20-4-v1.log", manifest);

    BOOST_REQUIRE(part->archival_meta_stm());
    const cloud_storage::partition_manifest& stm_manifest
      = part->archival_meta_stm()->manifest();
    verify_stm_manifest(stm_manifest, segments);

    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_last_uploaded_compacted_offset(), model::offset{});

    // Close current segment and add a new open segment, so both compacted and
    // non-compacted uploads run together.
    auto& last_segment = disk_log_impl()->segments().back();
    write_random_batches(last_segment, 20, 2);
    last_segment->appender().close().get();
    last_segment->release_appender();

    create_segment(
      {manifest_ntp,
       last_segment->offsets().get_committed_offset() + model::offset{1},
       model::term_id{4},
       1});

    // Self-compact the first segment and re-upload. One
    // compacted and one non-compacted segments are uploaded.
    //
    // NOTE: we can only compact up to what's been uploaded, since that
    // determines the max collectible offset.
    reset_http_call_state();
    auto seg = self_compact_next_segment(
      manifest.first_addressable_segment()->committed_offset);

    expected = archival::ntp_archiver::batch_result{{1, 0, 0}, {1, 0, 0}};
    upload_and_verify(archiver.value(), expected, model::offset::max());
    BOOST_REQUIRE_EQUAL(get_requests().size(), 5);

    verify_segment_request("0-1-v1.log", part->archival_meta_stm()->manifest());
    verify_segment_request(
      "30-5-v1.log", part->archival_meta_stm()->manifest());

    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_last_uploaded_compacted_offset(),
      seg->offsets().get_committed_offset());

    auto replaced = stm_manifest.replaced_segments();
    BOOST_REQUIRE_EQUAL(replaced.size(), 1);
    BOOST_REQUIRE_EQUAL(replaced[0].base_offset, model::offset{0});
}

FIXTURE_TEST(test_both_uploads_with_one_failing, reupload_fixture) {
    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(0), model::term_id(1), 20, 2},
      {manifest_ntp, model::offset(20), model::term_id(4), 10, 2},
    };

    initialize(segments);
    auto action = ss::defer([this] { archiver->stop().get(); });

    auto part = app.partition_manager.local().get(manifest_ntp);
    listen();

    // Upload two non compacted segments, no segment is compacted yet.
    archival::ntp_archiver::batch_result expected{{2, 0, 0}, {0, 0, 0}};
    upload_and_verify(archiver.value(), expected);

    BOOST_REQUIRE_EQUAL(get_requests().size(), 5);

    auto manifest = verify_manifest_request(*part);
    verify_segment_request("0-1-v1.log", manifest);
    verify_segment_request("20-4-v1.log", manifest);

    BOOST_REQUIRE(part->archival_meta_stm());
    const cloud_storage::partition_manifest& stm_manifest
      = part->archival_meta_stm()->manifest();
    verify_stm_manifest(stm_manifest, segments);

    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_last_uploaded_compacted_offset(), model::offset{});

    // Close current segment and add a new open segment, so both compacted and
    // non-compacted uploads run together.
    auto& last_segment = disk_log_impl()->segments().back();
    write_random_batches(last_segment, 20, 2);
    last_segment->appender().close().get();
    last_segment->release_appender();

    create_segment(
      {manifest_ntp,
       last_segment->offsets().get_committed_offset() + model::offset{1},
       model::term_id{4},
       1});

    // Self-compact the first segment and re-upload. One compacted
    // and one non-compacted segments are uploaded.
    reset_http_call_state();
    auto seg = self_compact_next_segment(disk_log_impl()
                                           ->segments()
                                           .begin()
                                           ->get()
                                           ->offsets()
                                           .get_committed_offset());

    // Fail the first compacted upload
    fail_request_if(
      [](const ss::http::request& request) {
          return request._url.find("0-19-") != ss::sstring::npos
                 && request._url.find("-1-v1.log") != ss::sstring::npos;
      },
      {.body
       = {archival_tests::error_payload.data(), archival_tests::error_payload.size()},
       .status = ss::http::reply::status_type::not_found});

    // The non-compacted uploads proceed as normal, the compacted upload fails.
    expected = archival::ntp_archiver::batch_result{{1, 0, 0}, {0, 1, 0}};
    upload_and_verify(archiver.value(), expected, model::offset::max());

    log_requests();
    BOOST_REQUIRE_EQUAL(get_requests().size(), 4);

    verify_segment_request(
      "30-5-v1.log", part->archival_meta_stm()->manifest());

    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_last_uploaded_compacted_offset(), model::offset{});

    BOOST_REQUIRE(stm_manifest.replaced_segments().empty());
}

FIXTURE_TEST(test_upload_when_compaction_disabled, reupload_fixture) {
    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(0), model::term_id(1), 1000, 2},
      {manifest_ntp, model::offset(1000), model::term_id(4), 10, 2},
    };

    // Disable compaction
    initialize(segments, false);
    auto action = ss::defer([this] { archiver->stop().get(); });

    auto part = app.partition_manager.local().get(manifest_ntp);
    listen();

    // Upload two non compacted segments, no segment is compacted yet.

    auto expected = archival::ntp_archiver::batch_result{{2, 0, 0}, {0, 0, 0}};
    upload_and_verify(archiver.value(), expected);
    BOOST_REQUIRE_EQUAL(get_requests().size(), 5);

    auto manifest = verify_manifest_request(*part);
    verify_segment_request("0-1-v1.log", manifest);
    verify_segment_request("1000-4-v1.log", manifest);

    BOOST_REQUIRE(part->archival_meta_stm());
    const cloud_storage::partition_manifest& stm_manifest
      = part->archival_meta_stm()->manifest();
    verify_stm_manifest(stm_manifest, segments);

    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_last_uploaded_compacted_offset(), model::offset{});

    // Self-compact the first segment, since the topic has compaction
    // disabled, and re-upload, nothing is uploaded.
    reset_http_call_state();
    auto seg = self_compact_next_segment();

    expected = archival::ntp_archiver::batch_result{{0, 0, 0}, {0, 0, 0}};
    upload_and_verify(archiver.value(), expected);
    BOOST_REQUIRE_EQUAL(get_requests().size(), 0);

    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_last_uploaded_compacted_offset(), model::offset{});

    BOOST_REQUIRE(stm_manifest.replaced_segments().empty());
}

FIXTURE_TEST(test_upload_when_reupload_disabled, reupload_fixture) {
    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(0), model::term_id(1), 1000, 2},
      {manifest_ntp, model::offset(1000), model::term_id(4), 10, 2},
    };

    initialize(segments);
    auto action = ss::defer([this] { archiver->stop().get(); });

    auto part = app.partition_manager.local().get(manifest_ntp);
    listen();

    // Upload two non compacted segments, no segment is compacted yet.

    auto expected = archival::ntp_archiver::batch_result{{2, 0, 0}, {0, 0, 0}};
    upload_and_verify(archiver.value(), expected);
    BOOST_REQUIRE_EQUAL(get_requests().size(), 5);

    auto manifest = verify_manifest_request(*part);
    verify_segment_request("0-1-v1.log", manifest);
    verify_segment_request("1000-4-v1.log", manifest);

    BOOST_REQUIRE(part->archival_meta_stm());
    const cloud_storage::partition_manifest& stm_manifest
      = part->archival_meta_stm()->manifest();
    verify_stm_manifest(stm_manifest, segments);

    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_last_uploaded_compacted_offset(), model::offset{});

    // Mark first segment compacted artificially, since the topic has compaction
    // disabled, and re-upload, nothing is uploaded.
    reset_http_call_state();
    auto seg = self_compact_next_segment();

    expected = archival::ntp_archiver::batch_result{{0, 0, 0}, {0, 0, 0}};

    // Disable re-upload
    config::shard_local_cfg()
      .get("cloud_storage_enable_compacted_topic_reupload")
      .set_value(false);
    upload_and_verify(archiver.value(), expected);
    BOOST_REQUIRE_EQUAL(get_requests().size(), 0);

    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_last_uploaded_compacted_offset(), model::offset{});

    BOOST_REQUIRE(stm_manifest.replaced_segments().empty());

    // Re-enable uploads so other tests do not fail!
    config::shard_local_cfg()
      .get("cloud_storage_enable_compacted_topic_reupload")
      .set_value(true);
}

FIXTURE_TEST(test_upload_limit, reupload_fixture) {
    // NOTE: different terms so compaction leaves one segment each.
    std::vector<segment_desc> segments = {
      {manifest_ntp, model::offset(0), model::term_id(1), 10, 2},
      {manifest_ntp, model::offset(10), model::term_id(2), 10, 2},
      {manifest_ntp, model::offset(20), model::term_id(3), 10, 2},
      {manifest_ntp, model::offset(30), model::term_id(4), 10, 2},
      {manifest_ntp, model::offset(40), model::term_id(5), 10, 2},
    };

    initialize(segments);
    auto action = ss::defer([this] { archiver->stop().get(); });

    auto part = app.partition_manager.local().get(manifest_ntp);
    listen();

    // 4 out of 5 segments uploaded due to archiver limit of 4
    archival::ntp_archiver::batch_result expected{{4, 0, 0}, {0, 0, 0}};
    upload_and_verify(archiver.value(), expected);

    BOOST_REQUIRE_EQUAL(get_requests().size(), 9);

    auto manifest = load_manifest(
      get_targets().find(manifest_url)->second.content);

    verify_segment_request("0-1-v1.log", manifest);
    verify_segment_request("10-2-v1.log", manifest);
    verify_segment_request("20-3-v1.log", manifest);
    verify_segment_request("30-4-v1.log", manifest);

    BOOST_REQUIRE(part->archival_meta_stm());
    const cloud_storage::partition_manifest& stm_manifest
      = part->archival_meta_stm()->manifest();

    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_last_uploaded_compacted_offset(), model::offset{});

    // Create four non-compacted segments to starve out the upload limit.
    // NOTE: uploaded 4 segments, so offset is 54 at the start
    for (auto i = 0; i < 3; ++i) {
        auto& last_segment = disk_log_impl()->segments().back();
        write_random_batches(last_segment, 10, 2);
        last_segment->appender().close().get();
        last_segment->release_appender();

        create_segment(
          {manifest_ntp,
           last_segment->offsets().get_committed_offset() + model::offset{1},
           model::term_id{6},
           10});
    }

    reset_http_call_state();

    // Mark four segments as compacted, so they are valid for upload
    ss::lw_shared_ptr<storage::segment> seg;
    seg = self_compact_next_segment(model::offset(39));

    expected = archival::ntp_archiver::batch_result{{4, 0, 0}, {0, 0, 0}};
    upload_and_verify(archiver.value(), expected, model::offset::max());
    BOOST_REQUIRE_EQUAL(get_requests().size(), 9);

    verify_segment_request(
      "40-5-v1.log", part->archival_meta_stm()->manifest());
    verify_segment_request(
      "50-6-v1.log", part->archival_meta_stm()->manifest());
    verify_segment_request(
      "65-6-v1.log", part->archival_meta_stm()->manifest());
    verify_segment_request(
      "85-6-v1.log", part->archival_meta_stm()->manifest());

    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_last_uploaded_compacted_offset(), model::offset{});

    auto replaced = stm_manifest.replaced_segments();
    BOOST_REQUIRE(replaced.empty());

    reset_http_call_state();
    expected = archival::ntp_archiver::batch_result{{0, 0, 0}, {1, 0, 0}};

    upload_and_verify(archiver.value(), expected);
    BOOST_REQUIRE_EQUAL(get_requests().size(), 3);

    verify_concat_segment_request(
      {
        "0-1-v1.log",
        "10-2-v1.log",
        "20-3-v1.log",
        "30-4-v1.log",
      },
      part->archival_meta_stm()->manifest());

    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_last_uploaded_compacted_offset(),
      seg->offsets().get_committed_offset());

    replaced = stm_manifest.replaced_segments();
    BOOST_REQUIRE_EQUAL(replaced.size(), 4);
}
