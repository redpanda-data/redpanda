/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "cloud_storage/anomalies_detector.h"
#include "cloud_storage/base_manifest.h"
#include "cloud_storage/inventory/utils.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/partition_path_utils.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/types.h"
#include "config/node_config.h"
#include "hashing/xx.h"
#include "http/tests/http_imposter.h"
#include "test_utils/fixture.h"
#include "test_utils/scoped_config.h"

#include <seastar/util/short_streams.hh>

#include <absl/container/flat_hash_set.h>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>

using namespace std::chrono_literals;

namespace cloud_storage {

bool operator==(const anomalies& lhs, const anomalies& rhs) {
    return lhs.missing_partition_manifest == rhs.missing_partition_manifest
           && lhs.missing_spillover_manifests == rhs.missing_spillover_manifests
           && lhs.missing_segments == rhs.missing_segments
           && lhs.segment_metadata_anomalies == rhs.segment_metadata_anomalies;

    // anomalies::last_complete_scrub is intentionally omitted
}

} // namespace cloud_storage

namespace {

cloud_storage::remote_path_provider path_provider(std::nullopt, std::nullopt);

ss::logger test_logger{"anomaly_detection_test"};

constexpr std::string_view stm_manifest = R"json(
{
  "version": 3,
  "namespace": "kafka",
  "topic": "panda-topic",
  "partition": 0,
  "revision": 1,
  "start_offset": 40,
  "last_offset": 59,
  "insync_offset": 100,
  "archive_start_offset": 0,
  "archive_start_offset_delta": 0,
  "archive_clean_offset": 0,
  "segments": {
      "40-1-v1.log": {
          "size_bytes": 1024,
          "base_offset": 40,
          "committed_offset": 49,
          "base_timestamp": 1000,
          "max_timestamp":  1000,
          "delta_offset": 8,
          "delta_offset_end": 10,
          "ntp_revision": 1,
          "archiver_term": 1,
          "segment_term": 1,
          "sname_format": 2
      },
      "50-1-v1.log": {
          "size_bytes": 1024,
          "base_offset": 50,
          "committed_offset": 59,
          "base_timestamp": 1000,
          "max_timestamp":  1000,
          "delta_offset": 10,
          "delta_offset_end": 12,
          "ntp_revision": 1,
          "archiver_term": 1,
          "segment_term": 1,
          "sname_format": 2
      }
  },
  "spillover": [
      {
          "size_bytes": 2048,
          "base_offset": 0,
          "committed_offset": 19,
          "base_timestamp": 1000,
          "max_timestamp":  1000,
          "delta_offset": 0,
          "delta_offset_end": 4,
          "ntp_revision": 1,
          "archiver_term": 1,
          "segment_term": 1,
          "sname_format": 3,
          "metadata_size_hint": 0
      },
      {
          "size_bytes": 2048,
          "base_offset": 20,
          "committed_offset": 39,
          "base_timestamp": 1000,
          "max_timestamp":  1000,
          "delta_offset": 4,
          "delta_offset_end": 8,
          "ntp_revision": 1,
          "archiver_term": 1,
          "segment_term": 1,
          "sname_format": 3,
          "metadata_size_hint": 0
      }
  ]
}
)json";

constexpr std::string_view spillover_manifest_at_0 = R"json(
{
  "version": 3,
  "namespace": "kafka",
  "topic": "panda-topic",
  "partition": 0,
  "revision": 1,
  "start_offset": 0,
  "last_offset": 19,
  "insync_offset": 10,
  "segments": {
      "0-1-v1.log": {
          "size_bytes": 1024,
          "base_offset": 0,
          "committed_offset": 9,
          "base_timestamp": 1000,
          "max_timestamp":  1000,
          "delta_offset": 0,
          "delta_offset_end": 2,
          "ntp_revision": 1,
          "archiver_term": 1,
          "segment_term": 1,
          "sname_format": 2
      },
      "10-1-v1.log": {
          "size_bytes": 1024,
          "base_offset": 10,
          "committed_offset": 19,
          "base_timestamp": 1000,
          "max_timestamp":  1000,
          "delta_offset": 2,
          "delta_offset_end": 4,
          "ntp_revision": 1,
          "archiver_term": 1,
          "segment_term": 1,
          "sname_format": 2
      }
  }
}
)json";

constexpr std::string_view spillover_manifest_at_20 = R"json(
{
  "version": 3,
  "namespace": "kafka",
  "topic": "panda-topic",
  "partition": 0,
  "revision": 1,
  "start_offset": 20,
  "last_offset": 39,
  "insync_offset": 20,
  "segments": {
      "20-1-v1.log": {
          "size_bytes": 1024,
          "base_offset": 20,
          "committed_offset": 29,
          "base_timestamp": 1000,
          "max_timestamp":  1000,
          "delta_offset": 4,
          "delta_offset_end": 6,
          "ntp_revision": 1,
          "archiver_term": 1,
          "segment_term": 1,
          "sname_format": 2
      },
      "30-1-v1.log": {
          "size_bytes": 1024,
          "base_offset": 30,
          "committed_offset": 39,
          "base_timestamp": 1000,
          "max_timestamp":  1000,
          "delta_offset": 6,
          "delta_offset_end": 8,
          "ntp_revision": 1,
          "archiver_term": 1,
          "segment_term": 1,
          "sname_format": 2
      }
  }
}
)json";

constexpr auto not_found_response = R"xml(
<Error>
  <Code>NoSuchKey</Code>
  <Message>Faked error message</Message>
  <RequestId>Faked request id</RequestId>
</Error>
)xml";

ss::input_stream<char> make_manifest_stream(std::string_view json) {
    iobuf i;
    i.append(json.data(), json.size());
    return make_iobuf_input_stream(std::move(i));
}

ss::sstring iobuf_to_string(iobuf buf) {
    auto input_stream = make_iobuf_input_stream(std::move(buf));
    return ss::util::read_entire_stream_contiguous(input_stream).get();
}

} // namespace

class bucket_view_fixture : public http_imposter_fixture {
public:
    static constexpr auto host_name = "localhost";
    static constexpr auto port = 4447;

    bucket_view_fixture()
      : http_imposter_fixture{port}
      , _root_rtc{_as}
      , _rtc_logger{test_logger, _root_rtc} {
        _pool
          .start(10, ss::sharded_parameter([this] {
                     return get_client_configuration();
                 }))
          .get();

        _io
          .start(
            std::ref(_pool),
            ss::sharded_parameter(
              [this] { return get_client_configuration(); }),
            ss::sharded_parameter(
              [] { return model::cloud_credentials_source::config_file; }))
          .get();
        _io.invoke_on_all([](cloud_io::remote& io) { return io.start(); })
          .get();
        _remote
          .start(std::ref(_io), ss::sharded_parameter([this] {
                     return get_client_configuration();
                 }))
          .get();

        _remote
          .invoke_on_all([](cloud_storage::remote& api) { return api.start(); })
          .get();
    }

    ~bucket_view_fixture() override {
        if (!_pool.local().shutdown_initiated()) {
            _pool.local().shutdown_connections();
        }
        _remote.stop().get();
        _io.stop().get();
        _pool.stop().get();
    }

    void init_view(
      std::string_view stm_manifest,
      std::vector<std::string_view> spillover_manifests) {
        parse_manifests(stm_manifest, std::move(spillover_manifests));

        check_spills_are_matching();

        set_expectations_for_manifest(_stm_manifest);
        remove_json_stm_manifest(_stm_manifest);

        for (const auto& spill : _spillover_manifests) {
            set_expectations_for_manifest(spill);
        }

        listen();

        _detector.emplace(
          cloud_storage_clients::bucket_name{"test-bucket"},
          _stm_manifest.get_ntp(),
          _stm_manifest.get_revision_id(),
          path_provider,
          _remote.local(),
          _rtc_logger,
          _as);
    }

    cloud_storage::anomalies_detector::result run_detector(
      cloud_storage::anomalies_detector::quota_limit quota,
      std::optional<model::offset> start_from = std::nullopt) {
        BOOST_REQUIRE(_detector.has_value());

        retry_chain_node anomaly_detection_rtc(1min, 100ms, &_root_rtc);
        auto res
          = _detector->run(anomaly_detection_rtc, quota, start_from).get();
        vlog(
          test_logger.info,
          "Anomalies detector run result: status={}, detected={}, "
          "last_scrubbed_offset={}",
          res.status,
          res.detected,
          res.last_scrubbed_offset);

        return res;
    }

    std::vector<cloud_storage::anomalies_detector::result>
    run_detector_until_log_end(archival::run_quota_t quota) {
        BOOST_REQUIRE(_detector.has_value());
        std::vector<cloud_storage::anomalies_detector::result> partial_results;

        auto iters = 1;

        auto res = run_detector(quota);
        partial_results.push_back(res);

        while (res.last_scrubbed_offset != std::nullopt) {
            BOOST_REQUIRE_LE(iters, 100);

            res = run_detector(quota, res.last_scrubbed_offset);
            partial_results.push_back(res);

            ++iters;
        }

        return partial_results;
    }

    cloud_storage::anomalies_detector::result flatten_partial_results(
      std::vector<cloud_storage::anomalies_detector::result> partial_results) {
        BOOST_REQUIRE(partial_results.size() > 0);
        auto res = *partial_results.begin();

        for (size_t i = 1; i < partial_results.size(); ++i) {
            res += std::move(partial_results[i]);
        }

        return res;
    }

    const cloud_storage::partition_manifest& get_stm_manifest() {
        return _stm_manifest;
    }

    cloud_storage::partition_manifest& get_stm_manifest_mut() {
        return _stm_manifest;
    }

    const std::vector<cloud_storage::spillover_manifest>&
    get_spillover_manifests() {
        return _spillover_manifests;
    }

    void remove_segment(
      const cloud_storage::partition_manifest& manifest,
      const cloud_storage::segment_meta& meta) {
        auto path = manifest.generate_segment_path(meta, path_provider);
        remove_object(ssx::sformat("/{}", path().string()));
    }

    void remove_manifest(const cloud_storage::partition_manifest& manifest) {
        auto path = manifest.get_manifest_path(path_provider);
        remove_object(ssx::sformat("/{}", path().string()));
    }

    struct write_hash_opts {
        absl::flat_hash_set<cloud_storage::segment_meta> skip_metas;
    };

    ss::future<> write_hashes_to_disk(const write_hash_opts& opts) {
        return cloud_storage::inventory::flush_ntp_hashes(
          config::node().cloud_storage_inventory_hash_path(),
          get_stm_manifest().get_ntp(),
          path_hashes(opts.skip_metas),
          0);
    }

    fragmented_vector<uint64_t> path_hashes(
      const absl::flat_hash_set<cloud_storage::segment_meta>& skip_metas) {
        std::vector<ss::sstring> paths;
        std::ranges::copy(
          manifest_paths(_stm_manifest, skip_metas), std::back_inserter(paths));
        for (const auto& m : _spillover_manifests) {
            std::ranges::copy(
              manifest_paths(m, skip_metas), std::back_inserter(paths));
        }
        return path_hashes(std::move(paths));
    }

private:
    void remove_json_stm_manifest(
      const cloud_storage::partition_manifest& manifest) {
        auto path = cloud_storage::prefixed_partition_manifest_json_path(
          manifest.get_ntp(), manifest.get_revision_id());
        remove_object(ssx::sformat("/{}", path));
    }

    void remove_object(ss::sstring full_path) {
        when()
          .request(full_path)
          .with_method(ss::httpd::operation_type::HEAD)
          .then_reply_with(
            std::vector<std::pair<ss::sstring, ss::sstring>>{
              {"x-amz-request-id", "fake-id"}},
            ss::http::reply::status_type::not_found);

        when()
          .request(full_path)
          .with_method(ss::httpd::operation_type::GET)
          .then_reply_with(
            not_found_response, ss::http::reply::status_type::not_found);
    }

    void parse_manifests(
      std::string_view stm_manifest,
      std::vector<std::string_view> spillover_manifests) {
        _stm_manifest
          .update(
            cloud_storage::manifest_format::json,
            make_manifest_stream(stm_manifest))
          .get();

        std::vector<cloud_storage::spillover_manifest> spills;
        for (const auto& spill_json : spillover_manifests) {
            cloud_storage::spillover_manifest spill_manifest{
              _stm_manifest.get_ntp(), _stm_manifest.get_revision_id()};
            spill_manifest
              .update(
                cloud_storage::manifest_format::json,
                make_manifest_stream(spill_json))
              .get();

            _spillover_manifests.emplace_back(std::move(spill_manifest));
        }
    }

    void check_spills_are_matching() {
        const auto& spill_map = _stm_manifest.get_spillover_map();
        for (const auto& spill : _spillover_manifests) {
            BOOST_REQUIRE(spill.get_start_offset().has_value());

            auto iter = spill_map.find(spill.get_start_offset().value());
            BOOST_REQUIRE(iter != spill_map.end());

            cloud_storage::spillover_manifest_path_components comp{
              .base = iter->base_offset,
              .last = iter->committed_offset,
              .base_kafka = iter->base_kafka_offset(),
              .next_kafka = iter->next_kafka_offset(),
              .base_ts = iter->base_timestamp,
              .last_ts = iter->max_timestamp,
            };

            auto spill_path = cloud_storage::remote_manifest_path{
              path_provider.spillover_manifest_path(_stm_manifest, comp)};
            BOOST_REQUIRE_EQUAL(
              spill_path, spill.get_manifest_path(path_provider));
        }
    }

    void set_expectations_for_manifest(
      const cloud_storage::partition_manifest& manifest) {
        const auto path = manifest.get_manifest_path(path_provider)().string();
        const auto reply_body = iobuf_to_string(manifest.to_iobuf());

        when()
          .request(fmt::format("/{}", path))
          .with_method(ss::httpd::operation_type::GET)
          .then_reply_with(reply_body);

        when()
          .request(fmt::format("/{}", path))
          .with_method(ss::httpd::operation_type::HEAD)
          .then_reply_with(
            {{"ETag", "blah-blah"},
             {"Content-Length", ssx::sformat("{}", reply_body.size())}},
            ss::http::reply::status_type::ok);

        set_expectations_for_segments(manifest);
    }

    void set_expectations_for_segments(
      const cloud_storage::partition_manifest& manifest) {
        for (const auto& seg : manifest) {
            auto path
              = manifest.generate_segment_path(seg, path_provider)().string();
            when()
              .request(fmt::format("/{}", path))
              .with_method(ss::httpd::operation_type::HEAD)
              .then_reply_with(
                {{"ETag", "blah-blah"},
                 {"Content-Length", ssx::sformat("{}", seg.size_bytes)}},
                ss::http::reply::status_type::ok);
        }
    }

    std::vector<ss::sstring> manifest_paths(
      const cloud_storage::partition_manifest& manifest,
      const absl::flat_hash_set<cloud_storage::segment_meta>& skip_metas) {
        std::vector<ss::sstring> paths;
        for (const auto& seg_meta : manifest) {
            if (!skip_metas.contains(seg_meta)) {
                paths.emplace_back(
                  manifest.generate_segment_path(seg_meta, path_provider)()
                    .string());
            }
        }
        return paths;
    }

    fragmented_vector<uint64_t> path_hashes(std::vector<ss::sstring> paths) {
        fragmented_vector<uint64_t> hashes;
        for (auto& path : paths) {
            hashes.push_back(xxhash_64(path.data(), path.size()));
        }
        return hashes;
    }

    cloud_storage_clients::s3_configuration get_client_configuration() {
        net::unresolved_address server_addr(host_name, port);

        cloud_storage_clients::s3_configuration conf;
        conf.uri = cloud_storage_clients::access_point_uri(host_name);
        conf.access_key = cloud_roles::public_key_str("access-key");
        conf.secret_key = cloud_roles::private_key_str("secret-key");
        conf.region = cloud_roles::aws_region_name("us-east-1");
        conf.url_style = cloud_storage_clients::s3_url_style::virtual_host;
        conf.server_addr = server_addr;
        conf._probe = ss::make_shared<cloud_storage_clients::client_probe>(
          net::metrics_disabled::yes,
          net::public_metrics_disabled::yes,
          cloud_roles::aws_region_name{"us-east-1"},
          cloud_storage_clients::endpoint_url{httpd_host_name});

        return conf;
    }

    ss::abort_source _as;
    retry_chain_node _root_rtc;
    retry_chain_logger _rtc_logger;

    ss::sharded<cloud_storage_clients::client_pool> _pool;
    ss::sharded<cloud_io::remote> _io;
    ss::sharded<cloud_storage::remote> _remote;

    cloud_storage::partition_manifest _stm_manifest;
    std::vector<cloud_storage::spillover_manifest> _spillover_manifests;

    std::optional<cloud_storage::anomalies_detector> _detector;
};

FIXTURE_TEST(test_no_anomalies, bucket_view_fixture) {
    init_view(
      stm_manifest, {spillover_manifest_at_0, spillover_manifest_at_20});

    {
        auto result = run_detector(archival::run_quota_t{100});
        BOOST_REQUIRE_EQUAL(result.status, cloud_storage::scrub_status::full);
        BOOST_REQUIRE(!result.detected.has_value());
        BOOST_REQUIRE(!result.last_scrubbed_offset.has_value());
        BOOST_REQUIRE(result.detected.segment_existence_checked);
    }

    {
        auto result = run_detector(archival::run_quota_t{5});
        BOOST_REQUIRE_EQUAL(
          result.status, cloud_storage::scrub_status::partial);
        BOOST_REQUIRE(!result.detected.has_value());

        // We have a quota of 5 requests:
        // * 1 download request for the partitions manifest
        // * 2 requests to check for the existence of spill manifests
        // * 2 requests to check for the existence of the segments in the stm
        // manifest
        BOOST_REQUIRE_EQUAL(
          result.last_scrubbed_offset, get_stm_manifest().get_last_offset());
        BOOST_REQUIRE(result.detected.segment_existence_checked);
    }
}

FIXTURE_TEST(test_missing_segments, bucket_view_fixture) {
    init_view(
      stm_manifest, {spillover_manifest_at_0, spillover_manifest_at_20});

    const auto& stm_segment = get_stm_manifest().begin();
    remove_segment(get_stm_manifest(), *stm_segment);

    const auto& first_spill = get_spillover_manifests().at(0);
    const auto& spill_segment = first_spill.begin();
    remove_segment(first_spill, *spill_segment);

    const auto result = run_detector(archival::run_quota_t{100});
    BOOST_REQUIRE_EQUAL(result.status, cloud_storage::scrub_status::full);

    BOOST_REQUIRE(result.detected.has_value());

    const auto& missing_segs = result.detected.missing_segments;
    BOOST_REQUIRE_EQUAL(missing_segs.size(), 2);
    BOOST_REQUIRE(missing_segs.contains(*stm_segment));
    BOOST_REQUIRE(missing_segs.contains(*spill_segment));

    auto partial_results = run_detector_until_log_end(archival::run_quota_t{6});

    BOOST_REQUIRE_EQUAL(
      result.detected, flatten_partial_results(partial_results).detected);
    BOOST_REQUIRE(result.detected.segment_existence_checked);
}

FIXTURE_TEST(test_segment_depth_limit, bucket_view_fixture) {
    // test that a segment limit is respected, by removing a segment and
    // checking results
    init_view(
      stm_manifest, {spillover_manifest_at_0, spillover_manifest_at_20});
    // precondition check: this test will limit the depth to 5 segments and
    // remove the 6th, so check that the layout is as expected
    BOOST_REQUIRE_EQUAL(get_stm_manifest().size(), 2);
    BOOST_REQUIRE_EQUAL(get_spillover_manifests().at(0).size(), 2);
    BOOST_REQUIRE_EQUAL(get_spillover_manifests().at(1).size(), 2);

    // check that with depth 0 we still process at least 1 segment
    auto fwd_progress_res = run_detector(
      cloud_storage::anomalies_detector::segment_depth_t{0});
    BOOST_CHECK_EQUAL(fwd_progress_res.detected.missing_segments.size(), 0);
    BOOST_CHECK_EQUAL(fwd_progress_res.segments_visited, 1);

    // remove the 6th segment
    remove_segment(
      get_spillover_manifests().at(0),
      *get_spillover_manifests().at(0).begin());

    // the check with depth limit of 5 should pass
    auto pass_result = run_detector(
      cloud_storage::anomalies_detector::segment_depth_t{5});
    BOOST_CHECK_EQUAL(pass_result.detected.missing_segments.size(), 0);
    BOOST_CHECK_EQUAL(pass_result.segments_visited, 5);

    // the check with depth limit of 6 should detect an anomaly
    auto fail_result = run_detector(
      cloud_storage::anomalies_detector::segment_depth_t{6});
    BOOST_CHECK_GT(fail_result.detected.missing_segments.size(), 0);
    BOOST_CHECK_EQUAL(fail_result.segments_visited, 6);
}

FIXTURE_TEST(test_missing_spillover_manifest, bucket_view_fixture) {
    init_view(
      stm_manifest, {spillover_manifest_at_0, spillover_manifest_at_20});

    const auto& first_spill = get_spillover_manifests().at(0);
    const auto& spill_segment = first_spill.begin();
    remove_manifest(first_spill);

    const auto result = run_detector(archival::run_quota_t{100});
    BOOST_REQUIRE_EQUAL(result.status, cloud_storage::scrub_status::full);

    BOOST_REQUIRE(result.detected.has_value());

    const auto& missing_spills = result.detected.missing_spillover_manifests;
    BOOST_REQUIRE_EQUAL(missing_spills.size(), 1);
    auto expected_path = cloud_storage::remote_manifest_path{
      path_provider.spillover_manifest_path(
        first_spill, *missing_spills.begin())};
    BOOST_REQUIRE_EQUAL(
      first_spill.get_manifest_path(path_provider), expected_path);

    auto partial_results = run_detector_until_log_end(archival::run_quota_t{6});

    BOOST_REQUIRE_EQUAL(
      result.detected, flatten_partial_results(partial_results).detected);
}

FIXTURE_TEST(test_missing_stm_manifest, bucket_view_fixture) {
    init_view(
      stm_manifest, {spillover_manifest_at_0, spillover_manifest_at_20});

    remove_manifest(get_stm_manifest());

    const auto result = run_detector(archival::run_quota_t{100});
    BOOST_REQUIRE_EQUAL(result.status, cloud_storage::scrub_status::full);

    BOOST_REQUIRE(result.detected.has_value());
    BOOST_REQUIRE_EQUAL(result.detected.missing_partition_manifest, true);

    auto partial_results = run_detector_until_log_end(archival::run_quota_t{6});

    BOOST_REQUIRE_EQUAL(
      result.detected, flatten_partial_results(partial_results).detected);
}

FIXTURE_TEST(test_metadata_anomalies, bucket_view_fixture) {
    /*
     * Test the detection of offset anomalies when the issues span manifest
     * boundaries. In this case the last spillover manifest overlaps with the
     * stm manifest, there's a gap between the two spillover manifests and the
     * delta offsets are non monotonical in the stm manifest.
     */

    constexpr std::string_view stm_man = R"json(
{
  "version": 3,
  "namespace": "kafka",
  "topic": "panda-topic",
  "partition": 0,
  "revision": 1,
  "start_offset": 21,
  "last_offset": 40,
  "insync_offset": 100,
  "segments": {
      "21-1-v1.log": {
          "size_bytes": 1024,
          "base_offset": 21,
          "committed_offset": 30,
          "base_timestamp": 1000,
          "max_timestamp":  1000,
          "delta_offset": 8,
          "delta_offset_end": 10,
          "ntp_revision": 1,
          "archiver_term": 1,
          "segment_term": 1,
          "sname_format": 2
      },
      "31-1-v1.log": {
          "size_bytes": 1024,
          "base_offset": 31,
          "committed_offset": 40,
          "base_timestamp": 1000,
          "max_timestamp":  1000,
          "delta_offset": 7,
          "delta_offset_end": 10,
          "ntp_revision": 1,
          "archiver_term": 1,
          "segment_term": 1,
          "sname_format": 2
      }
  },
  "spillover": [
      {
          "size_bytes": 2048,
          "base_offset": 0,
          "committed_offset": 8,
          "base_timestamp": 1000,
          "max_timestamp":  1000,
          "delta_offset": 0,
          "delta_offset_end": 4,
          "ntp_revision": 1,
          "archiver_term": 1,
          "segment_term": 1,
          "sname_format": 3,
          "metadata_size_hint": 0
      },
      {
          "size_bytes": 2048,
          "base_offset": 11,
          "committed_offset": 23,
          "base_timestamp": 1000,
          "max_timestamp":  1000,
          "delta_offset": 4,
          "delta_offset_end": 8,
          "ntp_revision": 1,
          "archiver_term": 1,
          "segment_term": 1,
          "sname_format": 3,
          "metadata_size_hint": 0
      }
  ]
}
)json";

    constexpr std::string_view first_spill_man = R"json(
{
  "version": 3,
  "namespace": "kafka",
  "topic": "panda-topic",
  "partition": 0,
  "revision": 1,
  "start_offset": 0,
  "last_offset": 8,
  "insync_offset": 10,
  "segments": {
      "0-1-v1.log": {
          "size_bytes": 1024,
          "base_offset": 0,
          "committed_offset": 8,
          "base_timestamp": 1000,
          "max_timestamp":  1000,
          "delta_offset": 0,
          "delta_offset_end": 4,
          "ntp_revision": 1,
          "archiver_term": 1,
          "segment_term": 1,
          "sname_format": 2
      }
  }
}
)json";

    constexpr std::string_view last_spill_man = R"json(
{
  "version": 3,
  "namespace": "kafka",
  "topic": "panda-topic",
  "partition": 0,
  "revision": 1,
  "start_offset": 11,
  "last_offset": 23,
  "insync_offset": 20,
  "segments": {
      "11-1-v1.log": {
          "size_bytes": 1024,
          "base_offset": 11,
          "committed_offset": 23,
          "base_timestamp": 1000,
          "max_timestamp":  1000,
          "delta_offset": 4,
          "delta_offset_end": 8,
          "ntp_revision": 1,
          "archiver_term": 1,
          "segment_term": 1,
          "sname_format": 2
      }
  }
}
)json";

    init_view(stm_man, {first_spill_man, last_spill_man});

    const auto result = run_detector(archival::run_quota_t{100});
    BOOST_REQUIRE_EQUAL(result.status, cloud_storage::scrub_status::full);

    BOOST_REQUIRE(result.detected.has_value());

    const auto& offset_anomalies = result.detected.segment_metadata_anomalies;
    for (const auto& a : offset_anomalies) {
        vlog(test_logger.info, "Offset anomaly detected: {}", a);
    }

    BOOST_REQUIRE_EQUAL(offset_anomalies.size(), 3);

    cloud_storage::anomalies expected;
    // Bad deltas in STM manifest
    expected.segment_metadata_anomalies.insert(cloud_storage::anomaly_meta{
      .type = cloud_storage::anomaly_type::non_monotonical_delta,
      .at = *get_stm_manifest().last_segment(),
      .previous = *get_stm_manifest().begin()});

    // Overlap between spillover and STM manifest
    expected.segment_metadata_anomalies.insert(cloud_storage::anomaly_meta{
      .type = cloud_storage::anomaly_type::offset_overlap,
      .at = *get_stm_manifest().begin(),
      .previous = get_spillover_manifests().at(1).last_segment()});

    // Gap between spillover manifests
    expected.segment_metadata_anomalies.insert(cloud_storage::anomaly_meta{
      .type = cloud_storage::anomaly_type::offset_gap,
      .at = *get_spillover_manifests().at(1).begin(),
      .previous = get_spillover_manifests().at(0).last_segment()});

    BOOST_REQUIRE(result.detected == expected);

    auto partial_results = run_detector_until_log_end(archival::run_quota_t{6});
    BOOST_REQUIRE_EQUAL(
      result.detected, flatten_partial_results(partial_results).detected);
}

FIXTURE_TEST(test_filtering_of_segment_merge, bucket_view_fixture) {
    /*
     * Test for perfect storm edge case:
     * 1. Scrubber downloads stm manifest
     * 2. Adjacent segment merger runs and finds a candidate
     * 3. Manifest is re-uploaded
     * 4. Housekeeping garbage collection runs and deletes replaced segments
     *
     * While unlikely, this sequence of events is valid and the anomaly
     * filtering should be smart enough to detect it.
     */
    constexpr std::string_view stm_man = R"json(
{
  "version": 3,
  "namespace": "kafka",
  "topic": "panda-topic",
  "partition": 0,
  "revision": 1,
  "start_offset": 0,
  "last_offset": 39,
  "insync_offset": 100,
  "segments": {
      "0-1-v1.log": {
          "size_bytes": 1024,
          "base_offset": 0,
          "committed_offset": 9,
          "base_timestamp": 1000,
          "max_timestamp":  1000,
          "delta_offset": 0,
          "delta_offset_end": 2,
          "ntp_revision": 1,
          "archiver_term": 1,
          "segment_term": 1,
          "sname_format": 2
      },
      "10-1-v1.log": {
          "size_bytes": 1024,
          "base_offset": 10,
          "committed_offset": 19,
          "base_timestamp": 1000,
          "max_timestamp":  1000,
          "delta_offset": 2,
          "delta_offset_end": 4,
          "ntp_revision": 1,
          "archiver_term": 1,
          "segment_term": 1,
          "sname_format": 2
      },
      "20-1-v1.log": {
          "size_bytes": 1024,
          "base_offset": 10,
          "committed_offset": 29,
          "base_timestamp": 1000,
          "max_timestamp":  1000,
          "delta_offset": 4,
          "delta_offset_end": 3,
          "ntp_revision": 1,
          "archiver_term": 1,
          "segment_term": 1,
          "sname_format": 2
      },
      "30-1-v1.log": {
          "size_bytes": 1024,
          "base_offset": 30,
          "committed_offset": 39,
          "base_timestamp": 1000,
          "max_timestamp":  1000,
          "delta_offset": 6,
          "delta_offset_end": 8,
          "ntp_revision": 1,
          "archiver_term": 1,
          "segment_term": 1,
          "sname_format": 2
      }
  }
}
)json";

    init_view(stm_man, {});

    const auto first_seg = *std::next(get_stm_manifest().begin());
    const auto last_seg = *std::next(get_stm_manifest().begin(), 2);

    remove_segment(get_stm_manifest(), first_seg);
    remove_segment(get_stm_manifest(), last_seg);

    const auto result = run_detector(archival::run_quota_t{100});
    BOOST_REQUIRE_EQUAL(result.status, cloud_storage::scrub_status::full);
    BOOST_REQUIRE(result.detected.has_value());
    BOOST_REQUIRE_EQUAL(result.detected.missing_segments.size(), 2);
    BOOST_REQUIRE_EQUAL(result.detected.segment_metadata_anomalies.size(), 1);

    // Run the detection again but under very strict quota. A complete
    // scrub will require multiple partial scrubs.
    auto partial_results = run_detector_until_log_end(archival::run_quota_t{2});

    BOOST_REQUIRE_EQUAL(
      result.detected, flatten_partial_results(partial_results).detected);

    cloud_storage::segment_meta merged_seg{
      .is_compacted = false,
      .size_bytes = first_seg.size_bytes + last_seg.size_bytes,
      .base_offset = first_seg.base_offset,
      .committed_offset = last_seg.committed_offset,
      .base_timestamp = first_seg.base_timestamp,
      .max_timestamp = last_seg.max_timestamp,
      .delta_offset = first_seg.delta_offset,
      .ntp_revision = first_seg.ntp_revision,
      .segment_term = first_seg.segment_term,
      .delta_offset_end = last_seg.delta_offset_end};

    BOOST_REQUIRE(get_stm_manifest_mut().safe_segment_meta_to_add(merged_seg));
    BOOST_REQUIRE(get_stm_manifest_mut().add(merged_seg));

    auto manifest_clone = get_stm_manifest().clone();

    get_stm_manifest_mut().process_anomalies(
      model::timestamp::now(),
      result.last_scrubbed_offset,
      result.status,
      result.detected);

    const auto& filtered_anomalies = get_stm_manifest().detected_anomalies();

    BOOST_REQUIRE_EQUAL(filtered_anomalies.missing_segments.size(), 0);
    BOOST_REQUIRE(!filtered_anomalies.has_value());

    for (const auto& result : partial_results) {
        manifest_clone.process_anomalies(
          model::timestamp::now(),
          result.last_scrubbed_offset,
          result.status,
          result.detected);
    }

    const auto& filtered_from_partials = manifest_clone.detected_anomalies();

    BOOST_REQUIRE_EQUAL(filtered_anomalies, filtered_from_partials);
}

FIXTURE_TEST(test_filtering_of_archive_segments, bucket_view_fixture) {
    /*
     * This test deletes two segment objects from the cloud: one in the STM
     * region region and another one in the archive region. Verify that the
     * filtering logic keeps the missing segment from the archive.
     */

    init_view(
      stm_manifest, {spillover_manifest_at_0, spillover_manifest_at_20});

    remove_segment(get_stm_manifest(), *get_stm_manifest().begin());
    remove_segment(
      get_spillover_manifests().at(0),
      *get_spillover_manifests().at(0).begin());

    const auto result = run_detector(archival::run_quota_t{100});
    BOOST_REQUIRE_EQUAL(result.status, cloud_storage::scrub_status::full);
    BOOST_REQUIRE(result.detected.has_value());
    BOOST_REQUIRE_EQUAL(result.detected.missing_segments.size(), 2);

    // Run the detection again but under very strict quota. A complete
    // scrub will require multiple partial scrubs.
    auto partial_results = run_detector_until_log_end(archival::run_quota_t{2});

    BOOST_REQUIRE_EQUAL(
      result.detected, flatten_partial_results(partial_results).detected);

    auto manifest_updated_by_full_scrub = get_stm_manifest().clone();
    auto manifest_updated_by_partial_scrubs = get_stm_manifest().clone();

    manifest_updated_by_full_scrub.process_anomalies(
      model::timestamp::now(),
      result.last_scrubbed_offset,
      result.status,
      result.detected);

    for (const auto& result : partial_results) {
        manifest_updated_by_partial_scrubs.process_anomalies(
          model::timestamp::now(),
          result.last_scrubbed_offset,
          result.status,
          result.detected);
    }

    const auto& filtered_from_partials
      = manifest_updated_by_full_scrub.detected_anomalies();
    const auto& filtered_from_full
      = manifest_updated_by_partial_scrubs.detected_anomalies();

    BOOST_REQUIRE_EQUAL(filtered_from_partials, filtered_from_full);
    BOOST_REQUIRE_EQUAL(filtered_from_partials.missing_segments.size(), 2);
}

BOOST_AUTO_TEST_CASE(test_offset_anomaly_detection) {
    using namespace cloud_storage;

    {
        segment_meta_anomalies anomalies;

        segment_meta prev{
          .base_offset = model::offset{0},
          .committed_offset = model::offset{10}};

        segment_meta crnt{
          .base_offset = model::offset{11},
          .committed_offset = model::offset{15}};

        scrub_segment_meta(crnt, prev, anomalies);
        BOOST_REQUIRE(anomalies.empty());

        prev.delta_offset = model::offset_delta{5};
        scrub_segment_meta(crnt, prev, anomalies);
        BOOST_REQUIRE_EQUAL(anomalies.size(), 1);

        const anomaly_meta expected = anomaly_meta{
          .type = anomaly_type::missing_delta, .at = crnt, .previous = prev};
        BOOST_REQUIRE_EQUAL(*anomalies.begin(), expected);
    }

    {
        segment_meta_anomalies anomalies;

        segment_meta prev{
          .base_offset = model::offset{0},
          .committed_offset = model::offset{10}};

        segment_meta crnt{
          .base_offset = model::offset{11},
          .committed_offset = model::offset{15},
          .delta_offset = model::offset_delta{2}};

        scrub_segment_meta(crnt, prev, anomalies);
        BOOST_REQUIRE(anomalies.empty());

        prev.delta_offset = model::offset_delta{4};
        scrub_segment_meta(crnt, prev, anomalies);
        BOOST_REQUIRE_EQUAL(anomalies.size(), 1);

        const anomaly_meta expected = anomaly_meta{
          .type = anomaly_type::non_monotonical_delta,
          .at = crnt,
          .previous = prev};
        BOOST_REQUIRE_EQUAL(*anomalies.begin(), expected);
    }

    {
        segment_meta_anomalies anomalies;

        segment_meta crnt{
          .base_offset = model::offset{11},
          .committed_offset = model::offset{15},
          .delta_offset = model::offset_delta{2},
          .delta_offset_end = model::offset_delta{4}};

        scrub_segment_meta(crnt, std::nullopt, anomalies);
        BOOST_REQUIRE(anomalies.empty());

        crnt.delta_offset = model::offset_delta{5};
        scrub_segment_meta(crnt, std::nullopt, anomalies);
        BOOST_REQUIRE_EQUAL(anomalies.size(), 1);

        const anomaly_meta expected = anomaly_meta{
          .type = anomaly_type::end_delta_smaller, .at = crnt};
        BOOST_REQUIRE_EQUAL(*anomalies.begin(), expected);
    }

    {
        segment_meta_anomalies anomalies;

        segment_meta prev{
          .base_offset = model::offset{0},
          .committed_offset = model::offset{10}};

        segment_meta crnt{
          .base_offset = model::offset{11},
          .committed_offset = model::offset{15}};

        scrub_segment_meta(crnt, prev, anomalies);
        BOOST_REQUIRE(anomalies.empty());

        prev.committed_offset = model::offset{8};
        scrub_segment_meta(crnt, prev, anomalies);
        BOOST_REQUIRE_EQUAL(anomalies.size(), 1);

        const anomaly_meta expected = anomaly_meta{
          .type = anomaly_type::offset_gap, .at = crnt, .previous = prev};
        BOOST_REQUIRE_EQUAL(*anomalies.begin(), expected);
    }

    {
        segment_meta_anomalies anomalies;

        segment_meta prev{
          .base_offset = model::offset{0},
          .committed_offset = model::offset{10}};

        segment_meta crnt{
          .base_offset = model::offset{11},
          .committed_offset = model::offset{15}};

        scrub_segment_meta(crnt, prev, anomalies);
        BOOST_REQUIRE(anomalies.empty());

        prev.committed_offset = model::offset{13};
        scrub_segment_meta(crnt, prev, anomalies);
        BOOST_REQUIRE_EQUAL(anomalies.size(), 1);

        const anomaly_meta expected = anomaly_meta{
          .type = anomaly_type::offset_overlap, .at = crnt, .previous = prev};
        BOOST_REQUIRE_EQUAL(*anomalies.begin(), expected);
    }
}

BOOST_AUTO_TEST_CASE(test_anomalies_size_limit) {
    using namespace cloud_storage;

    anomalies result;

    {
        anomalies tmp;
        for (int i = 0; i < 80; i++) {
            tmp.missing_segments.insert(segment_meta{
              .base_offset = model::offset{i},
            });
        }
        result += std::move(tmp);
    }
    BOOST_REQUIRE_EQUAL(result.missing_segments.size(), 80);
    BOOST_REQUIRE_EQUAL(result.num_discarded_missing_segments, 0);

    {
        anomalies tmp;
        for (int i = 100; i < 180; i++) {
            tmp.missing_segments.insert(segment_meta{
              .base_offset = model::offset{i},
            });
        }
        result += std::move(tmp);
    }
    BOOST_REQUIRE_EQUAL(result.missing_segments.size(), 100);
    BOOST_REQUIRE_EQUAL(result.num_discarded_missing_segments, 60);

    {
        anomalies tmp;
        for (int i = 200; i < 400; i++) {
            tmp.missing_segments.insert(segment_meta{
              .base_offset = model::offset{i},
            });
        }
        result += std::move(tmp);
    }
    BOOST_REQUIRE_EQUAL(result.missing_segments.size(), 100);
    BOOST_REQUIRE_EQUAL(result.num_discarded_missing_segments, 260);
}

BOOST_AUTO_TEST_CASE(test_anomalies_size_limit2) {
    // Test situation when the initial anomalies batch is large
    using namespace cloud_storage;

    anomalies result;
    {
        for (int i = 0; i < 200; i++) {
            result.missing_segments.insert(segment_meta{
              .base_offset = model::offset{i},
            });
        }
    }

    {
        anomalies tmp;
        for (int i = 200; i < 280; i++) {
            tmp.missing_segments.insert(segment_meta{
              .base_offset = model::offset{i},
            });
        }
        result += std::move(tmp);
    }
    BOOST_REQUIRE_EQUAL(result.missing_segments.size(), 100);
    BOOST_REQUIRE_EQUAL(result.num_discarded_missing_segments, 180);
}

SEASTAR_THREAD_TEST_CASE(test_should_make_api_call_when_inv_scrub_disabled) {
    // If inventory scrub is disabled, the usage of query object implies that
    // scrub is intentionally done without inventory data, so API calls should
    // be enabled.
    scoped_config sc{};
    sc.get("cloud_storage_inventory_based_scrub_enabled").set_value(false);
    auto q
      = cloud_storage::existence_query_context::load(false, model::ntp{}).get();
    BOOST_REQUIRE(!q.is_inv_data_available);
    BOOST_REQUIRE(q.should_lookup_in_cloud_storage({}));
}

SEASTAR_THREAD_TEST_CASE(test_should_not_make_api_call_when_inv_data_missing) {
    // Inv. scrub is enabled but no data found on disk.  The intention behind
    // using inventory data is to avoid API calls, so skip making calls because
    // we will have to make one call for every segment.
    scoped_config sc{};
    sc.get("cloud_storage_inventory_based_scrub_enabled").set_value(true);
    auto q
      = cloud_storage::existence_query_context::load(false, model::ntp{}).get();
    BOOST_REQUIRE(!q.is_inv_data_available);
    BOOST_REQUIRE(!q.should_lookup_in_cloud_storage({}));
}

SEASTAR_THREAD_TEST_CASE(
  test_should_make_api_call_when_inv_data_missing_and_override_set) {
    // If override is set, make calls even if the data set is missing
    scoped_config sc{};
    sc.get("cloud_storage_inventory_based_scrub_enabled").set_value(true);
    auto q
      = cloud_storage::existence_query_context::load(true, model::ntp{}).get();
    BOOST_REQUIRE(!q.is_inv_data_available);
    BOOST_REQUIRE(q.should_lookup_in_cloud_storage({}));
}

SEASTAR_THREAD_TEST_CASE(test_query_lookup_when_data_loaded_successfully) {
    // The happy path - scrub enabled, data found on disk.
    scoped_config sc{};
    sc.get("cloud_storage_inventory_based_scrub_enabled").set_value(true);

    model::ntp ntp{model::ns{"n"}, model::topic{"t"}, model::partition_id{0}};
    constexpr std::string_view test_path{"t"};

    cloud_storage::inventory::flush_ntp_hashes(
      config::node().cloud_storage_inventory_hash_path(),
      ntp,
      {xxhash_64(test_path.data(), test_path.size())},
      0)
      .get();
    auto q = cloud_storage::existence_query_context::load(false, ntp).get();

    BOOST_REQUIRE(q.is_inv_data_available);
    BOOST_REQUIRE(!q.should_lookup_in_cloud_storage(
      cloud_storage::remote_segment_path{test_path}));
}

namespace {

bool is_call_to_check_for_segment(const http_test_utils::request_info ri) {
    return ri.method == "HEAD" && ri.url.ends_with(".log.1");
}

} // namespace

FIXTURE_TEST(test_no_calls_made_for_segment_checks, bucket_view_fixture) {
    scoped_config sc{};
    sc.get("cloud_storage_inventory_based_scrub_enabled").set_value(true);

    init_view(
      stm_manifest, {spillover_manifest_at_0, spillover_manifest_at_20});

    write_hashes_to_disk({.skip_metas = {}}).get();

    // 5 ops can manage a full scrub: 1 stm download, 2 HEAD checks for spill, 2
    // downloads for spill
    auto result = run_detector(archival::run_quota_t{5});
    BOOST_REQUIRE_EQUAL(result.status, cloud_storage::scrub_status::full);
    BOOST_REQUIRE(!result.detected.has_value());
    BOOST_REQUIRE(!result.last_scrubbed_offset.has_value());

    // No segments are checked using API calls
    auto possible_calls_for_segment_check = get_requests(
      is_call_to_check_for_segment);
    BOOST_REQUIRE(possible_calls_for_segment_check.empty());
    BOOST_REQUIRE(result.detected.segment_existence_checked);
}

FIXTURE_TEST(test_calls_made_when_segment_missing, bucket_view_fixture) {
    scoped_config sc{};
    sc.get("cloud_storage_inventory_based_scrub_enabled").set_value(true);

    init_view(
      stm_manifest, {spillover_manifest_at_0, spillover_manifest_at_20});

    const auto& first_spill = get_spillover_manifests().at(0);
    const auto& spill_segment = first_spill.begin();
    remove_segment(first_spill, *spill_segment);

    write_hashes_to_disk({.skip_metas = {*spill_segment}}).get();

    auto result = run_detector(archival::run_quota_t{6});
    BOOST_REQUIRE_EQUAL(result.status, cloud_storage::scrub_status::full);
    BOOST_REQUIRE(result.detected.has_value());
    const auto& missing_segs = result.detected.missing_segments;
    BOOST_REQUIRE_EQUAL(missing_segs.size(), 1);
    BOOST_REQUIRE(missing_segs.contains(*spill_segment));

    auto possible_calls_for_segment_check = get_requests(
      is_call_to_check_for_segment);
    BOOST_REQUIRE_EQUAL(possible_calls_for_segment_check.size(), 1);
    BOOST_REQUIRE(result.detected.segment_existence_checked);

    auto path = first_spill.generate_segment_path(
      *spill_segment, path_provider);
    BOOST_REQUIRE_EQUAL(
      fmt::format("/{}", path().native()),
      possible_calls_for_segment_check[0].url);
}
