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
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/types.h"
#include "http/tests/http_imposter.h"
#include "test_utils/fixture.h"

#include <seastar/util/short_streams.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

namespace {

ss::logger test_logger{"anomaly_detection_test"};

constexpr std::string_view stm_manifest = R"json(
{
  "version": 3,
  "namespace": "kafka",
  "topic": "panda-topic",
  "partition": 0,
  "revision": 1,
  "start_offset": 0,
  "last_offset": 59,
  "insync_offset": 100,
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

class bucket_view_fixture : http_imposter_fixture {
public:
    static constexpr auto host_name = "127.0.0.1";
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

        _remote
          .start(
            std::ref(_pool),
            ss::sharded_parameter(
              [this] { return get_client_configuration(); }),
            ss::sharded_parameter(
              [] { return model::cloud_credentials_source::config_file; }))
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
          cloud_storage_clients::bucket_name{"test_bucket"},
          _stm_manifest.get_ntp(),
          _stm_manifest.get_revision_id(),
          _remote.local(),
          _rtc_logger,
          _as);
    }

    cloud_storage::anomalies_detector::result run_detector() {
        BOOST_REQUIRE(_detector.has_value());

        retry_chain_node anomaly_detection_rtc(1min, 100ms, &_root_rtc);
        auto res = _detector->run(anomaly_detection_rtc).get();
        vlog(
          test_logger.info,
          "Anomalies detector run result: status={}, detected={}",
          res.status,
          res.detected);

        return res;
    }

    const cloud_storage::partition_manifest& get_stm_manifest() {
        return _stm_manifest;
    }

    const std::vector<cloud_storage::spillover_manifest>&
    get_spillover_manifests() {
        return _spillover_manifests;
    }

    void remove_segment(
      const cloud_storage::partition_manifest& manifest,
      const cloud_storage::segment_meta& meta) {
        auto path = manifest.generate_segment_path(meta);
        remove_object(ssx::sformat("/{}", path().string()));
    }

    void remove_manifest(const cloud_storage::partition_manifest& manifest) {
        auto path = manifest.get_manifest_path();
        remove_object(ssx::sformat("/{}", path().string()));
    }

private:
    void remove_json_stm_manifest(
      const cloud_storage::partition_manifest& manifest) {
        auto path = manifest.get_manifest_path(
          cloud_storage::manifest_format::json);
        remove_object(ssx::sformat("/{}", path().string()));
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

            auto spill_path = cloud_storage::generate_spillover_manifest_path(
              _stm_manifest.get_ntp(), _stm_manifest.get_revision_id(), comp);
            BOOST_REQUIRE_EQUAL(spill_path, spill.get_manifest_path());
        }
    }

    void set_expectations_for_manifest(
      const cloud_storage::partition_manifest& manifest) {
        const auto path = manifest.get_manifest_path()().string();
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
            auto path = manifest.generate_segment_path(seg)().string();
            when()
              .request(fmt::format("/{}", path))
              .with_method(ss::httpd::operation_type::HEAD)
              .then_reply_with(
                {{"ETag", "blah-blah"},
                 {"Content-Length", ssx::sformat("{}", seg.size_bytes)}},
                ss::http::reply::status_type::ok);
        }
    }

    cloud_storage_clients::s3_configuration get_client_configuration() {
        net::unresolved_address server_addr(host_name, port);

        cloud_storage_clients::s3_configuration conf;
        conf.uri = cloud_storage_clients::access_point_uri(host_name);
        conf.access_key = cloud_roles::public_key_str("acess-key");
        conf.secret_key = cloud_roles::private_key_str("secret-key");
        conf.region = cloud_roles::aws_region_name("us-east-1");
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
    ss::sharded<cloud_storage::remote> _remote;

    cloud_storage::partition_manifest _stm_manifest;
    std::vector<cloud_storage::spillover_manifest> _spillover_manifests;

    std::optional<cloud_storage::anomalies_detector> _detector;
};

FIXTURE_TEST(test_no_anomalies, bucket_view_fixture) {
    init_view(
      stm_manifest, {spillover_manifest_at_0, spillover_manifest_at_20});

    auto result = run_detector();
    BOOST_REQUIRE_EQUAL(result.status, cloud_storage::scrub_status::full);

    BOOST_REQUIRE(!result.detected.has_value());
}

FIXTURE_TEST(test_missing_segments, bucket_view_fixture) {
    init_view(
      stm_manifest, {spillover_manifest_at_0, spillover_manifest_at_20});

    const auto& stm_segment = get_stm_manifest().begin();
    remove_segment(get_stm_manifest(), *stm_segment);

    const auto& first_spill = get_spillover_manifests().at(0);
    const auto& spill_segment = first_spill.begin();
    remove_segment(first_spill, *spill_segment);

    const auto result = run_detector();
    BOOST_REQUIRE_EQUAL(result.status, cloud_storage::scrub_status::full);

    BOOST_REQUIRE(result.detected.has_value());

    const auto& missing_segs = result.detected.missing_segments;
    BOOST_REQUIRE_EQUAL(missing_segs.size(), 2);
    BOOST_REQUIRE(missing_segs.contains(*stm_segment));
    BOOST_REQUIRE(missing_segs.contains(*spill_segment));
}

FIXTURE_TEST(test_missing_spillover_manifest, bucket_view_fixture) {
    init_view(
      stm_manifest, {spillover_manifest_at_0, spillover_manifest_at_20});

    const auto& first_spill = get_spillover_manifests().at(0);
    const auto& spill_segment = first_spill.begin();
    remove_manifest(first_spill);

    const auto result = run_detector();
    BOOST_REQUIRE_EQUAL(result.status, cloud_storage::scrub_status::full);

    BOOST_REQUIRE(result.detected.has_value());

    const auto& missing_spills = result.detected.missing_spillover_manifests;
    BOOST_REQUIRE_EQUAL(missing_spills.size(), 1);
    const auto expected_path = cloud_storage::generate_spillover_manifest_path(
      first_spill.get_ntp(),
      first_spill.get_revision_id(),
      *missing_spills.begin());
    BOOST_REQUIRE_EQUAL(first_spill.get_manifest_path(), expected_path);
}

FIXTURE_TEST(test_missing_stm_manifest, bucket_view_fixture) {
    init_view(
      stm_manifest, {spillover_manifest_at_0, spillover_manifest_at_20});

    remove_manifest(get_stm_manifest());

    const auto result = run_detector();
    BOOST_REQUIRE_EQUAL(result.status, cloud_storage::scrub_status::full);

    BOOST_REQUIRE(result.detected.has_value());
    BOOST_REQUIRE_EQUAL(result.detected.missing_partition_manifest, true);
}
