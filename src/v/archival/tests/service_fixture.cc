/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/tests/service_fixture.h"

#include "archival/types.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "cloud_storage/manifest.h"
#include "cluster/members_table.h"
#include "random/generators.h"
#include "s3/client.h"
#include "seastarx.h"
#include "storage/directories.h"
#include "storage/disk_log_impl.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/async.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/core/noncopyable.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <optional>

using namespace std::chrono_literals;

inline ss::logger fixt_log("fixture"); // NOLINT

static constexpr int16_t httpd_port_number = 4430;
static constexpr const char* httpd_host_name = "127.0.0.1";

static cloud_storage::manifest load_manifest_from_str(std::string_view v) {
    cloud_storage::manifest m;
    iobuf i;
    i.append(v.data(), v.size());
    auto s = make_iobuf_input_stream(std::move(i));
    m.update(std::move(s)).get();
    return std::move(m);
}

static void write_batches(
  ss::lw_shared_ptr<storage::segment> seg,
  ss::circular_buffer<model::record_batch> batches) { // NOLINT
    vlog(fixt_log.trace, "num batches {}", batches.size());
    for (auto& b : batches) {
        b.header().header_crc = model::internal_header_only_crc(b.header());
        auto res = seg->append(std::move(b)).get0();
        vlog(fixt_log.trace, "last-offset {}", res.last_offset);
    }
    seg->flush().get();
}

static segment_layout write_random_batches(
  ss::lw_shared_ptr<storage::segment> seg, size_t num_batches = 1) { // NOLINT
    segment_layout layout{
      .base_offset = seg->offsets().base_offset,
    };
    auto batches = storage::test::make_random_batches(
      seg->offsets().base_offset, num_batches);

    vlog(fixt_log.debug, "Generated {} random batches", batches.size());
    for (const auto& batch : batches) {
        vlog(fixt_log.debug, "Generated random batch {}", batch);
        layout.ranges.push_back({
          .base_offset = batch.base_offset(),
          .last_offset = batch.last_offset(),
        });
    }
    write_batches(seg, std::move(batches));
    return layout;
}

std::tuple<archival::configuration, cloud_storage::configuration>
get_configurations() {
    net::unresolved_address server_addr(httpd_host_name, httpd_port_number);
    s3::configuration s3conf{
      .uri = s3::access_point_uri(httpd_host_name),
      .access_key = s3::public_key_str("acess-key"),
      .secret_key = s3::private_key_str("secret-key"),
      .region = s3::aws_region_name("us-east-1"),
    };
    s3conf.server_addr = server_addr;
    archival::configuration aconf;
    aconf.bucket_name = s3::bucket_name("test-bucket");
    aconf.ntp_metrics_disabled = archival::per_ntp_metrics_disabled::yes;
    aconf.svc_metrics_disabled = archival::service_metrics_disabled::yes;
    aconf.initial_backoff = 100ms;
    aconf.segment_upload_timeout = 1s;
    aconf.manifest_upload_timeout = 1s;
    aconf.time_limit = std::nullopt;

    cloud_storage::configuration cconf;
    cconf.client_config = s3conf;
    cconf.bucket_name = s3::bucket_name("test-bucket");
    cconf.connection_limit = archival::s3_connection_limit(2);
    cconf.metrics_disabled = cloud_storage::remote_metrics_disabled::yes;
    return std::make_tuple(aconf, cconf);
}

s3_imposter_fixture::s3_imposter_fixture() {
    _server = ss::make_shared<ss::httpd::http_server_control>();
    _server->start().get();
    ss::ipv4_addr ip_addr = {httpd_host_name, httpd_port_number};
    _server_addr = ss::socket_address(ip_addr);
}

s3_imposter_fixture::~s3_imposter_fixture() { _server->stop().get(); }

const std::vector<ss::httpd::request>&
s3_imposter_fixture::get_requests() const {
    return _requests;
}

const std::multimap<ss::sstring, ss::httpd::request>&
s3_imposter_fixture::get_targets() const {
    return _targets;
}

void s3_imposter_fixture::set_expectations_and_listen(
  const std::vector<s3_imposter_fixture::expectation>& expectations) {
    _server
      ->set_routes([this, &expectations](ss::httpd::routes& r) {
          set_routes(r, expectations);
      })
      .get();
    _server->listen(_server_addr).get();
}

void s3_imposter_fixture::set_routes(
  ss::httpd::routes& r,
  const std::vector<s3_imposter_fixture::expectation>& expectations) {
    using namespace ss::httpd;
    struct content_handler {
        content_handler(
          const std::vector<expectation>& exp, s3_imposter_fixture& imp)
          : fixture(imp) {
            for (const auto& e : exp) {
                expectations[e.url] = e;
            }
        }
        ss::sstring handle(const_req request, reply& repl) {
            static const ss::sstring error_payload
              = R"xml(<?xml version="1.0" encoding="UTF-8"?>
                        <Error>
                            <Code>NoSuchKey</Code>
                            <Message>Object not found</Message>
                            <Resource>resource</Resource>
                            <RequestId>requestid</RequestId>
                        </Error>)xml";
            fixture._requests.push_back(request);
            fixture._targets.insert(std::make_pair(request._url, request));
            vlog(
              fixt_log.trace,
              "S3 imposter request {} - {} - {}",
              request._url,
              request.content_length,
              request._method);
            if (request._method == "GET") {
                auto it = expectations.find(request._url);
                if (it == expectations.end() || !it->second.body.has_value()) {
                    vlog(fixt_log.trace, "Reply GET request with error");
                    repl.set_status(reply::status_type::not_found);
                    return error_payload;
                }
                return *it->second.body;
            } else if (request._method == "PUT") {
                expectations[request._url] = {
                  .url = request._url, .body = request.content};
                return "";
            } else if (request._method == "DELETE") {
                auto it = expectations.find(request._url);
                if (it == expectations.end() || !it->second.body.has_value()) {
                    vlog(fixt_log.trace, "Reply DELETE request with error");
                    repl.set_status(reply::status_type::not_found);
                    return error_payload;
                }
                repl.set_status(reply::status_type::no_content);
                it->second.body = std::nullopt;
                return "";
            }
            BOOST_FAIL("Unexpected request");
            return "";
        }
        std::map<ss::sstring, expectation> expectations;
        s3_imposter_fixture& fixture;
    };
    auto hd = ss::make_shared<content_handler>(expectations, *this);
    _handler = std::make_unique<function_handler>(
      [hd](const_req req, reply& repl) { return hd->handle(req, repl); },
      "txt");
    r.add_default_handler(_handler.get());
}

// archiver service fixture

std::unique_ptr<storage::disk_log_builder>
archiver_fixture::get_started_log_builder(
  model::ntp ntp, model::revision_id rev) {
    storage::ntp_config ntp_cfg(
      std::move(ntp),
      config::node().data_directory().as_sstring(),
      nullptr,
      rev);

    auto conf = storage::log_config(
      storage::log_config::storage_type::disk,
      config::node().data_directory().as_sstring(),
      1_MiB,
      storage::debug_sanitize_files::yes);
    auto builder = std::make_unique<storage::disk_log_builder>(std::move(conf));
    builder->start(std::move(ntp_cfg)).get();
    return builder;
}
/// Wait unill all information will be replicated and the local node
/// will become a leader for 'ntp'.
void archiver_fixture::wait_for_partition_leadership(const model::ntp& ntp) {
    vlog(fixt_log.trace, "waiting for partition {}", ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [this, ntp] {
        auto& table = app.controller->get_partition_leaders().local();
        model::node_id node(0);
        int cnt = 0;
        auto self = app.controller->self();
        ss::lowres_clock::time_point deadline = ss::lowres_clock::now() + 100ms;
        return table.wait_for_leader(ntp, deadline, {}).get0() == self
               && app.partition_manager.local().get(ntp)->is_leader();
    }).get();
}
void archiver_fixture::delete_topic(model::ns ns, model::topic topic) {
    vlog(fixt_log.trace, "delete topic {}/{}", ns(), topic());
    app.controller->get_topics_frontend()
      .local()
      .delete_topics(
        {model::topic_namespace(std::move(ns), std::move(topic))},
        model::timeout_clock::now() + 100ms)
      .get();
}
void archiver_fixture::wait_for_topic_deletion(const model::ntp& ntp) {
    tests::cooperative_spin_wait_with_timeout(10s, [this, ntp] {
        return !app.partition_manager.local().get(ntp);
    }).get();
}
void archiver_fixture::add_topic_with_random_data(
  const model::ntp& ntp, int num_batches) {
    // In order to be picked up by archival service the topic should
    // exist in partition manager and on disk
    auto builder = get_started_log_builder(ntp);
    builder->add_segment(model::offset(0)).get();
    builder
      ->add_random_batches(
        model::offset(0), num_batches, storage::maybe_compress_batches::yes)
      .get();
    builder->stop().get();
    builder.reset();
    add_topic_with_archival_enabled(model::topic_namespace_view(ntp)).get();
}
ss::future<> archiver_fixture::add_topic_with_archival_enabled(
  model::topic_namespace_view tp_ns, int partitions) {
    cluster::topic_configuration cfg(tp_ns.ns, tp_ns.tp, partitions, 1);
    cfg.properties.shadow_indexing = model::shadow_indexing_mode::archival;
    std::vector<cluster::custom_assignable_topic_configuration> cfgs = {
      cluster::custom_assignable_topic_configuration(std::move(cfg))};
    return app.controller->get_topics_frontend()
      .local()
      .create_topics(std::move(cfgs), model::no_timeout)
      .then([this](std::vector<cluster::topic_result> results) {
          return wait_for_topics(std::move(results));
      });
}
storage::api& archiver_fixture::get_local_storage_api() {
    return app.storage.local();
}

void archiver_fixture::init_storage_api_local(
  const std::vector<segment_desc>& segm) {
    initialize_shard(get_local_storage_api(), segm);
}

void archiver_fixture::initialize_shard(
  storage::api& api, const std::vector<segment_desc>& segm) {
    absl::flat_hash_map<model::ntp, size_t> all_ntp;
    for (const auto& d : segm) {
        storage::ntp_config ntpc(d.ntp, data_dir.string());
        storage::directories::initialize(ntpc.work_directory()).get();
        vlog(fixt_log.trace, "make_log_segment {}", d.ntp.path());
        auto seg = api.log_mgr()
                     .make_log_segment(
                       storage::ntp_config(d.ntp, data_dir.string()),
                       d.base_offset,
                       d.term,
                       ss::default_priority_class(),
                       128_KiB,
                       10)
                     .get0();
        vlog(fixt_log.trace, "write random batches to segment");
        auto layout = write_random_batches(
          seg, d.num_batches ? d.num_batches.value() : 1);
        layouts[d.ntp].push_back(std::move(layout));
        vlog(fixt_log.trace, "segment close");
        seg->close().get();
        all_ntp[d.ntp] += 1;
    }
    wait_for_controller_leadership().get();
    auto broker = app.controller->get_members_table().local().get_broker(
      model::node_id(1));
    for (const auto& ntp : all_ntp) {
        vlog(
          fixt_log.trace,
          "manage {}, data-dir {}",
          ntp.first,
          data_dir.string());
        auto defaults
          = std::make_unique<storage::ntp_config::default_overrides>();
        defaults->shadow_indexing_mode = model::shadow_indexing_mode::archival;
        app.partition_manager.local()
          .manage(
            storage::ntp_config(ntp.first, data_dir.string()),
            raft::group_id(1),
            {*broker.value()})
          .get();
        BOOST_CHECK_EQUAL(
          api.log_mgr().get(ntp.first)->segment_count(), ntp.second);
        vlog(fixt_log.trace, "storage log {}", *api.log_mgr().get(ntp.first));
    }
    BOOST_CHECK(all_ntp.size() <= api.log_mgr().size()); // NOLINT
}

// segment matcher

template<class Fixture>
std::vector<ss::lw_shared_ptr<storage::segment>>
segment_matcher<Fixture>::list_segments(const model::ntp& ntp) {
    std::vector<ss::lw_shared_ptr<storage::segment>> result;
    auto log
      = static_cast<Fixture*>(this)->get_local_storage_api().log_mgr().get(ntp);
    if (auto dlog = dynamic_cast<storage::disk_log_impl*>(log->get_impl());
        dlog) {
        std::copy_if(
          dlog->segments().begin(),
          dlog->segments().end(),
          std::back_inserter(result),
          [](ss::lw_shared_ptr<storage::segment> seg) {
              return !seg->has_appender();
          });
    }
    return result;
}

template<class Fixture>
ss::lw_shared_ptr<storage::segment> segment_matcher<Fixture>::get_segment(
  const model::ntp& ntp, const archival::segment_name& name) {
    auto log
      = static_cast<Fixture*>(this)->get_local_storage_api().log_mgr().get(ntp);
    if (auto dlog = dynamic_cast<storage::disk_log_impl*>(log->get_impl());
        dlog) {
        for (const auto& s : dlog->segments()) {
            if (
              !s->has_appender()
              && boost::ends_with(s->reader().filename(), name())) {
                return s;
            }
        }
    }
    return nullptr;
}

template<class Fixture>
void segment_matcher<Fixture>::verify_segment(
  const model::ntp& ntp,
  const archival::segment_name& name,
  const ss::sstring& expected) {
    auto segment = get_segment(ntp, name);
    auto pos = segment->offsets().base_offset;
    auto size = segment->size_bytes();
    auto stream = segment->offset_data_stream(
      pos, ss::default_priority_class());
    auto tmp = stream.read_exactly(size).get0();
    ss::sstring actual = {tmp.get(), tmp.size()};
    vlog(
      fixt_log.info,
      "expected {} bytes, got {}",
      expected.size(),
      actual.size());
    BOOST_REQUIRE(actual == expected); // NOLINT
}

template<class Fixture>
void segment_matcher<Fixture>::verify_manifest(
  const cloud_storage::manifest& man) {
    auto all_segments = list_segments(man.get_ntp());
    BOOST_REQUIRE_EQUAL(all_segments.size(), man.size());
    for (const auto& s : all_segments) {
        auto sname = archival::segment_name(
          std::filesystem::path(s->reader().filename()).filename().string());
        auto base = s->offsets().base_offset;
        auto comm = s->offsets().committed_offset;
        auto size = s->size_bytes();
        auto comp = s->is_compacted_segment();
        auto m = man.get(sname);
        BOOST_REQUIRE(m != nullptr); // NOLINT
        BOOST_REQUIRE_EQUAL(base, m->base_offset);
        BOOST_REQUIRE_EQUAL(comm, m->committed_offset);
        BOOST_REQUIRE_EQUAL(size, m->size_bytes);
        BOOST_REQUIRE_EQUAL(comp, m->is_compacted);
    }
}

template<class Fixture>
void segment_matcher<Fixture>::verify_manifest_content(
  const ss::sstring& manifest_content) {
    cloud_storage::manifest m = load_manifest_from_str(manifest_content);
    verify_manifest(m);
}

template class segment_matcher<archiver_fixture>;

enable_cloud_storage_fixture::enable_cloud_storage_fixture() {
    ss::smp::invoke_on_all([]() {
        auto& cfg = config::shard_local_cfg();
        cfg.cloud_storage_enabled.set_value(true);
        cfg.cloud_storage_api_endpoint.set_value(
          std::optional<ss::sstring>{httpd_host_name});
        cfg.cloud_storage_api_endpoint_port.set_value(httpd_port_number);
        cfg.cloud_storage_access_key.set_value(
          std::optional<ss::sstring>{"access-key"});
        cfg.cloud_storage_secret_key.set_value(
          std::optional<ss::sstring>{"secret-key"});
        cfg.cloud_storage_region.set_value(
          std::optional<ss::sstring>{"us-east1"});
        cfg.cloud_storage_bucket.set_value(
          std::optional<ss::sstring>{"test-bucket"});
    }).get0();
}

enable_cloud_storage_fixture::~enable_cloud_storage_fixture() {
    config::shard_local_cfg().cloud_storage_enabled.set_value(false);
}

cloud_storage::manifest load_manifest(std::string_view v) {
    cloud_storage::manifest m;
    iobuf i;
    i.append(v.data(), v.size());
    auto s = make_iobuf_input_stream(std::move(i));
    m.update(std::move(s)).get();
    return std::move(m);
}

archival::remote_segment_path get_segment_path(
  const cloud_storage::manifest& manifest, const archival::segment_name& name) {
    auto meta = manifest.get(name);
    BOOST_REQUIRE(meta);
    auto key = cloud_storage::parse_segment_name(name);
    BOOST_REQUIRE(key);
    return manifest.generate_segment_path(*key, *meta);
}
