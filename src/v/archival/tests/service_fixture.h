/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "archival/ntp_archiver_service.h"
#include "cloud_storage/manifest.h"
#include "cloud_storage/types.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "redpanda/tests/fixture.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/httpd.hh>
#include <seastar/util/tmp_file.hh>

#include <chrono>
#include <exception>
#include <map>
#include <vector>

/// Emulates S3 REST API for testing purposes.
/// The imposter is a simple KV-store that contains a set of expectations.
/// Expectations are accessible by url via GET, PUT, and DELETE http calls.
/// Expectations are provided before impster starts to listen. They have
/// two field - url and optional body. If body is set to nullopt, attemtp
/// to read it using GET or delete it using DELETE requests will trigger an
/// http response with error code 404 and xml formatted error message.
/// If the body of the expectation is set by the user or PUT request it can
/// be retrieved using the GET request or deleted using the DELETE request.
class s3_imposter_fixture {
public:
    s3_imposter_fixture();
    ~s3_imposter_fixture();

    s3_imposter_fixture(const s3_imposter_fixture&) = delete;
    s3_imposter_fixture& operator=(const s3_imposter_fixture&) = delete;
    s3_imposter_fixture(s3_imposter_fixture&&) = delete;
    s3_imposter_fixture& operator=(s3_imposter_fixture&&) = delete;

    struct expectation {
        ss::sstring url;
        std::optional<ss::sstring> body;
    };

    /// Set expectaitions on REST API calls that supposed to be made
    /// Only the requests that described in this call will be possible
    /// to make. This method can only be called once per test run.
    ///
    /// \param expectations is a collection of access points that allow GET,
    /// PUT, and DELETE requests, each expectation has url and body. The body
    /// will be returned by GET call if it's set or trigger error if its null.
    /// The expectations are statefull. If the body of the expectation was set
    /// to null but there was PUT call that sent some data, subsequent GET call
    /// will retrieve this data.
    void
    set_expectations_and_listen(const std::vector<expectation>& expectations);

    /// Access all http requests ordered by time
    const std::vector<ss::httpd::request>& get_requests() const;

    /// Access all http requests ordered by target url
    const std::multimap<ss::sstring, ss::httpd::request>& get_targets() const;

private:
    void set_routes(
      ss::httpd::routes& r, const std::vector<expectation>& expectations);

    ss::socket_address _server_addr;
    ss::shared_ptr<ss::httpd::http_server_control> _server;
    /// Contains saved requests
    std::vector<ss::httpd::request> _requests;
    /// Contains all accessed target urls
    std::multimap<ss::sstring, ss::httpd::request> _targets;
};

struct segment_desc {
    model::ntp ntp;
    model::offset base_offset;
    model::term_id term;
    std::optional<size_t> num_batches;
};

struct offset_range {
    model::offset base_offset;
    model::offset last_offset;
};

struct segment_layout {
    model::offset base_offset;
    std::vector<offset_range> ranges;
};

/// This utility can be used to match content of the log
/// with manifest and request content. It's also can be
/// used to retrieve individual segments or iterate over
/// them.
///
/// The 'Fixture' is supposed to implement the following
/// method
/// - storage::api& get_local_storage_api();
/// - ss::sharded<storage::api>& get_storage_api();
template<class Fixture>
class segment_matcher {
public:
    /// \brief Get full list of segments that log contains
    ///
    /// \param ntp is an ntp of the log
    /// \return vector of pointers to log segments
    std::vector<ss::lw_shared_ptr<storage::segment>>
    list_segments(const model::ntp& ntp);

    /// \brief Get single segment by ntp and name
    ///
    /// \param ntp is an ntp of the log
    /// \param name is a segment file name "<base-offset>-<term>-<version>.log"
    /// \return pointer to segment or null if segment not found
    ss::lw_shared_ptr<storage::segment>
    get_segment(const model::ntp& ntp, const archival::segment_name& name);

    /// Verify 'expected' segment content using the actual segment from
    /// log_manager
    void verify_segment(
      const model::ntp& ntp,
      const archival::segment_name& name,
      const ss::sstring& expected);

    /// Verify manifest using log_manager's state,
    /// find matching segments and check the fields.
    void verify_manifest(const cloud_storage::manifest& man);

    /// Verify manifest content using log_manager's state,
    /// find matching segments and check the fields.
    void verify_manifest_content(const ss::sstring& manifest_content);
};
/// Archiver fixture that contains S3 mock and full redpanda stack.
class archiver_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public segment_matcher<archiver_fixture> {
public:
    std::unique_ptr<storage::disk_log_builder> get_started_log_builder(
      model::ntp ntp, model::revision_id rev = model::revision_id(0));
    /// Wait unill all information will be replicated and the local node
    /// will become a leader for 'ntp'.
    void wait_for_partition_leadership(const model::ntp& ntp);
    void delete_topic(model::ns ns, model::topic topic);
    void wait_for_topic_deletion(const model::ntp& ntp);
    void add_topic_with_random_data(const model::ntp& ntp, int num_batches);
    /// Provides access point for segment_matcher CRTP template
    storage::api& get_local_storage_api();
    /// \brief Init storage api for tests that require only storage
    /// The method doesn't add topics, only creates segments in data_dir
    void init_storage_api_local(std::vector<segment_desc>& segm);

    std::vector<segment_layout> get_layouts(const model::ntp& ntp) const {
        return layouts.find(ntp)->second;
    }

    ss::future<> add_topic_with_single_partition(model::ntp ntp) {
        co_await wait_for_controller_leadership();
        co_await add_topic(model::topic_namespace_view(
          model::topic_namespace(ntp.ns, ntp.tp.topic)));
    }

private:
    void
    initialize_shard(storage::api& api, const std::vector<segment_desc>& segm);

    std::unordered_map<model::ntp, std::vector<segment_layout>> layouts;
};

std::tuple<archival::configuration, cloud_storage::configuration>
get_configurations();
