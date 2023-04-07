/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "archival/ntp_archiver_service.h"
#include "archival/service.h"
#include "cloud_storage/types.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/http_imposter.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/httpd.hh>
#include <seastar/util/tmp_file.hh>

#include <chrono>
#include <exception>
#include <map>
#include <vector>

struct segment_desc {
    model::ntp ntp;
    model::offset base_offset;
    model::term_id term;
    std::optional<size_t> num_records;
    std::optional<size_t> records_per_batch;
    std::optional<model::timestamp> timestamp;
};

struct offset_range {
    model::offset base_offset;
    model::offset last_offset;
};

struct segment_layout {
    model::offset base_offset;
    std::vector<offset_range> ranges;
};

namespace archival_tests {
static constexpr std::string_view error_payload
  = R"xml(<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>NoSuchKey</Code>
    <Message>Object not found</Message>
    <Resource>resource</Resource>
    <RequestId>requestid</RequestId>
</Error>)xml";

static constexpr std::string_view forbidden_payload
  = R"xml(<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>AccessDenied</Code>
    <Message>Access Denied</Message>
    <Resource>resource</Resource>
    <RequestId>requestid</RequestId>
</Error>)xml";

} // namespace archival_tests

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

    /// Given a set of segments, verifies that a concatenated segment composed
    /// of the set was uploaded, by concatenating segments from disk log and
    /// comparing the content with request content.
    void verify_segments(
      const model::ntp& ntp,
      const std::vector<archival::segment_name>& names,
      const ss::sstring& expected);

    /// Verify manifest using log_manager's state,
    /// find matching segments and check the fields.
    void verify_manifest(const cloud_storage::partition_manifest& man);

    /// Verify manifest content using log_manager's state,
    /// find matching segments and check the fields.
    void verify_manifest_content(const ss::sstring& manifest_content);
};

class enable_cloud_storage_fixture {
public:
    enable_cloud_storage_fixture();
    ~enable_cloud_storage_fixture();
};

/// Archiver fixture that contains S3 mock and full redpanda stack.
class archiver_fixture
  : public http_imposter_fixture
  , public enable_cloud_storage_fixture
  , public redpanda_thread_fixture
  , public segment_matcher<archiver_fixture> {
public:
    archiver_fixture()
      : redpanda_thread_fixture(
        redpanda_thread_fixture::init_cloud_storage_tag{}) {}

    std::unique_ptr<storage::disk_log_builder> get_started_log_builder(
      model::ntp ntp, model::revision_id rev = model::revision_id(0));
    /// Wait unill all information will be replicated and the local node
    /// will become a leader for 'ntp'.
    void wait_for_partition_leadership(const model::ntp& ntp);
    void delete_topic(model::ns ns, model::topic topic);
    void wait_for_topic_deletion(const model::ntp& ntp);
    void add_topic_with_random_data(const model::ntp& ntp, int num_batches);
    void wait_for_lso(const model::ntp&);
    /// Provides access point for segment_matcher CRTP template
    storage::api& get_local_storage_api();
    /// Get archival scheduler service
    archival::internal::scheduler_service_impl& get_scheduler_service() {
        auto p = reinterpret_cast<archival::internal::scheduler_service_impl*>(
          &app.archival_scheduler.local());
        return *p;
    }
    /// \brief Init storage api for tests that require only storage
    /// The method doesn't add topics, only creates segments in data_dir
    void init_storage_api_local(
      const std::vector<segment_desc>& segm,
      std::optional<storage::ntp_config::default_overrides> overrides
      = std::nullopt,
      bool fit_segments = false);

    std::vector<segment_layout> get_layouts(const model::ntp& ntp) const {
        return layouts.find(ntp)->second;
    }

    ss::future<> add_topic_with_single_partition(model::ntp ntp) {
        co_await wait_for_controller_leadership();
        co_await add_topic(model::topic_namespace_view(
          model::topic_namespace(ntp.ns, ntp.tp.topic)));
    }

    ss::future<> add_topic_with_archival_enabled(
      model::topic_namespace_view tp_ns, int partitions = 1);

    ss::future<> create_archival_snapshot(
      const storage::ntp_config& cfg,
      cloud_storage::partition_manifest manifest);

    /// Stops the scheduler service before listening to incoming HTTP calls.
    /// Intended to be used in tests which create their own archiver, where
    /// stopping the scheduler first is necessary so that the built in archiver
    /// does not interfere with the archiver in test.
    void stop_archiver_scheduler_and_listen() {
        get_scheduler_service().stop().get();
        listen();
    }

private:
    void initialize_shard(
      storage::api& api,
      const std::vector<segment_desc>& segm,
      std::optional<storage::ntp_config::default_overrides> overrides,
      bool fit_segments);

    std::unordered_map<model::ntp, std::vector<segment_layout>> layouts;
};

std::tuple<archival::configuration, cloud_storage::configuration>
get_configurations();

cloud_storage::partition_manifest load_manifest(std::string_view v);

archival::remote_segment_path get_segment_path(
  const cloud_storage::partition_manifest&, const archival::segment_name&);

/// Specification for the segments and data to go into the log for a test
struct log_spec {
    // The base offsets for all segments. The difference in adjacent base
    // offsets is converted to how many records we will write into each segment
    // (as a single batch)
    std::vector<size_t> segment_starts;
    // The indices of the segments which will be marked as compacted for the
    // test. The segments are not actually compacted, only marked as such.
    std::vector<size_t> compacted_segment_indices;
    // The number of records in the final segment, required separately because
    // there is no delta to use for the last segment.
    size_t last_segment_num_records;
};

storage::disk_log_builder make_log_builder(std::string_view data_path);

void populate_log(storage::disk_log_builder& b, const log_spec& spec);

ss::future<archival::ntp_archiver::batch_result> upload_next_with_retries(
  archival::ntp_archiver&, std::optional<model::offset> lso = std::nullopt);

void upload_and_verify(
  archival::ntp_archiver&,
  archival::ntp_archiver::batch_result,
  std::optional<model::offset> lso = std::nullopt);

segment_layout write_random_batches(
  ss::lw_shared_ptr<storage::segment> seg,
  size_t records = 1,
  size_t records_per_batch = 1,
  std::optional<model::timestamp> timestamp = std::nullopt);
