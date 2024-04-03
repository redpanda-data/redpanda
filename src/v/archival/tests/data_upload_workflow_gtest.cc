// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "archival/archiver_data_upload_workflow.h"
#include "archival/archiver_operations_api.h"
#include "archival/archiver_scheduler_api.h"
#include "archival/logger.h"
#include "archival/types.h"
#include "base/vlog.h"
#include "bytes/iostream.h"
#include "config/configuration.h"
#include "gmock/gmock.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"
#include "utils/available_promise.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <gmock/gmock.h>

#include <exception>
#include <stdexcept>

std::ostream& operator<<(std::ostream& o, const retry_chain_node& rtc) {
    o << rtc();
    return o;
}

namespace archival {

class ops_api_mock : public archiver_operations_api {
public:
    /// Return upload candidate(s) if data is available or nullopt
    /// if there is not enough data to start an upload.
    MOCK_METHOD(
      ss::future<result<find_upload_candidates_result>>,
      find_upload_candidates,
      (retry_chain_node&, find_upload_candidates_arg),
      (override, noexcept));

    /// Upload data to S3 and return results
    MOCK_METHOD(
      ss::future<result<schedule_upload_results>>,
      schedule_uploads,
      (retry_chain_node&, find_upload_candidates_result, bool),
      (override, noexcept));

    /// Add metadata to the manifest by replicating archival metadata
    /// configuration batch
    MOCK_METHOD(
      ss::future<result<admit_uploads_result>>,
      admit_uploads,
      (retry_chain_node&, schedule_upload_results),
      (override, noexcept));

    MOCK_METHOD(
      ss::future<result<manifest_upload_result>>,
      upload_manifest,
      (retry_chain_node&, manifest_upload_arg),
      (override, noexcept));

    void expect_find_upload_candidates(
      find_upload_candidates_arg input,
      result<find_upload_candidates_result> output) {
        auto success
          = ss::make_ready_future<result<find_upload_candidates_result>>(
            std::move(output));
        EXPECT_CALL(*this, find_upload_candidates(testing::_, std::move(input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(success)));
    }

    void expect_find_upload_candidates(
      find_upload_candidates_arg input, std::exception_ptr output) {
        auto failure
          = ss::make_exception_future<result<find_upload_candidates_result>>(
            std::move(output));
        EXPECT_CALL(*this, find_upload_candidates(testing::_, std::move(input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(failure)));
    }

    void expect_schedule_uploads(
      find_upload_candidates_result input,
      bool inline_manifest,
      result<schedule_upload_results> output) {
        auto success = ss::make_ready_future<result<schedule_upload_results>>(
          std::move(output));
        EXPECT_CALL(
          *this,
          schedule_uploads(testing::_, std::move(input), inline_manifest))
          .Times(1)
          .WillOnce(::testing::Return(std::move(success)));
    }

    void expect_schedule_uploads(
      find_upload_candidates_result input,
      bool inline_manifest,
      std::exception_ptr output) {
        auto failure
          = ss::make_exception_future<result<schedule_upload_results>>(
            std::move(output));
        EXPECT_CALL(
          *this,
          schedule_uploads(testing::_, std::move(input), inline_manifest))
          .Times(1)
          .WillOnce(::testing::Return(std::move(failure)));
    }

    void expect_admit_uploads(
      schedule_upload_results input, result<admit_uploads_result> output) {
        auto success = ss::make_ready_future<result<admit_uploads_result>>(
          std::move(output));
        EXPECT_CALL(*this, admit_uploads(testing::_, std::move(input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(success)));
    }

    void expect_admit_uploads(
      schedule_upload_results input, std::exception_ptr output) {
        auto failure = ss::make_exception_future<result<admit_uploads_result>>(
          std::move(output));
        EXPECT_CALL(*this, admit_uploads(testing::_, std::move(input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(failure)));
    }

    void expect_upload_manifest(
      manifest_upload_arg input, result<manifest_upload_result> output) {
        auto success = ss::make_ready_future<result<manifest_upload_result>>(
          std::move(output));
        EXPECT_CALL(*this, upload_manifest(testing::_, std::move(input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(success)));
    }

    void expect_upload_manifest(
      manifest_upload_arg input, std::exception_ptr output) {
        auto failure
          = ss::make_exception_future<result<manifest_upload_result>>(
            std::move(output));
        EXPECT_CALL(*this, upload_manifest(testing::_, std::move(input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(failure)));
    }
};

class archiver_scheduler_mock : public archiver_scheduler_api {
public:
    /// Applies throttling or backoff
    ///
    /// \param usage describes resources used by the partition
    /// \param ntp is an partition that invokes the manager
    /// \returns true if throttling was applied
    MOCK_METHOD(
      ss::future<result<archiver_scheduler_api::next_upload_action_hint>>,
      maybe_suspend_upload,
      (suspend_upload_arg),
      (override, noexcept));

    void expect_maybe_suspend(
      suspend_upload_arg expected_input,
      result<next_upload_action_hint> expected_output) {
        auto out = ss::make_ready_future<
          result<archiver_scheduler_api::next_upload_action_hint>>(
          expected_output);
        EXPECT_CALL(*this, maybe_suspend_upload(std::move(expected_input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(out)));
    }

    void expect_maybe_suspend(
      suspend_upload_arg expected_input, std::exception_ptr expected_output) {
        auto out = ss::make_exception_future<
          result<archiver_scheduler_api::next_upload_action_hint>>(
          expected_output);
        EXPECT_CALL(*this, maybe_suspend_upload(std::move(expected_input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(out)));
    }
};

using next_action_hint = archiver_scheduler_api::next_upload_action_hint;
using suspend_request = archiver_scheduler_api::suspend_upload_arg;
using next_action_type = archiver_scheduler_api::next_upload_action_type;
using find_upload_candidates_arg
  = archiver_operations_api::find_upload_candidates_arg;
using segment_upload_candidate_t
  = archiver_operations_api::segment_upload_candidate_t;
using find_upload_candidates_result
  = archival::archiver_operations_api::find_upload_candidates_result;
using schedule_upload_results
  = archival::archiver_operations_api::schedule_upload_results;
using admit_uploads_result
  = archival::archiver_operations_api::admit_uploads_result;
using manifest_upload_arg
  = archival::archiver_operations_api::manifest_upload_arg;
using manifest_upload_result
  = archival::archiver_operations_api::manifest_upload_result;

const model::ktp ntp(model::topic("panda-topic"), model::partition_id(137));
const model::offset read_write_fence(137);
const model::offset manifest_clean_offset(138);
const model::offset manifest_dirty_offset(139);
const model::term_id archiver_term(42);
const std::optional<size_t> target_segment_size = 0x8000;
const std::optional<size_t> min_segment_size = 0x2000;
const size_t expected_num_put_requests = 3;
const size_t expected_num_bytes_sent = 0x1000;
const bool expected_inline_manifest = true;

auto expected_upload_result() {
    std::deque<std::optional<cloud_storage::segment_record_stats>> stats;
    std::deque<cloud_storage::upload_result> results;
    return schedule_upload_results{
      .ntp = ntp,
      .stats = stats,
      .results = results,
      .manifest_clean_offset = manifest_clean_offset,
      .num_put_requests = expected_num_put_requests,
      .num_bytes_sent = expected_num_bytes_sent,
    };
}

auto expected_upload_candidate_input() {
    find_upload_candidates_arg input{
      .ntp = ntp,
      .target_size = *target_segment_size,
      .min_size = *min_segment_size,
      .upload_size_quota = expected_num_bytes_sent,
      .upload_requests_quota = expected_num_put_requests,
      .compacted_reupload = config::shard_local_cfg()
                              .cloud_storage_enable_compacted_topic_reupload(),
      .inline_manifest = expected_inline_manifest,
    };
    return input;
}

auto expected_upload_candidate_result() {
    return segment_upload_candidate_t{
      .ntp = ntp,
      .payload = make_iobuf_input_stream(iobuf()),
      .size_bytes = 1,
      .metadata = cloud_storage::segment_meta{
        .base_offset = model::offset(0),
        .committed_offset = model::offset(1),
      }};
}

auto expected_suspend_request() {
    return suspend_request{
      .ntp = ntp,
      .manifest_dirty = true,
      .put_requests_used = expected_num_put_requests,
      .uploaded_bytes = expected_num_bytes_sent};
}

auto expected_admit_uploads_result() {
    return admit_uploads_result{
      .ntp = ntp,
      .num_succeeded = 1,
      .num_failed = 0,
      .manifest_dirty_offset = manifest_dirty_offset};
}

auto expected_next_upload_action_hint(bool inline_manifest = true) {
    return next_action_hint{
      .type = inline_manifest ? next_action_type::segment_with_manifest
                              : next_action_type::segment_upload,
      .requests_quota = expected_num_put_requests,
      .upload_size_quota = expected_num_bytes_sent,
    };
}

auto expected_next_manifest_upload_action_hint() {
    return next_action_hint{
      .type = next_action_type::manifest_upload,
      .requests_quota = expected_num_put_requests,
      .upload_size_quota = expected_num_bytes_sent,
    };
}

/// Create workflow and its dependencies
auto setup_test_suite() {
    auto rm_api = ss::make_shared<archiver_scheduler_mock>();
    auto op_api = ss::make_shared<ops_api_mock>();
    auto wf = make_data_upload_workflow(ntp, archiver_term, op_api, rm_api);

    auto cfg = ss::make_shared<scoped_config>();
    cfg->get("cloud_storage_segment_size_target")
      .set_value(target_segment_size);
    cfg->get("cloud_storage_segment_size_min").set_value(min_segment_size);

    return std::make_tuple(
      std::move(wf), std::move(rm_api), std::move(op_api), std::move(cfg));
}

TEST(data_upload_workflow_test, test_immediate_shutdown) {
    // Test case: the workflow is created and receives
    // a shutdown event immediately.
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    rm_api->expect_maybe_suspend(
      suspend_request{
        .ntp = ntp,
        .manifest_dirty = true,
      },
      make_error_code(error_outcome::shutting_down));

    wf->start().get();
    wf->stop().get();
}

TEST(data_upload_workflow_test, test_single_upload_cycle) {
    // Test case: the workflow executes one full upload cycle.
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    // Set up maybe_suspend. The first call returns success. The second call
    // returns shutdown failure to break the upload loop.
    {
        ::testing::InSequence seq;

        // Kick off workflow
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = true,
          },
          expected_next_upload_action_hint(expected_inline_manifest));

        // Triggered by last 'admit_uploads' call
        rm_api->expect_maybe_suspend(
          expected_suspend_request(),
          make_error_code(error_outcome::shutting_down));
    }

    // Set up find_upload_candidates call to return one upload candidate.
    {
        auto input = expected_upload_candidate_input();

        auto candidate = ss::make_lw_shared<segment_upload_candidate_t>(
          expected_upload_candidate_result());

        find_upload_candidates_result output;
        output.ntp = ntp;
        output.results.push_back(std::move(candidate));
        output.read_write_fence = read_write_fence;

        op_api->expect_find_upload_candidates(input, output);
    }

    // Set up schedule_uploads call.
    {
        auto candidate = ss::make_lw_shared<segment_upload_candidate_t>(
          expected_upload_candidate_result());
        find_upload_candidates_result input;
        input.results.push_back(std::move(candidate));
        input.read_write_fence = read_write_fence;
        op_api->expect_schedule_uploads(
          input, expected_inline_manifest, expected_upload_result());
    }

    // Set up admit_uploads
    {
        auto input = expected_upload_result();
        op_api->expect_admit_uploads(
          std::move(input), expected_admit_uploads_result());
    }

    auto f = wf->stop();
    wf->start().get();
    std::move(f).get();
}

TEST(data_upload_workflow_test, test_reconciliation_recoverable_errc) {
    // Test case: the workflow goes up to reconciliation step
    // and then recoverable error is triggered by the 'timedout' error code.
    const auto expected_error = error_outcome::timed_out;
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    // Set up maybe_suspend. First call returns success. Second call
    // returns shutdown error.
    {
        // Starts the loop
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = true,
          },
          expected_next_upload_action_hint(expected_inline_manifest));

        // Triggered by the failed 'find_upload_candidates' call
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .errc = expected_error,
          },
          error_outcome::shutting_down);
    }

    // Set up find_upload_candidates call to return timeout error.
    {
        auto input = expected_upload_candidate_input();

        op_api->expect_find_upload_candidates(std::move(input), expected_error);
    }

    auto f = wf->stop();
    wf->start().get();
    std::move(f).get();
}

TEST(data_upload_workflow_test, test_reconciliation_shutdown_exception) {
    // Test case: the workflow goes up to reconciliation step
    // and then shutdown is triggered by throwing an exception.
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    // Set up maybe_suspend. The call returns success.
    {
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = true,
          },
          expected_next_upload_action_hint(expected_inline_manifest));
    }

    // Set up find_upload_candidates call to return one upload candidate.
    {
        auto input = expected_upload_candidate_input();

        op_api->expect_find_upload_candidates(
          std::move(input),
          std::make_exception_ptr(ss::abort_requested_exception()));
    }

    auto f = wf->stop();
    wf->start().get();
    std::move(f).get();
}

TEST(data_upload_workflow_test, test_reconciliation_shutdown_errc) {
    // Test case: the workflow goes up to reconciliation step
    // and then shutdown is triggered using an error code
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    // Set up maybe_suspend. The call returns success.
    {
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = true,
          },
          expected_next_upload_action_hint(expected_inline_manifest));
    }

    // Set up find_upload_candidates call to return one upload candidate.
    {
        auto input = expected_upload_candidate_input();

        op_api->expect_find_upload_candidates(
          std::move(input), error_outcome::shutting_down);
    }

    auto f = wf->stop();
    wf->start().get();
    std::move(f).get();
}

TEST(data_upload_workflow_test, test_reconciliation_fatal_error) {
    // Test case: the workflow goes up to reconciliation step
    // and then FSM transitions to the terminal state due to fatal
    // error.
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    // Set up maybe_suspend. The call returns success.
    {
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = true,
          },
          expected_next_upload_action_hint(expected_inline_manifest));
    }

    // Set up find_upload_candidates call to return one upload candidate.
    {
        auto input = expected_upload_candidate_input();

        op_api->expect_find_upload_candidates(
          std::move(input),
          std::make_exception_ptr(std::runtime_error("fatal error")));
    }

    auto f = wf->stop();
    wf->start().get();

    ASSERT_THROW(std::move(f).get(), std::runtime_error);
}

TEST(data_upload_workflow_test, test_upload_shutdown_exception) {
    // Test case: the workflow goes up to upload step
    // and then shutdown is triggered by throwing an exception.
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    // Set up maybe_suspend. The call returns success.
    {
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = true,
          },
          expected_next_upload_action_hint(expected_inline_manifest));
    }

    // Set up find_upload_candidates call to return one upload candidate.
    {
        auto input = expected_upload_candidate_input();

        auto candidate = ss::make_lw_shared<segment_upload_candidate_t>(
          expected_upload_candidate_result());

        find_upload_candidates_result output;
        output.ntp = ntp;
        output.results.push_back(std::move(candidate));
        output.read_write_fence = read_write_fence;

        op_api->expect_find_upload_candidates(input, output);
    }

    // Set up schedule_uploads call.
    {
        auto candidate = ss::make_lw_shared<segment_upload_candidate_t>(
          expected_upload_candidate_result());
        find_upload_candidates_result input;
        input.results.push_back(std::move(candidate));
        input.read_write_fence = read_write_fence;
        op_api->expect_schedule_uploads(
          input,
          expected_inline_manifest,
          std::make_exception_ptr(ss::abort_requested_exception()));
    }

    auto f = wf->stop();
    wf->start().get();
    std::move(f).get();
}

TEST(data_upload_workflow_test, test_upload_shutdown_errc) {
    // Test case: the workflow goes up to upload step
    // and then shutdown is triggered by error code.
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    // Set up maybe_suspend. The only call returns success.
    {
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = true,
          },
          expected_next_upload_action_hint(expected_inline_manifest));
    }

    // Set up find_upload_candidates call to return one upload candidate.
    {
        auto input = expected_upload_candidate_input();

        auto candidate = ss::make_lw_shared<segment_upload_candidate_t>(
          expected_upload_candidate_result());

        find_upload_candidates_result output;
        output.ntp = ntp;
        output.results.push_back(std::move(candidate));
        output.read_write_fence = read_write_fence;

        op_api->expect_find_upload_candidates(input, output);
    }

    // Set up schedule_uploads call.
    {
        auto candidate = ss::make_lw_shared<segment_upload_candidate_t>(
          expected_upload_candidate_result());
        find_upload_candidates_result input;
        input.results.push_back(std::move(candidate));
        input.read_write_fence = read_write_fence;
        op_api->expect_schedule_uploads(
          input, expected_inline_manifest, error_outcome::shutting_down);
    }

    auto f = wf->stop();
    wf->start().get();
    std::move(f).get();
}

TEST(data_upload_workflow_test, test_upload_fatal_error) {
    // Test case: the workflow goes up to upload step
    // and then shutdown is triggered by throwing an exception.
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    // Set up maybe_suspend. The only call returns success.
    {
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = true,
          },
          expected_next_upload_action_hint(expected_inline_manifest));
    }

    // Set up find_upload_candidates call to return one upload candidate.
    {
        auto input = expected_upload_candidate_input();

        auto candidate = ss::make_lw_shared<segment_upload_candidate_t>(
          expected_upload_candidate_result());

        find_upload_candidates_result output;
        output.ntp = ntp;
        output.results.push_back(std::move(candidate));
        output.read_write_fence = read_write_fence;

        op_api->expect_find_upload_candidates(input, output);
    }

    // Set up schedule_uploads call.
    {
        auto candidate = ss::make_lw_shared<segment_upload_candidate_t>(
          expected_upload_candidate_result());
        find_upload_candidates_result input;
        input.results.push_back(std::move(candidate));
        input.read_write_fence = read_write_fence;
        op_api->expect_schedule_uploads(
          input,
          expected_inline_manifest,
          std::make_exception_ptr(std::runtime_error("fatal error")));
    }

    auto f = wf->stop();
    wf->start().get();
    ASSERT_THROW(std::move(f).get(), std::runtime_error);
}

TEST(data_upload_workflow_test, test_upload_recoverable_errc) {
    // Test case: the workflow goes up to upload step
    // and then timeout error is triggered by the error code.
    const auto expected_error = error_outcome::timed_out;
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    // Set up maybe_suspend to make two calls. First returns success
    // and the second returns shutdown.
    {
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = true,
          },
          expected_next_upload_action_hint(expected_inline_manifest));

        // Triggered by failed 'schedule_uploads' call
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .errc = expected_error,
          },
          error_outcome::shutting_down);
    }

    // Set up find_upload_candidates call to return one upload candidate.
    {
        auto input = expected_upload_candidate_input();

        auto candidate = ss::make_lw_shared<segment_upload_candidate_t>(
          expected_upload_candidate_result());

        find_upload_candidates_result output;
        output.ntp = ntp;
        output.results.push_back(std::move(candidate));
        output.read_write_fence = read_write_fence;

        op_api->expect_find_upload_candidates(input, output);
    }

    // Set up schedule_uploads call.
    {
        auto candidate = ss::make_lw_shared<segment_upload_candidate_t>(
          expected_upload_candidate_result());
        find_upload_candidates_result input;
        input.results.push_back(std::move(candidate));
        input.read_write_fence = read_write_fence;
        op_api->expect_schedule_uploads(
          input, expected_inline_manifest, expected_error);
    }

    auto f = wf->stop();
    wf->start().get();
    std::move(f).get();
}

TEST(data_upload_workflow_test, test_admit_shutdown_exception) {
    // Test case: the workflow goes up to admit step
    // and then shutdown is triggered by throwing an exception.
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    // Set up maybe_suspend. The only call returns success.
    {
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = true,
          },
          expected_next_upload_action_hint(expected_inline_manifest));
    }

    // Set up find_upload_candidates call to return one upload candidate.
    {
        auto input = expected_upload_candidate_input();

        auto candidate = ss::make_lw_shared<segment_upload_candidate_t>(
          expected_upload_candidate_result());

        find_upload_candidates_result output;
        output.ntp = ntp;
        output.results.push_back(std::move(candidate));
        output.read_write_fence = read_write_fence;

        op_api->expect_find_upload_candidates(input, output);
    }

    // Set up schedule_uploads call.
    {
        auto candidate = ss::make_lw_shared<segment_upload_candidate_t>(
          expected_upload_candidate_result());
        find_upload_candidates_result input;
        input.results.push_back(std::move(candidate));
        input.read_write_fence = read_write_fence;
        op_api->expect_schedule_uploads(
          input, expected_inline_manifest, expected_upload_result());
    }

    // Set up admit_uploads
    {
        auto input = expected_upload_result();
        op_api->expect_admit_uploads(
          std::move(input),
          std::make_exception_ptr(ss::abort_requested_exception()));
    }

    auto f = wf->stop();
    wf->start().get();
    std::move(f).get();
}

TEST(data_upload_workflow_test, test_admit_shutdown_errc) {
    // Test case: the workflow goes up to admit step
    // and then shutdown is triggered by returning an error code.
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    // Set up maybe_suspend. The call returns success.
    {
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = true,
          },
          expected_next_upload_action_hint(expected_inline_manifest));
    }

    // Set up find_upload_candidates call to return one upload candidate.
    {
        auto input = expected_upload_candidate_input();

        auto candidate = ss::make_lw_shared<segment_upload_candidate_t>(
          expected_upload_candidate_result());

        find_upload_candidates_result output;
        output.ntp = ntp;
        output.results.push_back(std::move(candidate));
        output.read_write_fence = read_write_fence;

        op_api->expect_find_upload_candidates(input, output);
    }

    // Set up schedule_uploads call.
    {
        auto candidate = ss::make_lw_shared<segment_upload_candidate_t>(
          expected_upload_candidate_result());
        find_upload_candidates_result input;
        input.results.push_back(std::move(candidate));
        input.read_write_fence = read_write_fence;
        op_api->expect_schedule_uploads(
          input, expected_inline_manifest, expected_upload_result());
    }

    // Set up admit_uploads
    {
        auto input = expected_upload_result();
        op_api->expect_admit_uploads(
          std::move(input), error_outcome::shutting_down);
    }

    auto f = wf->stop();
    wf->start().get();
    std::move(f).get();
}

TEST(data_upload_workflow_test, test_admit_fatal_error) {
    // Test case: the workflow goes up to admit step
    // and then fatal error is triggered by throwing an exception.
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    // Set up maybe_suspend. The only call returns success.
    {
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = true,
          },
          expected_next_upload_action_hint(expected_inline_manifest));
    }

    // Set up find_upload_candidates call to return one upload candidate.
    {
        auto input = expected_upload_candidate_input();

        auto candidate = ss::make_lw_shared<segment_upload_candidate_t>(
          expected_upload_candidate_result());

        find_upload_candidates_result output;
        output.ntp = ntp;
        output.results.push_back(std::move(candidate));
        output.read_write_fence = read_write_fence;

        op_api->expect_find_upload_candidates(input, output);
    }

    // Set up schedule_uploads call.
    {
        auto candidate = ss::make_lw_shared<segment_upload_candidate_t>(
          expected_upload_candidate_result());
        find_upload_candidates_result input;
        input.results.push_back(std::move(candidate));
        input.read_write_fence = read_write_fence;
        op_api->expect_schedule_uploads(
          input, expected_inline_manifest, expected_upload_result());
    }

    // Set up admit_uploads
    {
        auto input = expected_upload_result();
        op_api->expect_admit_uploads(
          std::move(input),
          std::make_exception_ptr(std::runtime_error("fatal error")));
    }

    auto f = wf->stop();
    wf->start().get();
    ASSERT_THROW(std::move(f).get(), std::runtime_error);
}

TEST(data_upload_workflow_test, test_admit_recoverable_errc) {
    // Test case: the workflow goes up to admit step
    // and then shutdown is triggered by returning an error code.
    const auto expected_error = error_outcome::timed_out;
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    // Set up maybe_suspend. First call returns success, second call returns an
    // error.
    {
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = true,
          },
          expected_next_upload_action_hint(expected_inline_manifest));

        // This one is called after 'admit_uploads' fails
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .errc = expected_error,
          },
          error_outcome::shutting_down);
    }

    // Set up find_upload_candidates call to return one upload candidate.
    {
        auto input = expected_upload_candidate_input();

        auto candidate = ss::make_lw_shared<segment_upload_candidate_t>(
          expected_upload_candidate_result());

        find_upload_candidates_result output;
        output.ntp = ntp;
        output.results.push_back(std::move(candidate));
        output.read_write_fence = read_write_fence;

        op_api->expect_find_upload_candidates(input, output);
    }

    // Set up schedule_uploads call.
    {
        auto candidate = ss::make_lw_shared<segment_upload_candidate_t>(
          expected_upload_candidate_result());
        find_upload_candidates_result input;
        input.results.push_back(std::move(candidate));
        input.read_write_fence = read_write_fence;
        op_api->expect_schedule_uploads(
          input, expected_inline_manifest, expected_upload_result());
    }

    // Set up admit_uploads
    {
        auto input = expected_upload_result();
        op_api->expect_admit_uploads(std::move(input), expected_error);
    }

    auto f = wf->stop();
    wf->start().get();
    std::move(f).get();
}

TEST(data_upload_workflow_test, test_reupload_manifest_cycle) {
    // Test case: the workflow reuploads the manifest after
    // the hint from the scheduler is received
    const size_t manifest_size = 12345;
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    // Setup maybe_suspend. The first call triggers manifest upload.
    // The second call triggers shutdown of the workflow.
    {
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = true,
          },
          expected_next_manifest_upload_action_hint());

        // called after 'upload_manifest' succeeds
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = false,
            .put_requests_used = 1,
            .uploaded_bytes = manifest_size,
          },
          error_outcome::shutting_down);
    }

    // Setup upload_manifest call to successfully return the result.
    {
        op_api->expect_upload_manifest(
          manifest_upload_arg{.ntp = ntp},
          manifest_upload_result{
            .ntp = ntp, .num_put_requests = 1, .size_bytes = manifest_size});
    }

    auto f = wf->stop();
    wf->start().get();
    std::move(f).get();
}

TEST(data_upload_workflow_test, test_reupload_manifest_shutdown_exception) {
    // Test case: the workflow fails to reupload manifest due to
    // the shutdown exception being thrown.
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    // Setup maybe_suspend. The first call triggers manifest upload.
    // The second call triggers shutdown of the workflow.
    {
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = true,
          },
          expected_next_manifest_upload_action_hint());
    }

    // Setup upload_manifest call to return shutdown error.
    {
        op_api->expect_upload_manifest(
          manifest_upload_arg{.ntp = ntp},
          std::make_exception_ptr(ss::abort_requested_exception()));
    }

    auto f = wf->stop();
    wf->start().get();
    std::move(f).get();
}

TEST(data_upload_workflow_test, test_reupload_manifest_shutdown_errc) {
    // Test case: the workflow fails to reupload manifest due to
    // the shutdown error returned via the error code.
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    // Setup maybe_suspend. The first call triggers manifest upload.
    // The second call triggers shutdown of the workflow.
    {
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = true,
          },
          expected_next_manifest_upload_action_hint());
    }

    // Setup upload_manifest call to return shutdown error.
    {
        op_api->expect_upload_manifest(
          manifest_upload_arg{.ntp = ntp}, error_outcome::shutting_down);
    }

    auto f = wf->stop();
    wf->start().get();
    std::move(f).get();
}

TEST(data_upload_workflow_test, test_reupload_manifest_fatal_error) {
    // Test case: the workflow fails to reupload manifest due to
    // the fatal error.
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    // Setup maybe_suspend. The first call triggers manifest upload.
    // The second call triggers shutdown of the workflow.
    {
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = true,
          },
          expected_next_manifest_upload_action_hint());
    }

    // Setup upload_manifest call to return shutdown error.
    {
        op_api->expect_upload_manifest(
          manifest_upload_arg{.ntp = ntp},
          std::make_exception_ptr(std::runtime_error("fatal error")));
    }

    auto f = wf->stop();
    wf->start().get();
    ASSERT_THROW(std::move(f).get(), std::runtime_error);
}

TEST(data_upload_workflow_test, test_reupload_manifest_recoverable_errc) {
    // Test case: the workflow fails to reupload manifest due to
    // the shutdown error returned via the error code.
    const auto error = error_outcome::timed_out;
    auto [wf, rm_api, op_api, cfg] = setup_test_suite();

    // Setup maybe_suspend. The first call triggers manifest upload.
    // The second call triggers shutdown of the workflow.
    {
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .manifest_dirty = true,
          },
          expected_next_manifest_upload_action_hint());

        // called after 'upload_manifest' returns error
        rm_api->expect_maybe_suspend(
          {
            .ntp = ntp,
            .errc = error,
          },
          error_outcome::shutting_down);
    }

    // Setup upload_manifest call to return shutdown error.
    op_api->expect_upload_manifest(manifest_upload_arg{.ntp = ntp}, error);

    auto f = wf->stop();
    wf->start().get();
    std::move(f).get();
}
} // namespace archival
