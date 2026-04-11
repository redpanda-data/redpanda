/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/tests/s3_imposter.h"
#include "cloud_storage_clients/client_pool.h"
#include "cloud_storage_clients/multipart_upload.h"
#include "cloud_storage_clients/s3_client.h"
#include "cloud_storage_clients/s3_error.h"
#include "cloud_storage_clients/tests/client_pool_builder.h"
#include "test_utils/random_bytes.h"

#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

using namespace cloud_storage_clients;
using namespace cloud_storage_clients::tests;

static ss::logger test_log("multipart_upload_test");
static constexpr size_t test_part_size = 5_MiB;

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_basic) {
    s3_imposter_fixture imposter;

    // Set up multipart expectations
    imposter.set_expectations_and_listen({
      // CreateMultipartUpload
      {.url = "test-key?uploads", .body = R"xml(<?xml version="1.0"?>
<InitiateMultipartUploadResult>
    <UploadId>test-upload-id-123</UploadId>
</InitiateMultipartUploadResult>)xml"},

      // UploadPart 1
      {.url = "test-key?partNumber=1&uploadId=test-upload-id-123",
       .body = std::nullopt},

      // UploadPart 2
      {.url = "test-key?partNumber=2&uploadId=test-upload-id-123",
       .body = std::nullopt},

      // CompleteMultipartUpload
      {.url = "test-key?uploadId=test-upload-id-123",
       .body = R"xml(<?xml version="1.0"?>
<CompleteMultipartUploadResult>
    <ETag>"final-etag"</ETag>
</CompleteMultipartUploadResult>)xml"},
    });

    // Create client pool
    auto conf = imposter.get_configuration();
    const client_pool_builder pool_builder{conf};

    ss::sharded<client_pool> pool;
    auto stop_guard = pool_builder.connections_per_shard(1).build(pool).get();

    // Test multipart upload
    const auto test_bucket = bucket_name_parts{
      .name = plain_bucket_name(imposter.bucket_name)};
    const auto key = object_key("test-key");
    auto timeout = std::chrono::seconds(30);

    // Acquire client from pool
    ss::abort_source as;
    auto lease = pool.local().acquire(test_bucket, as).get();

    auto state_result = lease.client
                          ->initiate_multipart_upload(
                            test_bucket.name, key, test_part_size, timeout)
                          .get();

    BOOST_REQUIRE(state_result.has_value());
    auto upload = ss::make_shared<multipart_upload>(
      std::move(state_result.value()), test_part_size, test_log);

    // Write 6 MiB of data (should trigger one part upload immediately)
    auto data = ::tests::random_iobuf(6_MiB);
    upload->put(std::move(data)).get();

    // Write another 5 MiB (should trigger second part)
    data = ::tests::random_iobuf(5_MiB);
    upload->put(std::move(data)).get();

    // Complete the upload
    upload->complete().get();

    // Verify requests
    const auto& requests = imposter.get_requests();
    BOOST_CHECK_GE(requests.size(), 4); // Init + 2 Parts + Complete
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_small_file_optimization) {
    s3_imposter_fixture imposter;

    // Set up expectations for regular PUT (not multipart)
    imposter.set_expectations_and_listen({
      {.url = "small-key", .body = std::nullopt},
    });

    // Create client pool
    auto conf = imposter.get_configuration();
    const client_pool_builder pool_builder{conf};

    ss::sharded<client_pool> pool;
    auto stop_guard = pool_builder.connections_per_shard(1).build(pool).get();

    // Test multipart upload with small file
    const auto test_bucket = bucket_name_parts{
      .name = plain_bucket_name(imposter.bucket_name)};
    const auto key = object_key("small-key");
    auto timeout = std::chrono::seconds(30);

    // Acquire client from pool
    ss::abort_source as;
    auto lease = pool.local().acquire(test_bucket, as).get();

    auto state_result = lease.client
                          ->initiate_multipart_upload(
                            test_bucket.name, key, test_part_size, timeout)
                          .get();

    BOOST_REQUIRE(state_result.has_value());
    auto upload = ss::make_shared<multipart_upload>(
      std::move(state_result.value()), test_part_size, test_log);

    // Write only 3 MiB (less than part_size)
    auto data = ::tests::random_iobuf(3_MiB);
    upload->put(std::move(data)).get();

    // Complete - should use regular PUT, not multipart
    upload->complete().get();

    // Verify only one request was made (the regular PUT)
    const auto& requests = imposter.get_requests();
    BOOST_REQUIRE_EQUAL(requests.size(), 1);
    BOOST_CHECK_EQUAL(requests[0].method, "PUT");
    // Check that URL does not contain multipart indicators
    std::string url_str(requests[0].url);
    BOOST_CHECK(url_str.find("uploads") == std::string::npos);
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_abort) {
    s3_imposter_fixture imposter;

    // Set up multipart expectations including abort
    imposter.set_expectations_and_listen({
      // CreateMultipartUpload
      {.url = "abort-key?uploads", .body = R"xml(<?xml version="1.0"?>
<InitiateMultipartUploadResult>
    <UploadId>test-upload-id-456</UploadId>
</InitiateMultipartUploadResult>)xml"},

      // UploadPart 1
      {.url = "abort-key?partNumber=1&uploadId=test-upload-id-456",
       .body = std::nullopt},

      // AbortMultipartUpload (DELETE needs a body to return success)
      {.url = "abort-key?uploadId=test-upload-id-456", .body = ""},
    });

    // Create client pool
    auto conf = imposter.get_configuration();
    const client_pool_builder pool_builder{conf};

    ss::sharded<client_pool> pool;
    auto stop_guard = pool_builder.connections_per_shard(1).build(pool).get();

    // Test multipart upload abort
    const auto test_bucket = bucket_name_parts{
      .name = plain_bucket_name(imposter.bucket_name)};
    const auto key = object_key("abort-key");
    auto timeout = std::chrono::seconds(30);

    // Acquire client from pool
    ss::abort_source as;
    auto lease = pool.local().acquire(test_bucket, as).get();

    auto state_result = lease.client
                          ->initiate_multipart_upload(
                            test_bucket.name, key, test_part_size, timeout)
                          .get();

    BOOST_REQUIRE(state_result.has_value());
    auto upload = ss::make_shared<multipart_upload>(
      std::move(state_result.value()), test_part_size, test_log);

    // Write data to trigger multipart initialization
    auto data = ::tests::random_iobuf(6_MiB);
    upload->put(std::move(data)).get();

    // Abort instead of completing
    upload->abort().get();

    // Verify abort was called
    const auto& requests = imposter.get_requests();
    BOOST_CHECK_GE(requests.size(), 3); // Init + Part + Abort

    // Check that the last request was DELETE (abort)
    bool found_abort = false;
    for (const auto& req : requests) {
        if (
          req.method == "DELETE"
          && req.url.find("uploadId=test-upload-id-456") != std::string::npos) {
            found_abort = true;
            break;
        }
    }
    BOOST_CHECK(found_abort);
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_complete_error_in_body) {
    s3_imposter_fixture imposter;

    // Set up multipart expectations where CompleteMultipartUpload returns
    // HTTP 200 OK but with an error XML body. This is a documented AWS
    // behavior.
    // See:
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
    imposter.set_expectations_and_listen({
      // CreateMultipartUpload
      {.url = "error-key?uploads", .body = R"xml(<?xml version="1.0"?>
<InitiateMultipartUploadResult>
    <UploadId>test-upload-id-err</UploadId>
</InitiateMultipartUploadResult>)xml"},

      // UploadPart 1
      {.url = "error-key?partNumber=1&uploadId=test-upload-id-err",
       .body = std::nullopt},

      // UploadPart 2
      {.url = "error-key?partNumber=2&uploadId=test-upload-id-err",
       .body = std::nullopt},

      // CompleteMultipartUpload - returns 200 OK but with error XML body
      {.url = "error-key?uploadId=test-upload-id-err",
       .body
       = R"xml(<?xml version="1.0"?><Error><Code>InternalError</Code><Message>We encountered an internal error. Please try again.</Message><RequestId>req-123</RequestId><Resource>error-key</Resource></Error>)xml"},
    });

    // Create client pool
    auto conf = imposter.get_configuration();
    const client_pool_builder pool_builder{conf};

    ss::sharded<client_pool> pool;
    auto stop_guard = pool_builder.connections_per_shard(1).build(pool).get();

    // Test multipart upload with error-in-200 response
    const auto test_bucket = bucket_name_parts{
      .name = plain_bucket_name(imposter.bucket_name)};
    const auto key = object_key("error-key");
    auto timeout = std::chrono::seconds(30);

    // Acquire client from pool
    ss::abort_source as;
    auto lease = pool.local().acquire(test_bucket, as).get();

    auto state_result = lease.client
                          ->initiate_multipart_upload(
                            test_bucket.name, key, test_part_size, timeout)
                          .get();

    BOOST_REQUIRE(state_result.has_value());
    auto upload = ss::make_shared<multipart_upload>(
      std::move(state_result.value()), test_part_size, test_log);

    // Write exactly 5 MiB (triggers first part upload, no leftover)
    auto data = ::tests::random_iobuf(5_MiB);
    upload->put(std::move(data)).get();

    // Write another 5 MiB (triggers second part, no leftover)
    data = ::tests::random_iobuf(5_MiB);
    upload->put(std::move(data)).get();

    // Complete should throw rest_error_response with the embedded error
    BOOST_CHECK_EXCEPTION(
      upload->complete().get(),
      rest_error_response,
      [](const rest_error_response& err) {
          return err.code_string() == "InternalError"
                 && err.message()
                      == "We encountered an internal error. Please try again.";
      });
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_put_after_abort_is_noop) {
    s3_imposter_fixture imposter;

    imposter.set_expectations_and_listen({
      // CreateMultipartUpload
      {.url = "abort-put-key?uploads", .body = R"xml(<?xml version="1.0"?>
<InitiateMultipartUploadResult>
    <UploadId>test-upload-id-ap</UploadId>
</InitiateMultipartUploadResult>)xml"},

      // UploadPart 1
      {.url = "abort-put-key?partNumber=1&uploadId=test-upload-id-ap",
       .body = std::nullopt},

      // AbortMultipartUpload
      {.url = "abort-put-key?uploadId=test-upload-id-ap", .body = ""},
    });

    auto conf = imposter.get_configuration();
    const client_pool_builder pool_builder{conf};

    ss::sharded<client_pool> pool;
    auto stop_guard = pool_builder.connections_per_shard(1).build(pool).get();

    const auto test_bucket = bucket_name_parts{
      .name = plain_bucket_name(imposter.bucket_name)};
    const auto key = object_key("abort-put-key");
    auto timeout = std::chrono::seconds(30);

    ss::abort_source as;
    auto lease = pool.local().acquire(test_bucket, as).get();

    auto state_result = lease.client
                          ->initiate_multipart_upload(
                            test_bucket.name, key, test_part_size, timeout)
                          .get();

    BOOST_REQUIRE(state_result.has_value());
    auto upload = ss::make_shared<multipart_upload>(
      std::move(state_result.value()), test_part_size, test_log);

    // Write enough data to trigger multipart initialization
    auto data = ::tests::random_iobuf(6_MiB);
    upload->put(std::move(data)).get();

    // Abort the upload
    upload->abort().get();

    const auto requests_after_abort = imposter.get_requests().size();

    // put() after abort should be a no-op: no exception, no extra requests
    data = ::tests::random_iobuf(1_MiB);
    upload->put(std::move(data)).get();

    BOOST_CHECK_EQUAL(imposter.get_requests().size(), requests_after_abort);

    // complete() after abort should also be a no-op
    upload->complete().get();

    BOOST_CHECK_EQUAL(imposter.get_requests().size(), requests_after_abort);
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_put_after_complete_is_noop) {
    s3_imposter_fixture imposter;

    // Use the small file path for a simpler setup
    imposter.set_expectations_and_listen({
      {.url = "complete-put-key", .body = std::nullopt},
    });

    auto conf = imposter.get_configuration();
    const client_pool_builder pool_builder{conf};

    ss::sharded<client_pool> pool;
    auto stop_guard = pool_builder.connections_per_shard(1).build(pool).get();

    const auto test_bucket = bucket_name_parts{
      .name = plain_bucket_name(imposter.bucket_name)};
    const auto key = object_key("complete-put-key");
    auto timeout = std::chrono::seconds(30);

    ss::abort_source as;
    auto lease = pool.local().acquire(test_bucket, as).get();

    auto state_result = lease.client
                          ->initiate_multipart_upload(
                            test_bucket.name, key, test_part_size, timeout)
                          .get();

    BOOST_REQUIRE(state_result.has_value());
    auto upload = ss::make_shared<multipart_upload>(
      std::move(state_result.value()), test_part_size, test_log);

    // Write small data and complete
    auto data = ::tests::random_iobuf(1_MiB);
    upload->put(std::move(data)).get();
    upload->complete().get();

    const auto requests_after_complete = imposter.get_requests().size();

    // put() after complete should be a no-op: no exception, no extra requests
    data = ::tests::random_iobuf(1_MiB);
    upload->put(std::move(data)).get();

    BOOST_CHECK_EQUAL(imposter.get_requests().size(), requests_after_complete);

    // abort() after complete should also be a no-op
    upload->abort().get();

    BOOST_CHECK_EQUAL(imposter.get_requests().size(), requests_after_complete);
}
