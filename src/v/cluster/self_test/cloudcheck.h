/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cluster/self_test/metrics.h"
#include "cluster/self_test_rpc_types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>

#include <exception>
#include <optional>
#include <vector>

namespace cluster::self_test {

class cloudcheck_exception : public std::runtime_error {
public:
    explicit cloudcheck_exception(const std::string& msg)
      : std::runtime_error(msg) {}
};

class cloudcheck {
public:
    cloudcheck(ss::sharded<cloud_storage::remote>& cloud_storage_api);

    /// Initialize the benchmark
    ss::future<> start();

    /// Stops the benchmark
    ss::future<> stop();

    /// Sets member variables and runs the cloud benchmark.
    ss::future<std::vector<self_test_result>> run(cloudcheck_opts opts);

    /// Signal to stop all work as soon as possible
    ///
    /// Immediately returns, waiter can expect to wait on the results to be
    /// returned by \run to be available shortly
    void cancel();

private:
    // Invokes the various cloud storage operations for testing.
    ss::future<std::vector<self_test_result>> run_benchmarks();

    // Make a random payload to be uploaded to cloud storage, and to be verified
    // once it is read back.
    iobuf make_random_payload(size_t size = 1024) const;

    // Generate an upload request.
    cloud_storage::upload_request make_upload_request(
      const cloud_storage_clients::bucket_name& bucket,
      const cloud_storage_clients::object_key& key,
      iobuf payload,
      retry_chain_node& rtc);

    // Generate a download request.
    cloud_storage::download_request make_download_request(
      const cloud_storage_clients::bucket_name& bucket,
      const cloud_storage_clients::object_key& key,
      iobuf& payload,
      retry_chain_node& rtc);

    // Wrapper around verify_tests for timing purposes, regardless of test
    // failure or success.
    template<
      typename Test,
      typename... Args,
      typename R = std::invoke_result_t<Test, cloudcheck, Args...>>
    R do_run_test(Test test, Args&&... args);

    struct verify_upload_result {
        self_test_result test_result;
    };

    // Verify that uploading (Put: write operation) to cloud storage works.
    ss::future<verify_upload_result> verify_upload(
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key key,
      const iobuf& payload);

    struct verify_list_result {
        cloud_storage::remote::list_result list_result;
        self_test_result test_result;
    };

    // Verify that listing (List: read operation) from cloud storage works.
    ss::future<verify_list_result> verify_list(
      cloud_storage_clients::bucket_name bucket,
      std::optional<cloud_storage_clients::object_key> prefix,
      size_t max_keys = num_default_objects);

    struct verify_head_result {
        self_test_result test_result;
    };

    // Verify that checking if an object exists (Head: read operation) from
    // cloud storage works.
    ss::future<verify_head_result> verify_head(
      cloud_storage_clients::bucket_name bucket,
      std::optional<cloud_storage_clients::object_key> key);

    struct verify_download_result {
        std::optional<iobuf> buf;
        self_test_result test_result;
    };

    // Verify that downloading (Get: read operation) from cloud storage works.
    ss::future<verify_download_result> verify_download(
      cloud_storage_clients::bucket_name bucket,
      std::optional<cloud_storage_clients::object_key> key);

    struct verify_delete_result {
        self_test_result test_result;
    };

    // Verify that deleting (Delete: write operation) from cloud storage works.
    ss::future<verify_delete_result> verify_delete(
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key key);

    struct verify_deletes_result {
        self_test_result test_result;
    };

    // Verify that deleting multiple (Plural Delete: write operation) from cloud
    // storage works.
    ss::future<verify_deletes_result> verify_deletes(
      cloud_storage_clients::bucket_name bucket,
      size_t num_objects = num_default_objects);

private:
    static constexpr size_t num_default_objects = 5;
    bool _cancelled{false};
    ss::abort_source _as;
    ss::gate _gate;
    retry_chain_node _rtc;
    cloudcheck_opts _opts;

    const cloud_storage_clients::object_key self_test_prefix
      = cloud_storage_clients::object_key{"self-test/"};

private:
    ss::sharded<cloud_storage::remote>& _cloud_storage_api;
};

} // namespace cluster::self_test
