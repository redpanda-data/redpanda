// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/uri.h"

#include <seastar/testing/perf_tests.hh>

PERF_TEST(from_uri, s3_compat) {
    auto converter = iceberg::uri_converter(cloud_io::s3_compat_provider{
      .scheme = "s3",
    });
    auto bucket = cloud_storage_clients::bucket_name("bucket");
    std::filesystem::path path(
      "a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z");
    auto uri = ss::sstring(
      "s3://bucket/a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z");

    perf_tests::start_measuring_time();
    auto result = converter.from_uri(bucket, iceberg::uri(uri));
    perf_tests::stop_measuring_time();

    vassert(result.has_value(), "Expected a value");
    vassert(result.value() == path, "Unexpected value");
}
