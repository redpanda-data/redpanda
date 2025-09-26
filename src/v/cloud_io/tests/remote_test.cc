/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "cloud_io/remote.h"
#include "cloud_io/tests/s3_imposter.h"
#include "cloud_storage_clients/client_pool.h"
#include "model/metadata.h"
#include "test_utils/async.h"
#include "test_utils/test.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/resource.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

using namespace std::chrono_literals;
using namespace cloud_io;

namespace {
static ss::abort_source never_abort;

constexpr model::cloud_credentials_source config_file{
  model::cloud_credentials_source::config_file};

iobuf make_iobuf(std::string_view str) {
    iobuf buf;
    buf.append(str.data(), str.size());
    return buf;
}
} // namespace

class remote_fixture
  : public s3_imposter_fixture
  , public seastar_test {
public:
    remote_fixture()
      : s3_imposter_fixture() {}

    ss::future<> SetUpAsync() override {
        co_await pool.start(10, ss::sharded_parameter([this] { return conf; }));
        co_await io.start(
          std::ref(pool),
          ss::sharded_parameter([this] { return conf; }),
          ss::sharded_parameter([] { return config_file; }),
          ss::sharded_parameter([] { return ss::default_scheduling_group(); }));
    }
    seastar::future<> TearDownAsync() override {
        pool.local().shutdown_connections();
        io.local().request_stop();
        co_await io.stop();
        co_await pool.stop();
    }

    ss::sharded<cloud_storage_clients::client_pool> pool;
    ss::sharded<cloud_io::remote> io;
};

TEST_F_CORO(remote_fixture, notify_on_get_object) {
    set_expectations_and_listen({
      expectation{.url = "payload-key", .body = "/payload", .slowdown = false},
    });

    retry_chain_node fib(never_abort, 500ms, 10ms);

    auto filter = remote::event_filter{};
    auto sub = io.local().subscribe(filter);

    iobuf payload;

    auto fut = io.local().download_object({
      .transfer_details = {
        .bucket = bucket_name,
        .key = cloud_storage_clients::object_key("payload-key"),
        .parent_rtc = fib,
      },
      .display_str = "payload",
      .payload = payload,
    });

    auto notification = co_await std::move(sub);
    EXPECT_EQ(notification.type, api_activity_type::object_download);
    EXPECT_EQ(notification.is_retry, false);
    std::ignore = co_await std::move(fut);
}

TEST_F_CORO(remote_fixture, notify_on_put_object) {
    co_return;
    set_expectations_and_listen({
      expectation{.url = "/payload-key", .slowdown = false},
    });

    retry_chain_node fib(never_abort, 500ms, 10ms);

    auto filter = remote::event_filter{};
    auto sub = io.local().subscribe(filter);

    auto fut = io.local().upload_object({
      .transfer_details = {
        .bucket = bucket_name,
        .key = cloud_storage_clients::object_key("/payload-key"),
        .parent_rtc = fib,
      },
      .display_str = "payload",
      .payload = make_iobuf("payload"),
    });

    auto notification = co_await std::move(sub);
    EXPECT_EQ(notification.type, api_activity_type::object_upload);
    std::ignore = co_await std::move(fut);
}
