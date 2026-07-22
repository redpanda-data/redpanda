/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iobuf.h"
#include "cloud_io/remote.h"
#include "cloud_io/tests/scoped_remote.h"
#include "cloud_storage_clients/configuration.h"
#include "cloud_storage_clients/types.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/net/api.hh>

#include <gtest/gtest.h>

#include <chrono>

using namespace std::chrono_literals;

namespace {

/// TCP server that accepts connections and discards everything it reads,
/// never sending a response. Emulates a blackholed S3 endpoint: the
/// client's request lands in an established socket and then no forward
/// progress ever happens.
class blackhole_server {
public:
    void start() {
        ss::listen_options lo;
        lo.reuse_address = true;
        _sock = ss::listen(
          ss::socket_address(ss::net::inet_address("127.0.0.1"), 0), lo);
        _port = _sock.local_address().port();
        _accept_loop = accept_loop();
    }

    uint16_t port() const { return _port; }

    ss::future<> stop() {
        _stopped = true;
        _sock.abort_accept();
        for (auto& s : _connections) {
            s.shutdown_input();
            s.shutdown_output();
        }
        co_await std::move(_accept_loop);
    }

private:
    ss::future<> accept_loop() {
        while (!_stopped) {
            try {
                auto ar = co_await _sock.accept();
                _connections.push_back(std::move(ar.connection));
                // Drain the request in the background; never respond.
                std::ignore = drain(_connections.back().input());
            } catch (...) {
                // abort_accept() lands here on stop.
                co_return;
            }
        }
    }

    static ss::future<> drain(ss::input_stream<char> in) {
        try {
            while (true) {
                auto buf = co_await in.read();
                if (buf.empty()) {
                    co_return;
                }
            }
        } catch (...) {
        }
    }

    ss::server_socket _sock;
    ss::future<> _accept_loop = ss::make_ready_future<>();
    std::vector<ss::connected_socket> _connections;
    uint16_t _port{0};
    bool _stopped{false};
};

cloud_storage_clients::s3_configuration blackhole_configuration(uint16_t port) {
    cloud_storage_clients::s3_configuration conf;
    conf.uri = cloud_storage_clients::access_point_uri("127.0.0.1");
    conf.access_key = cloud_roles::public_key_str("access-key");
    conf.secret_key = cloud_roles::private_key_str("secret-key");
    conf.region = cloud_roles::aws_region_name("us-east-1");
    conf.service = cloud_roles::aws_service_name("s3");
    conf.url_style = cloud_storage_clients::s3_url_style::path;
    conf.server_addr = net::unresolved_address("127.0.0.1", port);
    return conf;
}

enum class upload_outcome { finished_error, finished_success };

} // namespace

class RemoteAbortTest : public seastar_test {
public:
    void SetUp() override {
        _server.start();
        _scoped = cloud_io::scoped_remote::create(
          1, blackhole_configuration(_server.port()));
    }

    void TearDown() override {
        _scoped->request_stop();
        _scoped.reset();
        _server.stop().get();
    }

    cloud_io::remote& remote() { return _scoped->remote.local(); }

    blackhole_server _server;
    std::unique_ptr<cloud_io::scoped_remote> _scoped;
};

TEST_F(RemoteAbortTest, AbortShutsDownInflightUpload) {
    ss::abort_source as;
    // Deadline far in the future: promptness must come from the abort,
    // not from the retry-chain deadline.
    retry_chain_node rtc(as, 300s, 100ms);
    cloud_storage_clients::bucket_name bucket("test-bucket");
    cloud_storage_clients::object_key key("test-key");

    auto fut = remote().upload_object(
      {.transfer_details = {.bucket = bucket, .key = key, .parent_rtc = rtc},
       .display_str = "blackhole-upload",
       .payload = bytes_to_iobuf(bytes(bytes::initialized_zero{}, 1024))});

    // Let the PUT get in flight on the established, silent connection.
    ss::sleep(500ms).get();
    ASSERT_FALSE(fut.available()) << "upload finished against a blackhole "
                                     "server; test precondition broken";

    as.request_abort_ex(ss::timed_out_error{});

    // Map any completion (value or exception) to a value, so a timeout of
    // the bounded wait below unambiguously means "still parked".
    auto outcome_fut = std::move(fut).then_wrapped(
      [](ss::future<cloud_io::upload_result> f) {
          try {
              auto r = f.get();
              return r == cloud_io::upload_result::success
                       ? upload_outcome::finished_success
                       : upload_outcome::finished_error;
          } catch (...) {
              return upload_outcome::finished_error;
          }
      });
    try {
        auto outcome = ss::with_timeout(
                         ss::lowres_clock::now() + 10s, std::move(outcome_fut))
                         .get();
        ASSERT_EQ(outcome, upload_outcome::finished_error);
    } catch (const ss::timed_out_error&) {
        FAIL() << "upload_object still parked 10s after abort — abort did "
                  "not reach the in-flight attempt";
    }
}
