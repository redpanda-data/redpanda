/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "http/client.h"
#include "s3/client.h"
#include "s3/error.h"
#include "s3/signature.h"
#include "seastarx.h"
#include "syschecks/syschecks.h"
#include "utils/hdr_hist.h"
#include "vlog.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/report_exception.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/defer.hh>

#include <boost/optional/optional.hpp>
#include <boost/outcome/detail/value_storage.hpp>
#include <boost/system/system_error.hpp>
#include <gnutls/gnutls.h>

#include <chrono>
#include <exception>
#include <optional>
#include <stdexcept>
#include <string>

static ss::logger test_log{"test"};

void cli_opts(boost::program_options::options_description_easy_init opt) {
    namespace po = boost::program_options;

    opt(
      "object",
      po::value<std::string>()->default_value("test.txt"),
      "s3 object id");

    opt(
      "bucket",
      po::value<std::string>()->default_value("test-rps3support"),
      "s3 bucket");

    opt(
      "accesskey",
      po::value<std::string>()->default_value(""),
      "aws access key");

    opt(
      "secretkey",
      po::value<std::string>()->default_value(""),
      "aws secret key");

    opt(
      "region",
      po::value<std::string>()->default_value("us-east-1"),
      "aws region");

    opt(
      "in",
      po::value<std::string>()->default_value(""),
      "file to send to the S3 object");

    opt(
      "out",
      po::value<std::string>()->default_value(""),
      "file to receive data from S3 object");

    opt(
      "list-with-prefix", po::value<std::string>(), "list objects in a bucket");

    opt("delete", po::value<std::string>(), "delete object in a bucket");
}

struct test_conf {
    s3::bucket_name bucket;
    s3::object_key object;

    s3::configuration client_cfg;

    std::string in;
    std::string out;
    bool list_with_prefix;
    bool delete_object;
};

inline std::ostream& operator<<(std::ostream& out, const test_conf& cfg) {
    // make the output json-able so we can consume it in python for analysis
    return out << "["
               << "'bucket': " << cfg.bucket << ", "
               << "'object': " << cfg.object << "]";
}

test_conf cfg_from(boost::program_options::variables_map& m) {
    auto access_key = s3::public_key_str(m["accesskey"].as<std::string>());
    auto secret_key = s3::private_key_str(m["secretkey"].as<std::string>());
    auto region = s3::aws_region_name(m["region"].as<std::string>());
    s3::configuration client_cfg = s3::configuration::make_configuration(
                                     access_key, secret_key, region)
                                     .get0();
    vlog(test_log.info, "connecting to {}", client_cfg.server_addr);
    return test_conf{
      .bucket = s3::bucket_name(m["bucket"].as<std::string>()),
      .object = s3::object_key(m["object"].as<std::string>()),
      .client_cfg = std::move(client_cfg),
      .in = m["in"].as<std::string>(),
      .out = m["out"].as<std::string>(),
      .list_with_prefix = m.count("list-with-prefix") > 0,
      .delete_object = m.count("delete") > 0,
    };
}

static std::pair<ss::input_stream<char>, uint64_t>
get_input_file_as_stream(const std::filesystem::path& path) {
    auto file = ss::open_file_dma(path.native(), ss::open_flags::ro).get0();
    auto size = file.size().get0();
    return std::make_pair(ss::make_file_input_stream(std::move(file), 0), size);
}

static ss::sstring time_to_string(std::chrono::system_clock::time_point tp) {
    std::stringstream s;
    auto tt = std::chrono::system_clock::to_time_t(tp);
    auto tm = *std::gmtime(&tt);
    s << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S");
    return s.str();
}

static ss::output_stream<char>
get_output_file_as_stream(const std::filesystem::path& path) {
    auto file = ss::open_file_dma(
                  path.native(), ss::open_flags::rw | ss::open_flags::create)
                  .get0();
    return ss::make_file_output_stream(std::move(file)).get0();
}

int main(int args, char** argv, char** env) {
    syschecks::initialize_intrinsics();
    std::setvbuf(stdout, nullptr, _IOLBF, 1024);
    ss::app_template app;
    cli_opts(app.add_options());
    ss::sharded<s3::client> client;
    return app.run(args, argv, [&] {
        auto& cfg = app.configuration();
        return ss::async([&] {
            const test_conf lcfg = cfg_from(cfg);
            s3::configuration s3_cfg = lcfg.client_cfg;
            vlog(test_log.info, "config:{}", lcfg);
            vlog(test_log.info, "constructing client");
            client.start(s3_cfg).get();
            vlog(test_log.info, "connecting");
            client
              .invoke_on(
                0,
                [lcfg](s3::client& cli) {
                    vlog(test_log.info, "sending request");
                    if (!lcfg.out.empty()) {
                        vlog(test_log.info, "receiving file {}", lcfg.out);
                        auto out_file = get_output_file_as_stream(lcfg.out);
                        try {
                            auto resp = cli
                                          .get_object(
                                            lcfg.bucket,
                                            lcfg.object,
                                            http::default_connect_timeout)
                                          .get0()
                                          ->as_input_stream();
                            vlog(test_log.info, "response: OK");
                            ss::copy(resp, out_file).get();
                            vlog(test_log.info, "file write done");
                            resp.close().get();
                        } catch (const s3::rest_error_response& err) {
                            vlog(
                              test_log.error,
                              "Error response: {}, code: {}, "
                              "resource: {}, request_id: {}",
                              err.message(),
                              err.code(),
                              err.resource(),
                              err.request_id());
                        }
                        out_file.flush().get();
                        out_file.close().get();
                    } else if (!lcfg.in.empty()) {
                        // put
                        vlog(test_log.info, "sending file {}", lcfg.in);
                        auto [payload, payload_size] = get_input_file_as_stream(
                          lcfg.in);
                        try {
                            cli
                              .put_object(
                                lcfg.bucket,
                                lcfg.object,
                                payload_size,
                                std::move(payload),
                                {},
                                http::default_connect_timeout)
                              .get0();
                        } catch (const s3::rest_error_response& err) {
                            vlog(
                              test_log.error,
                              "Error response: {}, code: {}, "
                              "resource: {}, request_id: {}",
                              err.message(),
                              err.code(),
                              err.resource(),
                              err.request_id());
                        }
                    } else if (lcfg.list_with_prefix) {
                        // put
                        vlog(test_log.info, "listing objects");
                        try {
                            auto res = cli.list_objects_v2(lcfg.bucket).get0();
                            vlog(
                              test_log.info,
                              "ListBucketV2 result, prefix: {}, is-truncated: "
                              "{}, "
                              "contents:",
                              res.prefix,
                              res.is_truncated);
                            for (const auto& item : res.contents) {
                                vlog(
                                  test_log.info,
                                  "\tkey: {}, last_modified: {}, size_bytes: "
                                  "{}",
                                  item.key,
                                  time_to_string(item.last_modified),
                                  item.size_bytes);
                            }
                        } catch (const s3::rest_error_response& err) {
                            vlog(
                              test_log.error,
                              "Error response: {}, code: {}, "
                              "resource: {}, request_id: {}",
                              err.message(),
                              err.code(),
                              err.resource(),
                              err.request_id());
                        }
                    } else if (lcfg.delete_object) {
                        // put
                        vlog(test_log.info, "deleting objects");
                        try {
                            cli
                              .delete_object(
                                lcfg.bucket,
                                lcfg.object,
                                http::default_connect_timeout)
                              .get();
                            vlog(test_log.info, "DeleteObject completed");
                        } catch (const s3::rest_error_response& err) {
                            vlog(
                              test_log.error,
                              "Error response: {}, code: {}, "
                              "resource: {}, request_id: {}",
                              err.message(),
                              err.code(),
                              err.resource(),
                              err.request_id());
                        }
                    }
                })
              .get();
            client.stop().get();
            vlog(test_log.info, "done");
            ss::sleep(std::chrono::milliseconds(500)).get();
        });
    });
}
