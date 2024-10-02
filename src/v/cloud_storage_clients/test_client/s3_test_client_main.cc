/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/seastarx.h"
#include "base/vlog.h"
#include "bytes/iobuf.h"
#include "cloud_storage_clients/s3_client.h"
#include "cloud_storage_clients/s3_error.h"
#include "cloud_storage_clients/types.h"
#include "http/client.h"
#include "syschecks/syschecks.h"

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
      po::value<std::vector<std::string>>()->default_value({"test.txt"}),
      "s3 object id, can be called multiple times to use with "
      "--delete-multiple");

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
      "url_style",
      po::value<std::string>()->default_value(""),
      "aws addressing style");

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

    opt("delete-multiple", "issue a delete objects request");

    opt(
      "uri", po::value<std::string>(), "alternative uri for the api endpoint");

    opt("port", po::value<uint16_t>(), "alternative port for the api endpoint");

    opt("disable-tls", "disable tls for this connection");
}

struct test_conf {
    cloud_storage_clients::bucket_name bucket;
    std::vector<cloud_storage_clients::object_key> objects;

    cloud_storage_clients::s3_configuration client_cfg;

    std::string in;
    std::string out;
    bool list_with_prefix;
    bool delete_object;
    bool delete_multiple;
};

template<>
struct fmt::formatter<test_conf> : public fmt::formatter<std::string_view> {
    auto format(const test_conf& cfg, auto& ctx) const {
        // make the output json-able so we can consume it in python for analysis
        return formatter<std::string_view>::format(
          fmt::format(
            "[ 'bucket': '{}', 'objects': ['{}'], 'path style': {} ]",
            cfg.bucket,
            fmt::join(cfg.objects, "', '"),
            cfg.client_cfg.url_style),
          ctx);
    }
};

template<>
struct fmt::formatter<
  cloud_storage_clients::client::delete_objects_result::key_reason>
  : public fmt::formatter<std::string_view> {
    auto format(const auto& kr, auto& ctx) const {
        return formatter<std::string_view>::format(
          fmt::format(R"kr(key:"{}" reason:"{}")kr", kr.key, kr.reason), ctx);
    }
};

test_conf cfg_from(boost::program_options::variables_map& m) {
    auto access_key = cloud_roles::public_key_str(
      m["accesskey"].as<std::string>());
    auto secret_key = cloud_roles::private_key_str(
      m["secretkey"].as<std::string>());
    auto region = cloud_roles::aws_region_name(m["region"].as<std::string>());
    auto url_style =
      [&]() -> std::optional<cloud_storage_clients::s3_url_style> {
        const auto url_style_str = m["url_style"].as<std::string>();
        if (url_style_str == "virtual_host") {
            return cloud_storage_clients::s3_url_style::virtual_host;
        } else if (url_style_str == "path") {
            return cloud_storage_clients::s3_url_style::path;
        } else {
            return std::nullopt;
        }
    }();

    auto bucket_name = cloud_storage_clients::bucket_name(
      m["bucket"].as<std::string>());
    cloud_storage_clients::s3_configuration client_cfg
      = cloud_storage_clients::s3_configuration::make_configuration(
          access_key,
          secret_key,
          region,
          bucket_name,
          url_style,
          false,
          cloud_storage_clients::default_overrides{
            .endpoint =
              [&]() -> std::optional<cloud_storage_clients::endpoint_url> {
                if (m.count("uri") > 0) {
                    return cloud_storage_clients::endpoint_url{
                      m["uri"].as<std::string>()};
                }
                return std::nullopt;
            }(),
            .port = [&]() -> std::optional<uint16_t> {
                if (m.contains("port") > 0) {
                    return m["port"].as<uint16_t>();
                }
                return std::nullopt;
            }(),
            .disable_tls = m.contains("disable-tls") > 0,
          })
          .get();
    vlog(test_log.info, "connecting to {}", client_cfg.server_addr);
    return test_conf{
      .bucket = bucket_name,
      .objects =
        [&] {
            auto keys = m["object"].as<std::vector<std::string>>();
            auto out = std::vector<cloud_storage_clients::object_key>{};
            std::transform(
              keys.begin(),
              keys.end(),
              std::back_inserter(out),
              [](const auto& ks) {
                  return cloud_storage_clients::object_key(ks);
              });
            return out;
        }(),
      .client_cfg = std::move(client_cfg),
      .in = m["in"].as<std::string>(),
      .out = m["out"].as<std::string>(),
      .list_with_prefix = m.count("list-with-prefix") > 0,
      .delete_object = m.count("delete") > 0,
      .delete_multiple = m.count("delete-multiple") > 0,
    };
}

static std::pair<ss::input_stream<char>, uint64_t>
get_input_file_as_stream(const std::filesystem::path& path) {
    auto file = ss::open_file_dma(path.native(), ss::open_flags::ro).get();
    auto size = file.size().get();
    return std::make_pair(ss::make_file_input_stream(std::move(file), 0), size);
}

static ss::sstring time_to_string(std::chrono::system_clock::time_point tp) {
    std::stringstream s;
    auto tt = std::chrono::system_clock::to_time_t(tp);
    auto tm = *std::gmtime(&tt);
    s << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S");
    return s.str();
}

static ss::lw_shared_ptr<cloud_roles::apply_credentials>
make_credentials(const cloud_storage_clients::s3_configuration& cfg) {
    return ss::make_lw_shared(
      cloud_roles::make_credentials_applier(cloud_roles::aws_credentials{
        cfg.access_key.value(),
        cfg.secret_key.value(),
        std::nullopt,
        cfg.region}));
}

static ss::output_stream<char>
get_output_file_as_stream(const std::filesystem::path& path) {
    auto file = ss::open_file_dma(
                  path.native(), ss::open_flags::rw | ss::open_flags::create)
                  .get();
    return ss::make_file_output_stream(std::move(file)).get();
}

int main(int args, char** argv, char** env) {
    syschecks::initialize_intrinsics();
    std::setvbuf(stdout, nullptr, _IOLBF, 1024);
    ss::app_template app;
    cli_opts(app.add_options());
    ss::sharded<cloud_storage_clients::s3_client> client;

    return app.run(args, argv, [&] {
        auto& cfg = app.configuration();
        return ss::async([&] {
            const test_conf lcfg = cfg_from(cfg);
            cloud_storage_clients::s3_configuration s3_cfg = lcfg.client_cfg;
            vlog(test_log.info, "config:{}", lcfg);
            vlog(test_log.info, "constructing client");
            auto credentials_applier = make_credentials(s3_cfg);
            client.start(s3_cfg, credentials_applier).get();
            vlog(test_log.info, "connecting");
            client
              .invoke_on(
                0,
                [lcfg](cloud_storage_clients::s3_client& cli) {
                    vlog(test_log.info, "sending request");
                    if (!lcfg.out.empty()) {
                        vlog(test_log.info, "receiving file {}", lcfg.out);
                        auto out_file = get_output_file_as_stream(lcfg.out);
                        const auto result = cli
                                              .get_object(
                                                lcfg.bucket,
                                                lcfg.objects.front(),
                                                http::default_connect_timeout)
                                              .get();
                        if (result) {
                            auto resp = result.value()->as_input_stream();
                            vlog(test_log.info, "response: OK");
                            ss::copy(resp, out_file).get();
                            vlog(test_log.info, "file write done");
                            resp.close().get();
                        } else {
                            vlog(
                              test_log.error,
                              "GET request failed: {}",
                              result.error());
                        }
                        out_file.flush().get();
                        out_file.close().get();
                    } else if (!lcfg.in.empty()) {
                        // put
                        vlog(test_log.info, "sending file {}", lcfg.in);
                        auto [payload, payload_size] = get_input_file_as_stream(
                          lcfg.in);
                        const auto result = cli
                                              .put_object(
                                                lcfg.bucket,
                                                lcfg.objects.front(),
                                                payload_size,
                                                std::move(payload),
                                                http::default_connect_timeout)
                                              .get();

                        if (!result) {
                            vlog(
                              test_log.error,
                              "PUT request failed: {}",
                              result.error());
                        }
                    } else if (lcfg.list_with_prefix) {
                        vlog(test_log.info, "listing objects");
                        const auto result = cli.list_objects(lcfg.bucket).get();

                        if (result) {
                            const auto& val = result.value();
                            vlog(
                              test_log.info,
                              "ListBucketV2 result, prefix: {}, "
                              "is-truncated: "
                              "{}, "
                              "contents:",
                              val.prefix,
                              val.is_truncated);
                            for (const auto& item : val.contents) {
                                vlog(
                                  test_log.info,
                                  "\tkey: {}, last_modified: {}, "
                                  "size_bytes: "
                                  "{}",
                                  item.key,
                                  time_to_string(item.last_modified),
                                  item.size_bytes);
                            }
                        } else {
                            vlog(
                              test_log.error,
                              "List request failed: {}",
                              result.error());
                        }
                    } else if (lcfg.delete_object) {
                        vlog(test_log.info, "deleting objects");
                        const auto result = cli
                                              .delete_object(
                                                lcfg.bucket,
                                                lcfg.objects.front(),
                                                http::default_connect_timeout)
                                              .get();

                        if (result) {
                            vlog(test_log.info, "DeleteObject completed");
                        } else {
                            vlog(
                              test_log.error,
                              "Delete request failed: {}",
                              result.error());
                        }
                    } else if (lcfg.delete_multiple) {
                        vlog(test_log.info, "delete multiple objects");
                        if (auto undeleted = cli
                                               .delete_objects(
                                                 lcfg.bucket,
                                                 lcfg.objects,
                                                 http::default_connect_timeout)
                                               .get();
                            undeleted.has_value()) {
                            vlog(test_log.info, "DeleteObjects completed");
                            if (!undeleted.value().undeleted_keys.empty()) {
                                vlog(
                                  test_log.warn,
                                  "keys not deleted: [{}]",
                                  fmt::join(
                                    undeleted.value().undeleted_keys, "] ["));
                            }
                        } else {
                            vlog(
                              test_log.error,
                              "DeleteObject request failed: {}",
                              undeleted.error());
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
