/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "cloud_roles/types.h"
#include "cloud_storage_clients/abs_client.h"
#include "http/client.h"
#include "seastarx.h"
#include "syschecks/syschecks.h"
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
      "blob",
      po::value<std::vector<std::string>>()->default_value({"test.txt"}),
      "ABS blob id");

    opt(
      "storage-account",
      po::value<std::string>()->default_value("test-storage-account"),
      "ABS Storage Account");

    opt(
      "container",
      po::value<std::string>()->default_value("test-container"),
      "ABS Container");

    opt(
      "shared-key",
      po::value<std::string>()->default_value(""),
      "ABS Shared Key");

    opt(
      "in",
      po::value<std::string>()->default_value(""),
      "file to read data for ABS blob");

    opt(
      "out",
      po::value<std::string>()->default_value(""),
      "file to receive data from ABS blob");

    opt(
      "list-with-prefix",
      po::value<std::string>(),
      "list blobs in a container");

    opt("delete", po::value<std::string>(), "delete object in container");
    opt(
      "get-metadata",
      po::value<std::string>(),
      "get metadata for object in container");

    opt(
      "uri", po::value<std::string>(), "alternative uri for the api endpoint");

    opt("port", po::value<uint16_t>(), "alternative port for the api endpoint");

    opt("disable-tls", "disable tls for this connection");
    opt("hns-enabled", "HNS enabled for the storage account");
}

struct test_conf {
    cloud_roles::storage_account storage_account;
    cloud_storage_clients::bucket_name container;

    std::vector<cloud_storage_clients::object_key> blobs;

    cloud_storage_clients::abs_configuration client_cfg;

    std::string in;
    std::string out;

    bool delete_blob;
    bool get_metadata;

    std::optional<cloud_storage_clients::object_key> list_with_prefix;
};

template<>
struct fmt::formatter<test_conf> : public fmt::formatter<std::string_view> {
    auto format(const test_conf& cfg, auto& ctx) const {
        // make the output json-able so we can consume it in python for analysis
        return formatter<std::string_view>::format(
          fmt::format(
            "[ 'storage-account': '{}', 'container': '{}', 'blobs': ['{}'] ]",
            cfg.storage_account,
            cfg.container,
            fmt::join(cfg.blobs, "', '")),
          ctx);
    }
};

test_conf cfg_from(boost::program_options::variables_map& m) {
    auto shared_key = cloud_roles::private_key_str(
      m["shared-key"].as<std::string>());
    auto storage_acc = cloud_roles::storage_account(
      m["storage-account"].as<std::string>());
    auto container = cloud_storage_clients::bucket_name(
      m["container"].as<std::string>());

    std::optional<cloud_storage_clients::object_key> prefix = std::nullopt;
    if (m.contains("list-with-prefix")) {
        prefix = cloud_storage_clients::object_key{
          m["list-with-prefix"].as<std::string>()};
    }

    cloud_storage_clients::abs_configuration client_cfg
      = cloud_storage_clients::abs_configuration::make_configuration(
          shared_key,
          storage_acc,
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
          .get0();
    if (m.contains("hns-enabled")) {
        client_cfg.is_hns_enabled = true;
    }
    vlog(test_log.info, "connecting to {}", client_cfg.server_addr);
    return test_conf{
      .storage_account = storage_acc,
      .container = container,
      .blobs =
        [&] {
            auto keys = m["blob"].as<std::vector<std::string>>();
            auto out = std::vector<cloud_storage_clients::object_key>{};
            std::transform(
              keys.begin(),
              keys.end(),
              std::back_inserter(out),
              [](auto const& ks) {
                  return cloud_storage_clients::object_key(ks);
              });
            return out;
        }(),
      .client_cfg = std::move(client_cfg),
      .in = m["in"].as<std::string>(),
      .out = m["out"].as<std::string>(),
      .delete_blob = m.count("delete") > 0,
      .get_metadata = m.count("get-metadata") > 0,
      .list_with_prefix = prefix};
}

// TODO(vlad): factor this out
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

static ss::lw_shared_ptr<cloud_roles::apply_credentials>
make_credentials(const cloud_storage_clients::abs_configuration& cfg) {
    return ss::make_lw_shared(
      cloud_roles::make_credentials_applier(cloud_roles::abs_credentials{
        cfg.storage_account_name, cfg.shared_key.value()}));
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
    ss::sharded<cloud_storage_clients::abs_client> client;

    return app.run(args, argv, [&] {
        auto& cfg = app.configuration();
        return ss::async([&] {
            test_conf lcfg = cfg_from(cfg);
            cloud_storage_clients::abs_configuration abs_cfg = lcfg.client_cfg;
            vlog(test_log.info, "config:{}", lcfg);
            vlog(test_log.info, "constructing client");
            auto credentials_applier = make_credentials(abs_cfg);
            client.start(abs_cfg, credentials_applier).get();
            vlog(test_log.info, "connecting");
            client
              .invoke_on(
                0,
                [lcfg = std::move(lcfg)](
                  cloud_storage_clients::abs_client& cli) mutable {
                    vlog(test_log.info, "sending request");
                    if (!lcfg.out.empty()) {
                        // Get Blob
                        vlog(test_log.info, "receiving file {}", lcfg.out);
                        auto out_file = get_output_file_as_stream(lcfg.out);
                        const auto result = cli
                                              .get_object(
                                                lcfg.container,
                                                lcfg.blobs.front(),
                                                http::default_connect_timeout)
                                              .get0();
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
                        // Put Blob
                        vlog(test_log.info, "sending file {}", lcfg.in);
                        auto [payload, payload_size] = get_input_file_as_stream(
                          lcfg.in);
                        const auto result = cli
                                              .put_object(
                                                lcfg.container,
                                                lcfg.blobs.front(),
                                                payload_size,
                                                std::move(payload),
                                                http::default_connect_timeout)
                                              .get0();
                        if (!result) {
                            vlog(
                              test_log.error,
                              "PUT request failed: {}",
                              result.error());
                        }
                    } else if (lcfg.delete_blob) {
                        // Delete Blob
                        vlog(test_log.info, "Deleting blob");
                        const auto result = cli
                                              .delete_object(
                                                lcfg.container,
                                                lcfg.blobs.front(),
                                                http::default_connect_timeout)
                                              .get();

                        if (result) {
                            vlog(test_log.info, "Delete Blob completed");
                        } else {
                            vlog(
                              test_log.error,
                              "Delete request failed: {}",
                              result.error());
                        }
                    } else if (lcfg.get_metadata) {
                        // Get Blob Metadata
                        vlog(test_log.info, "Getting blob metadata");
                        const auto result = cli
                                              .head_object(
                                                lcfg.container,
                                                lcfg.blobs.front(),
                                                http::default_connect_timeout)
                                              .get();
                        if (result) {
                            vlog(
                              test_log.info,
                              "Get Blob Metadata completed: etag={}",
                              result.value().etag);
                        } else {
                            vlog(
                              test_log.error,
                              "Get Blob Metadata request failed: {}",
                              result.error());
                        }
                    } else if (lcfg.list_with_prefix.has_value()) {
                        vlog(test_log.info, "Listing blobs");

                        if (lcfg.list_with_prefix.value()() == "none") {
                            lcfg.list_with_prefix.reset();
                        }

                        const auto result = cli
                                              .list_objects(
                                                lcfg.container,
                                                lcfg.list_with_prefix)
                                              .get();
                        if (result) {
                            const auto& val = result.value();
                            vlog(
                              test_log.info,
                              "List Blobs result, prefix: {}, "
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
                              "Get Blob Metadata request failed: {}",
                              result.error());
                        }
                    }
                })
              .get();
            client.stop().get();
            vlog(test_log.info, "done");
            ss::sleep(std::chrono::seconds(1)).get();
        });
    });
}
