// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "http/client.h"

#include "net/transport.h"
#include "rpc/types.h"
#include "seastarx.h"
#include "syschecks/syschecks.h"
#include "vlog.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/report_exception.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include <boost/optional/optional.hpp>
#include <boost/outcome/detail/value_storage.hpp>
#include <boost/system/system_error.hpp>

#include <exception>
#include <stdexcept>
#include <string>

static ss::logger test_log{"test"};

void cli_opts(boost::program_options::options_description_easy_init opt) {
    namespace po = boost::program_options;

    opt(
      "ip", po::value<std::string>()->default_value("127.0.0.1"), "server IP");

    opt(
      "target", po::value<std::string>()->default_value("/"), "request target");

    opt("port", po::value<uint16_t>()->default_value(80), "server port");

    opt(
      "chunk-size",
      po::value<std::size_t>()->default_value(0x8000),
      "fragment data_size by this chunk size step (32KB default)");

    opt(
      "data",
      po::value<std::string>()->default_value(""),
      "POST data (optional)");

    opt(
      "method",
      po::value<std::string>()->default_value("GET"),
      "http method (GET/POST/PUT/etc)");
}

struct test_conf {
    std::size_t chunk_size;
    std::string data;
    std::string target;
    boost::beast::http::verb method;
    rpc::transport_configuration client_cfg;
};

inline std::ostream& operator<<(std::ostream& out, const test_conf& cfg) {
    // make the output json-able so we can consume it in python for analysis
    return out << "["
               << "'chunk_size': " << cfg.chunk_size << ", "
               << "'data': " << cfg.data << ", "
               << "'target': " << cfg.target << ", "
               << "'method': " << cfg.method << ", "
               << "'server_addr': " << cfg.client_cfg.server_addr << "]";
}

test_conf cfg_from(boost::program_options::variables_map& m) {
    rpc::transport_configuration client_cfg;
    client_cfg.server_addr = net::unresolved_address(
      m["ip"].as<std::string>(), m["port"].as<uint16_t>());
    return test_conf{
      .chunk_size = m["chunk-size"].as<std::size_t>(),
      .data = m["data"].as<std::string>(),
      .target = m["target"].as<std::string>(),
      .method = boost::beast::http::string_to_verb(
        m["method"].as<std::string>()),
      .client_cfg = std::move(client_cfg)};
}

http::client::request_header make_header(const test_conf& cfg) {
    http::client::request_header header;
    header.method(cfg.method);
    header.target(cfg.target);
    header.insert(
      boost::beast::http::field::content_length,
      boost::beast::to_static_string(cfg.data.length()));
    auto host = fmt::format("{}", cfg.client_cfg.server_addr);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_type, "application/json");
    return header;
}

ss::future<> send_post_request(
  http::client& cli, http::client::request_header&& header, iobuf&& body) {
    vlog(test_log.info, "send POST request");
    auto [req, resp] = cli.make_request(std::move(header)).get0();
    // send request
    req->send_some(std::move(body)).get();
    req->send_eof().get();
    while (!resp->is_done()) {
        iobuf buf = resp->recv_some().get0();
        for (auto& fragm : buf) {
            vlog(test_log.info, "{}", std::string(fragm.get(), fragm.size()));
        }
    }
    return ss::make_ready_future<>();
}

int main(int args, char** argv, char** env) {
    syschecks::initialize_intrinsics();
    std::setvbuf(stdout, nullptr, _IOLBF, 1024);
    ss::app_template app;
    cli_opts(app.add_options());
    ss::sharded<http::client> client;
    return app.run(args, argv, [&] {
        auto& cfg = app.configuration();
        return ss::async([&] {
            const test_conf lcfg = cfg_from(cfg);
            net::base_transport::configuration transport_cfg;
            transport_cfg.server_addr = lcfg.client_cfg.server_addr;
            transport_cfg.credentials = lcfg.client_cfg.credentials;
            vlog(test_log.info, "config:{}", lcfg);
            vlog(test_log.info, "constructing client");
            client.start(transport_cfg).get();
            auto cd = ss::defer([&client] {
                vlog(test_log.info, "defer:stop");
                client.stop().get();
            });
            vlog(test_log.info, "connecting");
            client
              .invoke_on(
                0,
                [lcfg](http::client& cli) {
                    vlog(test_log.info, "send POST request");
                    http::client::request_header header = make_header(lcfg);
                    iobuf body;
                    body.append(lcfg.data.data(), lcfg.data.size());
                    send_post_request(cli, std::move(header), std::move(body))
                      .get();
                })
              .get();
            vlog(test_log.info, "done");
        });
    });
}
