// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "net/server.h"

#include "rpc/demo/demo_utils.h"
#include "rpc/demo/simple_service.h"
#include "rpc/simple_protocol.h"
#include "syschecks/syschecks.h"
#include "vlog.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>
#include <seastar/net/tls.hh>

#include <string>

static ss::logger lgr{"demo server"};

struct service final : demo::simple_service {
    using demo::simple_service::simple_service;
    ss::future<demo::simple_reply>
    put(demo::simple_request&&, rpc::streaming_context&) final {
        return ss::make_ready_future<demo::simple_reply>(
          demo::simple_reply{{}});
    }
    ss::future<demo::complex_reply>
    put_complex(demo::complex_request&&, rpc::streaming_context&) final {
        return ss::make_ready_future<demo::complex_reply>(
          demo::complex_reply{{}});
    }
    ss::future<demo::interspersed_reply> put_interspersed(
      demo::interspersed_request&&, rpc::streaming_context&) final {
        return ss::make_ready_future<demo::interspersed_reply>(
          demo::interspersed_reply{{}});
    }
};

void cli_opts(boost::program_options::options_description_easy_init o) {
    namespace po = boost::program_options;
    o("ip",
      po::value<std::string>()->default_value("127.0.0.1"),
      "ip to connect to");
    o("port", po::value<uint16_t>()->default_value(20776), "port for service");
    o("key",
      po::value<std::string>()->default_value(""),
      "key for TLS seccured connection");
    o("cert",
      po::value<std::string>()->default_value(""),
      "cert for TLS seccured connection");
}

int main(int args, char** argv, char** env) {
    syschecks::initialize_intrinsics();
    std::setvbuf(stdout, nullptr, _IOLBF, 1024);
    ss::sharded<net::server> serv;
    ss::app_template app;
    cli_opts(app.add_options());
    return app.run_deprecated(args, argv, [&] {
        ss::engine().at_exit([&serv] {
            // clang-format off
            return ss::do_with(hdr_hist{}, [&serv](hdr_hist& h) {
                  auto begin = boost::make_counting_iterator(uint32_t(0));
                  auto end = boost::make_counting_iterator(uint32_t(ss::smp::count));
                  return ss::do_for_each(begin, end, [&h, &serv](uint32_t i) {
                              return serv.invoke_on(i, [&h](const net::server& s) {
                                    h += s.histogram();
                              });
                     }) .then([&h] { return write_histogram("server.hdr", h); });
              }).then([&serv] { return serv.stop(); });
            // clang-format on
        });
        auto& cfg = app.configuration();
        return ss::async([&] {
            net::server_configuration scfg("demo_rpc");
            auto key = cfg["key"].as<std::string>();
            auto cert = cfg["cert"].as<std::string>();
            ss::shared_ptr<ss::tls::server_credentials> creds;
            if (key != "" && cert != "") {
                auto builder = ss::tls::credentials_builder();
                builder.set_dh_level(ss::tls::dh_params::level::MEDIUM);
                builder
                  .set_x509_key_file(cert, key, ss::tls::x509_crt_format::PEM)
                  .get();
                creds = builder.build_reloadable_server_credentials().get0();
            }
            scfg.addrs.emplace_back(
              ss::socket_address(ss::ipv4_addr(
                cfg["ip"].as<std::string>(), cfg["port"].as<uint16_t>())),
              creds);
            scfg.max_service_memory_per_core
              = ss::memory::stats().total_memory() * .9 /*90%*/;

            serv.start(scfg).get();
            vlog(lgr.info, "registering service on all cores");
            serv
              .invoke_on_all([](net::server& s) {
                  auto proto = std::make_unique<rpc::simple_protocol>();
                  proto->register_service<service>(
                    ss::default_scheduling_group(),
                    ss::default_smp_service_group());
                  s.set_protocol(std::move(proto));
              })
              .get();
            vlog(lgr.info, "Invoking rpc start on all cores");
            serv.invoke_on_all(&net::server::start).get();
        });
    });
}
