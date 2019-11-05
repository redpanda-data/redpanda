#include "rpc/server.h"

#include "rpc/demo/demo_utils.h"
#include "rpc/demo/simple_service.h"
#include "seastarx.h"
#include "syschecks/syschecks.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>

#include <string>

static logger lgr{"demo server"};

struct service final : demo::simple_service {
    using demo::simple_service::simple_service;
    future<demo::simple_reply>
    put(demo::simple_request&&, rpc::streaming_context&) final {
        return make_ready_future<demo::simple_reply>(demo::simple_reply{{}});
    }
    future<demo::complex_reply>
    put_complex(demo::complex_request&&, rpc::streaming_context&) final {
        return make_ready_future<demo::complex_reply>(demo::complex_reply{{}});
    }
    future<demo::interspersed_reply> put_interspersed(
      demo::interspersed_request&&, rpc::streaming_context&) final {
        return make_ready_future<demo::interspersed_reply>(
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
    seastar::sharded<rpc::server> serv;
    seastar::app_template app;
    cli_opts(app.add_options());
    return app.run_deprecated(args, argv, [&] {
        seastar::engine().at_exit([&serv] {
            // clang-format off
            return do_with(hdr_hist{}, [&serv](hdr_hist& h) {
                  auto begin = boost::make_counting_iterator(uint32_t(0));
                  auto end = boost::make_counting_iterator(uint32_t(smp::count));
                  return do_for_each(begin, end, [&h, &serv](uint32_t i) {
                              return serv.invoke_on(i, [&h](const rpc::server& s) {
                                    h += s.histogram();
                              });
                     }) .then([&h] { return write_histogram("server.hdr", h); });
              }).then([&serv] { return serv.stop(); });
            // clang-format on
        });
        auto& cfg = app.configuration();
        return async([&] {
            rpc::server_configuration scfg;
            scfg.addrs.push_back(socket_address(ipv4_addr(
              cfg["ip"].as<std::string>(), cfg["port"].as<uint16_t>())));
            scfg.max_service_memory_per_core = memory::stats().total_memory()
                                               * .9 /*90%*/;
            auto key = cfg["key"].as<std::string>();
            auto cert = cfg["cert"].as<std::string>();
            if (key != "" && cert != "") {
                auto builder = tls::credentials_builder();
                builder.set_dh_level(tls::dh_params::level::MEDIUM);
                builder.set_x509_key_file(cert, key, tls::x509_crt_format::PEM)
                  .get();
                scfg.credentials = std::move(builder);
            }
            serv.start(scfg).get();
            lgr.info("registering service on all cores");
            serv
              .invoke_on_all([](rpc::server& s) {
                  s.register_service<service>(
                    default_scheduling_group(), default_smp_service_group());
              })
              .get();
            lgr.info("Invoking rpc start on all cores");
            serv.invoke_on_all(&rpc::server::start).get();
        });
    });
}
