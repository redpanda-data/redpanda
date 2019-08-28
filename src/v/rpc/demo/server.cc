#include "rpc/server.h"

#include "rpc/demo/types.h"
#include "seastarx.h"
#include "syschecks/syschecks.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>

#include <string>

static logger lgr{"demo server"};

void cli_opts(boost::program_options::options_description_easy_init o) {
    namespace po = boost::program_options;
    o("ip",
      po::value<std::string>()->default_value("127.0.0.1"),
      "ip to connect to");
    o("port", po::value<uint16_t>()->default_value(20776), "port for service");
}

int main(int args, char** argv, char** env) {
    syschecks::initialize_intrinsics();
    std::setvbuf(stdout, nullptr, _IOLBF, 1024);
    seastar::distributed<rpc::server> serv;
    seastar::app_template app;
    cli_opts(app.add_options());
    return app.run_deprecated(args, argv, [&] {
        seastar::engine().at_exit([&serv] { return serv.stop(); });
        auto& cfg = app.configuration();
        rpc::server_configuration scfg;
        scfg.addrs.push_back(socket_address(
          ipv4_addr(cfg["ip"].as<std::string>(), cfg["port"].as<uint16_t>())));
        scfg.max_service_memory_per_core = memory::stats().total_memory()
                                           * .9 /*90%*/;
        return serv.start(scfg)
          .then([&serv] {
              lgr.info("registering service");
              return serv.invoke_on_all(
                &rpc::server::register_service<demo::simple_service>);
          })
          .then([&serv] {
              lgr.info("Invoking rpc start on all cores");
              return serv.invoke_on_all(&rpc::server::start);
          });
    });
}
