
#include "base/vlog.h"
#include "kafka/client/direct_consumer/verifier/application.h"
#include "redpanda/cli_parser.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
using namespace kafka::client;

int main(int ac, char* av[], char** /*env*/) {
    listener_configuration conf{};
    namespace po = boost::program_options;
    ss::app_template::config app_cfg;
    app_cfg.name = "direct_consumer_verifier";
    app_cfg.description = "Direct consumer verifier application";
    ss::app_template app_template(app_cfg);

    app_template.add_options()(
      "port", po::value<int16_t>()->default_value(9999), "Port to use")(
      "hostname",
      po::value<std::string>()->default_value("localhost"),
      "Hostname to bind to ");

    po::variables_map vm;
    if (!cli_parser{
          ac,
          av,
          cli_parser::app_opts{app_template.get_options_description()},
          cli_parser::ss_opts{app_template.get_conf_file_options_description()},
          v_logger}
           .validate_into(vm)) {
        return 1;
    }

    conf.port = vm["port"].as<int16_t>();
    conf.host = vm["hostname"].as<std::string>();
    try {
        verifier_application app;
        return app_template.run(ac, av, [&]() { return app.run(conf); });

    } catch (...) {
        vlog(
          v_logger.error,
          "Error running verifier: {}",
          std::current_exception());
        return 1;
    }
}
