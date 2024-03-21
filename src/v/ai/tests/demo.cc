/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "ai/service.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>

#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>

namespace ai {

bool readline(std::string& line) {
    std::cout << "> ";
    return bool(std::getline(std::cin, line));
}

ss::future<> run(const boost::program_options::variables_map& cfg) {
    ss::sharded<service> s;
    co_await s.start();
    service::config service_config{
      .model_file = cfg["model_file"].as<std::filesystem::path>(),
    };
    co_await s.invoke_on_all(
      [&service_config](service& s) { return s.start(service_config); });
    service::generate_text_options opts = {
      .max_tokens = cfg["max_tokens"].as<int32_t>(),
    };
    std::string line;
    while (readline(line)) {
        auto response = co_await s.local().generate_text(line, opts);
        std::cout << response << std::endl;
    }
    co_await s.stop();
}
} // namespace ai

int main(int argc, char** argv) {
    ss::app_template app;

    namespace po = boost::program_options;
    app.add_options()(
      "model_file",
      po::value<std::filesystem::path>(),
      "The path to the ML model");
    constexpr int32_t default_max_tokens = 150;
    app.add_options()(
      "max_tokens",
      po::value<int32_t>()->default_value(default_max_tokens),
      "The max number of tokens to generate");

    return app.run(argc, argv, [&] {
        const boost::program_options::variables_map& cfg = app.configuration();
        return ai::run(cfg).then([] { return 0; });
    });
}
