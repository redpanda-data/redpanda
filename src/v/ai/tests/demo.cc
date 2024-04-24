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
    fflush(stdout);
    fflush(stderr);
    std::cout << "> ";
    return bool(std::getline(std::cin, line));
}

ss::future<> run(const boost::program_options::variables_map& cfg) {
    ss::sharded<service> s;
    service::config service_config{
      .llm_path = "/tmp/ai/llm/",
      .embeddings_path = "/tmp/ai/embd/",
    };
    try {
        co_await s.start(service_config);
        co_await s.invoke_on_all(&service::start);
        co_await s.local().deploy_text_generation_model(huggingface_file{
          .repo = "IlyaGusev/saiga_llama3_8b_gguf",
          .filename = "model-q4_K.gguf",
        });
        for (const auto& [id, name] : co_await s.local().list_models()) {
            std::cout << "name: " << name() << "\n";
        }
        service::generate_text_options opts = {
          .max_tokens = cfg["max_tokens"].as<int32_t>(),
        };
        std::string line;
        while (readline(line)) {
            // auto response = co_await s.local().generate_text(line, opts);
            // std::cout << response << "\n";
            fflush(stdout);
        }
    } catch (const std::exception& ex) {
        std::cout << "error: " << ex.what() << "\n";
    }
    co_await s.stop();
}
} // namespace ai

int main(int argc, char** argv) {
    ss::app_template app;

    namespace po = boost::program_options;
    app.add_options()(
      "repo", po::value<ss::sstring>(), "The hugging face model");
    app.add_options()(
      "file", po::value<ss::sstring>(), "The path to the ML model");
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
