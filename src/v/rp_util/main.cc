/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "boost/program_options.hpp"
#include "compat/run.h"
#include "redpanda/cluster_config_schema_util.h"
#include "seastar/core/app-template.hh"
#include "seastarx.h"
#include "version.h"

#include <filesystem>
#include <iostream>

namespace {
int corpus_write(char** argv, std::filesystem::path dir) {
    seastar::app_template app;
    try {
        return app.run(1, argv, [dir = std::move(dir)]() {
            return compat::write_corpus(dir).then([] { return 0; });
        });
    } catch (...) {
        std::cerr << std::current_exception() << "\n";
        return 1;
    }
}

int corpus_check(char** argv, std::filesystem::path path) {
    seastar::app_template app;
    try {
        return app.run(1, argv, [path = std::move(path)]() -> ss::future<int> {
            return compat::check_type(path).then([] { return 0; });
        });
    } catch (...) {
        std::cerr << std::current_exception() << "\n";
        return 1;
    }
}
} // namespace

/**
 * This binary is meant to host developer friendly utilities related to core.
 * Few things to note when adding new capabilities to this.
 *
 * - This is _not_ customer facing tooling.
 * - This is _not_ a CLI tool to access Redpanda services.
 * - This may _not_ be shipped as a part of official release artifacts.
 * - This tool provides _no backward compatibility_ of any sorts.
 */
int main(int ac, char* av[]) {
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");

    // clang-format off
    desc.add_options()
      ("help", "Allowed options")
      ("config_schema_json", "Generates JSON schema for cluster configuration")
      ("corpus_write", po::value<std::filesystem::path>(), "Writes data structure corpus")
      ("corpus_check", po::value<std::filesystem::path>(), "Check a corpus test case")
      ("version", "Redpanda core version for this utility");
    // clang-format on

    po::variables_map vm;
    po::store(po::parse_command_line(ac, av, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << "\n";
    } else if (vm.count("config_schema_json")) {
        std::cout << util::generate_json_schema(config::configuration())._res
                  << "\n";
    } else if (vm.count("version")) {
        std::cout << redpanda_version() << "\n";
    } else if (vm.count("corpus_write")) {
        return corpus_write(av, vm["corpus_write"].as<std::filesystem::path>());
    } else if (vm.count("corpus_check")) {
        return corpus_check(av, vm["corpus_check"].as<std::filesystem::path>());
    }

    return 0;
}
