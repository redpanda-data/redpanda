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
#include "version.h"

#include <iostream>

namespace po = boost::program_options;

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
    po::options_description desc("Allowed options");
    // clang-format off
    desc.add_options()
      ("help", "Allowed options")
      ("config_schema_json", "Generates JSON schema for cluster configuration")
      ("write_corpus", "Writes data in binary+JSON format for compat checking")
      ("read_corpus", "Reades data in binary+JSON format and compares contents")
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
    } else if (vm.count("write_corpus") || vm.count("read_corpus")) {
        seastar::app_template app;
        try {
            return app.run(ac, av, [ac, av]() -> ss::future<int> {
                co_return co_await compat::run(ac, av);
            });
        } catch (...) {
            std::cerr << "Couldn't start application: "
                      << std::current_exception() << "\n";
            return 1;
        }
        return 0;
    } else if (vm.count("version")) {
        std::cout << redpanda_version() << "\n";
    }
    return 0;
}
