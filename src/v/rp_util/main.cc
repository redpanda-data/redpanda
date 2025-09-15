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

#include "base/seastarx.h"
#include "base/vassert.h"
#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "redpanda/admin/cluster_config_schema_util.h"
#include "version/version.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/iostream.hh>

#include <cerrno>
#include <filesystem>
#include <iostream>
#include <memory>

namespace {

int run_seastar(std::function<ss::future<int>()> main) {
    ss::app_template::seastar_options seastar_config;
    // Use a small footprint for this tool.
    seastar_config.smp_opts.smp.set_value(1);
    seastar_config.smp_opts.memory_allocator = ss::memory_allocator::standard;
    seastar_config.reactor_opts.overprovisioned.set_value();
    seastar_config.log_opts.default_log_level.set_value(ss::log_level::warn);
    seastar::app_template app(std::move(seastar_config));
    ss::sstring program_name = "rp_util";
    std::array<char*, 1> args = {program_name.data()};
    try {
        return app.run(args.size(), args.data(), std::move(main));
    } catch (...) {
        std::cerr << std::current_exception() << "\n";
        return 1;
    }
}

int print_cluster_config_schema() {
    return run_seastar([]() -> ss::future<int> {
        auto schema = util::generate_json_schema(config::configuration());
        if (!schema._body_writer) {
            vassert(
              !schema._res.empty(),
              "expected string if there is no body writer");
            std::cout << schema._res << "\n";
            return ss::as_ready_future(0);
        }
        vassert(
          schema._res.empty(),
          "expected empty string result if there is a body writer, got: {}",
          schema._res);
        auto buf = std::make_unique<iobuf>();
        return schema._body_writer(make_iobuf_ref_output_stream(*buf.get()))
          .then([buf = std::move(buf)]() -> int {
              for (const auto& chunk : *buf) {
                  std::cout << std::string_view{chunk.get(), chunk.size()};
              }
              std::cout << "\n";
              return 0;
          });
    });
}
} // namespace

/**
 * This binary is meant to host developer friendly utilities related to core.
 * Few things to note when adding new capabilities to this.
 *
 * - This is _not_ customer facing tooling.
 * - This is _not_ a CLI tool to access Redpanda services.
 * - This may _not_ be shipped as a part of official release artifacts.
 */
int main(int ac, char* av[]) {
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");

    // clang-format off
    desc.add_options()
      ("help", "Allowed options")
      ("config_schema_json", "Generates JSON schema for cluster configuration")
      ("version", "Redpanda core version for this utility");
    // clang-format on

    po::variables_map vm;
    po::store(po::parse_command_line(ac, av, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << "\n";
    } else if (vm.count("config_schema_json")) {
        return print_cluster_config_schema();
    } else if (vm.count("version")) {
        std::cout << redpanda_version() << "\n";
    } else {
        std::cout << "missing option" << "\n";
        std::cout << desc << "\n";
        return 1;
    }

    return 0;
}
