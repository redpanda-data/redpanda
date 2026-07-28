/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/seastarx.h"
#include "crash_tracker/types.h"
#include "model/timestamp.h"
#include "utils/file_io.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/smp_options.hh>
#include <seastar/util/log.hh>

#include <fmt/format.h>

#include <cstdio>
#include <exception>

int main(int ac, char* av[]) {
    int crash_type_int = static_cast<int>(crash_tracker::crash_type::unknown);
    std::string message, stacktrace;
    std::filesystem::path output_file;
    int64_t timestamp = 0;
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help", "Allowed options")(
      "type", po::value<int>(&crash_type_int), "set the crash type")(
      "crash-message", po::value<std::string>(&message), "Crash message")(
      "stacktrace",
      po::value<std::string>(&stacktrace),
      "Stack trace to print")(
      "output-file",
      po::value<std::filesystem::path>(&output_file),
      "output file")(
      "timestamp",
      po::value<int64_t>(&timestamp)
        ->default_value(
          std::chrono::system_clock::now().time_since_epoch().count()));

    po::variables_map vm;
    po::store(po::parse_command_line(ac, av, desc), vm);
    po::notify(vm);

    if (vm.count("help") || output_file.empty()) {
        std::cout << desc << "\n";
        return 1;
    }

    auto crash_type = static_cast<crash_tracker::crash_type>(crash_type_int);

    ss::app_template::seastar_options sscfg;
    sscfg.smp_opts.smp.set_value(1);
    sscfg.smp_opts.memory_allocator = ss::memory_allocator::standard;
    sscfg.reactor_opts.overprovisioned.set_value();
    sscfg.log_opts.default_log_level.set_value(ss::log_level::warn);
    ss::app_template app(std::move(sscfg));
    ss::sstring prog_name = "crash-report-generator";
    std::array<char*, 1> args = {prog_name.data()};
    try {
        return app.run(args.size(), args.data(), [&]() {
            crash_tracker::crash_description cd;
            cd.crash_message = crash_tracker::reserved_string<
              crash_tracker::crash_description::string_buffer_reserve>{message};
            cd.crash_time = model::timestamp{timestamp};
            cd.stacktrace = crash_tracker::reserved_string<
              crash_tracker::crash_description::string_buffer_reserve>{
              stacktrace};
            cd.type = crash_type;
            iobuf buf;
            serde::write(buf, std::move(cd));
            return write_fully(output_file, std::move(buf)).then([] {
                return 0;
            });
        });
    } catch (...) {
        fmt::print(stderr, "{}\n", std::current_exception());
        return 1;
    }
}
