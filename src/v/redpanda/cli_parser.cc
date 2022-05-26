/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cli_parser.h"

#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include <iostream>

namespace po = boost::program_options;

cli_parser::cli_parser(
  int ac,
  char** av,
  const po::options_description& opts_desc,
  const po::options_description& conf_file_opts_desc)
  : _ac{ac}
  , _av{av}
  , _opts_desc{opts_desc}
  , _conf_opts_desc{conf_file_opts_desc} {}

bool cli_parser::validate() {
    po::options_description desc;
    // Copy the cli options added by redpanda, plus the seastar options added in
    // app-template constructor
    desc.add(_opts_desc);
    desc.add(_conf_opts_desc);

    // help-loggers is not added in app-template constructor but is handled
    // specially in the app_template::run_deprecated method by seastar
    desc.add_options()("help-loggers", "");

    try {
        po::variables_map vm;
        po::store(
          po::command_line_parser{_ac, _av}.options(desc).positional({}).run(),
          vm);
    } catch (const std::exception& err) {
        std::cerr << "Error: " << err.what() << "\n";
        return false;
    }

    return true;
}
