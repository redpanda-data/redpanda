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

#include "vlog.h"

#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include <iostream>
#include <utility>

namespace po = boost::program_options;

cli_parser::cli_parser(
  int ac, char** av, app_opts opts_desc, ss_opts seastar_opts, ss::logger& log)
  : _ac{ac}
  , _av{av}
  , _opts_desc{std::move(opts_desc)}
  , _seastar_opts_desc{std::move(seastar_opts)}
  , _log{log} {}

bool cli_parser::validate_into(po::variables_map& vm) {
    po::options_description desc;
    // Copy the cli options added by redpanda, plus
    desc.add(_opts_desc);

    // Copy the seastar options added in app-template constructor
    desc.add(_seastar_opts_desc);

    try {
        po::store(
          po::command_line_parser{_ac, _av}.options(desc).positional({}).run(),
          vm);
    } catch (const std::exception& err) {
        vlog(_log.error, "Argument parse error: {}", err.what());
        return false;
    }

    return true;
}
