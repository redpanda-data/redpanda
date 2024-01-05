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

#pragma once

#include "base/seastarx.h"
#include "utils/named_type.h"

#include <seastar/util/log.hh>

#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>

/// Parses command line options passed to the redpanda binary. Validates the
/// options against a set of registered values. The values should include both
/// those explicitly registered against the redpanda application as well as
/// those that seastar adds during its startup process.
///
/// The parser in its current state rejects any positional arguments as these
/// are not supported with redpanda yet.
struct cli_parser {
    using opts_desc = boost::program_options::options_description;
    using app_opts = named_type<opts_desc, struct app_opts_tag>;
    using ss_opts = named_type<opts_desc, struct ss_opts_tag>;

    cli_parser(
      int ac,
      char** av,
      app_opts opts_desc,
      ss_opts seastar_opts,
      ss::logger& log);

    // Validates the arguments, returning true if the input arguments match
    // those expected by the configured options. Upon success, the resulting
    // variables are placed into 'vm'.
    bool validate_into(boost::program_options::variables_map& vm);

private:
    int _ac;
    char** _av;
    app_opts _opts_desc;
    ss_opts _seastar_opts_desc;
    ss::logger& _log;
};
