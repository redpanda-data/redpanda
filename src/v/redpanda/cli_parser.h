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

namespace boost::program_options {
class options_description;
}

struct cli_parser {
    cli_parser(
      int ac,
      char** av,
      const boost::program_options::options_description& opts_desc,
      const boost::program_options::options_description& conf_file_opts_desc);

    bool validate();

private:
    int _ac;
    char** _av;
    const boost::program_options::options_description& _opts_desc;
    const boost::program_options::options_description& _conf_opts_desc;
};
