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

#define BOOST_TEST_MODULE cli_parser
#include "redpanda/cli_parser.h"

#include <seastar/core/app-template.hh>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/test/unit_test.hpp>

static ss::logger test_log{"test"};

namespace po = boost::program_options;

struct argv {
    explicit argv(const std::string& cmdline) { tokenize_and_store(cmdline); }

    std::pair<int, char**> args() { return {_av.size() - 1, _av.data()}; }

private:
    void tokenize_and_store(const std::string& cmdline) {
        boost::split(_tokens, cmdline, boost::is_any_of(" "));
        _av.resize(_tokens.size() + 1);
        _av.back() = nullptr;
        std::transform(
          _tokens.begin(), _tokens.end(), _av.begin(), [](auto& s) {
              return s.data();
          });
    }
    std::vector<char*> _av;
    std::vector<std::string> _tokens{};
};

BOOST_AUTO_TEST_CASE(test_positional_args_rejected) {
    po::options_description unused;
    argv a{"redpanda foo bar"};
    auto [ac, av] = a.args();
    cli_parser parser{
      ac,
      av,
      cli_parser::app_opts{unused},
      cli_parser::ss_opts{unused},
      test_log};
    po::variables_map vm;
    BOOST_REQUIRE(!parser.validate_into(vm));
    BOOST_REQUIRE(vm.empty());
}

BOOST_AUTO_TEST_CASE(test_help_flag) {
    po::options_description help;
    po::options_description ss;

    help.add_options()("help", "");

    argv a{"redpanda --help"};
    auto [ac, av] = a.args();

    cli_parser parser{
      ac, av, cli_parser::app_opts{help}, cli_parser::ss_opts{ss}, test_log};
    po::variables_map vm;
    BOOST_REQUIRE(parser.validate_into(vm));
    BOOST_REQUIRE(!vm.empty());
}

BOOST_AUTO_TEST_CASE(test_help_mixed_with_bad_pos_arg) {
    po::options_description help;
    po::options_description ss;

    help.add_options()("help", "");

    argv a{"redpanda --help bad"};
    auto [ac, av] = a.args();
    cli_parser parser{
      ac, av, cli_parser::app_opts{help}, cli_parser::ss_opts{ss}, test_log};
    po::variables_map vm;
    BOOST_REQUIRE(!parser.validate_into(vm));
    BOOST_REQUIRE(vm.empty());
}

BOOST_AUTO_TEST_CASE(test_flag_with_arguments) {
    po::options_description cfg;
    po::options_description ss;

    cfg.add_options()("redpanda-cfg", po::value<std::string>(), "");

    {
        argv a{"redpanda --redpanda-cfg f.yaml"};
        auto [ac, av] = a.args();
        cli_parser parser{
          ac, av, cli_parser::app_opts{cfg}, cli_parser::ss_opts{ss}, test_log};
        po::variables_map vm;
        BOOST_REQUIRE(parser.validate_into(vm));
        BOOST_REQUIRE(!vm.empty());
    }

    {
        argv a{"redpanda --redpanda-cfg=f.yaml"};
        auto [ac, av] = a.args();
        cli_parser parser{
          ac, av, cli_parser::app_opts{cfg}, cli_parser::ss_opts{ss}, test_log};
        po::variables_map vm;
        BOOST_REQUIRE(parser.validate_into(vm));
        BOOST_REQUIRE(!vm.empty());
    }
}

BOOST_AUTO_TEST_CASE(test_flags_with_arguments_and_bad_pos_arg) {
    po::options_description cfg;
    po::options_description ss;

    cfg.add_options()("redpanda-cfg", po::value<std::string>(), "");

    argv a{"redpanda --redpanda-cfg f.yaml testing"};
    auto [ac, av] = a.args();
    cli_parser parser{
      ac, av, cli_parser::app_opts{cfg}, cli_parser::ss_opts{ss}, test_log};
    po::variables_map vm;
    BOOST_REQUIRE(!parser.validate_into(vm));
    BOOST_REQUIRE(vm.empty());
}

BOOST_AUTO_TEST_CASE(test_redpanda_and_ss_opts) {
    seastar::app_template app;

    app.add_options()("redpanda-cfg", po::value<std::string>(), "");
    const auto& cfg = app.get_options_description();
    const auto& ss = app.get_conf_file_options_description();

    {
        argv a{"redpanda --redpanda-cfg f.yaml --smp 2 --memory 4G --mbind 1 "
               "--num-io-groups=1"};
        auto [ac, av] = a.args();
        cli_parser parser{
          ac, av, cli_parser::app_opts{cfg}, cli_parser::ss_opts{ss}, test_log};
        po::variables_map vm;
        BOOST_REQUIRE(parser.validate_into(vm));
        BOOST_REQUIRE(!vm.empty());
    }

    {
        argv a{"redpanda --help-loggers"};
        auto [ac, av] = a.args();
        cli_parser parser{
          ac, av, cli_parser::app_opts{cfg}, cli_parser::ss_opts{ss}, test_log};
        po::variables_map vm;
        BOOST_REQUIRE(parser.validate_into(vm));
        BOOST_REQUIRE(!vm.empty());
    }
}
