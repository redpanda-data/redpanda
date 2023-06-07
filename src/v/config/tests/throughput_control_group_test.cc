/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "config/config_store.h"
#include "config/property.h"
#include "config/throughput_control_group.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/interface.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <yaml-cpp/emitterstyle.h>
#include <yaml-cpp/exceptions.h>

#include <algorithm>
#include <exception>
#include <iterator>
#include <locale>
#include <vector>

using namespace std::string_literals;

SEASTAR_THREAD_TEST_CASE(throughput_control_group_test) {
    std::vector<config::throughput_control_group> tcgv;
    tcgv.emplace_back();

    struct test_config : public config::config_store {
        config::property<std::vector<config::throughput_control_group>> cgroups;
        test_config()
          : cgroups(*this, "cgroups", "") {}
    };

    auto cfg_node = YAML::Load(R"(
cgroups:
    - name: ""
      client_id: client_id-1
    - client_id: cli.+_id-\d+
    - client_id: another unnamed group indended to verify this passes validation
    - name: match-nothing-group
      client_id: ""
    - name: unspecified client_id
      client_id: +empty
    - name: cgroup-catchall because no clientid group given
)");
    cfg_node.SetStyle(YAML::EmitterStyle::Flow);

    test_config cfg;
    BOOST_TEST(cfg.read_yaml(cfg_node).empty());
    BOOST_TEST(
      YAML::Dump(config::to_yaml(cfg, config::redact_secrets{false}))
      == YAML::Dump(cfg_node));
    BOOST_TEST(cfg.cgroups().size() == 6);
    for (auto& cg : cfg.cgroups()) {
        BOOST_TEST(!cg.throughput_limit_node_in_bps);
        BOOST_TEST(!cg.throughput_limit_node_out_bps);
        BOOST_TEST(cg.validate().empty());
    }
    BOOST_TEST(cfg.cgroups()[0].name == "");
    BOOST_TEST(!cfg.cgroups()[0].is_noname());
    BOOST_TEST(cfg.cgroups()[1].name != "");
    BOOST_TEST(cfg.cgroups()[1].is_noname());
    BOOST_TEST(cfg.cgroups()[3].name == "match-nothing-group");
    BOOST_TEST(!validate_throughput_control_groups(
      cfg.cgroups().cbegin(), cfg.cgroups().cend()));

    // Matches
    const auto get_match_index =
      [&cfg](
        std::optional<std::string_view> client_id) -> std::optional<size_t> {
        if (const auto i = config::find_throughput_control_group(
              cfg.cgroups().cbegin(), cfg.cgroups().cend(), client_id);
            i != cfg.cgroups().cend()) {
            return std::distance(cfg.cgroups().cbegin(), i);
        }
        return std::nullopt;
    };
    BOOST_TEST(get_match_index("client_id-1") == 0);
    BOOST_TEST(get_match_index("clinet_id-2") == 1);
    BOOST_TEST(get_match_index("") == 3);
    BOOST_TEST(get_match_index(std::nullopt) == 4);
    BOOST_TEST(get_match_index("nonclient_id") == 5);

    // Failure cases

    // Control characters in names. In YAML, control characters [are not
    // allowed](https://yaml.org/spec/1.2.2/#51-character-set) in the stream and
    // should be [escaped](https://yaml.org/spec/1.2.2/#57-escaped-characters).
    // Yaml-cpp does not recognize escape sequences. It fails with
    // YAML::ParseException on some control characters (like \0), but lets
    // others through (like \b). throughput_control_group parser should not
    // let any through though.
    BOOST_CHECK_THROW(
      cfg.read_yaml(YAML::Load("cgroups: [{name: n\0, client_id: c1}]"s)),
      YAML::Exception);
    BOOST_TEST(
      !cfg.read_yaml(YAML::Load("cgroups: [{name: n1, client_id: c-1-\b-2}]"s))
         .empty());

    // Invalid regex syntax
    BOOST_TEST(
      !cfg.read_yaml(YAML::Load(R"(cgroups: [{name: n, client_id: "[A-Z}"}])"s))
         .empty());
    BOOST_TEST(
      !cfg.read_yaml(YAML::Load(R"(cgroups: [{name: n, client_id: "*"}])"s))
         .empty());

    // Specify any throupghput limit
    BOOST_TEST(
      !cfg
         .read_yaml(YAML::Load(
           R"(cgroups: [{name: n, client_id: c, throughput_limit_node_in_bps: 0}])"s))
         .empty());
    BOOST_TEST(
      !cfg
         .read_yaml(YAML::Load(
           R"(cgroups: [{name: n, client_id: c, throughput_limit_node_out_bps: 100}])"s))
         .empty());

    // Duplicate group names other than unnamed
    BOOST_TEST(cfg
                 .read_yaml(YAML::Load(
                   R"(cgroups: [{name: developers}, {name: developers}])"s))
                 .empty());
    BOOST_TEST(
      validate_throughput_control_groups(
        cfg.cgroups().cbegin(), cfg.cgroups().cend())
        .value_or("")
        .find("uplicate")
      != ss::sstring::npos);
}
