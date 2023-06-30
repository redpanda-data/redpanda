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
#include "security/acl.h"

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
#include <optional>
#include <vector>

using namespace std::string_literals;

struct test_config : public config::config_store {
    config::property<std::vector<config::throughput_control_group>> cgroups;
    test_config()
      : cgroups(
        *this, "cgroups", "", {.needs_restart = config::needs_restart::no}) {}

    auto get_match_index(std::optional<std::string_view> client_id) const
      -> std::optional<size_t> {
        if (const auto i = config::find_throughput_control_group(
              cgroups().cbegin(), cgroups().cend(), client_id, nullptr);
            i != cgroups().cend()) {
            return std::distance(cgroups().cbegin(), i);
        }
        return std::nullopt;
    };

    auto get_match_index(
      const security::principal_type type, ss::sstring principal_name) const
      -> std::optional<size_t> {
        const security::acl_principal p(type, std::move(principal_name));
        if (const auto i = config::find_throughput_control_group(
              cgroups().cbegin(), cgroups().cend(), std::nullopt, &p);
            i != cgroups().cend()) {
            return std::distance(cgroups().cbegin(), i);
        }
        return std::nullopt;
    }
};

SEASTAR_THREAD_TEST_CASE(throughput_control_group_by_clientid_test) {
    std::vector<config::throughput_control_group> tcgv;
    tcgv.emplace_back();

    auto cfg_node = YAML::Load(R"(
cgroups:
    - name: ""
      client_id: client_id-1
    - client_id: cli.+_id-\d+
    - client_id: another unnamed group intended to verify this passes validation
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
    BOOST_REQUIRE(cfg.cgroups().size() == 6);
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

    // Equality
    for (size_t k = 0; k != cfg.cgroups().size(); ++k) {
        for (size_t l = 0; l != cfg.cgroups().size(); ++l) {
            BOOST_TEST(
              (cfg.cgroups()[k] == cfg.cgroups()[l]) == (k == l),
              "k=" << k << " l=" << l);
        }
    }

    // Matches
    BOOST_TEST(cfg.get_match_index("client_id-1") == 0);
    BOOST_TEST(cfg.get_match_index("clinet_id-2") == 1);
    BOOST_TEST(cfg.get_match_index("") == 3);
    BOOST_TEST(cfg.get_match_index(std::nullopt) == 4);
    BOOST_TEST(cfg.get_match_index("nonclient_id") == 5);

    // Copying
    config::throughput_control_group p4 = cfg.cgroups()[0];
    BOOST_TEST(p4 == cfg.cgroups()[0]);
    BOOST_TEST(fmt::format("{}", p4) == fmt::format("{}", cfg.cgroups()[0]));

    // Binding a property of
    auto binding = cfg.cgroups.bind();
    BOOST_TEST(binding() == cfg.cgroups());
    BOOST_TEST(
      fmt::format("{}", binding()) == fmt::format("{}", cfg.cgroups()));

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

    // Specify any throughput limit
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

SEASTAR_THREAD_TEST_CASE(throughput_control_group_by_principal_test) {
    std::vector<config::throughput_control_group> tcgv;
    tcgv.emplace_back();

    auto cfg_node = YAML::Load(R"(
cgroups:
    - name: empty principals list in useless as it matches nothing, but still a valid config
      principals:
    - name: match a specific user
      principals:
        - user: alpha
    - name: match a list of users (principal 'user')
      principals:
        - user: beta
        - user: gamma
        - user: delta
#    - name: (future) match a group (principal 'group')
#      principals:
#        - group: greek_letters
    - name: match schema registry (principal 'ephemeral user')
      principals:
        - service: schema registry
    - name: match PP (principal 'ephemeral user')
      principals:
        - service: panda proxy
    - name: match heterogeneous set of principals
      principals:
        - user: servicebot
        - service: schema registry
        - service: panda proxy
    - name: match _any_ authenticated user
      principals:
        - user: "*"
    - name: catch-all - no "principal" matches anything
)");
    cfg_node.SetStyle(YAML::EmitterStyle::Flow);

    test_config cfg;
    BOOST_TEST(cfg.read_yaml(cfg_node).empty());
    BOOST_TEST(
      YAML::Dump(config::to_yaml(cfg, config::redact_secrets{false}))
      == YAML::Dump(cfg_node));
    BOOST_REQUIRE(cfg.cgroups().size() == 8);
    for (auto& cg : cfg.cgroups()) {
        BOOST_TEST(!cg.throughput_limit_node_in_bps);
        BOOST_TEST(!cg.throughput_limit_node_out_bps);
        BOOST_TEST(cg.validate().empty());
    }
    BOOST_TEST(!validate_throughput_control_groups(
      cfg.cgroups().cbegin(), cfg.cgroups().cend()));

    // Equality
    for (size_t k = 0; k != cfg.cgroups().size(); ++k) {
        for (size_t l = 0; l != cfg.cgroups().size(); ++l) {
            BOOST_TEST(
              (cfg.cgroups()[k] == cfg.cgroups()[l]) == (k == l),
              "k=" << k << " l=" << l);
        }
    }

    // Matches
    using pt = security::principal_type;
    BOOST_TEST(cfg.get_match_index(std::nullopt) == 7);
    BOOST_TEST(cfg.get_match_index(pt::user, "alpha") == 1);
    BOOST_TEST(cfg.get_match_index(pt::user, "beta") == 2);
    BOOST_TEST(cfg.get_match_index(pt::user, "gamma") == 2);
    BOOST_TEST(cfg.get_match_index(pt::user, "delta") == 2);
    BOOST_TEST(cfg.get_match_index(pt::user, "epsilon") == 6);
    BOOST_TEST(cfg.get_match_index(pt::user, "zeta") == 6);
    BOOST_TEST(
      cfg.get_match_index(pt::ephemeral_user, "__schema_registry") == 3);
    BOOST_TEST(cfg.get_match_index(pt::ephemeral_user, "__pandaproxy") == 4);
    BOOST_TEST(cfg.get_match_index(pt::user, "servicebot") == 5);

    // Copying
    config::throughput_control_group p4 = cfg.cgroups()[1];
    BOOST_TEST(p4 == cfg.cgroups()[1]);
    BOOST_TEST(fmt::format("{}", p4) == fmt::format("{}", cfg.cgroups()[1]));

    // Binding a property of
    auto binding = cfg.cgroups.bind();
    BOOST_TEST(binding() == cfg.cgroups());
    BOOST_TEST(
      fmt::format("{}", binding()) == fmt::format("{}", cfg.cgroups()));

    // Failure cases

    // Invalid keys in principal
    BOOST_TEST(!cfg
                  .read_yaml(YAML::Load(
                    R"(cgroups: [{principals: [{invalid_key: value}]}])"s))
                  .empty());

    // Invalid service name
    BOOST_TEST(!cfg
                  .read_yaml(YAML::Load(
                    R"(cgroups: [{principals: [{service: sepulkarium}]}])"s))
                  .empty());

    // Control characters
    BOOST_TEST(!cfg
                  .read_yaml(YAML::Load(
                    "cgroups: [{principals: [{\auser: tarantoga}]}]"s))
                  .empty());
    BOOST_TEST(!cfg
                  .read_yaml(YAML::Load(
                    "cgroups: [{principals: [{user: tarantoga\001}]}]"s))
                  .empty());
    BOOST_TEST(!cfg
                  .read_yaml(YAML::Load(
                    "cgroups: [{principals: [{service: panda proxy\002}]}]"s))
                  .empty());
}
