/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "config/configuration.h"
#include "config/node_overrides.h"
#include "model/fundamental.h"
#include "utils/uuid.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <yaml-cpp/exceptions.h>
#include <yaml-cpp/yaml.h>

std::vector<config::node_id_override>
read_from_yaml(const ss::sstring& yaml_string) {
    auto node = YAML::Load(yaml_string);
    return node["node_id_overrides"]
      .as<std::vector<config::node_id_override>>();
}

SEASTAR_THREAD_TEST_CASE(test_overrides_decode_empty) {
    auto empty_uuid = "node_id_overrides: []\n";
    auto empty_uuid_cfg = read_from_yaml(empty_uuid);
    BOOST_CHECK(empty_uuid_cfg.empty());
}

SEASTAR_THREAD_TEST_CASE(test_overrides_decode) {
    static model::node_uuid node_uuid{uuid_t::create()};
    static model::node_uuid other_uuid{uuid_t::create()};
    auto node_id = model::node_id{0};
    static auto some_override = ssx::sformat(
      "node_id_overrides:\n"
      "  - current_uuid: {}\n"
      "    new_uuid: {}\n"
      "    new_id: {}\n"
      "  - current_uuid: {}\n"
      "    new_uuid: {}\n"
      "    new_id: {}\n",
      node_uuid,
      other_uuid,
      node_id,
      node_uuid,
      other_uuid,
      node_id);
    auto some_override_cfg = read_from_yaml(some_override);
    BOOST_CHECK_EQUAL(some_override_cfg.size(), 2);
    for (const auto& u : some_override_cfg) {
        BOOST_CHECK_EQUAL(u.key, node_uuid);
        BOOST_CHECK_EQUAL(u.uuid, other_uuid);
        BOOST_CHECK_EQUAL(u.id, node_id);
    }
}

SEASTAR_THREAD_TEST_CASE(test_overrides_decode_errors) {
    static constexpr std::string_view entry_fmt = "node_id_overrides:\n"
                                                  "  - current_uuid: {}\n"
                                                  "    new_uuid: {}\n"
                                                  "    new_id: {}\n";

    BOOST_CHECK_THROW(
      read_from_yaml(fmt::format(
        entry_fmt,
        model::node_uuid{uuid_t::create()},
        23 /* does not parse to uuid */,
        model::node_id{0})),
      YAML::TypedBadConversion<model::node_uuid>);

    BOOST_CHECK_THROW(
      read_from_yaml(fmt::format(
        entry_fmt,
        model::node_uuid{uuid_t::create()},
        model::node_uuid{uuid_t::create()},
        model::node_uuid{uuid_t::create()} /* does not parse to node ID */)),
      YAML::TypedBadConversion<model::node_id::type>);

    BOOST_CHECK_THROW(
      read_from_yaml(fmt::format(
        "node_id_overrides:\n"
        "  - current_uuid: {}\n"
        "    new_uuid: {}\n" /* missing new_id field */,
        model::node_uuid{uuid_t::create()},
        model::node_uuid{uuid_t::create()})),
      YAML::TypedBadConversion<config::node_id_override>);

    BOOST_CHECK_THROW(
      read_from_yaml(fmt::format(
        "node_id_overrides:\n"
        "  - current_uuid: {}\n"
        "    new_id: {}\n" /* missing new_uuid field */,
        model::node_uuid{uuid_t::create()},
        model::node_id{0})),
      YAML::TypedBadConversion<config::node_id_override>);
}

SEASTAR_THREAD_TEST_CASE(test_overrides_lexical_cast) {
    model::node_uuid uuid_a{uuid_t::create()};
    model::node_uuid uuid_b{uuid_t::create()};
    model::node_id id{0};

    auto option = ssx::sformat("{}:{}:{}", uuid_a, uuid_b, id);

    config::node_id_override ovr;
    auto convert = [&ovr](std::string_view s) {
        ovr = boost::lexical_cast<config::node_id_override>(s);
    };

    BOOST_REQUIRE_NO_THROW(std::invoke(convert, option));
    BOOST_CHECK_EQUAL(ovr.key, uuid_a);
    BOOST_CHECK_EQUAL(ovr.uuid, uuid_b);
    BOOST_CHECK_EQUAL(ovr.id, id);
    std::stringstream ss;
    ss << ovr;
    BOOST_CHECK_EQUAL(ss.str(), option);

    option = ssx::sformat("{}", uuid_a);
    BOOST_CHECK_THROW(std::invoke(convert, option), std::runtime_error);
    option = ssx::sformat("{}:", uuid_a);
    BOOST_CHECK_THROW(std::invoke(convert, option), std::runtime_error);
    option = ssx::sformat("{}:{}", uuid_a, uuid_b);
    BOOST_CHECK_THROW(std::invoke(convert, option), std::runtime_error);
    option = ssx::sformat("{}:{}:", uuid_a, uuid_b);
    BOOST_CHECK_THROW(std::invoke(convert, option), std::runtime_error);
    option = ssx::sformat("{}:{}:{}", "not-a-uuid", uuid_b, id);
    BOOST_CHECK_THROW(std::invoke(convert, option), boost::bad_lexical_cast);
    option = ssx::sformat("{}:{}:{}", uuid_a, uuid_b, "not-an-id");
    BOOST_CHECK_THROW(std::invoke(convert, option), boost::bad_lexical_cast);
}

SEASTAR_THREAD_TEST_CASE(test_overrides_store) {
    static model::node_uuid some_uuid{uuid_t::create()};
    static model::node_uuid other_uuid{uuid_t::create()};
    static constexpr model::node_id some_id{23};
    static constexpr model::node_id other_id{0};

    std::vector<config::node_id_override> ovr_vec{
      config::node_id_override{some_uuid, some_uuid, some_id},
      config::node_id_override{other_uuid, other_uuid, other_id},
    };

    // Picks up the specified override based on key
    for (const auto& o : ovr_vec) {
        config::node_override_store store;
        store.maybe_set_overrides(o.key, ovr_vec);
        BOOST_CHECK_EQUAL(store.node_uuid(), o.uuid);
        BOOST_CHECK_EQUAL(store.node_id(), o.id);
    }

    // Ignores anything that doesn't match curr_uuid
    {
        config::node_override_store store;
        store.maybe_set_overrides(model::node_uuid{uuid_t::create()}, ovr_vec);
        BOOST_CHECK(!store.node_uuid().has_value());
        BOOST_CHECK(!store.node_id().has_value());
    }

    // It's an error to set multiple overrides for a single target UUID
    {
        config::node_override_store store;
        std::vector<config::node_id_override> ovr_vec{
          config::node_id_override{some_uuid, some_uuid, some_id},
          config::node_id_override{some_uuid, other_uuid, other_id},
        };

        BOOST_CHECK_THROW(
          store.maybe_set_overrides(some_uuid, ovr_vec), std::runtime_error);
    }

    // It's also an error to set the same override twice
    {
        config::node_override_store store;
        std::vector<config::node_id_override> ovr_vec{
          config::node_id_override{some_uuid, some_uuid, some_id}};

        BOOST_CHECK_NO_THROW(store.maybe_set_overrides(some_uuid, ovr_vec));
        BOOST_CHECK_THROW(
          store.maybe_set_overrides(some_uuid, ovr_vec), std::runtime_error);
    }
}
