// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"
#include "kafka/server/group.h"
#include "kafka/server/group_metadata.h"
#include "utils/to_string.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

namespace kafka {

static auto split_member_id(const ss::sstring& m) {
    auto p = m.find("-");
    auto id = m.substr(0, p);
    auto uuid = m.substr(p + 1);
    return std::tuple(id, uuid);
}

static bool is_uuid(const ss::sstring& uuid) {
    try {
        boost::uuids::string_generator g;
        g(uuid.c_str());
        return true;
    } catch (...) {
        return false;
    }
}

/**
 * TODO
 *  - should share some of this common setup with the other tests once we get a
 *  good covering set of scenarios.
 */
static group get() {
    static config::configuration conf;
    ss::sharded<cluster::tx_gateway_frontend> fr;
    ss::sharded<features::feature_table> feature_table;
    return group(
      kafka::group_id("g"),
      group_state::empty,
      conf,
      nullptr,
      fr,
      feature_table,
      make_consumer_offsets_serializer(),
      enable_group_metrics::no);
}

static const std::vector<member_protocol> test_group_protos = {
  {kafka::protocol_name("n0"), "d0"}, {kafka::protocol_name("n1"), "d1"}};

static member_ptr get_group_member(
  ss::sstring id = "m",
  std::vector<member_protocol> protos = test_group_protos) {
    return ss::make_lw_shared<group_member>(
      kafka::member_id(id),
      kafka::group_id("g"),
      kafka::group_instance_id(fmt::format("i-{}", id)),
      kafka::client_id("client-id"),
      kafka::client_host("client-host"),
      std::chrono::seconds(1),
      std::chrono::milliseconds(2),
      kafka::protocol_type("p"),
      protos);
}

static join_group_response join_resp() {
    return join_group_response(
      error_code::none,
      kafka::generation_id(0),
      kafka::protocol_name("p"),
      kafka::member_id("l"),
      kafka::member_id("m"));
}

SEASTAR_THREAD_TEST_CASE(id) {
    auto g = get();
    BOOST_TEST(g.id() == "g");
}

SEASTAR_THREAD_TEST_CASE(state) {
    auto g = get();
    BOOST_TEST(g.state() == group_state::empty);
}

SEASTAR_THREAD_TEST_CASE(in_state) {
    auto g = get();
    BOOST_TEST(g.in_state(group_state::empty));
}

SEASTAR_THREAD_TEST_CASE(set_state) {
    auto g = get();
    BOOST_TEST(g.in_state(group_state::empty));
    g.set_state(group_state::preparing_rebalance);
    BOOST_TEST(g.in_state(group_state::preparing_rebalance));
    BOOST_TEST(g.state() == group_state::preparing_rebalance);
}

SEASTAR_THREAD_TEST_CASE(get_generation) {
    auto g = get();
    BOOST_TEST(g.generation() == 0);
}

SEASTAR_THREAD_TEST_CASE(get_member_throws_on_empty) {
    auto g = get();
    BOOST_CHECK_THROW(g.get_member(kafka::member_id("m")), std::out_of_range);
}

SEASTAR_THREAD_TEST_CASE(get_member_returns_member) {
    auto g = get();
    auto m = get_group_member();
    (void)g.add_member(m);
    BOOST_TEST(g.get_member(kafka::member_id("m")) == m);
}

SEASTAR_THREAD_TEST_CASE(contains_member) {
    auto g = get();
    BOOST_TEST(!g.contains_member(kafka::member_id("m")));
    auto m = get_group_member();
    (void)g.add_member(m);
    BOOST_TEST(g.contains_member(kafka::member_id("m")));
    BOOST_TEST(!g.contains_member(kafka::member_id("n")));
}

SEASTAR_THREAD_TEST_CASE(has_members) {
    auto g = get();
    BOOST_TEST(!g.has_members());
    auto m = get_group_member();
    (void)g.add_member(m);
    BOOST_TEST(g.has_members());
}

SEASTAR_THREAD_TEST_CASE(pending_members) {
    auto g = get();
    BOOST_TEST(!g.contains_pending_member(kafka::member_id("m")));
    g.add_pending_member(kafka::member_id("m"), 5s);
    BOOST_TEST(g.contains_pending_member(kafka::member_id("m")));
    g.remove_pending_member(kafka::member_id("m"));
    BOOST_TEST(!g.contains_pending_member(kafka::member_id("m")));
}

SEASTAR_THREAD_TEST_CASE(pending_members_expire) {
    auto g = get();
    BOOST_TEST(!g.contains_pending_member(kafka::member_id("m")));
    g.add_pending_member(kafka::member_id("m"), 1s);
    BOOST_TEST(g.contains_pending_member(kafka::member_id("m")));
    ss::sleep(2s).get();
    BOOST_TEST(!g.contains_pending_member(kafka::member_id("m")));
}

SEASTAR_THREAD_TEST_CASE(rebalance_timeout_throws_when_empty) {
    auto g = get();
    BOOST_CHECK_THROW(g.rebalance_timeout(), std::runtime_error);
}

SEASTAR_THREAD_TEST_CASE(rebalance_timeout) {
    auto g = get();

    auto m0 = ss::make_lw_shared<group_member>(
      kafka::member_id("m"),
      kafka::group_id("g"),
      kafka::group_instance_id("i-1"),
      kafka::client_id("client-id"),
      kafka::client_host("client-host"),
      std::chrono::seconds(1),
      std::chrono::milliseconds(2),
      kafka::protocol_type("p"),
      test_group_protos);

    auto m1 = ss::make_lw_shared<group_member>(
      kafka::member_id("n"),
      kafka::group_id("g"),
      kafka::group_instance_id("i-2"),
      kafka::client_id("client-id"),
      kafka::client_host("client-host"),
      std::chrono::seconds(1),
      std::chrono::seconds(3),
      kafka::protocol_type("p"),
      test_group_protos);

    (void)g.add_member(m0);
    BOOST_TEST(g.rebalance_timeout() == std::chrono::milliseconds(2));

    (void)g.add_member(m1);
    BOOST_TEST(g.rebalance_timeout() == std::chrono::seconds(3));
}

SEASTAR_THREAD_TEST_CASE(add_member_sets_leader) {
    auto g = get();
    BOOST_TEST(!g.is_leader(kafka::member_id("m")));
    BOOST_TEST(!g.leader());

    auto m = get_group_member();
    (void)g.add_member(m);

    BOOST_TEST(g.is_leader(kafka::member_id("m")));
    BOOST_TEST(g.leader());
    BOOST_TEST(*g.leader() == "m");
}

SEASTAR_THREAD_TEST_CASE(add_member_sets_protocol_type) {
    auto g = get();
    BOOST_TEST(!g.protocol_type());

    auto m = get_group_member();
    (void)g.add_member(m);

    BOOST_TEST(g.protocol_type());
    BOOST_TEST(*g.protocol_type() == "p");
}

SEASTAR_THREAD_TEST_CASE(add_missing_assignments) {
    auto g = get();

    auto m = get_group_member("m");
    (void)g.add_member(m);
    auto m2 = get_group_member("n");
    (void)g.add_member(m2);

    assignments_type a;
    g.add_missing_assignments(a);
    BOOST_TEST(a.size() == 2);
    BOOST_TEST(a[kafka::member_id("m")] == bytes());
    BOOST_TEST(a[kafka::member_id("n")] == bytes());

    a.clear();
    a[kafka::member_id("m")] = bytes("d1");
    a[kafka::member_id("o")] = bytes("d2");
    g.add_missing_assignments(a);
    BOOST_TEST(a.size() == 3);
    BOOST_TEST(a[kafka::member_id("m")] == bytes("d1"));
    BOOST_TEST(a[kafka::member_id("n")] == bytes());
    BOOST_TEST(a[kafka::member_id("o")] == bytes("d2"));
}

SEASTAR_THREAD_TEST_CASE(set_and_clear_assignments) {
    auto g = get();

    auto m = get_group_member("m");
    (void)g.add_member(m);
    auto m2 = get_group_member("n");
    (void)g.add_member(m2);

    BOOST_TEST(m->assignment() == bytes());
    BOOST_TEST(m2->assignment() == bytes());

    assignments_type a;
    a[kafka::member_id("m")] = bytes("d1");
    a[kafka::member_id("n")] = bytes("d2");
    g.set_assignments(a);

    BOOST_TEST(m->assignment() == bytes("d1"));
    BOOST_TEST(m2->assignment() == bytes("d2"));

    g.clear_assignments();
    BOOST_TEST(m->assignment() == bytes());
    BOOST_TEST(m2->assignment() == bytes());
}

SEASTAR_THREAD_TEST_CASE(all_members_joined) {
    auto g = get();
    auto m = get_group_member();
    (void)g.add_member(m);
    BOOST_TEST(g.all_members_joined());
    g.add_pending_member(kafka::member_id("x"), 5s);
    BOOST_TEST(!g.all_members_joined());
}

SEASTAR_THREAD_TEST_CASE(advance_generation_empty) {
    auto g = get();
    g.set_state(group_state::preparing_rebalance);
    g.advance_generation();
    BOOST_TEST(g.in_state(group_state::empty));
    BOOST_TEST(g.generation() == 1);
    BOOST_TEST(!g.protocol());
}

SEASTAR_THREAD_TEST_CASE(advance_generation_non_empty) {
    auto g = get();
    auto m = get_group_member();
    (void)g.add_member(m);
    g.set_state(group_state::preparing_rebalance);
    g.advance_generation();
    BOOST_TEST(g.in_state(group_state::completing_rebalance));
    BOOST_TEST(g.generation() == 1);
    BOOST_TEST(g.protocol());
    BOOST_TEST(*g.protocol() == "n0");
}

SEASTAR_THREAD_TEST_CASE(member_metadata) {
    auto g = get();

    auto protos = std::vector<member_protocol>{
      {kafka::protocol_name("p0"), bytes()},
      {kafka::protocol_name("p1"), bytes("foo")},
      {kafka::protocol_name("p2"), bytes()}};
    auto m0 = get_group_member("m", protos);

    protos = std::vector<member_protocol>{
      {kafka::protocol_name("p1"), bytes("bar")},
      {kafka::protocol_name("p2"), bytes()},
      {kafka::protocol_name("p3"), bytes()}};
    auto m1 = get_group_member("n", protos);

    (void)g.add_member(m0);
    (void)g.add_member(m1);
    g.set_state(group_state::preparing_rebalance);
    g.advance_generation();

    BOOST_TEST(g.protocol() == "p1");
    auto md = g.member_metadata();
    std::unordered_map<kafka::member_id, join_group_response_member> conf;
    for (auto& m : md) {
        conf[m.member_id] = m;
    }
    BOOST_TEST(conf[kafka::member_id("m")].metadata == bytes("foo"));
    BOOST_TEST(conf[kafka::member_id("n")].metadata == bytes("bar"));
}

SEASTAR_THREAD_TEST_CASE(select_protocol) {
    auto g = get();

    auto protos = std::vector<member_protocol>{
      {kafka::protocol_name("p0"), bytes()},
      {kafka::protocol_name("p1"), bytes()},
      {kafka::protocol_name("p2"), bytes()}};
    auto m0 = get_group_member("m", protos);

    protos = std::vector<member_protocol>{
      {kafka::protocol_name("p1"), bytes()},
      {kafka::protocol_name("p2"), bytes()},
      {kafka::protocol_name("p3"), bytes()}};
    auto m1 = get_group_member("n", protos);

    // p1 and p2 are supported by both members
    (void)g.add_member(m0);
    (void)g.add_member(m1);

    BOOST_TEST(g.select_protocol() == "p1");

    // p2 is supported by all
    protos = std::vector<member_protocol>{
      {kafka::protocol_name("p2"), bytes()},
      {kafka::protocol_name("p3"), bytes()}};
    auto m2 = get_group_member("o", protos);

    (void)g.add_member(m2);
    BOOST_TEST(g.select_protocol() == "p2");
}

SEASTAR_THREAD_TEST_CASE(supports_protocols) {
    auto g = get();

    join_group_request r;

    // empty group -> request needs protocol type
    r.data.protocol_type = kafka::protocol_type("");
    r.data.protocols = std::vector<join_group_request_protocol>{
      {kafka::protocol_name(""), bytes()}};
    BOOST_TEST(!g.supports_protocols(r));

    // empty group -> request needs protocols
    r.data.protocol_type = kafka::protocol_type("p");
    r.data.protocols.clear();
    BOOST_TEST(!g.supports_protocols(r));

    // group is empty and request can init group state
    r.data.protocol_type = kafka::protocol_type("p");
    r.data.protocols = std::vector<join_group_request_protocol>{
      {kafka::protocol_name(""), bytes()}};
    BOOST_TEST(g.supports_protocols(r));

    // adding first member will initialize some group state
    auto m = ss::make_lw_shared<group_member>(
      kafka::member_id("m"),
      kafka::group_id("g"),
      kafka::group_instance_id("i-1"),
      kafka::client_id("client-id"),
      kafka::client_host("client-host"),
      std::chrono::seconds(1),
      std::chrono::seconds(3),
      kafka::protocol_type("p"),
      test_group_protos);

    (void)g.add_member(m);
    g.set_state(group_state::preparing_rebalance);

    // protocol type doesn't match the group's protocol type
    r.data.protocol_type = kafka::protocol_type("x");
    r.data.protocols = std::vector<join_group_request_protocol>{
      {kafka::protocol_name(""), bytes()}};
    BOOST_TEST(!g.supports_protocols(r));

    // now it matches, but the protocols don't
    r.data.protocol_type = kafka::protocol_type("p");
    BOOST_TEST(!g.supports_protocols(r));

    // now it contains a matching protocol
    r.data.protocols = std::vector<join_group_request_protocol>{
      {kafka::protocol_name("n0"), bytes()}};
    BOOST_TEST(g.supports_protocols(r));

    // add member with disjoint set of protocols
    auto m2 = ss::make_lw_shared<group_member>(
      kafka::member_id("n"),
      kafka::group_id("g"),
      kafka::group_instance_id("i-2"),
      kafka::client_id("client-id"),
      kafka::client_host("client-host"),
      std::chrono::seconds(1),
      std::chrono::seconds(3),
      kafka::protocol_type("p"),
      std::vector<member_protocol>{{kafka::protocol_name("n2"), "d0"}});
    (void)g.add_member(m2);

    // n2 is not supported bc the first member doesn't support it
    r.data.protocols = std::vector<join_group_request_protocol>{
      {kafka::protocol_name("n2"), bytes()}};
    BOOST_TEST(!g.supports_protocols(r));
}

SEASTAR_THREAD_TEST_CASE(leader_rejoined) {
    auto g = get();

    // no leader
    BOOST_TEST(!g.leader_rejoined());

    auto m0 = get_group_member("m");
    (void)g.add_member(m0);

    // leader is joining
    BOOST_TEST(g.leader_rejoined());

    // simulate that the leader is now not joining for some reason. since there
    // is only one member, a replacement can't be chosen.
    m0->set_join_response(join_resp());
    BOOST_TEST(!g.leader_rejoined());

    // now add a new member. m is still leader
    auto m1 = get_group_member("n");
    (void)g.add_member(m1);
    BOOST_TEST(g.leader() == "m");

    // and it can be chosen
    BOOST_TEST(g.leader_rejoined());
    BOOST_TEST(g.leader() == "n");
}

SEASTAR_THREAD_TEST_CASE(generate_member_id) {
    join_group_request r;

    r.client_id = kafka::client_id(ss::sstring("dog"));
    r.data.group_instance_id = std::nullopt;
    auto m = group::generate_member_id(r);
    auto [id, uuid] = split_member_id(m);
    BOOST_TEST(id == "dog");
    BOOST_TEST(is_uuid(uuid));

    r.client_id = kafka::client_id(ss::sstring("dog"));
    r.data.group_instance_id = kafka::group_instance_id("cat");
    m = group::generate_member_id(r);
    std::tie(id, uuid) = split_member_id(m);
    BOOST_TEST(id == "cat");
    BOOST_TEST(is_uuid(uuid));
}

SEASTAR_THREAD_TEST_CASE(group_output) {
    auto g = get();
    auto s = fmt::format("{}", g);
    BOOST_TEST(s.find("id={g}") != std::string::npos);
}

SEASTAR_THREAD_TEST_CASE(group_state_output) {
    auto s = fmt::format("{}", group_state::preparing_rebalance);
    BOOST_TEST(s == "PreparingRebalance");
}

} // namespace kafka
