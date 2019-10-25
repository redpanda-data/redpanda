//#define BOOST_TEST_MODULE kafka group
#include "redpanda/kafka/groups/group.h"
#include "seastarx.h"
#include "utils/to_string.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

namespace kafka::groups {

static auto split_member_id(const sstring& m) {
    auto p = m.find("-");
    auto id = m.substr(0, p);
    auto uuid = m.substr(p + 1);
    return std::tuple(id, uuid);
}

static bool is_uuid(const sstring& uuid) {
    try {
        boost::uuids::string_generator g;
        auto _ = g(uuid.c_str());
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
    return group(kafka::group_id("g"), group_state::empty);
}

static const std::vector<member_protocol> test_protos = {
  {kafka::protocol_name("n0"), "d0"}, {kafka::protocol_name("n1"), "d1"}};

static member_ptr get_member(
  sstring id = "m", std::vector<member_protocol> protos = test_protos) {
    return make_lw_shared<group_member>(
      kafka::member_id(id),
      kafka::group_id("g"),
      kafka::group_instance_id("i"),
      std::chrono::seconds(1),
      std::chrono::milliseconds(2),
      kafka::protocol_type("p"),
      protos);
}

static requests::join_group_response join_resp() {
    return requests::join_group_response(
      errors::error_code::none,
      kafka::generation_id(0),
      kafka::protocol_name("p"),
      kafka::member_id("l"),
      kafka::member_id("m"));
}

BOOST_AUTO_TEST_CASE(id) {
    auto g = get();
    BOOST_TEST(g.id() == "g");
}

BOOST_AUTO_TEST_CASE(state) {
    auto g = get();
    BOOST_TEST(g.state() == group_state::empty);
}

BOOST_AUTO_TEST_CASE(in_state) {
    auto g = get();
    BOOST_TEST(g.in_state(group_state::empty));
}

BOOST_AUTO_TEST_CASE(set_state) {
    auto g = get();
    BOOST_TEST(g.in_state(group_state::empty));
    g.set_state(group_state::preparing_rebalance);
    BOOST_TEST(g.in_state(group_state::preparing_rebalance));
    BOOST_TEST(g.state() == group_state::preparing_rebalance);
}

BOOST_AUTO_TEST_CASE(generation) {
    auto g = get();
    BOOST_TEST(g.generation() == 0);
}

BOOST_AUTO_TEST_CASE(get_member_throws_on_empty) {
    auto g = get();
    BOOST_CHECK_THROW(g.get_member(kafka::member_id("m")), std::out_of_range);
}

BOOST_AUTO_TEST_CASE(get_member_returns_member) {
    auto g = get();
    auto m = get_member();
    (void)g.add_member(m);
    BOOST_TEST(g.get_member(kafka::member_id("m")) == m);
}

BOOST_AUTO_TEST_CASE(contains_member) {
    auto g = get();
    BOOST_TEST(!g.contains_member(kafka::member_id("m")));
    auto m = get_member();
    (void)g.add_member(m);
    BOOST_TEST(g.contains_member(kafka::member_id("m")));
    BOOST_TEST(!g.contains_member(kafka::member_id("n")));
}

BOOST_AUTO_TEST_CASE(has_members) {
    auto g = get();
    BOOST_TEST(!g.has_members());
    auto m = get_member();
    (void)g.add_member(m);
    BOOST_TEST(g.has_members());
}

BOOST_AUTO_TEST_CASE(pending_members) {
    auto g = get();
    BOOST_TEST(!g.contains_pending_member(kafka::member_id("m")));
    g.add_pending_member(kafka::member_id("m"));
    BOOST_TEST(g.contains_pending_member(kafka::member_id("m")));
    g.remove_pending_member(kafka::member_id("m"));
    BOOST_TEST(!g.contains_pending_member(kafka::member_id("m")));
}

BOOST_AUTO_TEST_CASE(rebalance_timeout_throws_when_empty) {
    auto g = get();
    BOOST_CHECK_THROW(g.rebalance_timeout(), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(rebalance_timeout) {
    auto g = get();

    auto m0 = make_lw_shared<group_member>(
      kafka::member_id("m"),
      kafka::group_id("g"),
      kafka::group_instance_id("i"),
      std::chrono::seconds(1),
      std::chrono::milliseconds(2),
      kafka::protocol_type("p"),
      test_protos);

    auto m1 = make_lw_shared<group_member>(
      kafka::member_id("n"),
      kafka::group_id("g"),
      kafka::group_instance_id("i"),
      std::chrono::seconds(1),
      std::chrono::seconds(3),
      kafka::protocol_type("p"),
      test_protos);

    (void)g.add_member(m0);
    BOOST_TEST(g.rebalance_timeout() == std::chrono::milliseconds(2));

    (void)g.add_member(m1);
    BOOST_TEST(g.rebalance_timeout() == std::chrono::seconds(3));
}

BOOST_AUTO_TEST_CASE(add_member_sets_leader) {
    auto g = get();
    BOOST_TEST(!g.is_leader(kafka::member_id("m")));
    BOOST_TEST(!g.leader());

    auto m = get_member();
    (void)g.add_member(m);

    BOOST_TEST(g.is_leader(kafka::member_id("m")));
    BOOST_TEST(g.leader());
    BOOST_TEST(*g.leader() == "m");
}

BOOST_AUTO_TEST_CASE(add_member_sets_protocol_type) {
    auto g = get();
    BOOST_TEST(!g.protocol_type());

    auto m = get_member();
    (void)g.add_member(m);

    BOOST_TEST(g.protocol_type());
    BOOST_TEST(*g.protocol_type() == "p");
}

BOOST_AUTO_TEST_CASE(remove_unjoined_members) {
    auto g = get();

    auto m = get_member();
    (void)g.add_member(m);
    BOOST_TEST(g.contains_member(m->id()));

    // member is joining..
    g.remove_unjoined_members();
    BOOST_TEST(g.contains_member(m->id()));

    g.set_state(group_state::preparing_rebalance);
    g.advance_generation();

    g.finish_joining_members();
    BOOST_TEST(g.contains_member(m->id()));

    g.remove_unjoined_members();
    BOOST_TEST(!g.contains_member(m->id()));
}

BOOST_AUTO_TEST_CASE(add_missing_assignments) {
    auto g = get();

    auto m = get_member("m");
    (void)g.add_member(m);
    auto m2 = get_member("n");
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

BOOST_AUTO_TEST_CASE(set_and_clear_assignments) {
    auto g = get();

    auto m = get_member("m");
    (void)g.add_member(m);
    auto m2 = get_member("n");
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

BOOST_AUTO_TEST_CASE(all_members_joined) {
    auto g = get();
    auto m = get_member();
    (void)g.add_member(m);
    BOOST_TEST(g.all_members_joined());
    g.add_pending_member(kafka::member_id("x"));
    BOOST_TEST(!g.all_members_joined());
}

BOOST_AUTO_TEST_CASE(advance_generation_empty) {
    auto g = get();
    g.set_state(group_state::preparing_rebalance);
    g.advance_generation();
    BOOST_TEST(g.in_state(group_state::empty));
    BOOST_TEST(g.generation() == 1);
    BOOST_TEST(!g.protocol());
}

BOOST_AUTO_TEST_CASE(advance_generation_non_empty) {
    auto g = get();
    auto m = get_member();
    (void)g.add_member(m);
    g.set_state(group_state::preparing_rebalance);
    g.advance_generation();
    BOOST_TEST(g.in_state(group_state::completing_rebalance));
    BOOST_TEST(g.generation() == 1);
    BOOST_TEST(g.protocol());
    BOOST_TEST(*g.protocol() == "n0");
}

BOOST_AUTO_TEST_CASE(member_metadata) {
    auto g = get();

    auto protos = std::vector<member_protocol>{
      {kafka::protocol_name("p0"), bytes()},
      {kafka::protocol_name("p1"), bytes("foo")},
      {kafka::protocol_name("p2"), bytes()}};
    auto m0 = get_member("m", protos);

    protos = std::vector<member_protocol>{
      {kafka::protocol_name("p1"), bytes("bar")},
      {kafka::protocol_name("p2"), bytes()},
      {kafka::protocol_name("p3"), bytes()}};
    auto m1 = get_member("n", protos);

    (void)g.add_member(m0);
    (void)g.add_member(m1);
    g.set_state(group_state::preparing_rebalance);
    g.advance_generation();

    BOOST_TEST(g.protocol() == "p1");
    auto md = g.member_metadata();
    std::unordered_map<
      kafka::member_id,
      requests::join_group_response::member_config>
      conf;
    for (auto& m : md) {
        conf[m.member_id] = m;
    }
    BOOST_TEST(conf[kafka::member_id("m")].metadata == bytes("foo"));
    BOOST_TEST(conf[kafka::member_id("n")].metadata == bytes("bar"));
}

BOOST_AUTO_TEST_CASE(select_protocol) {
    auto g = get();

    auto protos = std::vector<member_protocol>{
      {kafka::protocol_name("p0"), bytes()},
      {kafka::protocol_name("p1"), bytes()},
      {kafka::protocol_name("p2"), bytes()}};
    auto m0 = get_member("m", protos);

    protos = std::vector<member_protocol>{
      {kafka::protocol_name("p1"), bytes()},
      {kafka::protocol_name("p2"), bytes()},
      {kafka::protocol_name("p3"), bytes()}};
    auto m1 = get_member("n", protos);

    // p1 and p2 are supported by both members
    (void)g.add_member(m0);
    (void)g.add_member(m1);

    BOOST_TEST(g.select_protocol() == "p1");

    // p2 is supported by all
    protos = std::vector<member_protocol>{
      {kafka::protocol_name("p2"), bytes()},
      {kafka::protocol_name("p3"), bytes()}};
    auto m2 = get_member("o", protos);

    (void)g.add_member(m2);
    BOOST_TEST(g.select_protocol() == "p2");
}

BOOST_AUTO_TEST_CASE(supports_protocols) {
    auto g = get();

    requests::join_group_request r;

    // empty group -> request needs protocol type
    r.protocol_type = kafka::protocol_type("");
    r.protocols = std::vector<member_protocol>{
      {kafka::protocol_name(""), bytes()}};
    BOOST_TEST(!g.supports_protocols(r));

    // empty group -> request needs protocols
    r.protocol_type = kafka::protocol_type("p");
    r.protocols.clear();
    BOOST_TEST(!g.supports_protocols(r));

    // group is empty and request can init group state
    r.protocol_type = kafka::protocol_type("p");
    r.protocols = std::vector<member_protocol>{
      {kafka::protocol_name(""), bytes()}};
    BOOST_TEST(g.supports_protocols(r));

    // adding first member will initialize some group state
    auto m = make_lw_shared<group_member>(
      kafka::member_id("m"),
      kafka::group_id("g"),
      kafka::group_instance_id("i"),
      std::chrono::seconds(1),
      std::chrono::seconds(3),
      kafka::protocol_type("p"),
      test_protos);

    (void)g.add_member(m);
    g.set_state(group_state::preparing_rebalance);

    // protocol type doesn't match the group's protocol type
    r.protocol_type = kafka::protocol_type("x");
    r.protocols = std::vector<member_protocol>{
      {kafka::protocol_name(""), bytes()}};
    BOOST_TEST(!g.supports_protocols(r));

    // now it matches, but the protocols don't
    r.protocol_type = kafka::protocol_type("p");
    BOOST_TEST(!g.supports_protocols(r));

    // now it contains a matching protocol
    r.protocols = std::vector<member_protocol>{
      {kafka::protocol_name("n0"), bytes()}};
    BOOST_TEST(g.supports_protocols(r));

    // add member with disjoint set of protocols
    auto m2 = make_lw_shared<group_member>(
      kafka::member_id("n"),
      kafka::group_id("g"),
      kafka::group_instance_id("i"),
      std::chrono::seconds(1),
      std::chrono::seconds(3),
      kafka::protocol_type("p"),
      std::vector<member_protocol>{{kafka::protocol_name("n2"), "d0"}});
    (void)g.add_member(m2);

    // n2 is not supported bc the first member doesn't support it
    r.protocols = std::vector<member_protocol>{
      {kafka::protocol_name("n2"), bytes()}};
    BOOST_TEST(!g.supports_protocols(r));
}

SEASTAR_THREAD_TEST_CASE(finish_syncing) {
    auto g = get();

    auto m = get_member();
    m->set_assignment(bytes("foo"));

    // ignore the join
    (void)g.add_member(m);

    auto f = m->get_sync_response();
    g.finish_syncing_members(errors::error_code::none);
    auto resp = f.get0();
    BOOST_TEST(resp.assignment == bytes("foo"));
    BOOST_TEST(resp.error == errors::error_code::none);
}

SEASTAR_THREAD_TEST_CASE(finish_joining) {
    auto g = get();

    auto protos = std::vector<member_protocol>{
      {kafka::protocol_name("p0"), bytes()},
      {kafka::protocol_name("p1"), bytes("foo")},
      {kafka::protocol_name("p2"), bytes()}};
    auto m0 = get_member("m", protos);

    protos = std::vector<member_protocol>{
      {kafka::protocol_name("p1"), bytes("bar")},
      {kafka::protocol_name("p2"), bytes()},
      {kafka::protocol_name("p3"), bytes()}};
    auto m1 = get_member("n", protos);

    auto f0 = g.add_member(m0);
    auto f1 = g.add_member(m1);
    g.set_state(group_state::preparing_rebalance);
    g.advance_generation();

    BOOST_TEST(g.protocol() == "p1");
    BOOST_TEST(g.leader() == "m");

    g.finish_joining_members();

    // leader gets assignments
    auto resp = f0.get0();
    BOOST_TEST(resp.member_id == "m");
    std::unordered_map<
      kafka::member_id,
      requests::join_group_response::member_config>
      conf;
    for (auto& m : resp.members) {
        conf[m.member_id] = m;
    }
    BOOST_TEST(conf[kafka::member_id("m")].metadata == bytes("foo"));
    BOOST_TEST(conf[kafka::member_id("n")].metadata == bytes("bar"));

    // follower
    resp = f1.get0();
    BOOST_TEST(resp.member_id == "n");
    BOOST_TEST(resp.members.empty());
}

BOOST_AUTO_TEST_CASE(leader_rejoined) {
    auto g = get();

    // no leader
    BOOST_TEST(!g.leader_rejoined());

    auto m0 = get_member("m");
    (void)g.add_member(m0);

    // leader is joining
    BOOST_TEST(g.leader_rejoined());

    // simulate that the leader is now not joining for some reason. since there
    // is only one member, a replacement can't be chosen.
    m0->set_join_response(join_resp());
    BOOST_TEST(!g.leader_rejoined());

    // now add a new member. m is still leader
    auto m1 = get_member("n");
    (void)g.add_member(m1);
    BOOST_TEST(g.leader() == "m");

    // and it can be chosen
    BOOST_TEST(g.leader_rejoined());
    BOOST_TEST(g.leader() == "n");
}

BOOST_AUTO_TEST_CASE(generate_member_id) {
    requests::join_group_request r;

    r.client_id = sstring("dog");
    r.group_instance_id = std::nullopt;
    auto m = group::generate_member_id(r);
    auto [id, uuid] = split_member_id(m);
    BOOST_TEST(id == "dog");
    BOOST_TEST(is_uuid(uuid));

    r.client_id = sstring("dog");
    r.group_instance_id = kafka::group_instance_id("cat");
    m = group::generate_member_id(r);
    std::tie(id, uuid) = split_member_id(m);
    BOOST_TEST(id == "cat");
    BOOST_TEST(is_uuid(uuid));
}

BOOST_AUTO_TEST_CASE(group_output) {
    auto g = get();
    auto s = fmt::format("{}", g);
    BOOST_TEST(s.find("id={g}") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(group_state_output) {
    auto s = fmt::format("{}", group_state::preparing_rebalance);
    BOOST_TEST(s == "preparing_rebalance");
}

} // namespace kafka::groups
