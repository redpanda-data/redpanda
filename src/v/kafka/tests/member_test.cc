#include "kafka/groups/member.h"
#include "kafka/types.h"
#include "utils/to_string.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>
#include <fmt/ostream.h>

namespace kafka {

static const std::vector<member_protocol> test_protos = {
  {kafka::protocol_name("n0"), "d0"}, {kafka::protocol_name("n1"), "d1"}};

static group_member get_member() {
    return group_member(
      kafka::member_id("m"),
      kafka::group_id("g"),
      kafka::group_instance_id("i"),
      std::chrono::seconds(1),
      std::chrono::milliseconds(2),
      kafka::protocol_type("p"),
      test_protos);
}

static join_group_response make_join_response() {
    return join_group_response(
      error_code::none,
      kafka::generation_id(9248282),
      kafka::protocol_name("p"),
      kafka::member_id("m"),
      kafka::member_id("m"));
}

static sync_group_response make_sync_response() {
    return sync_group_response(error_code::none, bytes("this is some bytes"));
}

SEASTAR_THREAD_TEST_CASE(constructor) {
    auto m = get_member();

    BOOST_TEST(m.id() == "m");
    BOOST_TEST(m.group_id() == "g");
    BOOST_TEST(*m.group_instance_id() == "i");
    BOOST_TEST(m.session_timeout() == std::chrono::milliseconds(1000));
    BOOST_TEST(m.rebalance_timeout() == std::chrono::milliseconds(2));
    BOOST_TEST(m.protocol_type() == "p");

    BOOST_TEST(!m.is_joining());
    BOOST_TEST(!m.is_syncing());
}

SEASTAR_THREAD_TEST_CASE(assignment) {
    auto m = get_member();

    BOOST_TEST(m.assignment() == bytes());

    m.set_assignment(bytes("abc"));
    BOOST_TEST(m.assignment() == bytes("abc"));

    m.clear_assignment();
    BOOST_TEST(m.assignment() == bytes());
}

SEASTAR_THREAD_TEST_CASE(get_protocol_metadata) {
    auto m = get_member();
    BOOST_TEST(
      m.get_protocol_metadata(test_protos[0].name) == test_protos[0].metadata);
    BOOST_TEST(
      m.get_protocol_metadata(test_protos[1].name) == test_protos[1].metadata);
    BOOST_CHECK_THROW(
      m.get_protocol_metadata(kafka::protocol_name("dne")), std::out_of_range);
}

SEASTAR_THREAD_TEST_CASE(protocols) {
    // protocols compare equal
    auto m = get_member();
    BOOST_TEST(m.protocols() == test_protos);

    // and the negative test
    auto protos = test_protos;
    protos[0].name = kafka::protocol_name("x");
    BOOST_TEST(m.protocols() != protos);

    // can set new protocols
    m.set_protocols(protos);
    BOOST_TEST(m.protocols() != test_protos);
    BOOST_TEST(m.protocols() == protos);
}

SEASTAR_THREAD_TEST_CASE(response_futs) {
    auto m = get_member();

    BOOST_TEST(!m.is_joining());
    BOOST_TEST(!m.is_syncing());

    auto join_response = m.get_join_response();
    auto sync_response = m.get_sync_response();

    BOOST_TEST(m.is_joining());
    BOOST_TEST(m.is_syncing());

    m.set_join_response(make_join_response());
    m.set_sync_response(make_sync_response());

    BOOST_TEST(
      join_response.get0().data.generation_id
      == make_join_response().data.generation_id);
    BOOST_TEST(
      sync_response.get0().data.assignment
      == make_sync_response().data.assignment);

    BOOST_TEST(!m.is_joining());
    BOOST_TEST(!m.is_syncing());
}

SEASTAR_THREAD_TEST_CASE(vote_for_protocols) {
    auto m = get_member();

    // no matching candidates (empty)
    absl::flat_hash_set<protocol_name> c;
    BOOST_CHECK_THROW(m.vote_for_protocol(c), std::out_of_range);

    // no matching candidates (non-empty)
    c.insert(kafka::protocol_name("n3"));
    BOOST_CHECK_THROW(m.vote_for_protocol(c), std::out_of_range);

    // single candidate wins
    c.clear();
    c.insert(kafka::protocol_name("n1"));
    BOOST_TEST(m.vote_for_protocol(c) == "n1");

    // priority order is respected
    c.clear();
    c.insert(kafka::protocol_name("n0"));
    c.insert(kafka::protocol_name("n1"));
    BOOST_TEST(m.vote_for_protocol(c) == "n0");

    // ignores unsupported candidate
    c.clear();
    c.insert(kafka::protocol_name("n2"));
    c.insert(kafka::protocol_name("n1"));
    BOOST_TEST(m.vote_for_protocol(c) == "n1");
}

SEASTAR_THREAD_TEST_CASE(output_stream) {
    auto m = get_member();
    auto s = fmt::format("{}", m);
    BOOST_TEST(s.find("id={m}") != std::string::npos);
}

SEASTAR_THREAD_TEST_CASE(member_serde) {
    // serialize a member's state to iobuf
    auto m0 = get_member();
    m0.set_assignment(bytes("assignment"));
    auto m0_state = m0.state().copy();
    auto m0_iobuf = reflection::to_iobuf(std::move(m0_state));

    auto m1_state = reflection::adl<kafka::member_state>{}.from(
      std::move(m0_iobuf));
    auto m1 = kafka::group_member(std::move(m1_state), m0.group_id());

    BOOST_REQUIRE(m1.state() == m0.state());
}

} // namespace kafka
