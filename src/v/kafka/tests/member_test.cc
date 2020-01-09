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

BOOST_AUTO_TEST_CASE(constructor) {
    auto m = get_member();

    BOOST_TEST(m.id() == "m");
    BOOST_TEST(m.group_id() == "g");
    BOOST_TEST(*m.group_instance_id() == "i");
    BOOST_TEST(m.session_timeout() == std::chrono::milliseconds(1000));
    BOOST_TEST(m.rebalance_timeout() == std::chrono::milliseconds(2));
    BOOST_TEST(m.protocol_type() == "p");

    std::vector<member_protocol> out;
    m.for_each_protocol([&out](const member_protocol& p) { out.push_back(p); });
    BOOST_TEST(out == test_protos);

    BOOST_TEST(!m.is_joining());
    BOOST_TEST(!m.is_syncing());

    BOOST_TEST(m.metadata(test_protos[0].name) == test_protos[0].metadata);
    BOOST_TEST(m.metadata(test_protos[1].name) == test_protos[1].metadata);
    BOOST_CHECK_THROW(
      m.metadata(kafka::protocol_name("dne")), std::out_of_range);
}

BOOST_AUTO_TEST_CASE(assignment) {
    auto m = get_member();

    BOOST_TEST(m.assignment() == bytes());

    m.set_assignment(bytes("abc"));
    BOOST_TEST(m.assignment() == bytes("abc"));

    m.clear_assignment();
    BOOST_TEST(m.assignment() == bytes());
}

BOOST_AUTO_TEST_CASE(protocols) {
    auto r = join_group_request();
    r.protocols = test_protos;

    auto m = get_member();
    BOOST_TEST(m.matching_protocols(r));
    r.protocols[0].name = kafka::protocol_name("x");
    BOOST_TEST(!m.matching_protocols(r));

    std::vector<member_protocol> test_protos2 = {
      {kafka::protocol_name("n1"), "d1"}};
    m.set_protocols(test_protos2);
    r.protocols = test_protos;
    BOOST_TEST(!m.matching_protocols(r));
    r.protocols = test_protos2;
    BOOST_TEST(m.matching_protocols(r));

    std::vector<member_protocol> out;
    m.for_each_protocol([&out](const member_protocol& p) { out.push_back(p); });
    BOOST_TEST(out == test_protos2);
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
      join_response.get0().generation_id == make_join_response().generation_id);
    BOOST_TEST(
      sync_response.get0().assignment == make_sync_response().assignment);

    BOOST_TEST(!m.is_joining());
    BOOST_TEST(!m.is_syncing());
}

BOOST_AUTO_TEST_CASE(vote) {
    auto m = get_member();

    std::set<protocol_name> c;
    BOOST_CHECK_THROW(m.vote(c), std::out_of_range);

    c.insert(kafka::protocol_name("n1"));
    BOOST_TEST(m.vote(c) == "n1");

    c.clear();
    c.insert(kafka::protocol_name("n0"));
    c.insert(kafka::protocol_name("n1"));
    BOOST_TEST(m.vote(c) == "n0");

    c.clear();
    c.insert(kafka::protocol_name("n2"));
    c.insert(kafka::protocol_name("n1"));
    BOOST_TEST(m.vote(c) == "n1");
}

BOOST_AUTO_TEST_CASE(output) {
    auto m = get_member();
    auto s = fmt::format("{}", m);
    BOOST_TEST(s.find("id={m}") != std::string::npos);
}

} // namespace kafka
