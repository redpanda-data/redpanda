// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "container/fragmented_vector.h"
#include "kafka/protocol/wire.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/member.h"
#include "utils/to_string.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>
#include <fmt/ostream.h>

namespace kafka {

static const chunked_vector<member_protocol> test_protos = {
  {kafka::protocol_name("n0"), bytes::from_string("d0")},
  {kafka::protocol_name("n1"), bytes::from_string("d1")}};

static group_member get_member() {
    return group_member(
      kafka::member_id("m"),
      kafka::group_id("g"),
      kafka::group_instance_id("i"),
      kafka::client_id("client-id"),
      kafka::client_host("client-host"),
      std::chrono::seconds(1),
      std::chrono::milliseconds(2),
      kafka::protocol_type("p"),
      test_protos.copy());
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
    return sync_group_response(
      error_code::none, bytes::from_string("this is some bytes"));
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

    m.set_assignment(bytes::from_string("abc"));
    BOOST_TEST(m.assignment() == bytes::from_string("abc"));

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
    auto protos = test_protos.copy();
    protos[0].name = kafka::protocol_name("x");
    BOOST_TEST(m.protocols() != protos);

    // can set new protocols
    m.set_protocols(protos.copy());
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
      join_response.get().data.generation_id
      == make_join_response().data.generation_id);
    BOOST_TEST(
      sync_response.get().data.assignment
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
    BOOST_TEST(s.find("id=m") != std::string::npos);
}

SEASTAR_THREAD_TEST_CASE(member_serde) {
    // serialize a member's state to iobuf
    auto m0 = get_member();
    m0.set_assignment(bytes::from_string("assignment"));
    auto m0_state = m0.state().copy();
    iobuf m0_iobuf;
    auto writer = kafka::protocol::encoder(m0_iobuf);
    member_state::encode(writer, m0_state);
    kafka::protocol::decoder reader(m0_iobuf.copy());
    auto m1_state = member_state::decode(reader);
    auto m1 = kafka::group_member(
      std::move(m1_state),
      m0.group_id(),
      kafka::protocol_type("p"),
      test_protos.copy());

    BOOST_REQUIRE(m1.state() == m0.state());
}

} // namespace kafka
