/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/client/fetch_session.h"
#include "kafka/client/test/fixture.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <limits>

using namespace std::chrono_literals;

// fetch_session's handling of a session the client and broker disagree about
// rests on claims about what the broker sends back. Assert those against a real
// broker rather than a hand-built fetch_response: every response here comes off
// the wire and is handed to fetch_session the way consumer::fetch_round does,
// so a change in broker behaviour surfaces here instead of quietly leaving the
// client tested against a fiction.
class FetchSessionBrokerTest
  : public kafka_client_fixture
  , public seastar_test {
public:
    FetchSessionBrokerTest()
      : kafka_client_fixture() {}

protected:
    /// \brief A topic with data, plus a connected raw kafka client, so a test
    /// can send fetch requests whose session fields it controls.
    void connect_to_topic_with_data() {
        wait_for_controller_leadership().get();
        auto tp_ns = make_data(get_next_partition_revision_id().get(), 1);
        auto ntp = model::ntp(tp_ns.ns, tp_ns.tp, model::partition_id{0});
        wait_for_lso(ntp, model::offset{1}).get();
        _tp = model::topic_partition(tp_ns.tp, model::partition_id{0});

        _client.emplace(make_kafka_client().get());
        _client->connect().get();
    }

    seastar::future<> TearDownAsync() override {
        if (_client) {
            co_await _client->stop();
            _client->shutdown();
        }
    }

    kafka::fetch_response fetch(
      kafka::fetch_session_id id,
      kafka::fetch_session_epoch epoch,
      model::offset fetch_offset) {
        kafka::fetch_request req;
        req.data.max_bytes = std::numeric_limits<int32_t>::max();
        req.data.min_bytes = 1;
        req.data.max_wait_ms = 100ms;
        req.data.session_id = id;
        req.data.session_epoch = epoch;
        req.data.topics.emplace_back(
          kafka::fetch_topic{
            .topic = _tp.topic,
            .partitions = {{
              .partition = _tp.partition,
              .fetch_offset = fetch_offset,
            }},
          });
        return _client->dispatch(std::move(req), kafka::api_version(12)).get();
    }

    /// \brief A full fetch, which opens a session.
    kafka::fetch_response open_session(model::offset fetch_offset) {
        return fetch(
          kafka::invalid_fetch_session_id,
          kafka::initial_fetch_session_epoch,
          fetch_offset);
    }

    const model::topic_partition& tp() const { return _tp; }

private:
    model::topic_partition _tp{model::topic{}, model::partition_id{0}};
    std::optional<kafka::client::transport> _client;
};

// Two full fetches each open their own session, which is what two overlapping
// fetches on one consumer do: both snapshot epoch 0, so the second response
// names a session this side never adopted.
TEST_F(FetchSessionBrokerTest, ResponseNamingAnotherSessionResets) {
    connect_to_topic_with_data();

    kc::fetch_session s;
    auto first = open_session(model::offset{0});
    s.apply(first);
    const auto first_session_id = s.id();
    ASSERT_NE(first_session_id, kafka::invalid_fetch_session_id);
    const auto position = s.offset(tp());

    auto second = open_session(position);
    ASSERT_NE(
      kafka::fetch_session_id(second.data.session_id), first_session_id);

    s.apply(second);

    EXPECT_EQ(s.id(), kafka::invalid_fetch_session_id);
    EXPECT_EQ(s.epoch(), kafka::initial_fetch_session_epoch);
    // Positions live client-side and survive the reset.
    EXPECT_GE(s.offset(tp()), position);

    // So the next full fetch re-establishes a session and resumes from the
    // surviving position.
    auto reestablished = open_session(s.offset(tp()));
    s.apply(reestablished);
    EXPECT_NE(s.id(), kafka::invalid_fetch_session_id);
    EXPECT_EQ(s.epoch(), kafka::fetch_session_epoch{1});
}

// The broker rejects a request carrying an epoch it has already left behind.
TEST_F(FetchSessionBrokerTest, RejectedRequestResets) {
    connect_to_topic_with_data();

    kc::fetch_session s;
    auto opened = open_session(model::offset{0});
    s.apply(opened);
    ASSERT_NE(s.id(), kafka::invalid_fetch_session_id);
    ASSERT_EQ(s.epoch(), kafka::fetch_session_epoch{1});
    const auto session_id = s.id();
    const auto position = s.offset(tp());

    // Spend the epoch this side holds without applying the response, which is
    // the state a consumer is in while its first fetch is still in flight: the
    // broker has moved on, the client has not.
    auto accepted = fetch(session_id, s.epoch(), position);
    ASSERT_EQ(accepted.data.error_code, kafka::error_code::none);

    // So the next request carries a stale epoch, exactly what the second of two
    // overlapping fetches sends.
    auto rejected = fetch(session_id, s.epoch(), position);
    EXPECT_EQ(
      rejected.data.error_code, kafka::error_code::invalid_fetch_session_epoch);
    EXPECT_EQ(rejected.data.session_id, kafka::invalid_fetch_session_id);
    // The rejection is not an empty response: the requested partition is echoed
    // back as a placeholder with a none error_code and no records, so applying
    // it must not move the position.
    ASSERT_EQ(rejected.data.responses.size(), 1);
    ASSERT_EQ(rejected.data.responses[0].partitions.size(), 1);
    const auto& rejected_part = rejected.data.responses[0].partitions[0];
    EXPECT_EQ(rejected_part.error_code, kafka::error_code::none);
    EXPECT_TRUE(!rejected_part.records || rejected_part.records->empty());

    s.apply(rejected);

    EXPECT_EQ(s.id(), kafka::invalid_fetch_session_id);
    EXPECT_EQ(s.epoch(), kafka::initial_fetch_session_epoch);
    EXPECT_EQ(s.offset(tp()), position);
}
