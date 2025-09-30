// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/client.h"
#include "kafka/client/test/fixture.h"
#include "kafka/protocol/errors.h"
#include "model/fundamental.h"
#include "test_utils/boost_fixture.h"

#include <boost/test/tools/old/interface.hpp>

inline const model::topic_partition unknown_tp{
  model::topic{"unknown"}, model::partition_id{0}};

class list_offsets_fixture : public kafka_client_fixture {};

FIXTURE_TEST(test_unknown_topic, list_offsets_fixture) {
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    client.set_retry_base_backoff(10ms);
    client.set_max_retries(size_t(5));

    BOOST_REQUIRE_EXCEPTION(
      client.list_offsets(unknown_tp).get(),
      kafka::exception_base,
      [](kafka::exception_base ex) {
          return ex.error == kafka::error_code::unknown_topic_or_partition;
      });
}

FIXTURE_TEST(test_list_multiple_ntps, list_offsets_fixture) {
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    const auto t1 = create_topic(1, 1);
    const auto t2 = create_topic(2, 2);

    auto req = kafka::list_offsets_request{};
    auto add_tp = [&](auto topic, auto n_partitions) {
        auto& elem = req.data.topics.emplace_back(
          kafka::list_offset_topic{.name = topic.tp});
        for (int i = 0; i < n_partitions; ++i) {
            elem.partitions.emplace_back(
              kafka::list_offset_partition{
                .partition_index = model::partition_id{i}});
        }
    };
    add_tp(t1, 1);
    add_tp(t2, 2);

    const auto resp = client.list_offsets(std::move(req)).get();

    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 2);

    auto t1_res = std::ranges::find_if(
      resp.data.topics, [&](auto& tp_data) { return tp_data.name == t1.tp; });
    BOOST_REQUIRE(t1_res != resp.data.topics.end());
    BOOST_REQUIRE_EQUAL(t1_res->partitions.size(), 1);
    BOOST_REQUIRE_EQUAL(
      t1_res->partitions[0].error_code, kafka::error_code::none);

    auto t2_res = std::ranges::find_if(
      resp.data.topics, [&](auto& tp_data) { return tp_data.name == t2.tp; });
    BOOST_REQUIRE(t2_res != resp.data.topics.end());
    BOOST_REQUIRE_EQUAL(t2_res->partitions.size(), 2);
    BOOST_REQUIRE_EQUAL(
      t2_res->partitions[0].error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(
      t2_res->partitions[1].error_code, kafka::error_code::none);
}
