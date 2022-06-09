// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/simple_batch_builder.h"
#include "cluster/types.h"
#include "model/record_utils.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "test_utils/logs.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

struct log_record_key {
    enum class type : int8_t {
        partition_assignment,
        topic_configuration,
        checkpoint,
        topic_deletion,
    };

    type record_type;
};

cluster::partition_assignment create_test_assignment(uint32_t p, uint16_t rf) {
    std::vector<model::broker_shard> replicas;
    for (int i = 0; i < rf; i++) {
        replicas.push_back({.node_id = model::node_id(i), .shard = 0});
    }

    return cluster::partition_assignment{
      raft::group_id(p), model::partition_id(p), std::move(replicas)};
}

SEASTAR_THREAD_TEST_CASE(simple_batch_builder_batch_test) {
    auto pa_key = log_record_key{log_record_key::type::partition_assignment};
    auto batch
      = std::move(
          cluster::simple_batch_builder(
            model::record_batch_type::topic_management_cmd, model::offset(0))
            .add_kv(
              log_record_key{log_record_key::type::topic_configuration},
              cluster::topic_configuration(
                model::ns("test"), model::topic{"a_topic"}, 3, 1))
            .add_kv(pa_key, create_test_assignment(0, 1))
            .add_kv(pa_key, create_test_assignment(1, 1))
            .add_kv(pa_key, create_test_assignment(2, 1)))
          .build();

    BOOST_REQUIRE_EQUAL(batch.record_count(), 4);
    BOOST_REQUIRE_EQUAL(batch.header().last_offset_delta, 3);

    BOOST_REQUIRE_EQUAL(batch.header().crc, model::crc_record_batch(batch));
}
SEASTAR_THREAD_TEST_CASE(round_trip_test) {
    auto pa_key = log_record_key{log_record_key::type::partition_assignment};
    auto batch
      = std::move(
          cluster::simple_batch_builder(
            model::record_batch_type::topic_management_cmd, model::offset(0))
            .add_kv(
              log_record_key{log_record_key::type::topic_configuration},
              cluster::topic_configuration(
                model::ns("test"), model::topic{"a_topic"}, 3, 1))
            .add_kv(pa_key, create_test_assignment(0, 1))
            .add_kv(pa_key, create_test_assignment(1, 1))
            .add_kv(pa_key, create_test_assignment(2, 1)))
          .build();
    int32_t current_crc = batch.header().crc;
    ss::sstring base_dir = "test.dir_"
                           + random_generators::gen_alphanum_string(4);
    model::ntp test_ntp(
      model::ns("test_ns"), model::topic("test_topic"), model::partition_id(0));
    ss::circular_buffer<model::record_batch> batches;
    batches.push_back(std::move(batch));

    tests::persist_log_file(base_dir, test_ntp, std::move(batches)).get0();
    auto read = tests::read_log_file(base_dir, test_ntp).get0();

    BOOST_REQUIRE_EQUAL(read.size(), 1);
    BOOST_REQUIRE_EQUAL(read[0].header().last_offset_delta, 3);
    BOOST_REQUIRE_EQUAL(read[0].header().crc, current_crc);
}
