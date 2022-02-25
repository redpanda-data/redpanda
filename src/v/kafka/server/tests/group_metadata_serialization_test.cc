// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "kafka/protocol/request_reader.h"
#include "kafka/protocol/response_writer.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/protocol.h"
#include "kafka/types.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/record_batch_builder.h"
#include "test_utils/fixture.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

#include <boost/range/irange.hpp>
#include <boost/test/tools/old/interface.hpp>

struct fixture {
    static ss::logger logger;

    template<typename T>
    void roundtrip_test(const T& value) {
        iobuf buffer;
        logger.info("encoding: {}", value);
        kafka::response_writer writer(buffer);
        T::encode(writer, value);

        kafka::request_reader reader(std::move(buffer));

        auto decoded = T::decode(reader);
        logger.info("decoded: {}", decoded);
        BOOST_REQUIRE_EQUAL(value, decoded);
    }
};
ss::logger fixture::logger = ss::logger("test-logger");

template<typename T>
std::optional<T> random_optional() {
    if (random_generators::get_int(10) < 5) {
        return T{random_generators::gen_alphanum_string(10)};
    } else {
        return std::nullopt;
    }
}

template<typename T>
T random_named_string() {
    return T{random_generators::gen_alphanum_string(10)};
}

template<typename T>
T random_named_int() {
    return T{random_generators::get_int(30000)};
}

kafka::member_state random_member_state() {
    kafka::member_state state;
    state.id = random_named_string<kafka::member_id>();
    state.instance_id = random_optional<kafka::group_instance_id>();
    state.client_id = random_named_string<kafka::client_id>();
    state.client_host = random_named_string<kafka::client_host>();
    state.rebalance_timeout = random_named_int<std::chrono::milliseconds>();
    state.session_timeout = random_named_int<std::chrono::milliseconds>();
    state.subscription = bytes_to_iobuf(random_generators::get_bytes());
    state.assignment = bytes_to_iobuf(random_generators::get_bytes());

    return state;
}

FIXTURE_TEST(metadata_rt_test, fixture) {
    kafka::group_metadata_key group_md_key;
    group_md_key.group_id = random_named_string<kafka::group_id>();

    roundtrip_test(group_md_key);
    auto state = random_member_state();
    roundtrip_test(state);

    kafka::group_metadata_value group_md;
    group_md.protocol_type = random_named_string<kafka::protocol_type>();
    group_md.generation = random_named_int<kafka::generation_id>();
    group_md.leader = random_optional<kafka::member_id>();
    group_md.protocol = random_optional<kafka::protocol_name>();
    group_md.leader = random_optional<kafka::member_id>();
    group_md.state_timestamp = model::timestamp::now();
    for (auto i : boost::irange(0, random_generators::get_int(0, 10))) {
        group_md.members.push_back(random_member_state());
    }

    roundtrip_test(group_md);

    kafka::offset_metadata_key offset_key;
    offset_key.group_id = random_named_string<kafka::group_id>();
    offset_key.topic = random_named_string<model::topic>();
    offset_key.partition = random_named_int<model::partition_id>();

    roundtrip_test(offset_key);

    kafka::offset_metadata_value offset_md;

    offset_md.offset = random_named_int<model::offset>();
    offset_md.leader_epoch = random_named_int<kafka::leader_epoch>();
    offset_md.metadata = random_named_string<ss::sstring>();
    offset_md.commit_timestamp = model::timestamp::now();

    roundtrip_test(offset_md);
}
