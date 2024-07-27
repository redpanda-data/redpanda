// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "bytes/random.h"
#include "kafka/protocol/wire.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/server.h"
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

#include <limits>
#include <optional>

struct fixture {
    static ss::logger logger;

    template<typename T>
    void roundtrip_test(const T& value) {
        iobuf buffer;
        logger.info("encoding: {}", value);
        kafka::protocol::encoder writer(buffer);
        T::encode(writer, value);

        kafka::protocol::decoder reader(std::move(buffer));

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
    for ([[maybe_unused]] auto i :
         boost::irange(0, random_generators::get_int(0, 10))) {
        group_md.members.push_back(random_member_state());
    }

    roundtrip_test(group_md);

    kafka::offset_metadata_key offset_key;
    offset_key.group_id = random_named_string<kafka::group_id>();
    offset_key.topic = random_named_string<model::topic>();
    offset_key.partition = random_named_int<model::partition_id>();

    roundtrip_test(offset_key);
    // version 1
    kafka::offset_metadata_value offset_md_v1;

    offset_md_v1.offset = random_named_int<model::offset>();
    offset_md_v1.metadata = random_named_string<ss::sstring>();
    offset_md_v1.commit_timestamp = model::timestamp::now();
    offset_md_v1.expiry_timestamp = model::timestamp::now();
    // a non-{-1} timestamp results in v1 being chosen
    vassert(offset_md_v1.expiry_timestamp != model::timestamp(-1), "force v1");

    roundtrip_test(offset_md_v1);

    // version 3
    kafka::offset_metadata_value offset_md;

    offset_md.offset = random_named_int<model::offset>();
    offset_md.leader_epoch = random_named_int<kafka::leader_epoch>();
    offset_md.metadata = random_named_string<ss::sstring>();
    offset_md.commit_timestamp = model::timestamp::now();

    roundtrip_test(offset_md);
}

template<typename K, typename V>
model::record_batch to_record_batch(K key, std::optional<V> value) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    if (value) {
        builder.add_raw_kv(
          reflection::to_iobuf(std::move(key)),
          reflection::to_iobuf(std::move(value.value())));
    } else {
        builder.add_raw_kv(reflection::to_iobuf(std::move(key)), std::nullopt);
    }
    return std::move(builder).build();
}

template<typename T>
model::record to_record(T t) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));

    builder.add_raw_kv(std::move(t.key), std::move(t.value));
    auto records = std::move(builder).build().copy_records();
    return std::move(records.front());
}

FIXTURE_TEST(test_consumer_offsets_serializer, fixture) {
    auto serializer = kafka::make_consumer_offsets_serializer();

    kafka::group_metadata_key group_md_key;
    group_md_key.group_id = random_named_string<kafka::group_id>();

    kafka::group_metadata_value group_md;
    group_md.protocol_type = random_named_string<kafka::protocol_type>();
    group_md.generation = random_named_int<kafka::generation_id>();
    group_md.leader = random_optional<kafka::member_id>();
    group_md.protocol = random_optional<kafka::protocol_name>();
    group_md.leader = random_optional<kafka::member_id>();
    group_md.state_timestamp = model::timestamp::now();
    for ([[maybe_unused]] auto i :
         boost::irange(0, random_generators::get_int(0, 10))) {
        group_md.members.push_back(random_member_state());
    }
    auto group_kv = serializer.to_kv(kafka::group_metadata_kv{
      .key = group_md_key,
      .value = group_md.copy(),
    });

    auto group_md_kv = serializer.decode_group_metadata(
      to_record(std::move(group_kv)));

    BOOST_REQUIRE_EQUAL(group_md_key, group_md_kv.key);
    BOOST_REQUIRE_EQUAL(group_md, group_md_kv.value);

    kafka::offset_metadata_key offset_key;
    offset_key.group_id = random_named_string<kafka::group_id>();
    offset_key.topic = random_named_string<model::topic>();
    offset_key.partition = random_named_int<model::partition_id>();

    kafka::offset_metadata_value offset_md;

    offset_md.offset = random_named_int<model::offset>();
    offset_md.leader_epoch = random_named_int<kafka::leader_epoch>();
    offset_md.metadata = random_named_string<ss::sstring>();
    offset_md.commit_timestamp = model::timestamp::now();

    auto offset_kv = serializer.to_kv(kafka::offset_metadata_kv{
      .key = offset_key,
      .value = offset_md,
    });

    auto offset_md_kv = serializer.decode_offset_metadata(
      to_record(std::move(offset_kv)));

    BOOST_REQUIRE_EQUAL(offset_key, offset_md_kv.key);
    BOOST_REQUIRE_EQUAL(offset_md, offset_md_kv.value);
}

model::record build_tombstone_record(iobuf buffer) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));

    builder.add_raw_kv(std::move(buffer), std::nullopt);
    auto records = std::move(builder).build().copy_records();
    return std::move(records.front());
}

FIXTURE_TEST(test_unwrapping_tombstones_from_iobuf, fixture) {
    std::vector<kafka::group_metadata_serializer> serializers;
    serializers.reserve(1);
    serializers.push_back(kafka::make_consumer_offsets_serializer());

    for (auto& serializer : serializers) {
        kafka::group_metadata_key group_md_key;
        group_md_key.group_id = random_named_string<kafka::group_id>();

        auto group_kv = serializer.to_kv(kafka::group_metadata_kv{
          .key = group_md_key,
        });
        auto group_tombstone = build_tombstone_record(group_kv.key.copy());
        // not wrapped in iobuf
        auto decoded_group_md_kv = serializer.decode_group_metadata(
          std::move(group_tombstone));

        auto iobuf_decoded_group_md_kv = serializer.decode_group_metadata(
          build_tombstone_record(reflection::to_iobuf(group_kv.key.copy())));

        BOOST_REQUIRE_EQUAL(group_md_key, decoded_group_md_kv.key);
        BOOST_REQUIRE_EQUAL(group_md_key, iobuf_decoded_group_md_kv.key);

        kafka::offset_metadata_key offset_key;
        offset_key.group_id = random_named_string<kafka::group_id>();
        ;
        offset_key.topic = random_named_string<model::topic>();
        offset_key.partition = random_named_int<model::partition_id>();

        auto offset_kv = serializer.to_kv(kafka::offset_metadata_kv{
          .key = offset_key,
        });

        // not wrapped in iobuf
        auto offset_md_kv = serializer.decode_offset_metadata(
          build_tombstone_record(offset_kv.key.copy()));

        auto iobuf_offset_md_kv = serializer.decode_offset_metadata(
          build_tombstone_record(reflection::to_iobuf(offset_kv.key.copy())));

        BOOST_REQUIRE_EQUAL(offset_key, offset_md_kv.key);
        BOOST_REQUIRE_EQUAL(offset_key, iobuf_offset_md_kv.key);
    }
}
