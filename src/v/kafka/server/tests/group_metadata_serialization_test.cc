// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "container/chunked_vector.h"
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
#include "serde/rw/rw.h"
#include "storage/record_batch_builder.h"
#include "test_utils/boost_fixture.h"
#include "test_utils/random_bytes.h"
#include "utils/uuid.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

#include <boost/range/irange.hpp>
#include <boost/test/tools/old/interface.hpp>

#include <optional>

struct fixture {
    static ss::logger logger;

    /// Round-trips through whichever codec the type actually uses: serde for
    /// the ConsumerGroup* values, Kafka wire for the keys and classic records.
    template<typename T>
    void roundtrip_test(const T& value) {
        logger.info("encoding: {}", value);
        auto decoded = [&value] {
            if constexpr (serde::is_envelope<T>) {
                return serde::from_iobuf<T>(serde::to_iobuf(value));
            } else {
                iobuf buffer;
                kafka::protocol::encoder writer(buffer);
                T::encode(writer, value);
                kafka::protocol::decoder reader(std::move(buffer));
                return T::decode(reader);
            }
        }();
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
    state.subscription = bytes_to_iobuf(tests::random_bytes());
    state.assignment = bytes_to_iobuf(tests::random_bytes());

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
    auto group_kv = kafka::group_metadata_serializer::to_kv(
      kafka::group_metadata_kv{
        .key = group_md_key,
        .value = group_md.copy(),
      });

    auto group_md_kv = kafka::group_metadata_serializer::decode_group_metadata(
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

    auto offset_kv = kafka::group_metadata_serializer::to_kv(
      kafka::offset_metadata_kv{
        .key = offset_key,
        .value = offset_md,
      });

    auto offset_md_kv
      = kafka::group_metadata_serializer::decode_offset_metadata(
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
    kafka::group_metadata_key group_md_key;
    group_md_key.group_id = random_named_string<kafka::group_id>();

    auto group_kv = kafka::group_metadata_serializer::to_kv(
      kafka::group_metadata_kv{
        .key = group_md_key,
      });
    auto group_tombstone = build_tombstone_record(group_kv.key.copy());
    // not wrapped in iobuf
    auto decoded_group_md_kv
      = kafka::group_metadata_serializer::decode_group_metadata(
        std::move(group_tombstone));

    auto iobuf_decoded_group_md_kv
      = kafka::group_metadata_serializer::decode_group_metadata(
        build_tombstone_record(reflection::to_iobuf(group_kv.key.copy())));

    BOOST_REQUIRE_EQUAL(group_md_key, decoded_group_md_kv.key);
    BOOST_REQUIRE_EQUAL(group_md_key, iobuf_decoded_group_md_kv.key);

    kafka::offset_metadata_key offset_key;
    offset_key.group_id = random_named_string<kafka::group_id>();
    ;
    offset_key.topic = random_named_string<model::topic>();
    offset_key.partition = random_named_int<model::partition_id>();

    auto offset_kv = kafka::group_metadata_serializer::to_kv(
      kafka::offset_metadata_kv{
        .key = offset_key,
      });

    // not wrapped in iobuf
    auto offset_md_kv
      = kafka::group_metadata_serializer::decode_offset_metadata(
        build_tombstone_record(offset_kv.key.copy()));

    auto iobuf_offset_md_kv
      = kafka::group_metadata_serializer::decode_offset_metadata(
        build_tombstone_record(reflection::to_iobuf(offset_kv.key.copy())));

    BOOST_REQUIRE_EQUAL(offset_key, offset_md_kv.key);
    BOOST_REQUIRE_EQUAL(offset_key, iobuf_offset_md_kv.key);
}

/*
 * ConsumerGroup* records (KIP-848).
 */

template<typename T>
chunked_vector<T> random_vector(size_t min, size_t max, auto&& generator) {
    chunked_vector<T> ret;
    for ([[maybe_unused]] auto i :
         boost::irange(size_t{0}, random_generators::get_int(min, max))) {
        ret.push_back(generator());
    }
    return ret;
}

kafka::classic_protocol random_classic_protocol() {
    return kafka::classic_protocol{
      .name = random_named_string<kafka::protocol_name>(),
      .metadata = tests::random_bytes(),
    };
}

kafka::classic_member_metadata random_classic_member_metadata() {
    return kafka::classic_member_metadata{
      .session_timeout = random_named_int<std::chrono::milliseconds>(),
      .supported_protocols = random_vector<kafka::classic_protocol>(
        1, 3, random_classic_protocol),
    };
}

kafka::consumer_group_member_metadata_value
random_consumer_group_member_metadata_value() {
    kafka::consumer_group_member_metadata_value value{
      .instance_id = random_optional<kafka::group_instance_id>(),
      .rack_id = random_optional<model::rack_id>(),
      .client_id = random_named_string<kafka::client_id>(),
      .client_host = random_named_string<kafka::client_host>(),
      .subscribed_topic_names = random_vector<model::topic>(
        0, 4, random_named_string<model::topic>),
      .subscribed_topic_regex = random_optional<ss::sstring>(),
      .rebalance_timeout = random_named_int<std::chrono::milliseconds>(),
      .server_assignor = random_optional<ss::sstring>(),
    };
    if (random_generators::get_int(10) < 5) {
        value.classic_metadata = random_classic_member_metadata();
    }
    return value;
}

chunked_vector<model::partition_id> random_partitions() {
    return random_vector<model::partition_id>(
      1, 5, random_named_int<model::partition_id>);
}

kafka::target_assignment_topic_partitions
random_target_assignment_topic_partitions() {
    return kafka::target_assignment_topic_partitions{
      .topic_id = model::topic_id::create(),
      .partitions = random_partitions(),
    };
}

/// Kafka keeps the epochs parallel to the partitions they fence.
chunked_vector<int32_t> random_epochs(size_t count) {
    return random_vector<int32_t>(
      count, count, [] { return random_generators::get_int(1000); });
}

kafka::current_assignment_topic_partitions
random_current_assignment_topic_partitions() {
    kafka::current_assignment_topic_partitions ret{
      .topic_id = model::topic_id::create(),
      .partitions = random_partitions(),
    };
    if (random_generators::get_int(10) < 5) {
        ret.assignment_epochs = random_epochs(ret.partitions.size());
    }
    return ret;
}

kafka::consumer_group_metadata_value random_consumer_group_metadata_value() {
    return kafka::consumer_group_metadata_value{
      .epoch = random_generators::get_int(1000),
      .metadata_hash = random_generators::get_int<int64_t>(1, 1000000),
    };
}

kafka::consumer_group_current_member_assignment_value
random_current_member_assignment_value() {
    return kafka::consumer_group_current_member_assignment_value{
      .member_epoch = random_generators::get_int(1000),
      .previous_member_epoch = random_generators::get_int(1000),
      .state = random_generators::random_choice(
        {kafka::consumer_group_member_state::stable,
         kafka::consumer_group_member_state::unrevoked_partitions,
         kafka::consumer_group_member_state::unreleased_partitions}),
      .assigned_partitions
      = random_vector<kafka::current_assignment_topic_partitions>(
        0, 3, random_current_assignment_topic_partitions),
      .partitions_pending_revocation
      = random_vector<kafka::current_assignment_topic_partitions>(
        0, 3, random_current_assignment_topic_partitions),
    };
}

FIXTURE_TEST(consumer_group_metadata_rt_test, fixture) {
    roundtrip_test(
      kafka::consumer_group_metadata_key{
        .group_id = random_named_string<kafka::group_id>()});
    roundtrip_test(
      kafka::consumer_group_metadata_value{
        .epoch = random_generators::get_int(1000), .metadata_hash = 0});
    roundtrip_test(random_consumer_group_metadata_value());

    roundtrip_test(random_classic_protocol());
    roundtrip_test(random_classic_member_metadata());
    roundtrip_test(
      kafka::consumer_group_member_metadata_key{
        .group_id = random_named_string<kafka::group_id>(),
        .member_id = random_named_string<kafka::member_id>()});
    // classic_metadata is absent for a consumer-protocol member.
    auto member_metadata = random_consumer_group_member_metadata_value();
    member_metadata.classic_metadata = std::nullopt;
    roundtrip_test(member_metadata);
    member_metadata.classic_metadata = random_classic_member_metadata();
    roundtrip_test(member_metadata);

    roundtrip_test(
      kafka::consumer_group_target_assignment_metadata_key{
        .group_id = random_named_string<kafka::group_id>()});
    roundtrip_test(
      kafka::consumer_group_target_assignment_metadata_value{
        .assignment_epoch = random_generators::get_int(1000),
        .assignment_timestamp = 0});
    roundtrip_test(
      kafka::consumer_group_target_assignment_metadata_value{
        .assignment_epoch = random_generators::get_int(1000),
        .assignment_timestamp = model::timestamp::now().value()});

    roundtrip_test(random_target_assignment_topic_partitions());
    roundtrip_test(
      kafka::consumer_group_target_assignment_member_key{
        .group_id = random_named_string<kafka::group_id>(),
        .member_id = random_named_string<kafka::member_id>()});
    roundtrip_test(
      kafka::consumer_group_target_assignment_member_value{
        .topic_partitions
        = random_vector<kafka::target_assignment_topic_partitions>(
          0, 3, random_target_assignment_topic_partitions)});

    auto assigned = random_current_assignment_topic_partitions();
    assigned.assignment_epochs = {};
    roundtrip_test(assigned);
    assigned.assignment_epochs = random_epochs(assigned.partitions.size());
    roundtrip_test(assigned);

    roundtrip_test(
      kafka::consumer_group_current_member_assignment_key{
        .group_id = random_named_string<kafka::group_id>(),
        .member_id = random_named_string<kafka::member_id>()});
    roundtrip_test(random_current_member_assignment_value());
}

FIXTURE_TEST(test_consumer_group_key_versions, fixture) {
    auto type_of = [](auto kv) {
        return kafka::group_metadata_serializer::get_metadata_type(
          kafka::group_metadata_serializer::to_kv(std::move(kv)).key);
    };
    auto group_id = random_named_string<kafka::group_id>();
    auto member_id = random_named_string<kafka::member_id>();

    BOOST_REQUIRE_EQUAL(
      type_of(kafka::consumer_group_metadata_kv{.key = {.group_id = group_id}}),
      kafka::group_metadata_type::consumer_group_metadata);
    BOOST_REQUIRE_EQUAL(
      type_of(
        kafka::consumer_group_member_metadata_kv{
          .key = {.group_id = group_id, .member_id = member_id}}),
      kafka::group_metadata_type::consumer_group_member_metadata);
    BOOST_REQUIRE_EQUAL(
      type_of(
        kafka::consumer_group_target_assignment_metadata_kv{
          .key = {.group_id = group_id}}),
      kafka::group_metadata_type::consumer_group_target_assignment_metadata);
    BOOST_REQUIRE_EQUAL(
      type_of(
        kafka::consumer_group_target_assignment_member_kv{
          .key = {.group_id = group_id, .member_id = member_id}}),
      kafka::group_metadata_type::consumer_group_target_assignment_member);
    BOOST_REQUIRE_EQUAL(
      type_of(
        kafka::consumer_group_current_member_assignment_kv{
          .key = {.group_id = group_id, .member_id = member_id}}),
      kafka::group_metadata_type::consumer_group_current_member_assignment);

    // The classic records must keep their own versions.
    BOOST_REQUIRE_EQUAL(
      type_of(kafka::group_metadata_kv{.key = {.group_id = group_id}}),
      kafka::group_metadata_type::group_metadata);
    BOOST_REQUIRE_EQUAL(
      type_of(
        kafka::offset_metadata_kv{
          .key
          = {.group_id = group_id, .topic = random_named_string<model::topic>(), .partition = random_named_int<model::partition_id>()}}),
      kafka::group_metadata_type::offset_commit);

    // Kafka's ConsumerGroupPartitionMetadata (key version 4) is deprecated and
    // never written by us, so it stays undecodable.
    iobuf key_v4;
    kafka::protocol::encoder writer(key_v4);
    writer.write(int16_t{4});
    writer.write(group_id);
    BOOST_REQUIRE_THROW(
      kafka::group_metadata_serializer::get_metadata_type(key_v4.copy()),
      std::invalid_argument);
}

FIXTURE_TEST(test_consumer_group_serializer, fixture) {
    auto member_value = random_consumer_group_member_metadata_value();
    auto member_kv = kafka::consumer_group_member_metadata_kv{
      .key
      = {.group_id = random_named_string<kafka::group_id>(), .member_id = random_named_string<kafka::member_id>()},
      .value = member_value.copy()};

    auto decoded
      = kafka::group_metadata_serializer::decode_consumer_group_member_metadata(
        to_record(kafka::group_metadata_serializer::to_kv(member_kv.copy())));

    BOOST_REQUIRE_EQUAL(member_kv.key, decoded.key);
    BOOST_REQUIRE_EQUAL(member_value, decoded.value);

    // A tombstone carries the key only.
    auto tombstone_kv = kafka::group_metadata_serializer::to_kv(
      kafka::consumer_group_current_member_assignment_kv{
        .key = {
          .group_id = random_named_string<kafka::group_id>(),
          .member_id = random_named_string<kafka::member_id>()}});
    auto tombstone = kafka::group_metadata_serializer::
      decode_consumer_group_current_member_assignment(
        build_tombstone_record(tombstone_kv.key.copy()));
    BOOST_REQUIRE(!tombstone.value.has_value());
}
