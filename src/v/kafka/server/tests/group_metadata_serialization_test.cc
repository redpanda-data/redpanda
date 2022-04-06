// Copyright 2020 Redpanda Data, Inc.
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

void assert_equal(
  const kafka::group_metadata_value& g_md_new,
  const kafka::old::group_log_group_metadata& g_md_old) {
    BOOST_REQUIRE_EQUAL(g_md_new.leader, g_md_old.leader);
    BOOST_REQUIRE_EQUAL(g_md_new.generation, g_md_old.generation);
    BOOST_REQUIRE_EQUAL(g_md_new.protocol_type, g_md_old.protocol_type);
    BOOST_REQUIRE_EQUAL(g_md_new.protocol, g_md_old.protocol);
    // BOOST_REQUIRE_EQUAL(
    //   group_md.value->state_timestamp.value(), value_cp.state_timestamp);

    for (int i = 0; i < g_md_new.members.size(); ++i) {
        BOOST_REQUIRE_EQUAL(g_md_new.members[i].id, g_md_old.members[i].id);
        BOOST_REQUIRE_EQUAL(
          g_md_new.members[i].instance_id, g_md_new.members[i].instance_id);
        BOOST_REQUIRE_EQUAL(
          g_md_new.members[i].client_id, g_md_old.members[i].client_id);
        BOOST_REQUIRE_EQUAL(
          g_md_new.members[i].client_host, g_md_old.members[i].client_host);
        BOOST_REQUIRE_EQUAL(
          g_md_new.members[i].rebalance_timeout,
          g_md_old.members[i].rebalance_timeout);
        BOOST_REQUIRE_EQUAL(
          g_md_new.members[i].session_timeout,
          g_md_old.members[i].session_timeout);
        BOOST_REQUIRE_EQUAL(
          g_md_new.members[i].assignment, g_md_old.members[i].assignment);
    }
}

FIXTURE_TEST(test_backward_compatible_serializer_metadata_type, fixture) {
    auto serializer = kafka::make_backward_compatible_serializer();

    // offset metadata
    {
        kafka::old::group_log_offset_key lok{
          .group = random_named_string<kafka::group_id>(),
          .topic = random_named_string<model::topic>(),
          .partition = random_named_int<model::partition_id>()};

        kafka::old::group_log_record_key key{
          .record_type = kafka::old::group_log_record_key::type::offset_commit,
          .key = reflection::to_iobuf(kafka::old::group_log_offset_key{lok}),
        };

        kafka::old::group_log_offset_metadata value{
          .offset = random_named_int<model::offset>(),
          .leader_epoch = random_generators::get_int<int32_t>(100),
          .metadata = random_optional<ss::sstring>(),
        };

        auto batch = to_record_batch(std::move(key), std::make_optional(value));
        auto records = batch.copy_records();

        // old -> new type
        auto type = serializer.get_metadata_type(records.back().share_key());
        BOOST_REQUIRE_EQUAL(type, kafka::group_metadata_type::offset_commit);

        auto o_md = serializer.decode_offset_metadata(records.back().copy());

        BOOST_REQUIRE_EQUAL(o_md.key.group_id, lok.group);
        BOOST_REQUIRE_EQUAL(o_md.key.topic, lok.topic);
        BOOST_REQUIRE_EQUAL(o_md.key.partition, lok.partition);

        BOOST_REQUIRE_EQUAL(o_md.value->offset, value.offset);
        BOOST_REQUIRE_EQUAL(o_md.value->leader_epoch, value.leader_epoch);
        BOOST_REQUIRE_EQUAL(o_md.value->metadata, value.metadata.value_or(""));

        // new type -> old
        auto kv = serializer.to_kv(o_md);
        auto deserialized_key
          = reflection::from_iobuf<kafka::old::group_log_record_key>(
            std::move(kv.key));

        auto deserialized_value
          = reflection::from_iobuf<kafka::old::group_log_offset_metadata>(
            std::move(*kv.value));

        BOOST_REQUIRE(
          deserialized_key.record_type
          == kafka::old::group_log_record_key::type::offset_commit);
        BOOST_REQUIRE_EQUAL(
          reflection::from_iobuf<kafka::old::group_log_offset_key>(
            std::move(deserialized_key.key)),
          lok);

        BOOST_REQUIRE_EQUAL(
          deserialized_value.metadata, value.metadata.value_or(""));
        BOOST_REQUIRE_EQUAL(deserialized_value.offset, value.offset);
        BOOST_REQUIRE_EQUAL(
          deserialized_value.leader_epoch, value.leader_epoch);
    }

    // group metadata
    {
        auto group_id = random_named_string<kafka::group_id>();
        kafka::old::group_log_record_key key{
          .record_type = kafka::old::group_log_record_key::type::group_metadata,
          .key = reflection::to_iobuf(group_id),
        };

        kafka::old::group_log_group_metadata value{
          .protocol_type = random_named_string<kafka::protocol_type>(),
          .generation = random_named_int<kafka::generation_id>(),
          .protocol = random_optional<kafka::protocol_name>(),
          .leader = random_named_string<kafka::member_id>(),
          .state_timestamp = random_generators::get_int<int32_t>(),
        };

        kafka::old::group_log_group_metadata value_cp{
          .protocol_type = value.protocol_type,
          .generation = value.generation,
          .protocol = value.protocol,
          .leader = value.leader,
          .state_timestamp = random_generators::get_int<int32_t>(),
        };

        for (int i = 0; i < random_generators::get_int(5); ++i) {
            kafka::old::member_state state;
            state.id = random_named_string<kafka::member_id>();
            state.instance_id = random_optional<kafka::group_instance_id>();
            state.client_id = random_named_string<kafka::client_id>();
            state.client_host = random_named_string<kafka::client_host>();
            state.rebalance_timeout
              = random_named_int<std::chrono::milliseconds>();
            state.session_timeout
              = random_named_int<std::chrono::milliseconds>();
            state.assignment = bytes_to_iobuf(random_generators::get_bytes());

            value_cp.members.push_back(state.copy());
            value.members.push_back(std::move(state));
        }

        auto batch = to_record_batch(
          std::move(key), std::make_optional(std::move(value)));
        auto records = batch.copy_records();

        // old -> new type
        auto type = serializer.get_metadata_type(records.back().share_key());
        BOOST_REQUIRE_EQUAL(type, kafka::group_metadata_type::group_metadata);

        auto group_md = serializer.decode_group_metadata(records.back().copy());
        BOOST_REQUIRE_EQUAL(group_md.key.group_id, group_id);
        assert_equal(*group_md.value, value_cp);

        // new type -> old
        auto kv = serializer.to_kv(kafka::group_metadata_kv{
          .key = group_md.key,
          .value = group_md.value->copy(),
        });
        auto deserialized_key
          = reflection::from_iobuf<kafka::old::group_log_record_key>(
            std::move(kv.key));

        auto deserialized_value
          = reflection::from_iobuf<kafka::old::group_log_group_metadata>(
            std::move(*kv.value));

        BOOST_REQUIRE(
          deserialized_key.record_type
          == kafka::old::group_log_record_key::type::group_metadata);

        BOOST_REQUIRE_EQUAL(
          reflection::from_iobuf<kafka::group_id>(
            std::move(deserialized_key.key)),
          group_md.key.group_id);

        assert_equal(*group_md.value, deserialized_value);
    }

    // checkpoint
    {
        kafka::old::group_log_record_key key{
          .record_type = kafka::old::group_log_record_key::type::noop,
        };
        auto batch = to_record_batch(std::move(key), std::optional<iobuf>());
        // old -> new type
        auto type = serializer.get_metadata_type(
          batch.copy_records().back().share_key());
    }
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
    for (auto i : boost::irange(0, random_generators::get_int(0, 10))) {
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
