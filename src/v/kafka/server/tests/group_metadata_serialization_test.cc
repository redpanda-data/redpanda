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
    // metadata_hash is a tagged field, so exercise both present and absent.
    roundtrip_test(
      kafka::consumer_group_metadata_value{
        .epoch = random_generators::get_int(1000), .metadata_hash = 0});
    roundtrip_test(
      kafka::consumer_group_metadata_value{
        .epoch = random_generators::get_int(1000),
        .metadata_hash = random_generators::get_int<int64_t>(1, 1000000)});

    roundtrip_test(random_classic_protocol());
    roundtrip_test(random_classic_member_metadata());
    roundtrip_test(
      kafka::consumer_group_member_metadata_key{
        .group_id = random_named_string<kafka::group_id>(),
        .member_id = random_named_string<kafka::member_id>()});
    // null, default and populated classic_metadata take three different wire
    // forms: a tag holding a null marker, an omitted tag, and a tag holding the
    // struct.
    auto member_metadata = random_consumer_group_member_metadata_value();
    member_metadata.classic_metadata = std::nullopt;
    roundtrip_test(member_metadata);
    member_metadata.classic_metadata = kafka::classic_member_metadata{};
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

    // Likewise for null, empty and populated assignment_epochs: a tag holding a
    // null marker, an omitted tag, and a tag holding the array.
    auto assigned = random_current_assignment_topic_partitions();
    assigned.assignment_epochs = std::nullopt;
    roundtrip_test(assigned);
    assigned.assignment_epochs = chunked_vector<int32_t>{};
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

namespace {

/// A consumer_group_metadata_value up to its tagged-fields section, then
/// whatever varints the caller wants in place of a well-formed one.
iobuf truncated_value(std::initializer_list<uint32_t> varints) {
    iobuf buffer;
    kafka::protocol::encoder writer(buffer);
    writer.write(int16_t{0});
    writer.write(int32_t{7});
    for (auto v : varints) {
        writer.write_unsigned_varint(v);
    }
    return buffer;
}

} // namespace

FIXTURE_TEST(test_consumer_group_tag_section_bounds, fixture) {
    // A count with nothing behind it. Any non-zero count fails the same way,
    // since the buffer ends where the count does.
    kafka::protocol::decoder too_many_tags(truncated_value({
      127, /* count=127 */
           /* no more bytes! ERROR */
    }));
    BOOST_REQUIRE_THROW(
      kafka::consumer_group_metadata_value::decode(too_many_tags),
      std::out_of_range);

    // Three bytes behind a count of two: enough for one entry's id and size,
    // one short of a second. A naive count-against-bytes check would let this
    // through, since 2 <= 3; the floor of two bytes per entry catches it.
    kafka::protocol::decoder count_exceeds_two_bytes_each(truncated_value({
      2, /* count=2, so at least four bytes must follow! ERROR */
      5, /* tag=5 */
      0, /* size=0, entry one is complete */
      6, /* entry two's tag, with no size byte left for it */
    }));
    BOOST_REQUIRE_THROW(
      kafka::consumer_group_metadata_value::decode(
        count_exceeds_two_bytes_each),
      std::out_of_range);

    // One unknown tag claiming a 127 byte payload, with none present. Unknown
    // because that is the path where the size reaches read_bytes.
    kafka::protocol::decoder oversized_payload(truncated_value({
      1,   /* count=1  */
      9,   /* tag=9 */
      127, /*size=127*/
           /* no more bytes! ERROR */
    }));
    BOOST_REQUIRE_THROW(
      kafka::consumer_group_metadata_value::decode(oversized_payload),
      std::out_of_range);

    // Tag 5 twice with *different* payloads: the encoding the ascending-order
    // rule exists to reject, since accepting it would leave which of the two
    // values wins up to the reader. Every length here is honest, so this passes
    // all the size checks and is malformed only in its ordering. Tag 5 rather
    // than 0 so the payloads reach consume_unknown_tag instead of a known
    // handler expecting an int64.
    kafka::protocol::decoder duplicate_tag(truncated_value({
      2, /* count=2 */
      5, /* tag=5 */
      2, /* size=2 */
      1, /* payload[0] */
      2, /* payload[1] */
      5, /* tag=5 again! ERROR */
      2, /* size=2 */
      3, /* payload[0], a different value under the same tag */
      4, /* payload[1] */
    }));
    BOOST_REQUIRE_THROW(
      kafka::consumer_group_metadata_value::decode(duplicate_tag),
      std::out_of_range);

    // Descending ids, rejected by the same comparison. Worth having alongside
    // the duplicate: these ids are distinct, so consume_unknown_tag would store
    // both without complaint and the record would decode, where a repeated id
    // trips its duplicate-key check regardless of the ordering.
    kafka::protocol::decoder descending_tags(truncated_value({
      2, /* count=2 */
      5, /* tag=5 */
      2, /* size=2 */
      1, /* payload[0] */
      2, /* payload[1] */
      3, /* tag=3, lower than 5! ERROR */
      2, /* size=2 */
      3, /* payload[0] */
      4, /* payload[1] */
    }));
    BOOST_REQUIRE_THROW(
      kafka::consumer_group_metadata_value::decode(descending_tags),
      std::out_of_range);
}

FIXTURE_TEST(test_consumer_group_tag_size_mismatch, fixture) {
    // Tag 0 is consumer_group_metadata_value's metadata_hash, so the handler
    // reads exactly the eight bytes of an int64 whatever the size says.

    // Nine bytes declared and present, eight of them read. All the sizes here
    // are within bounds, so nothing else objects: the loop simply ends with a
    // byte of the payload unread.
    kafka::protocol::decoder over_declared(truncated_value({
      1, /* count=1 */
      0, /* tag=0, metadata_hash */
      9, /* size=9, one more byte than an int64! ERROR */
      1, /* payload[0] */
      2, /* payload[1] */
      3, /* payload[2] */
      4, /* payload[3] */
      5, /* payload[4] */
      6, /* payload[5] */
      7, /* payload[6] */
      8, /* payload[7] */
      9, /* payload[8], past where the handler stops */
    }));
    BOOST_REQUIRE_THROW(
      kafka::consumer_group_metadata_value::decode(over_declared),
      std::out_of_range);

    // Four bytes declared, eight read. The handler runs off the end of its own
    // payload and into whatever follows, which for a real record is the next
    // entry's tag id.
    kafka::protocol::decoder under_declared(truncated_value({
      1, /* count=1 */
      0, /* tag=0, metadata_hash */
      4, /* size=4, half an int64! ERROR */
      1, /* payload[0] */
      2, /* payload[1] */
      3, /* payload[2] */
      4, /* payload[3] */
      5, /* the handler reads these four as well */
      6,
      7,
      8,
    }));
    BOOST_REQUIRE_THROW(
      kafka::consumer_group_metadata_value::decode(under_declared),
      std::out_of_range);
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

template<typename T>
ss::sstring encode_to_hex(const T& value) {
    iobuf buffer;
    kafka::protocol::encoder writer(buffer);
    T::encode(writer, value);
    return to_hex(iobuf_to_bytes(buffer));
}

/*
 * Byte-shape checks against Kafka's encoding of the same records: a key is
 * prefixed with its record type and written non-flex (int16-length strings), a
 * value is prefixed with its own version and written flex (compact
 * strings/arrays, unsigned-varint lengths of size+1) with a trailing
 * tagged-fields section.
 *
 * Each expectation is the output of Kafka's generated serializer for the same
 * logical record: the classes in org.apache.kafka.coordinator.group.generated
 * run through MessageUtil.toVersionPrefixedBytes(), which is what
 * GroupCoordinatorRecordSerde writes to __consumer_offsets.
 *
 * Regenerate with kafka_oracle/GroupRecordVectors.java when Kafka's
 * ConsumerGroup*.json schemas move. Nothing does that automatically. The table
 * below is in the order the harness prints, and each entry names the harness
 * label that produced it, so its output can be diffed against this table.
 */
namespace vectors {

/// consumer_group_metadata_key{g1}
/// 0003 type | 0002 "g1"
constexpr auto metadata_key = "000300026731";

/// consumer_group_metadata_value{epoch=7,hash=0}
/// 0000 version | 00000007 epoch | 00 no tags
constexpr auto metadata_value = "00000000000700";

/// consumer_group_metadata_value{epoch=7,hash=0x1122334455667788}
/// ... | 01 one tag | 00 tag id | 08 size | hash
constexpr auto metadata_value_with_hash = "0000000000070100081122334455667788";

/// consumer_group_member_metadata_key{g1,m1}
/// 0005 type | 0002 "g1" | 0002 "m1"
constexpr auto member_metadata_key = "00050002673100026d31";

/// consumer_group_member_metadata_value{c,h,[t],1000}
/// 0000 version | 00 null instance_id | 00 null rack_id | 0263 "c" | 0268 "h" |
/// 02 one topic | 0274 "t" | 00 null regex | 000003e8 timeout | 00 null
/// assignor | 01 one tag | 00 tag id | 01 size | 00 absent struct
constexpr auto member_metadata_value
  = "000000000263026802027400000003e80001000100";

/// consumer_group_member_metadata_value{+classic}
/// The same with classic_metadata present: the tag payload becomes 01
/// (present) followed by 00007530 timeout, 02 one protocol, 06 "range", 03 two
/// metadata bytes, then the protocol's and the struct's own tags.
constexpr auto member_metadata_value_with_classic
  = "000000000263026802027400000003e8000100"
    "110100007530020672616e67650301020000";

/// target_assignment_metadata_key{g1}
/// 0006 type | 0002 "g1"
constexpr auto target_assignment_metadata_key = "000600026731";

/// target_assignment_metadata_value{epoch=3,ts=0}
/// 0000 version | 00000003 epoch | 00 no tags
constexpr auto target_assignment_metadata_value = "00000000000300";

/// target_assignment_member_key{g1,m1}
/// 0007 type | 0002 "g1" | 0002 "m1"
constexpr auto target_assignment_member_key = "00070002673100026d31";

/// target_assignment_member_value{topic,[0]}
/// 0000 version | 02 one topic (id | 02 one partition | 00000000 | 00 no tags)
/// | 00 no tags
constexpr auto target_assignment_member_value
  = "0000020102030405060708090a0b0c0d0e0f1002000000000000";

/// current_member_assignment_key{g1,m1}
/// 0008 type | 0002 "g1" | 0002 "m1"
constexpr auto current_member_assignment_key = "00080002673100026d31";

/// current_member_assignment_value{epochs=[],[0]}
/// Epochs present but empty: the tag is omitted entirely.
constexpr auto current_member_assignment_empty_epochs
  = "0000000000010000000000020102030405060708090a0b0c0d0e0f10"
    "0200000000000100";

/// current_member_assignment_value{1,0,stable,[0]}
/// Epochs absent: tag 0 is written, carrying a one-byte null marker.
constexpr auto current_member_assignment_absent_epochs
  = "0000000000010000000000020102030405060708090a0b0c0d0e0f10"
    "0200000000010001000100";

/// current_member_assignment_value{epochs=[9],[5]}
/// Epochs present and non-empty: the compact array rides in the tag payload.
constexpr auto current_member_assignment_with_epochs
  = "0000000000010000000000020102030405060708090a0b0c0d0e0f10"
    "020000000501000502000000090100";

} // namespace vectors

FIXTURE_TEST(test_consumer_group_byte_shape, fixture) {
    BOOST_REQUIRE_EQUAL(
      encode_to_hex(
        kafka::consumer_group_metadata_key{.group_id = kafka::group_id("g1")}),
      vectors::metadata_key);

    BOOST_REQUIRE_EQUAL(
      encode_to_hex(
        kafka::consumer_group_metadata_value{.epoch = 7, .metadata_hash = 0}),
      vectors::metadata_value);

    BOOST_REQUIRE_EQUAL(
      encode_to_hex(
        kafka::consumer_group_metadata_value{
          .epoch = 7, .metadata_hash = 0x1122334455667788}),
      vectors::metadata_value_with_hash);

    BOOST_REQUIRE_EQUAL(
      encode_to_hex(
        kafka::consumer_group_member_metadata_key{
          .group_id = kafka::group_id("g1"),
          .member_id = kafka::member_id("m1")}),
      vectors::member_metadata_key);

    BOOST_REQUIRE_EQUAL(
      encode_to_hex(
        kafka::consumer_group_member_metadata_value{
          .client_id = kafka::client_id("c"),
          .client_host = kafka::client_host("h"),
          .subscribed_topic_names = {model::topic("t")},
          .rebalance_timeout = std::chrono::milliseconds(1000),
          // Explicit, because the field defaults to present-and-default, which
          // is the omitted form rather than this null marker.
          .classic_metadata = std::nullopt}),
      vectors::member_metadata_value);

    chunked_vector<kafka::classic_protocol> protocols;
    protocols.push_back(
      kafka::classic_protocol{
        .name = kafka::protocol_name("range"), .metadata = bytes{1, 2}});

    auto with_classic_metadata = kafka::consumer_group_member_metadata_value{
      .client_id = kafka::client_id("c"),
      .client_host = kafka::client_host("h"),
      .subscribed_topic_names = {model::topic("t")},
      .rebalance_timeout = std::chrono::milliseconds(1000),
      .classic_metadata = kafka::classic_member_metadata{
        .session_timeout = std::chrono::milliseconds(30000),
        .supported_protocols = std::move(protocols)}};

    BOOST_REQUIRE_EQUAL(
      encode_to_hex(with_classic_metadata),
      vectors::member_metadata_value_with_classic);

    BOOST_REQUIRE_EQUAL(
      encode_to_hex(
        kafka::consumer_group_target_assignment_metadata_key{
          .group_id = kafka::group_id("g1")}),
      vectors::target_assignment_metadata_key);

    BOOST_REQUIRE_EQUAL(
      encode_to_hex(
        kafka::consumer_group_target_assignment_metadata_value{
          .assignment_epoch = 3, .assignment_timestamp = 0}),
      vectors::target_assignment_metadata_value);

    auto topic_id = model::topic_id(uuid_t(
      std::vector<uint8_t>{
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}));

    BOOST_REQUIRE_EQUAL(
      encode_to_hex(
        kafka::consumer_group_target_assignment_member_key{
          .group_id = kafka::group_id("g1"),
          .member_id = kafka::member_id("m1")}),
      vectors::target_assignment_member_key);

    chunked_vector<kafka::target_assignment_topic_partitions> target;
    target.push_back(
      kafka::target_assignment_topic_partitions{
        .topic_id = topic_id, .partitions = {model::partition_id(0)}});

    auto target_value = kafka::consumer_group_target_assignment_member_value{
      .topic_partitions = std::move(target)};

    BOOST_REQUIRE_EQUAL(
      encode_to_hex(target_value), vectors::target_assignment_member_value);

    BOOST_REQUIRE_EQUAL(
      encode_to_hex(
        kafka::consumer_group_current_member_assignment_key{
          .group_id = kafka::group_id("g1"),
          .member_id = kafka::member_id("m1")}),
      vectors::current_member_assignment_key);

    // One assigned topic holding a single partition, with assignment_epochs in
    // a given state.
    auto assignment_of = [&topic_id](
                           model::partition_id partition,
                           std::optional<chunked_vector<int32_t>> epochs) {
        chunked_vector<kafka::current_assignment_topic_partitions> assigned;
        assigned.push_back(
          kafka::current_assignment_topic_partitions{
            .topic_id = topic_id,
            .partitions = {partition},
            .assignment_epochs = std::move(epochs)});
        return kafka::consumer_group_current_member_assignment_value{
          .member_epoch = 1,
          .previous_member_epoch = 0,
          .state = kafka::consumer_group_member_state::stable,
          .assigned_partitions = std::move(assigned)};
    };

    BOOST_REQUIRE_EQUAL(
      encode_to_hex(
        assignment_of(model::partition_id(0), chunked_vector<int32_t>{})),
      vectors::current_member_assignment_empty_epochs);

    BOOST_REQUIRE_EQUAL(
      encode_to_hex(assignment_of(model::partition_id(0), std::nullopt)),
      vectors::current_member_assignment_absent_epochs);

    BOOST_REQUIRE_EQUAL(
      encode_to_hex(
        assignment_of(model::partition_id(5), chunked_vector<int32_t>{9})),
      vectors::current_member_assignment_with_epochs);
}
