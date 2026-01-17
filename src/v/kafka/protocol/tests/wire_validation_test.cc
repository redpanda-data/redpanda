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

#include "bytes/iobuf.h"
#include "kafka/protocol/wire.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <cstdint>
#include <limits>

/// Test that read_array throws when array length exceeds remaining bytes
SEASTAR_THREAD_TEST_CASE(read_array_length_exceeds_buffer) {
    // Create a buffer that claims array length of 1000 but only has space for 1
    // element Format: int32 length (1000) + one int32 element
    iobuf buf;
    kafka::protocol::encoder writer(buf);
    writer.write(int32_t{1000}); // Claim 1000 elements
    writer.write(int32_t{42});   // But only provide 1 element (4 bytes)

    kafka::protocol::decoder reader(std::move(buf));

    // This should fail because we claim 1000 elements but don't have the data
    BOOST_CHECK_THROW(
      reader.read_array(
        [](kafka::protocol::decoder& r) { return r.read_int32(); }),
      std::out_of_range);
}

/// Test that read_array throws for negative length (not -1 which means null)
SEASTAR_THREAD_TEST_CASE(read_array_negative_length) {
    iobuf buf;
    kafka::protocol::encoder writer(buf);
    writer.write(int32_t{-2}); // Invalid negative length

    kafka::protocol::decoder reader(std::move(buf));

    BOOST_CHECK_THROW(
      reader.read_array(
        [](kafka::protocol::decoder& r) { return r.read_int32(); }),
      std::out_of_range);
}

/// Test that read_array with max int32 length fails appropriately
SEASTAR_THREAD_TEST_CASE(read_array_max_length) {
    iobuf buf;
    kafka::protocol::encoder writer(buf);
    writer.write(std::numeric_limits<int32_t>::max()); // 2^31 - 1 elements
    writer.write(int32_t{42});                         // Only 1 element of data

    kafka::protocol::decoder reader(std::move(buf));

    BOOST_CHECK_THROW(
      reader.read_array(
        [](kafka::protocol::decoder& r) { return r.read_int32(); }),
      std::out_of_range);
}

/// Test that read_flex_array throws when length exceeds remaining bytes
SEASTAR_THREAD_TEST_CASE(read_flex_array_length_exceeds_buffer) {
    iobuf buf;
    kafka::protocol::encoder writer(buf);
    // Flex array uses varint where value = actual_length + 1
    // So varint 1001 means 1000 elements
    writer.write_unsigned_varint(1001);
    writer.write(int32_t{42}); // Only 1 element

    kafka::protocol::decoder reader(std::move(buf));

    BOOST_CHECK_THROW(
      reader.read_flex_array(
        [](kafka::protocol::decoder& r) { return r.read_int32(); }),
      std::out_of_range);
}

/// Test that read_flex_array with 0 length throws (0 means null in flex)
SEASTAR_THREAD_TEST_CASE(read_flex_array_zero_length) {
    iobuf buf;
    kafka::protocol::encoder writer(buf);
    writer.write_unsigned_varint(0); // 0 = null, invalid for non-nullable

    kafka::protocol::decoder reader(std::move(buf));

    BOOST_CHECK_THROW(
      reader.read_flex_array(
        [](kafka::protocol::decoder& r) { return r.read_int32(); }),
      std::out_of_range);
}

/// Test that read_flex_string throws when length exceeds remaining bytes
SEASTAR_THREAD_TEST_CASE(read_flex_string_length_exceeds_buffer) {
    iobuf buf;
    kafka::protocol::encoder writer(buf);
    // Flex string: varint 1001 means 1000 bytes
    writer.write_unsigned_varint(1001);
    // Only provide 3 bytes of actual data
    buf.append("abc", 3);

    kafka::protocol::decoder reader(std::move(buf));

    BOOST_CHECK_THROW(reader.read_flex_string(), std::out_of_range);
}

/// Test that read_flex_string with 0 length throws
SEASTAR_THREAD_TEST_CASE(read_flex_string_zero_length) {
    iobuf buf;
    kafka::protocol::encoder writer(buf);
    writer.write_unsigned_varint(0); // 0 = invalid for non-nullable flex string

    kafka::protocol::decoder reader(std::move(buf));

    BOOST_CHECK_THROW(reader.read_flex_string(), std::out_of_range);
}

/// Test that read_flex_bytes throws when length exceeds remaining bytes
SEASTAR_THREAD_TEST_CASE(read_flex_bytes_length_exceeds_buffer) {
    iobuf buf;
    kafka::protocol::encoder writer(buf);
    writer.write_unsigned_varint(1001); // 1000 bytes
    buf.append("abc", 3);               // Only 3 bytes

    kafka::protocol::decoder reader(std::move(buf));

    BOOST_CHECK_THROW(reader.read_flex_bytes(), std::out_of_range);
}

/// Test that read_string throws when length exceeds remaining bytes
SEASTAR_THREAD_TEST_CASE(read_string_length_exceeds_buffer) {
    iobuf buf;
    kafka::protocol::encoder writer(buf);
    writer.write(int16_t{1000}); // Claim 1000 bytes
    buf.append("abc", 3);        // Only 3 bytes

    kafka::protocol::decoder reader(std::move(buf));

    BOOST_CHECK_THROW(reader.read_string(), std::out_of_range);
}

/// Test that read_string with negative length (not -1) throws
SEASTAR_THREAD_TEST_CASE(read_string_negative_length) {
    iobuf buf;
    kafka::protocol::encoder writer(buf);
    writer.write(int16_t{-2}); // Invalid negative

    kafka::protocol::decoder reader(std::move(buf));

    BOOST_CHECK_THROW(reader.read_string(), std::out_of_range);
}

/// Test that read_tags throws when num_tags exceeds available data
SEASTAR_THREAD_TEST_CASE(read_tags_count_exceeds_buffer) {
    iobuf buf;
    kafka::protocol::encoder writer(buf);
    // Claim 1000 tags but provide none
    writer.write_unsigned_varint(1000);

    kafka::protocol::decoder reader(std::move(buf));

    BOOST_CHECK_THROW(reader.read_tags(), std::out_of_range);
}

/// Test that read_tags throws for duplicate tag IDs
SEASTAR_THREAD_TEST_CASE(read_tags_duplicate_ids) {
    iobuf buf;
    kafka::protocol::encoder writer(buf);
    writer.write_unsigned_varint(2); // 2 tags
    writer.write_unsigned_varint(0); // tag id 0
    writer.write_unsigned_varint(0); // tag size 0
    writer.write_unsigned_varint(0); // tag id 0 again (duplicate!)
    writer.write_unsigned_varint(0); // tag size 0

    kafka::protocol::decoder reader(std::move(buf));

    BOOST_CHECK_THROW(reader.read_tags(), std::out_of_range);
}

/// Test that read_tags throws for non-ascending tag IDs
SEASTAR_THREAD_TEST_CASE(read_tags_non_ascending_ids) {
    iobuf buf;
    kafka::protocol::encoder writer(buf);
    writer.write_unsigned_varint(2); // 2 tags
    writer.write_unsigned_varint(5); // tag id 5
    writer.write_unsigned_varint(0); // tag size 0
    writer.write_unsigned_varint(3); // tag id 3 (less than 5, invalid!)
    writer.write_unsigned_varint(0); // tag size 0

    kafka::protocol::decoder reader(std::move(buf));

    BOOST_CHECK_THROW(reader.read_tags(), std::out_of_range);
}

/// Test that read_tags throws when tag size exceeds remaining bytes
SEASTAR_THREAD_TEST_CASE(read_tags_size_exceeds_buffer) {
    iobuf buf;
    kafka::protocol::encoder writer(buf);
    writer.write_unsigned_varint(1);    // 1 tag
    writer.write_unsigned_varint(0);    // tag id 0
    writer.write_unsigned_varint(1000); // tag size 1000 (but no data follows)

    kafka::protocol::decoder reader(std::move(buf));

    BOOST_CHECK_THROW(reader.read_tags(), std::out_of_range);
}

/// Test that read_bytes throws when length exceeds remaining bytes
SEASTAR_THREAD_TEST_CASE(read_bytes_length_exceeds_buffer) {
    iobuf buf;
    kafka::protocol::encoder writer(buf);
    writer.write(int32_t{1000}); // Claim 1000 bytes
    buf.append("abc", 3);        // Only 3 bytes

    kafka::protocol::decoder reader(std::move(buf));

    BOOST_CHECK_THROW(reader.read_bytes(), std::out_of_range);
}

/// Test valid array parsing still works
SEASTAR_THREAD_TEST_CASE(read_array_valid) {
    iobuf buf;
    kafka::protocol::encoder writer(buf);
    writer.write(int32_t{3}); // 3 elements
    writer.write(int32_t{1});
    writer.write(int32_t{2});
    writer.write(int32_t{3});

    kafka::protocol::decoder reader(std::move(buf));

    auto result = reader.read_array(
      [](kafka::protocol::decoder& r) { return r.read_int32(); });

    BOOST_REQUIRE_EQUAL(result.size(), 3);
    BOOST_CHECK_EQUAL(result[0], 1);
    BOOST_CHECK_EQUAL(result[1], 2);
    BOOST_CHECK_EQUAL(result[2], 3);
}

/// Test valid flex string parsing still works
SEASTAR_THREAD_TEST_CASE(read_flex_string_valid) {
    iobuf buf;
    kafka::protocol::encoder writer(buf);
    writer.write_flex(std::string_view{"abc"});

    kafka::protocol::decoder reader(std::move(buf));

    auto result = reader.read_flex_string();
    BOOST_CHECK_EQUAL(result, "abc");
}

/// Test valid tags parsing still works
SEASTAR_THREAD_TEST_CASE(read_tags_valid) {
    iobuf buf;
    kafka::protocol::encoder writer(buf);
    writer.write_unsigned_varint(2); // 2 tags
    writer.write_unsigned_varint(0); // tag id 0
    writer.write_unsigned_varint(2); // tag size 2
    buf.append("ab", 2);             // tag data
    writer.write_unsigned_varint(5); // tag id 5 (ascending)
    writer.write_unsigned_varint(1); // tag size 1
    buf.append("c", 1);              // tag data

    kafka::protocol::decoder reader(std::move(buf));

    auto tags = reader.read_tags();
    BOOST_CHECK_EQUAL(tags().size(), 2);
}
