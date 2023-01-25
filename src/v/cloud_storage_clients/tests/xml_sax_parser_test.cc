/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/util.h"
#include "cloud_storage_clients/xml_sax_parser.h"

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

static constexpr std::string_view payload = R"XML(
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name>
  <Prefix>test-prefix</Prefix>
  <KeyCount>2</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <Delimiter>/</Delimiter>
  <IsTruncated>false</IsTruncated>
  <NextContinuationToken>next</NextContinuationToken>
  <Contents>
    <Key>test-key1</Key>
    <LastModified>2021-01-10T01:00:00.000Z</LastModified>
    <ETag>test-etag-1</ETag>
    <Size>111</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <Contents>
    <Key>test-key2</Key>
    <LastModified>2021-01-10T02:00:00.000Z</LastModified>
    <ETag>test-etag-2</ETag>
    <Size>222</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <CommonPrefixes>
    <Prefix>test-prefix</Prefix>
  </CommonPrefixes>
</ListBucketResult>
)XML";

inline ss::logger test_log("test");

BOOST_AUTO_TEST_CASE(test_parse_payload) {
    cloud_storage_clients::xml_sax_parser p{};
    ss::temporary_buffer<char> buffer(payload.data(), payload.size());
    p.start_parse();
    p.parse_chunk(std::move(buffer));
    p.end_parse();
    auto result = p.result();
    BOOST_REQUIRE_EQUAL(result.contents.size(), 2);
    BOOST_REQUIRE_EQUAL(result.contents[0].key, "test-key1");
    BOOST_REQUIRE_EQUAL(result.contents[0].size_bytes, 111);
    BOOST_REQUIRE_EQUAL(result.contents[0].etag, "test-etag-1");
    BOOST_REQUIRE(
      result.contents[0].last_modified
      == cloud_storage_clients::util::parse_timestamp(
        "2021-01-10T01:00:00.000Z"));
    BOOST_REQUIRE_EQUAL(result.contents[1].key, "test-key2");
    BOOST_REQUIRE_EQUAL(result.contents[1].etag, "test-etag-2");
    BOOST_REQUIRE(
      result.contents[1].last_modified
      == cloud_storage_clients::util::parse_timestamp(
        "2021-01-10T02:00:00.000Z"));
    BOOST_REQUIRE_EQUAL(result.contents[1].size_bytes, 222);
    BOOST_REQUIRE_EQUAL(result.is_truncated, false);
    BOOST_REQUIRE_EQUAL(result.next_continuation_token, "next");
    BOOST_REQUIRE_EQUAL(result.prefix, "test-prefix");
}

BOOST_AUTO_TEST_CASE(test_invalid_xml) {
    cloud_storage_clients::xml_sax_parser p{};
    std::string_view bad_payload = "not xml!";
    ss::temporary_buffer<char> buffer(bad_payload.data(), bad_payload.size());
    p.start_parse();

    // BOOST_REQUIRE_THROWS breaks static analysis because of a use-after-move
    // inference
    try {
        p.parse_chunk(std::move(buffer));
        BOOST_FAIL("unexpected successful parse");
    } catch (const cloud_storage_clients::xml_parse_exception&) {
    } catch (...) {
        BOOST_FAIL("unexpected exception");
    }
}
