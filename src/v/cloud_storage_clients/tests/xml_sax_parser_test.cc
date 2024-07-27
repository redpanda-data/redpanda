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

static constexpr std::string_view abs_payload = R"XML(
<EnumerationResults ServiceEndpoint="http://myaccount.blob.core.windows.net/"  ContainerName="mycontainer">
  <Prefix>prefix</Prefix>
  <Marker>string-value</Marker>
  <MaxResults>int-value</MaxResults>
  <Delimiter>string-value</Delimiter>
  <Blobs>
    <Blob>
      <Name>blob-name</Name>
      <Snapshot>date-time-value</Snapshot>
      <VersionId>date-time-vlue</VersionId>
      <IsCurrentVersion>true</IsCurrentVersion>
      <Deleted>true</Deleted>
      <Properties>
        <Creation-Time>date-time-value</Creation-Time>
        <Last-Modified>2021-01-10T02:00:00.000Z</Last-Modified>
        <Etag>etag</Etag>
        <Owner>owner user id</Owner>
        <Group>owning group id</Group>
        <Permissions>permission string</Permissions>
        <Acl>access control list</Acl>
        <ResourceType>file | directory</ResourceType>
        <Placeholder>true</Placeholder>
        <Content-Length>1112</Content-Length>
        <Content-Type>blob-content-type</Content-Type>
        <Content-Encoding />
        <Content-Language />
        <Content-MD5 />
        <Cache-Control />
        <x-ms-blob-sequence-number>sequence-number</x-ms-blob-sequence-number>
        <BlobType>BlockBlob|PageBlob|AppendBlob</BlobType>
        <AccessTier>tier</AccessTier>
        <LeaseStatus>locked|unlocked</LeaseStatus>
        <LeaseState>available | leased | expired | breaking | broken</LeaseState>
        <LeaseDuration>infinite | fixed</LeaseDuration>
        <CopyId>id</CopyId>
        <CopyStatus>pending | success | aborted | failed </CopyStatus>
        <CopySource>source url</CopySource>
        <CopyProgress>bytes copied/bytes total</CopyProgress>
        <CopyCompletionTime>datetime</CopyCompletionTime>
        <CopyStatusDescription>error string</CopyStatusDescription>
        <ServerEncrypted>true</ServerEncrypted>
        <CustomerProvidedKeySha256>encryption-key-sha256</CustomerProvidedKeySha256>
        <EncryptionScope>encryption-scope-name</EncryptionScope>
        <IncrementalCopy>true</IncrementalCopy>
        <AccessTierInferred>true</AccessTierInferred>
        <AccessTierChangeTime>datetime</AccessTierChangeTime>
        <DeletedTime>datetime</DeletedTime>
        <RemainingRetentionDays>no-of-days</RemainingRetentionDays>
        <TagCount>number of tags between 1 to 10</TagCount>
        <RehydratePriority>rehydrate priority</RehydratePriority>
        <Expiry-Time>date-time-value</Expiry-Time>
      </Properties>
      <Metadata>
        <Name>value</Name>
      </Metadata>
      <Tags>
          <TagSet>
              <Tag>
                  <Key>TagName</Key>
                  <Value>TagValue</Value>
              </Tag>
          </TagSet>
      </Tags>
      <OrMetadata />
    </Blob>
    <BlobPrefix>
      <Name>blob-prefix</Name>
    </BlobPrefix>
  </Blobs>
  <NextMarker />
</EnumerationResults>
)XML";

static constexpr std::string_view abs_payload_with_continuation = R"XML(
<EnumerationResults ServiceEndpoint="http://myaccount.blob.core.windows.net/"  ContainerName="mycontainer">
  <Prefix>prefix</Prefix>
  <Marker>string-value</Marker>
  <MaxResults>int-value</MaxResults>
  <Delimiter>string-value</Delimiter>
  <Blobs>
    <Blob>
      <Name>blob-name</Name>
      <Snapshot>date-time-value</Snapshot>
      <VersionId>date-time-vlue</VersionId>
      <IsCurrentVersion>true</IsCurrentVersion>
      <Deleted>true</Deleted>
      <Properties>
        <Creation-Time>date-time-value</Creation-Time>
        <Last-Modified>2021-01-10T02:00:00.000Z</Last-Modified>
        <Etag>etag</Etag>
        <Acl>access control list</Acl>
        <ResourceType>file | directory</ResourceType>
        <Placeholder>true</Placeholder>
        <Content-Length>1112</Content-Length>
        <Content-Type>blob-content-type</Content-Type>
        <Content-Encoding />
        <Content-Language />
        <Content-MD5 />
        <Cache-Control />
        <x-ms-blob-sequence-number>sequence-number</x-ms-blob-sequence-number>
        <BlobType>BlockBlob|PageBlob|AppendBlob</BlobType>
        <RemainingRetentionDays>no-of-days</RemainingRetentionDays>
        <TagCount>number of tags between 1 to 10</TagCount>
        <Expiry-Time>date-time-value</Expiry-Time>
      </Properties>
    </Blob>
    <BlobPrefix>
      <Name>blob-prefix</Name>
    </BlobPrefix>
  </Blobs>
  <NextMarker>nnn</NextMarker>
</EnumerationResults>
)XML";

static constexpr std::string_view abs_payload_with_blob_prefix = R"XML(
<EnumerationResults ServiceEndpoint="http://myaccount.blob.core.windows.net/"  ContainerName="mycontainer">
  <Prefix>prefix</Prefix>
  <Marker>string-value</Marker>
  <MaxResults>int-value</MaxResults>
  <Delimiter>string-value</Delimiter>
  <Blobs>
    <BlobPrefix>
      <Name>cluster_metadata/bb7527f1-3227-4d55-86da-c133ec955ea9/manifests/2/</Name>
      <Properties>
	<Creation-Time>Thu, 25 Jul 2024 14:07:26 GMT</Creation-Time>
	<Last-Modified>Thu, 25 Jul 2024 14:07:26 GMT</Last-Modified>
	<Etag>0x8DCACB31CE7DB5C</Etag>
	<ResourceType>directory</ResourceType>
	<Content-Length>0</Content-Length>
	<BlobType>BlockBlob</BlobType>
        <AccessTier>Hot</AccessTier>
	<AccessTierInferred>true</AccessTierInferred>
	<LeaseStatus>unlocked</LeaseStatus>
	<LeaseState>available</LeaseState>
	<ServerEncrypted>true</ServerEncrypted>
      </Properties>
    </BlobPrefix>
  </Blobs>
  <NextMarker />
</EnumerationResults>
)XML";

inline ss::logger test_log("test");

BOOST_AUTO_TEST_CASE(test_parse_payload) {
    cloud_storage_clients::xml_sax_parser p{};
    ss::temporary_buffer<char> buffer(payload.data(), payload.size());
    p.start_parse(std::make_unique<cloud_storage_clients::aws_parse_impl>());
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
    p.start_parse(std::make_unique<cloud_storage_clients::aws_parse_impl>());

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

BOOST_AUTO_TEST_CASE(test_parse_abs) {
    cloud_storage_clients::xml_sax_parser p{};
    ss::temporary_buffer<char> buffer(abs_payload.data(), abs_payload.size());

    p.start_parse(std::make_unique<cloud_storage_clients::abs_parse_impl>());
    p.parse_chunk(std::move(buffer));
    p.end_parse();

    auto result = p.result();
    BOOST_REQUIRE_EQUAL(result.contents.size(), 1);
    BOOST_REQUIRE_EQUAL(result.contents[0].key, "blob-name");
    BOOST_REQUIRE_EQUAL(result.contents[0].size_bytes, 1112);
    BOOST_REQUIRE_EQUAL(result.contents[0].etag, "etag");
    BOOST_REQUIRE_EQUAL(result.is_truncated, false);
    BOOST_REQUIRE_EQUAL(result.prefix, "prefix");
    std::vector<ss::sstring> common_prefixes{"blob-prefix"};

    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      result.common_prefixes.begin(),
      result.common_prefixes.end(),
      common_prefixes.begin(),
      common_prefixes.end());

    BOOST_REQUIRE(
      result.contents[0].last_modified
      == cloud_storage_clients::util::parse_timestamp(
        "2021-01-10T02:00:00.000Z"));
}

BOOST_AUTO_TEST_CASE(test_parse_abs_with_continuation) {
    cloud_storage_clients::xml_sax_parser p{};
    ss::temporary_buffer<char> buffer(
      abs_payload_with_continuation.data(),
      abs_payload_with_continuation.size());

    p.start_parse(std::make_unique<cloud_storage_clients::abs_parse_impl>());
    p.parse_chunk(std::move(buffer));
    p.end_parse();

    auto result = p.result();
    BOOST_REQUIRE_EQUAL(result.is_truncated, true);
    BOOST_REQUIRE_EQUAL(result.next_continuation_token, "nnn");
}

BOOST_AUTO_TEST_CASE(test_parse_abs_with_blob_prefix) {
    cloud_storage_clients::xml_sax_parser p{};
    ss::temporary_buffer<char> buffer(
      abs_payload_with_blob_prefix.data(), abs_payload_with_blob_prefix.size());

    p.start_parse(std::make_unique<cloud_storage_clients::abs_parse_impl>());
    p.parse_chunk(std::move(buffer));
    p.end_parse();

    auto result = p.result();
    BOOST_REQUIRE(result.contents.empty());
    BOOST_REQUIRE_EQUAL(result.common_prefixes.size(), 1);
    BOOST_REQUIRE_EQUAL(
      result.common_prefixes[0],
      "cluster_metadata/bb7527f1-3227-4d55-86da-c133ec955ea9/manifests/2/");
}
