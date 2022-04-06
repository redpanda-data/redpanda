/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/types.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

using namespace cloud_storage;

static constexpr std::string_view empty_manifest_json = R"json({
    "version": 1,
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 0,
    "last_offset": 0
})json";
static constexpr std::string_view complete_manifest_json = R"json({
    "version": 1,
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 1,
    "last_offset": 39,
    "segments": {
        "10-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 1024,
            "base_offset": 10,
            "committed_offset": 19
        },
        "20-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 2048,
            "base_offset": 20,
            "committed_offset": 29,
            "max_timestamp": 1234567890
        },
        "30-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 4096,
            "base_offset": 30,
            "committed_offset": 39,
            "max_timestamp": 1234567890
        }
    }
})json";
static const model::ntp manifest_ntp(
  model::ns("test-ns"), model::topic("test-topic"), model::partition_id(42));

inline ss::input_stream<char> make_manifest_stream(std::string_view json) {
    iobuf i;
    i.append(json.data(), json.size());
    return make_iobuf_input_stream(std::move(i));
}

SEASTAR_THREAD_TEST_CASE(test_manifest_type) {
    partition_manifest m;
    BOOST_REQUIRE(m.get_manifest_type() == manifest_type::partition);
}

SEASTAR_THREAD_TEST_CASE(test_segment_path) {
    auto path = generate_remote_segment_path(
      manifest_ntp,
      model::initial_revision_id(0),
      segment_name("22-11-v1.log"),
      model::term_id{123});
    // use pre-calculated murmur hash value from full ntp path + file name
    BOOST_REQUIRE_EQUAL(
      path, "2bea9275/test-ns/test-topic/42_0/22-11-v1.log.123");
}

SEASTAR_THREAD_TEST_CASE(test_manifest_name_parsing) {
    std::filesystem::path path
      = "b0000000/meta/kafka/redpanda-test/4_2/manifest.json";
    auto res = cloud_storage::get_partition_manifest_path_components(path);
    BOOST_REQUIRE_EQUAL(res->_origin, path);
    BOOST_REQUIRE_EQUAL(res->_ns(), "kafka");
    BOOST_REQUIRE_EQUAL(res->_topic(), "redpanda-test");
    BOOST_REQUIRE_EQUAL(res->_part(), 4);
    BOOST_REQUIRE_EQUAL(res->_rev(), 2);
}

SEASTAR_THREAD_TEST_CASE(test_manifest_name_parsing_failure_1) {
    std::filesystem::path path
      = "b0000000/meta/kafka/redpanda-test/a_b/manifest.json";
    auto res = cloud_storage::get_partition_manifest_path_components(path);
    BOOST_REQUIRE(res.has_value() == false);
}

SEASTAR_THREAD_TEST_CASE(test_manifest_name_parsing_failure_2) {
    std::filesystem::path path
      = "b0000000/kafka/redpanda-test/4_2/manifest.json";
    auto res = cloud_storage::get_partition_manifest_path_components(path);
    BOOST_REQUIRE(res.has_value() == false);
}

SEASTAR_THREAD_TEST_CASE(test_manifest_name_parsing_failure_3) {
    std::filesystem::path path
      = "b0000000/meta/kafka/redpanda-test//manifest.json";
    auto res = cloud_storage::get_partition_manifest_path_components(path);
    BOOST_REQUIRE(res.has_value() == false);
}

SEASTAR_THREAD_TEST_CASE(test_manifest_name_parsing_failure_4) {
    std::filesystem::path path
      = "b0000000/meta/kafka/redpanda-test/4_2/foo.bar";
    auto res = cloud_storage::get_partition_manifest_path_components(path);
    BOOST_REQUIRE(res.has_value() == false);
}

SEASTAR_THREAD_TEST_CASE(test_segment_name_parsing) {
    segment_name name{"3587-1-v1.log"};
    auto res = parse_segment_name(name);
    BOOST_REQUIRE(res);
    BOOST_REQUIRE_EQUAL(res->base_offset(), 3587);
    BOOST_REQUIRE_EQUAL(res->term(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_segment_name_parsing_failure_1) {
    segment_name name{"-1-v1.log"};
    auto res = parse_segment_name(name);
    BOOST_REQUIRE(res.has_value() == false);
}

SEASTAR_THREAD_TEST_CASE(test_segment_name_parsing_failure_2) {
    segment_name name{"abc-1-v1.log"};
    auto res = parse_segment_name(name);
    BOOST_REQUIRE(res.has_value() == false);
}

SEASTAR_THREAD_TEST_CASE(test_manifest_path) {
    partition_manifest m(manifest_ntp, model::initial_revision_id(0));
    auto path = m.get_manifest_path();
    BOOST_REQUIRE_EQUAL(
      path, "20000000/meta/test-ns/test-topic/42_0/manifest.json");
}

SEASTAR_THREAD_TEST_CASE(test_empty_manifest_update) {
    partition_manifest m;
    m.update(make_manifest_stream(empty_manifest_json)).get0();
    auto path = m.get_manifest_path();
    BOOST_REQUIRE_EQUAL(
      path, "20000000/meta/test-ns/test-topic/42_0/manifest.json");
}

SEASTAR_THREAD_TEST_CASE(test_complete_manifest_update) {
    partition_manifest m;
    m.update(make_manifest_stream(complete_manifest_json)).get0();
    auto path = m.get_manifest_path();
    BOOST_REQUIRE_EQUAL(
      path, "60000000/meta/test-ns/test-topic/42_1/manifest.json");
    BOOST_REQUIRE_EQUAL(m.size(), 3);
    std::map<ss::sstring, partition_manifest::segment_meta> expected = {
      {"10-1-v1.log",
       partition_manifest::segment_meta{
         false, 1024, model::offset(10), model::offset(19)}},
      {"20-1-v1.log",
       partition_manifest::segment_meta{
         false,
         2048,
         model::offset(20),
         model::offset(29),
         model::timestamp(1234567890),
         model::timestamp(1234567890)}},
      {"30-1-v1.log",
       partition_manifest::segment_meta{
         false,
         4096,
         model::offset(30),
         model::offset(39),
         model::timestamp(1234567890),
         model::timestamp(1234567890)}},
    };
    for (const auto& actual : m) {
        auto sn = generate_segment_name(
          actual.first.base_offset, actual.first.term);
        auto it = expected.find(sn());
        BOOST_REQUIRE(it != expected.end());
        BOOST_REQUIRE_EQUAL(it->second.base_offset, actual.second.base_offset);
        BOOST_REQUIRE_EQUAL(
          it->second.committed_offset, actual.second.committed_offset);
        BOOST_REQUIRE_EQUAL(
          it->second.is_compacted, actual.second.is_compacted);
        BOOST_REQUIRE_EQUAL(it->second.size_bytes, actual.second.size_bytes);
        BOOST_REQUIRE_EQUAL(
          it->second.max_timestamp, actual.second.max_timestamp);
    }
}

SEASTAR_THREAD_TEST_CASE(test_manifest_serialization) {
    partition_manifest m(manifest_ntp, model::initial_revision_id(0));
    m.add(
      segment_name("10-1-v1.log"),
      {
        .is_compacted = false,
        .size_bytes = 1024,
        .base_offset = model::offset(10),
        .committed_offset = model::offset(19),
        .max_timestamp = model::timestamp::missing(),
        .ntp_revision = model::initial_revision_id(0),
      });
    m.add(
      segment_name("20-1-v1.log"),
      {
        .is_compacted = false,
        .size_bytes = 2048,
        .base_offset = model::offset(20),
        .committed_offset = model::offset(29),
        .max_timestamp = model::timestamp::missing(),
        .ntp_revision = model::initial_revision_id(3),
      });
    auto [is, size] = m.serialize();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    partition_manifest restored;
    restored.update(std::move(rstr)).get0();

    BOOST_REQUIRE(m == restored);
}

SEASTAR_THREAD_TEST_CASE(test_manifest_difference) {
    partition_manifest a(manifest_ntp, model::initial_revision_id(0));
    a.add(segment_name("1-1-v1.log"), {});
    a.add(segment_name("2-2-v1.log"), {});
    a.add(segment_name("3-3-v1.log"), {});
    partition_manifest b(manifest_ntp, model::initial_revision_id(0));
    b.add(segment_name("1-1-v1.log"), {});
    b.add(segment_name("2-2-v1.log"), {});
    {
        auto c = a.difference(b);
        BOOST_REQUIRE(c.size() == 1);
        auto res = *c.begin();
        auto expected = partition_manifest::key{
          .base_offset = model::offset(3), .term = model::term_id(3)};
        BOOST_REQUIRE(res.first == expected);
    }
    // check that set difference is not symmetrical
    b.add(segment_name("3-3-v1.log"), {});
    b.add(segment_name("4-4-v1.log"), {});
    {
        auto c = a.difference(b);
        BOOST_REQUIRE(c.size() == 0);
    }
}

// modeled after cluster::archival_metadata_stm::segment
struct metadata_stm_segment
  : public serde::envelope<
      metadata_stm_segment,
      serde::version<0>,
      serde::compat_version<0>> {
    cloud_storage::segment_name name;
    cloud_storage::partition_manifest::segment_meta meta;

    bool operator==(const metadata_stm_segment&) const = default;
};

namespace old {
struct segment_meta_v0 {
    using value_t = segment_meta_v0;
    static constexpr serde::version_t redpanda_serde_version = 0;
    static constexpr serde::version_t redpanda_serde_compat_version = 0;

    bool is_compacted;
    size_t size_bytes;
    model::offset base_offset;
    model::offset committed_offset;
    model::timestamp base_timestamp;
    model::timestamp max_timestamp;
    model::offset delta_offset;

    auto operator<=>(const segment_meta_v0&) const = default;
};

struct metadata_stm_segment
  : public serde::envelope<
      metadata_stm_segment,
      serde::version<0>,
      serde::compat_version<0>> {
    cloud_storage::segment_name name;
    segment_meta_v0 meta;

    bool operator==(const old::metadata_stm_segment&) const = default;
};

} // namespace old

SEASTAR_THREAD_TEST_CASE(test_segment_meta_serde_compat) {
    auto timestamp = model::timestamp::now();

    cloud_storage::partition_manifest::segment_meta meta_new{
      .is_compacted = false,
      .size_bytes = 1234,
      .base_offset = model::offset{12},
      .committed_offset = model::offset{34},
      .base_timestamp = timestamp,
      .max_timestamp = timestamp,
      .delta_offset = model::offset{7},
      .ntp_revision = model::initial_revision_id{42},
      .archiver_term = model::term_id{123},
    };

    cloud_storage::partition_manifest::segment_meta meta_wo_new_fields
      = meta_new;
    meta_wo_new_fields.ntp_revision = model::initial_revision_id{};
    meta_wo_new_fields.archiver_term = model::term_id{};

    old::segment_meta_v0 meta_old{
      .is_compacted = meta_new.is_compacted,
      .size_bytes = meta_new.size_bytes,
      .base_offset = meta_new.base_offset,
      .committed_offset = meta_new.committed_offset,
      .base_timestamp = meta_new.base_timestamp,
      .max_timestamp = meta_new.max_timestamp,
      .delta_offset = meta_new.delta_offset,
    };

    BOOST_CHECK(
      serde::from_iobuf<cloud_storage::partition_manifest::segment_meta>(
        serde::to_iobuf(meta_old))
      == meta_wo_new_fields);

    BOOST_CHECK(
      serde::from_iobuf<old::segment_meta_v0>(serde::to_iobuf(meta_new))
      == meta_old);

    auto name = segment_name{"12-11-v1.log"};

    metadata_stm_segment segment_new{
      .name = name,
      .meta = meta_new,
    };
    metadata_stm_segment segment_wo_new_fields{
      .name = name,
      .meta = meta_wo_new_fields,
    };
    old::metadata_stm_segment segment_old{
      .name = name,
      .meta = meta_old,
    };

    BOOST_CHECK(
      serde::from_iobuf<metadata_stm_segment>(serde::to_iobuf(segment_old))
      == segment_wo_new_fields);

    BOOST_CHECK(
      serde::from_iobuf<old::metadata_stm_segment>(serde::to_iobuf(segment_new))
      == segment_old);
}
