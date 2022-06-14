/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/protocol/api_versions.h"
#include "kafka/protocol/request_reader.h"
#include "kafka/protocol/response_writer.h"
#include "kafka/protocol/types.h"
#include "kafka/types.h"
#include "random/generators.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/iterator/counting_iterator.hpp>

kafka::tagged_fields make_random_tags(size_t n) {
    kafka::tagged_fields tags;
    for (uint32_t i = 0; i < n; ++i) {
        tags.emplace_back(n, bytes_to_iobuf(random_generators::get_bytes()));
    }
    return tags;
}

kafka::tagged_fields copy_tags(const kafka::tagged_fields& otags) {
    kafka::tagged_fields tags;
    std::transform(
      otags.begin(), otags.end(), std::back_inserter(tags), [](auto& t) {
          auto& [tag_id, tag] = t;
          return std::make_tuple(tag_id, tag.copy());
      });
    return tags;
}

SEASTAR_THREAD_TEST_CASE(serde_tags) {
    iobuf buf;
    auto tags = make_random_tags(10);

    /// Serialize the random tags into an iobuf
    kafka::response_writer writer(buf);
    writer.write_tags(std::move(copy_tags(tags)));

    /// Copy the result to use for a later comparison
    iobuf copy = buf.copy();

    /// Deserialize the tags with the kafka::request_reader
    kafka::request_reader reader(std::move(buf));
    auto deser_tags = reader.read_tags();

    /// Verify the inital values are equivalent
    BOOST_REQUIRE(copy_tags(tags) == deser_tags);

    /// Re-serialize these tags to compare against the previous
    iobuf result;
    kafka::response_writer end_writer(result);
    end_writer.write_tags(std::move(deser_tags));

    /// Perform checks against serialized copies
    BOOST_REQUIRE_EQUAL(result.size_bytes(), copy.size_bytes());
    BOOST_CHECK_EQUAL(iobuf_to_bytes(result), iobuf_to_bytes(copy));
}

struct test_struct {
    ss::sstring field_a;
    int32_t field_b;

    static test_struct make_random() {
        return test_struct{
          .field_a = random_generators::gen_alphanum_string(5),
          .field_b = random_generators::get_int(0, 15)};
    }

    friend bool operator==(const test_struct& a, const test_struct& b) {
        return a.field_a == b.field_a && a.field_b == b.field_b;
    }

    friend std::ostream& operator<<(std::ostream& os, const test_struct& ts) {
        os << "field_a: " << ts.field_a << " field_b: " << ts.field_b;
        return os;
    }
};

/// Only includes impls of supported types used in following unit tests
template<typename T>
void write_flex(T& type, iobuf& buf) {
    kafka::response_writer writer(buf);
    if constexpr (std::is_same_v<T, std::vector<test_struct>>) {
        writer.write_flex_array(
          type, [](test_struct& ts, kafka::response_writer& writer) {
              writer.write_flex(ts.field_a);
              writer.write(ts.field_b);
              writer.write_tags();
          });
    } else if constexpr (std::is_same_v<
                           T,
                           std::optional<std::vector<test_struct>>>) {
        writer.write_nullable_flex_array(
          type, [](test_struct& ts, kafka::response_writer& writer) {
              writer.write_flex(ts.field_a);
              writer.write(ts.field_b);
              writer.write_tags();
          });
    } else if constexpr (std::is_same_v<T, kafka::uuid>) {
        writer.write(type);
    } else {
        writer.write_flex(type);
    }
}

template<typename T>
T read_flex(iobuf buf) {
    kafka::request_reader reader(std::move(buf));
    if constexpr (std::is_same_v<T, ss::sstring>) {
        return reader.read_flex_string();
    } else if constexpr (std::is_same_v<T, kafka::uuid>) {
        return reader.read_uuid();
    } else if constexpr (std::is_same_v<T, std::optional<ss::sstring>>) {
        return reader.read_nullable_flex_string();
    } else if constexpr (std::is_same_v<T, bytes>) {
        return reader.read_flex_bytes();
    } else if constexpr (std::is_same_v<T, std::vector<test_struct>>) {
        return reader.read_flex_array([](kafka::request_reader& reader) {
            test_struct v;
            v.field_a = reader.read_flex_string();
            v.field_b = reader.read_int32();
            reader.consume_tags();
            return v;
        });
    } else if constexpr (std::is_same_v<
                           T,
                           std::optional<std::vector<test_struct>>>) {
        return reader.read_nullable_flex_array(
          [](kafka::request_reader& reader) {
              test_struct v;
              v.field_a = reader.read_flex_string();
              v.field_b = reader.read_int32();
              reader.consume_tags();
              return v;
          });
    }
}

template<typename T>
T serde_flex(T& type) {
    iobuf buf;
    write_flex(type, buf);
    return read_flex<T>(std::move(buf));
}

SEASTAR_THREAD_TEST_CASE(serde_flex_types) {
    auto gen_random_string = []() {
        const auto str_len = random_generators::get_int(0, 20);
        return random_generators::gen_alphanum_string(15);
    };
    {
        /// uuid
        auto bytes = random_generators::get_bytes(16);
        auto encoded = bytes_to_base64(bytes);
        auto uuid = kafka::uuid::from_string(encoded);
        auto rt = serde_flex(uuid);
        BOOST_CHECK_EQUAL(uuid.view(), rt.view());
        BOOST_CHECK_EQUAL(encoded, rt.to_string());
    }
    {
        /// flex strings
        auto str = gen_random_string();
        BOOST_CHECK_EQUAL(str, serde_flex(str));
    }
    {
        /// optional flex strings
        auto opt_str = std::optional<ss::sstring>(gen_random_string());
        BOOST_CHECK(opt_str == serde_flex(opt_str));

        {
            /// .. of a value of nullopt
            std::optional<ss::sstring> null_str;
            BOOST_CHECK(null_str == serde_flex(null_str));
        }
    }
    {
        /// flex bytes
        auto b = random_generators::get_bytes();
        BOOST_CHECK(b == serde_flex(b));
    }
    {
        /// flex array
        std::vector<test_struct> v;
        std::for_each(
          boost::counting_iterator<int>(0),
          boost::counting_iterator<int>(100),
          [&v](int) { v.push_back(test_struct::make_random()); });
        BOOST_CHECK_EQUAL(v, serde_flex(v));
    }
    {
        /// optional flex array
        std::optional<std::vector<test_struct>> v = std::vector<test_struct>();
        std::for_each(
          boost::counting_iterator<int>(0),
          boost::counting_iterator<int>(100),
          [&v](int) { v->push_back(test_struct::make_random()); });
        BOOST_CHECK(v == serde_flex(v));

        {
            /// ... of a value of nullopt
            std::optional<std::vector<test_struct>> null_vec;
            BOOST_CHECK(null_vec == serde_flex(null_vec));
        }
    }
    {
        /// flex iobuf
        auto str = gen_random_string();
        iobuf data, writers_buf, copy;
        data.append(str.begin(), str.length());
        copy.append(str.begin(), str.length());
        kafka::response_writer writer(writers_buf);
        writer.write_flex(std::move(data));

        kafka::request_reader reader(std::move(writers_buf));
        auto result = reader.read_fragmented_nullable_flex_bytes();
        BOOST_REQUIRE(result.has_value());
        BOOST_CHECK_EQUAL(iobuf_to_bytes(*result), iobuf_to_bytes(copy));
    }
}

namespace rg = random_generators;

template<>
struct rg::gen<kafka::api_versions_response_key> {
    static auto generate() {
        return kafka::api_versions_response_key{
          .api_key = rg::get_int<int16_t>(),
          .min_version = rg::get_int<int16_t>(),
          .max_version = rg::get_int<int16_t>()};
    }
};

template<>
struct rg::gen<kafka::supported_feature_key> {
    static auto generate() {
        return kafka::supported_feature_key{
          .name = rg::gen_alphanum_string(10),
          .min_version = rg::get_int<int16_t>(),
          .max_version = rg::get_int<int16_t>()};
    }
};

template<>
struct rg::gen<kafka::finalized_feature_key> {
    static auto generate() {
        return kafka::finalized_feature_key{
          .name = rg::gen_alphanum_string(10),
          .max_version_level = rg::get_int<int16_t>(),
          .min_version_level = rg::get_int<int16_t>()};
    }
};

template<>
struct rg::gen<kafka::api_versions_response> {
    static auto generate(bool with_tags = true) {
        kafka::api_versions_response response;
        response.data.error_code = kafka::error_code(rg::get_int<int16_t>());
        response.data.api_keys
          = rg::gen_array<kafka::api_versions_response_key>(100);
        response.data.throttle_time_ms = std::chrono::milliseconds(
          rg::get_int<int32_t>());
        if (with_tags) {
            response.data.supported_features
              = rg::gen_array<kafka::supported_feature_key>(100);
            response.data.finalized_features_epoch = rg::get_int<int64_t>();
            response.data.finalized_features
              = rg::gen_array<kafka::finalized_feature_key>(100);
        }
        return response;
    }
};

template<typename T>
concept KafkaApiResponse = requires(
  T response, kafka::response_writer& writer, iobuf b, kafka::api_version v) {
    { response.encode(writer, v) } -> std::same_as<void>;
    { response.decode(std::move(b), v) } -> std::same_as<void>;
};

template<KafkaApiResponse T>
T serde_kafka(T kafka_type, kafka::api_version version) {
    iobuf b;
    {
        kafka::response_writer writer(b);
        kafka_type.encode(writer, version);
    }
    T r2;
    r2.decode(std::move(b), version);
    return r2;
}

/// Tests the parsers generated by our kafka code generator - generator.py
/// Picks one type to test - api_versions_response as that request contains tags
SEASTAR_THREAD_TEST_CASE(serde_flex_request_types) {
    {
        /// Test serde on random data
        ss::parallel_for_each(boost::irange<int32_t>(0, 100), [&](int32_t i) {
            auto response = rg::gen<kafka::api_versions_response>::generate(
              (i % 2) == 0);
            auto serded = serde_kafka(response, kafka::api_version(3));
            BOOST_CHECK_EQUAL(response, serded);
            return ss::now();
        }).get();
    }
    {
        /// Test backward compatability support, given a newer struct, do not
        /// encode / decode tags
        auto response = rg::gen<kafka::api_versions_response>::generate(true);
        auto serded = serde_kafka(response, kafka::api_version(1));
        response.data.supported_features.clear();
        response.data.finalized_features.clear();
        response.data.finalized_features_epoch = -1; // default
        BOOST_CHECK_EQUAL(response, serded);
    }
    {
        /// Test that newer versions without tags don't encode them
        auto response = rg::gen<kafka::api_versions_response>::generate(false);
        auto serded = serde_kafka(response, kafka::api_version(3));
        BOOST_CHECK_EQUAL(response, serded);
        BOOST_CHECK(serded.data.finalized_features.empty());
        BOOST_CHECK(serded.data.supported_features.empty());
        BOOST_CHECK_EQUAL(serded.data.finalized_features_epoch, -1);
    }
    /// Measure a static expected size
    kafka::api_versions_response response{
      .data = kafka::api_versions_response_data{
        .api_keys = rg::gen_array<kafka::api_versions_response_key>(5)}};
    {
        iobuf b;
        kafka::response_writer writer(b);
        response.encode(writer, kafka::api_version(3));
        /// error_code = 2 bytes
        /// throttle_time_ms = 4 bytes
        /// api_keys =
        ///    size = 5 encoded as uvarint = (1 byte)
        ///    api_versions_response_key = 6 bytes + 1 zero byte for empty tags
        ///    <- repeated 5 times = 35 bytes total
        /// outer tags = 1 byte (to denote empty)
        /// total = 2 + 4 + (1 + 35) + 1
        BOOST_CHECK_EQUAL(43, b.size_bytes());
    }
    {
        /// fill in a non default tag
        response.data.finalized_features_epoch = 52;
        iobuf b;
        kafka::response_writer writer(b);
        response.encode(writer, kafka::api_version(3));
        /// Main difference is tag section will no longer be 1 byte
        ///
        /// size of tags = 1 byte uvarint encoded
        /// encoding tag itself = 1 byte uvarint encoded
        /// encoding of the value = 8 bytes encoded as int64_t
        /// size of the value itself = 1 byte (sizeof(int64_t))
        /// total = 11 for the tags section, delta of 10 bytes
        BOOST_CHECK_EQUAL(53, b.size_bytes());
    }
}

// The following hex data was generated by the go code pasted below. The
// idea is to have our parsers test correctly formatted and serialized
// binary data generated by franz-go
static constexpr auto test_api_version_response_data = std::to_array<uint8_t>(
  {0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x06, 0x00, 0x09, 0x02, 0x03, 0x68,
   0x69, 0x00, 0x00, 0x00, 0x03, 0x00, 0x01, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00,
   0x00, 0x00, 0x17, 0x02, 0x94, 0x01, 0x02, 0x8d, 0x01, 0x61, 0x62, 0x63, 0x64,
   0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36,
   0x37, 0x38, 0x39, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a,
   0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61, 0x62, 0x63,
   0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35,
   0x36, 0x37, 0x38, 0x39, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69,
   0x6a, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61, 0x62,
   0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x30, 0x31, 0x32, 0x33, 0x34,
   0x35, 0x36, 0x37, 0x38, 0x39, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
   0x69, 0x6a, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61,
   0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x30, 0x31, 0x32, 0x33,
   0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x00, 0x03, 0x00, 0x00, 0x00, 0x0a, 0x09,
   0x09, 0x68, 0x69, 0x5f, 0x74, 0x68, 0x65, 0x72, 0x65, 0x0b, 0x01, 0x00, 0x0c,
   0x03, 0x03, 0x68, 0x69});

// Peforming this test without serialization of an unknown tags section to
// test that redpanda can sucessfully re-serialize the data and test for
// 100% match. Redpanda doesn't store unknown tags so attempting to
// re-serialize the above data would result in a smaller structure and a
// failed match
static constexpr auto test_api_version_response_data_no_unknown_tags
  = std::to_array<uint8_t>({0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x03,
                            0x00, 0x09, 0x02, 0x03, 0x68, 0x69, 0x00, 0x00,
                            0x00, 0x03, 0x00, 0x01, 0x08, 0x00, 0x00, 0x00,
                            0x00, 0x00, 0x00, 0x00, 0x17, 0x02, 0x09, 0x02,
                            0x03, 0x68, 0x69, 0x00, 0x03, 0x00, 0x00, 0x00});

// package main
//
// import (
// 	"fmt"
// 	"github.com/twmb/franz-go/pkg/kbin"
// 	"github.com/twmb/franz-go/pkg/kmsg"
// )
// func main() {
// 	var tags kmsg.Tags
// 	tags.Set(10, kbin.AppendCompactString(nil, "hi_there"))
// 	tags.Set(11, kbin.AppendCompactNullableString(nil, nil))
// 	tags.Set(12, kbin.AppendCompactNullableString(nil,
// 		kmsg.StringPtr("hi")))
// 	req := kmsg.ApiVersionsResponse{
// 		Version:        3,
// 		ThrottleMillis: 2,
// 		SupportedFeatures: []kmsg.ApiVersionsResponseSupportedFeature{{
// 			Name:       "hi",
// 			MaxVersion: 3,
// 		}},
// 		FinalizedFeaturesEpoch: 23,
// 		FinalizedFeatures: []kmsg.ApiVersionsResponseFinalizedFeature{{
// 			Name: <variable>,
// 			MaxVersionLevel: 3,
// 		}},
// 		UnknownTags: <conditionally omitted>,
// 	}
// 	fmt.Printf("%02x\n", req.AppendTo(nil))
// }

SEASTAR_THREAD_TEST_CASE(test_sample_against_franz_go) {
    /// Test that a serde roundtrip produces the same hex data
    kafka::api_versions_response r;
    {
        iobuf iob;
        iob.append(
          test_api_version_response_data_no_unknown_tags.data(),
          test_api_version_response_data_no_unknown_tags.size());
        r.decode(std::move(iob), kafka::api_version(3));
    }

    bytes b;
    {
        iobuf iob;
        kafka::response_writer writer(iob);
        r.encode(writer, kafka::api_version(3));
        b = iobuf_to_bytes(iob);
    }
    BOOST_REQUIRE_EQUAL(
      b.size(), test_api_version_response_data_no_unknown_tags.size());
    BOOST_CHECK_EQUAL(
      0,
      memcmp(
        b.data(),
        test_api_version_response_data_no_unknown_tags.data(),
        b.size()));
}

SEASTAR_THREAD_TEST_CASE(test_sample_against_franz_go_with_unknown_tags) {
    kafka::api_versions_response r;
    {
        iobuf iob;
        iob.append(
          test_api_version_response_data.data(),
          test_api_version_response_data.size());
        r.decode(std::move(iob), kafka::api_version(3));
    }
    BOOST_CHECK(r.data.throttle_time_ms == std::chrono::milliseconds(2));
    BOOST_CHECK_EQUAL(r.data.error_code, kafka::error_code::none);
    BOOST_CHECK_EQUAL(r.data.api_keys.size(), 0);
    BOOST_REQUIRE_EQUAL(r.data.supported_features.size(), 1);
    BOOST_REQUIRE_EQUAL(r.data.finalized_features.size(), 1);
    BOOST_CHECK_EQUAL(r.data.finalized_features_epoch, 23);

    static constexpr auto larger_than_127_char_string
      = "abcdefghij0123456789abcdefghij0123456789abcdefghij0123456789abcdefghij"
        "0123456789abcdefghij0123456789abcdefghij0123456789abcdefghij012345678"
        "9";
    const auto& ffk = r.data.finalized_features[0];
    BOOST_CHECK_EQUAL(ffk.name, larger_than_127_char_string);
    BOOST_CHECK_EQUAL(ffk.max_version_level, 3);
    BOOST_CHECK_EQUAL(ffk.min_version_level, 0);

    const auto& sfk = r.data.supported_features[0];
    BOOST_CHECK_EQUAL(sfk.name, "hi");
    BOOST_CHECK_EQUAL(sfk.min_version, 0);
    BOOST_CHECK_EQUAL(sfk.max_version, 3);
}
