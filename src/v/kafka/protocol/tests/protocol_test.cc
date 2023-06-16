// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/handler_interface.h"
#include "kafka/server/handlers/handlers.h"

#include <boost/process.hpp>
#include <boost/test/unit_test.hpp>

#include <sstream>

#define TEST_COMPAT_CHECK_NO_THROW(Source, Api, Version, IsRequest)            \
    try {                                                                      \
        Source;                                                                \
    } catch (const std::exception& ex) {                                       \
        BOOST_TEST(                                                            \
          false,                                                               \
          fmt::format(                                                         \
            "API protocol incompatability detected at api: {}, version: "      \
            "{}, IsRequest: {}, exception reported: {}",                       \
            Api,                                                               \
            Version,                                                           \
            IsRequest,                                                         \
            ex.what()));                                                       \
    }

namespace kafka {

/// Signifies a request if true, otherwise is_a kafka response
using is_kafka_request = ss::bool_class<struct is_kafka_request_tag>;

bytes invoke_franz_harness(
  api_key key, api_version v, is_kafka_request is_request) {
    static const boost::filesystem::path generator_path = []() {
        /// This env var is passed via CMake
        const char* gen_cstr = std::getenv("GENERATOR_BIN");
        vassert(gen_cstr, "Missing generator binary path in test env");
        boost::filesystem::path p(gen_cstr);
        vassert(
          boost::filesystem::exists(p),
          "Harness error, provided GENERATOR_BIN not found: {}",
          p);
        return p;
    }();

    ss::sstring stdout;
    {
        boost::process::ipstream is;
        boost::process::child c(
          generator_path.string(),
          boost::process::args({
            "-api",
            std::to_string(key()),
            "-version",
            std::to_string(v()),
            (is_request == is_kafka_request::yes ? "-is-request=true"
                                                 : "-is-request=false"),
          }),
          boost::process::std_out > is);

        /// If the program doesn't exit with success, issue is with test binary
        /// fail hard as author should fix to account for diff in feature set
        c.wait();
        vassert(
          c.exit_code() == 0,
          "kafka-request-generator exited with non-zero status");

        /// Capture data on stdout
        std::stringstream ss;
        ss << is.rdbuf();
        stdout = ss.str();
    }
    auto result = ss::uninitialized_string<bytes>(stdout.size());
    std::copy_n(stdout.begin(), stdout.size(), result.begin());
    return result;
}

/// To adapt the generic test method below for the cases in which some generated
/// types don't adhere to the standard api for type::decode
template<typename T>
concept HasPrimitiveDecode = requires(T t, iobuf iob, api_version v) {
    { t.decode(std::move(iob), v) } -> std::same_as<void>;
};

/// If there is an issue with decoding of legacy batches, an exception will not
/// be thrown. To make the test aware of these potential issues, each
/// kafka_batch_adapter for every partition in a request must be queried for its
/// legacy success status
bool legacy_batch_failures(
  const kafka::produce_request_data& req, api_version) {
    return std::any_of(
      req.topics.cbegin(), req.topics.cend(), [](const auto& topic) {
          return std::any_of(
            topic.partitions.cbegin(),
            topic.partitions.cend(),
            [](const auto& pp_data) {
                /// If records is null, means no records were sent, so batch was
                /// parsed with no error up-until this point, all OK
                return !pp_data.records
                         ? false
                         : (pp_data.records->adapter.legacy_error);
            });
      });
}

/// Comprehensive kafka protocol tests
///
/// This test is performed for each kafka api, at all supported levels, for
/// request and response types
//
/// A small go program that depends on franz-go generates a sample request or
/// response for a given api/version combination. Redpanda attempts to
/// deserialize and reserialize the data, comparing the binary representation
/// with the initial.
template<typename T>
void check_kafka_binary_format(
  api_key key, api_version version, is_kafka_request is_request) {
    decltype(T::data) r;
    auto result = invoke_franz_harness(key, version, is_request);
    {
        if constexpr (HasPrimitiveDecode<decltype(r)>) {
            r.decode(bytes_to_iobuf(result), version);
        } else {
            kafka::request_reader rdr(bytes_to_iobuf(result));
            r.decode(rdr, version);
        }
    }
    /// re-encode back to bytes, confirming equality
    bytes b;
    {
        if constexpr (std::is_same_v<T, produce_request>) {
            /// Redpanda currently does not support encoding legacy batches,
            /// it is not possible to re-convert back to bytes and compare
            BOOST_TEST(
              !legacy_batch_failures(r, version),
              fmt::format(
                "produce_request encountered when decoding legacy batches, "
                "version: {}",
                version));
            return;
        }
        iobuf iob;
        kafka::response_writer rw(iob);
        r.encode(rw, version);
        b = iobuf_to_bytes(iob);
    }
    BOOST_TEST(
      b == result,
      fmt::format(
        "Mismatched binary data detected for api: {} at version: {} "
        "is_request: {}",
        key,
        version,
        is_request));
}

template<kafka::KafkaApiHandlerAny H>
void check_proto_compat() {
    for (auto version = H::min_supported; version <= H::max_supported();
         ++version) {
        TEST_COMPAT_CHECK_NO_THROW(
          check_kafka_binary_format<typename H::api::request_type>(
            H::api::key, version, is_kafka_request::yes),
          H::api::key,
          version,
          true);
        TEST_COMPAT_CHECK_NO_THROW(
          check_kafka_binary_format<typename H::api::response_type>(
            H::api::key, version, is_kafka_request::no),
          H::api::key,
          version,
          false);
    }
}

template<typename... Ts>
void check_all_requests(kafka::type_list<Ts...>) {
    (check_proto_compat<Ts>(), ...);
}

template<typename Request, api_version::type version, bool is_request>
requires(KafkaApiHandler<Request>) struct tag_field_entry {
    using api = typename Request::api;
    static constexpr api_version test_version = api_version(version);
    static constexpr bool request = is_request;
};

// Instructions on adding optional tag field messages
// When a new kafka message is added that contains optional tag fields,
// you will do the following:
// 1. Add the entry to `tag_field_entries`, providing
//    a. The handler
//    b. minimum api version
//    c. true for request, false for response (needs to be bool for constexpr)
// 2. Create a specialized `create_default_and_non_default_data` function
//    a. Have it set non-default values and default values for each
//    b. Provide the difference in size the encoded buffers will be

using tag_field_entries = kafka::type_list<
  tag_field_entry<create_topics_handler, 5, false>,
  tag_field_entry<api_versions_handler, 3, false>>;

template<typename T>
long create_default_and_non_default_data(T& non_default_data, T& default_data);

template<>
long create_default_and_non_default_data(
  decltype(create_topics_response::data)& non_default_data,
  decltype(create_topics_response::data)& default_data) {
    non_default_data.throttle_time_ms = std::chrono::milliseconds(1000);

    non_default_data.topics.emplace_back(creatable_topic_result{
      model::topic{"topic1"},
      {},
      kafka::error_code{1},
      "test_error_message",
      3,
      16,
      std::nullopt,
      kafka::error_code{2}});

    default_data = non_default_data;

    default_data.topics.at(0).topic_config_error_code = kafka::error_code{0};

    // int16 (2 bytes) + tag (2 bytes)
    return 4;
}

template<>
long create_default_and_non_default_data(
  decltype(api_versions_response::data)& non_default_data,
  decltype(api_versions_response::data)& default_data) {
    non_default_data.finalized_features_epoch = 0;
    default_data = non_default_data;
    default_data.finalized_features_epoch = -1;

    // int64 (8 bytes) + tag (2 bytes)
    return 10;
}

template<typename T>
bool validate_buffer_against_data(
  const T& check_data, api_version version, const bytes& buffer) {
    T r;
    if constexpr (HasPrimitiveDecode<decltype(r)>) {
        r.decode(bytes_to_iobuf(buffer), version);
    } else {
        kafka::request_reader rdr(bytes_to_iobuf(buffer));
        r.decode(rdr, version);
    }

    return r == check_data;
}

template<typename T>
void check_kafka_tag_format(api_version version, bool is_request) {
    decltype(T::data) non_default_data{};
    decltype(T::data) default_data{};

    auto size_diff = create_default_and_non_default_data(
      non_default_data, default_data);

    BOOST_REQUIRE_NE(non_default_data, default_data);

    bytes non_default_encoded, default_encoded;

    {
        iobuf iob;
        kafka::response_writer rw(iob);
        non_default_data.encode(rw, version);
        non_default_encoded = iobuf_to_bytes(iob);
    }

    {
        iobuf iob;
        kafka::response_writer rw(iob);
        default_data.encode(rw, version);
        default_encoded = iobuf_to_bytes(iob);
    }

    BOOST_CHECK_EQUAL(
      non_default_encoded.size() - default_encoded.size(), size_diff);

    BOOST_TEST_CHECK(validate_buffer_against_data(
      non_default_data, version, non_default_encoded));
    BOOST_TEST_CHECK(
      validate_buffer_against_data(default_data, version, default_encoded));
}

template<typename T>
concept FieldEntry = kafka::KafkaApi<typename T::api> && requires(T t) {
    { T::test_version } -> std::convertible_to<const api_version&>;
    { T::request } -> std::convertible_to<const bool>;
};

template<FieldEntry T>
void check_tag_request() {
    if constexpr (T::request) {
        check_kafka_tag_format<typename T::api::request_type>(
          T::test_version, T::request);
    } else {
        check_kafka_tag_format<typename T::api::response_type>(
          T::test_version, T::request);
    }
}

template<typename... Ts>
void check_all_tag_requests(kafka::type_list<Ts...>) {
    (check_tag_request<Ts>(), ...);
}

BOOST_AUTO_TEST_CASE(test_kafka_protocol_compat) {
    check_all_requests(kafka::request_types{});
}

BOOST_AUTO_TEST_CASE(test_optional_tag_values) {
    check_all_tag_requests(tag_field_entries{});
}

} // namespace kafka
