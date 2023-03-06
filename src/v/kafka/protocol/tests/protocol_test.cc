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
#include "protocol_utils.h"

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
        "is_request: {} re-encoded size_bytes: {} expected size_bytes: {}",
        key,
        version,
        is_request,
        b.size(),
        result.size()));
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

BOOST_AUTO_TEST_CASE(test_kafka_protocol_compat) {
    check_all_requests(kafka::request_types{});
}

} // namespace kafka
