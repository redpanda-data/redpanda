/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "absl/strings/escaping.h"
#include "base/units.h"
#include "gtest/gtest.h"
#include "proto/redpanda/core/testing/example.pb.h"
#include "proto/redpanda/core/testing/example.proto.h"
#include "serde/protobuf/json.h"
#include "src/v/serde/protobuf/tests/codegen_test.pb.h"
#include "src/v/serde/protobuf/tests/codegen_test.proto.h"
#include "src/v/serde/protobuf/tests/test_messages_edition2023.pb.h"
#include "src/v/serde/protobuf/tests/test_messages_edition2023.proto.h"

#include <google/protobuf/json/json.h>
#include <google/protobuf/util/message_differencer.h>
#include <protobuf_mutator/mutator.h>

TEST(ProtobufCompat, RoundtripSerdeSimple) {
    proto::example::person_phone_number original;
    original.set_number("123-456-7890");
    original.set_type(proto::example::person_phone_number_phone_type::mobile);
    proto::example::person_phone_number deserialized;
    iobuf serialized = original.to_proto().get();
    EXPECT_NO_THROW(
      (deserialized = proto::example::person_phone_number::from_proto(
                        std::move(serialized))
                        .get()));
    EXPECT_EQ(original, deserialized);
}

TEST(ProtobufCompat, RoundtripSerdeEmptyConformanceProto) {
    protobuf_test_messages::editions::test_all_types_edition2023 original;
    auto serialized = original.to_proto().get();
    protobuf_test_messages::editions::test_all_types_edition2023 deserialized;
    EXPECT_NO_THROW(
      (deserialized = protobuf_test_messages::editions::
                        test_all_types_edition2023::from_proto(
                          std::move(serialized))
                          .get()));
    EXPECT_EQ(original, deserialized)
      << "original: " << original.to_json().get().linearize_to_string()
      << "\ndeserialized: "
      << deserialized.to_json().get().linearize_to_string();
    deserialized = {};
    EXPECT_NO_THROW(
      deserialized
      = protobuf_test_messages::editions::test_all_types_edition2023::from_json(
          original.to_json().get())
          .get());
    EXPECT_EQ(original, deserialized)
      << "original: " << original.to_json().get().linearize_to_string()
      << "\ndeserialized: "
      << deserialized.to_json().get().linearize_to_string();
}

TEST(ProtobufCompat, RoundtripSerde) {
    proto::example::person original;
    original.set_id(1);
    original.set_name("John Doe");
    original.set_gender(proto::example::person_gender::male);
    original.set_home_address("123 Main St");
    original.set_metadata({{"one", "two"}});
    auto& phone_one = original.get_phones().emplace_back();
    phone_one.set_number("123-456-7890");
    phone_one.set_type(proto::example::person_phone_number_phone_type::mobile);
    auto& phone_two = original.get_phones().emplace_back();
    phone_two.set_number("987-654-3210");
    phone_two.set_type(proto::example::person_phone_number_phone_type::work);
    auto serialized = original.to_proto().get();
    proto::example::person deserialized;
    EXPECT_NO_THROW(
      deserialized
      = proto::example::person::from_proto(std::move(serialized)).get());
    EXPECT_EQ(original, deserialized);
}

TEST(ProtobufCompat, LibprotobufCompat) {
    proto::example::person serde_version;
    {
        serde_version.set_id(1);
        serde_version.set_name("John Doe");
        serde_version.set_gender(proto::example::person_gender::male);
        serde_version.set_home_address("123 Main St");
        serde_version.set_metadata({{"one", "two"}});
        auto& phone_one = serde_version.get_phones().emplace_back();
        phone_one.set_number("123-456-7890");
        phone_one.set_type(
          proto::example::person_phone_number_phone_type::mobile);
        auto& phone_two = serde_version.get_phones().emplace_back();
        phone_two.set_number("987-654-3210");
        phone_two.set_type(
          proto::example::person_phone_number_phone_type::work);
    }
    rich::example::Person libpb_version;
    {
        libpb_version.set_id(1);
        libpb_version.set_name("John Doe");
        libpb_version.set_gender(
          rich::example::Person_Gender::Person_Gender_MALE);
        libpb_version.set_home_address("123 Main St");
        libpb_version.mutable_metadata()->insert({"one", "two"});
        auto* phone_one = libpb_version.add_phones();
        phone_one->set_number("123-456-7890");
        phone_one->set_type(
          rich::example::Person_PhoneNumber_PhoneType::
            Person_PhoneNumber_PhoneType_MOBILE);
        auto* phone_two = libpb_version.add_phones();
        phone_two->set_number("987-654-3210");
        phone_two->set_type(
          rich::example::Person_PhoneNumber_PhoneType::
            Person_PhoneNumber_PhoneType_WORK);
    }
    // Protobuf
    std::string libpb_serialized;
    EXPECT_TRUE(libpb_version.SerializeToString(&libpb_serialized));
    auto serde_serialized
      = serde_version.to_proto().get().linearize_to_string();
    proto::example::person serde_parsed;
    EXPECT_NO_THROW(
      serde_parsed = proto::example::person::from_proto(
                       iobuf::from(libpb_serialized))
                       .get());
    EXPECT_EQ(serde_version, serde_parsed);
    rich::example::Person libpb_parsed;
    EXPECT_TRUE(libpb_parsed.ParseFromString(serde_serialized));
    google::protobuf::util::MessageDifferencer differencer;
    std::string diff;
    differencer.ReportDifferencesToString(&diff);
    EXPECT_TRUE(differencer.Compare(libpb_version, libpb_parsed)) << diff;
    // JSON
    std::vector<google::protobuf::json::PrintOptions> options = {
      {},
      {.always_print_enums_as_ints = true},
      {.preserve_proto_field_names = true},
      {.unquote_int64_if_possible = true},
    };
    for (auto opts : options) {
        libpb_serialized = "";
        EXPECT_TRUE(
          google::protobuf::json::MessageToJsonString(
            libpb_version, &libpb_serialized, opts)
            .ok());
        auto p = serde::pb::json::peekable_parser(
          iobuf::from(libpb_serialized));
        serde_parsed = {};
        EXPECT_NO_THROW(
          proto::example::person::from_json(&p, &serde_parsed).get())
          << libpb_serialized;
        EXPECT_EQ(serde_version, serde_parsed);
    }
    serde_serialized = serde_version.to_json().get().linearize_to_string();
    libpb_parsed = {};
    EXPECT_TRUE(
      google::protobuf::json::JsonStringToMessage(
        serde_serialized, &libpb_parsed)
        .ok());
    diff = "";
    EXPECT_TRUE(differencer.Compare(libpb_version, libpb_parsed)) << diff;
}

TEST(ProtobufCompat, RandomizedConformanceTest) {
    protobuf_mutator::Mutator mutator;
    mutator.Seed(99);
    protobuf_test_messages::editions::TestAllTypesEdition2023 libpb;
    for (size_t i = 0; i < 100; ++i) {
        mutator.Mutate(&libpb, 1_MiB);
        auto libpb_serialized = libpb.SerializeAsString();
        std::optional<
          protobuf_test_messages::editions::test_all_types_edition2023>
          serde;
        EXPECT_NO_THROW(
          serde = protobuf_test_messages::editions::test_all_types_edition2023::
                    from_proto(iobuf::from(libpb_serialized))
                      .get());
        if (!serde) {
            continue;
        }
        auto serde_serialized = serde->to_proto().get().linearize_to_string();
        protobuf_test_messages::editions::TestAllTypesEdition2023 libpb_parsed;
        if (!libpb_parsed.ParseFromString(serde_serialized)) {
            FAIL() << "Failed to parse libpb from serde serialized data";
            continue;
        }
        google::protobuf::util::MessageDifferencer differencer;
        std::string diff;
        differencer.ReportDifferencesToString(&diff);
        differencer.set_message_field_comparison(
          google::protobuf::util::MessageDifferencer::MessageFieldComparison::
            EQUIVALENT);
        ASSERT_TRUE(differencer.Compare(libpb, libpb_parsed))
          << absl::CHexEscape(libpb_serialized) << "\n"
          << absl::CHexEscape(serde_serialized) << "\n"
          << diff;
    }
}

TEST(ProtobufCompat, RandomizedConformanceJsonTest) {
    protobuf_mutator::Mutator mutator;
    mutator.Seed(99);
    protobuf_test_messages::editions::TestAllTypesEdition2023 libpb;
    for (size_t i = 0; i < 100; ++i) {
        mutator.Mutate(&libpb, 1_MiB);
        std::vector<google::protobuf::json::PrintOptions> options = {
          {},
          {.always_print_enums_as_ints = true},
          {.preserve_proto_field_names = true},
          {.unquote_int64_if_possible = true},
        };
        for (auto opts : options) {
            std::string libpb_serialized;
            ASSERT_TRUE(
              google::protobuf::json::MessageToJsonString(
                libpb, &libpb_serialized, opts)
                .ok());
            auto p = serde::pb::json::peekable_parser(
              iobuf::from(libpb_serialized));
            protobuf_test_messages::editions::test_all_types_edition2023 serde;
            ASSERT_NO_THROW(
              protobuf_test_messages::editions::test_all_types_edition2023::
                from_json(&p, &serde)
                  .get())
              << libpb_serialized;
            auto serde_serialized = serde.to_json().get().linearize_to_string();
            protobuf_test_messages::editions::TestAllTypesEdition2023
              libpb_parsed;
            ASSERT_TRUE(
              google::protobuf::json::JsonStringToMessage(
                serde_serialized, &libpb_parsed)
                .ok())
              << serde_serialized;
            google::protobuf::util::MessageDifferencer differencer;
            std::string diff;
            differencer.ReportDifferencesToString(&diff);
            differencer.set_float_comparison(
              google::protobuf::util::MessageDifferencer::FloatComparison::
                APPROXIMATE);
            differencer.set_message_field_comparison(
              google::protobuf::util::MessageDifferencer::
                MessageFieldComparison::EQUIVALENT);
            ASSERT_TRUE(differencer.Compare(libpb, libpb_parsed))
              << libpb_serialized << "\n"
              << serde_serialized << "\n"
              << diff;
            ;
        }
    }
}

TEST(ProtobufCompat, Wellknown) {
    proto::example::well_known_protos original;
    original.set_single_duration(
      absl::Seconds(123456) + absl::Nanoseconds(789012));
    original.get_repeated_duration().emplace_back(absl::Seconds(1));
    original.get_repeated_duration().emplace_back(
      absl::Seconds(123) + absl::Nanoseconds(789));
    original.get_repeated_duration().emplace_back(
      absl::Now() - absl::UnixEpoch());
    original.get_repeated_duration().emplace_back(absl::Seconds(-1234));
    original.get_duration_map().insert(
      {"foo", absl::Seconds(1) + absl::Nanoseconds(7)});
    original.set_single_timestamp(absl::Now());
    original.get_repeated_timestamp().emplace_back(absl::UnixEpoch());
    original.get_repeated_timestamp().emplace_back(
      absl::Now() + absl::Minutes(123123));
    original.get_repeated_timestamp().emplace_back(
      absl::UnixEpoch() - absl::Minutes(12354324));
    original.get_timestamp_map().insert(
      {"foo", absl::Now() + absl::Nanoseconds(7)});
    original.set_single_field_mask(
      {.paths = {{"foo_bar"}, {"really", "long_path", "nested"}}});
    original.get_repeated_field_mask().push_back(
      {.paths = {{"foo_bar"}, {"really", "long_path", "nested"}}});
    original.get_field_mask_map().insert(
      {"qux", {.paths = {{"tada"}, {"really", "nested"}}}});
    proto::example::well_known_protos deserialized;
    iobuf serialized = original.to_proto().get();
    EXPECT_NO_THROW(
      (deserialized = proto::example::well_known_protos::from_proto(
                        serialized.copy())
                        .get()));
    EXPECT_EQ(original, deserialized)
      << "Proto: " << original.to_json().get().linearize_to_string()
      << "\nDeserialized: "
      << deserialized.to_json().get().linearize_to_string();
    deserialized = {};
    example::WellKnownProtos libpb;
    EXPECT_TRUE(libpb.ParseFromString(serialized.linearize_to_string()));
    EXPECT_NO_THROW(
      (deserialized = proto::example::well_known_protos::from_proto(
                        iobuf::from(libpb.SerializeAsString()))
                        .get()));
    EXPECT_EQ(libpb.repeated_duration().at(0).nanos(), 0);
    EXPECT_EQ(libpb.repeated_duration().at(0).seconds(), 1);
    EXPECT_EQ(original, deserialized);

    deserialized = {};
    serialized = original.to_json().get();
    EXPECT_NO_THROW(
      (deserialized
       = proto::example::well_known_protos::from_json(serialized.copy()).get()))
      << "JSON: " << serialized.linearize_to_string();
    EXPECT_EQ(original, deserialized)
      << "Proto: " << original.to_json().get().linearize_to_string()
      << "\nDeserialized: "
      << deserialized.to_json().get().linearize_to_string();

    libpb = {};
    deserialized = {};
    ASSERT_TRUE(
      google::protobuf::json::JsonStringToMessage(
        serialized.linearize_to_string(), &libpb, {})
        .ok());
    std::string libpb_serialized;
    ASSERT_TRUE(
      google::protobuf::json::MessageToJsonString(libpb, &libpb_serialized, {})
        .ok());
    EXPECT_NO_THROW(
      (deserialized = proto::example::well_known_protos::from_json(
                        iobuf::from(libpb_serialized))
                        .get()))
      << "JSON: " << libpb_serialized;
    EXPECT_EQ(libpb.repeated_duration().at(0).nanos(), 0);
    EXPECT_EQ(libpb.repeated_duration().at(0).seconds(), 1);
    EXPECT_EQ(original, deserialized)
      << "Proto: " << fmt::format("{}", original)
      << "Deserialized: " << fmt::format("{}", deserialized);
}

TEST(ProtobufCompat, DebugRedact) {
    proto::example::super_duper_secret secret;
    secret.set_value("12345");
    EXPECT_EQ("{value: <redacted>}", fmt::format("{}", secret));
}

// string fields are limited to 128KiB, but iobuf-backed fields should be able
// to handle larger sizes. This test verifies that the iobuf-backed field can
// roundtrip larger data without error and without truncation.
TEST(ProtobufCompat, RoundtripLargeIOBufString) {
    const size_t large_size = 150 * 1024;
    std::string large_data(large_size, 'A');

    proto::example::io_buf_string_field original;
    original.set_large_data(iobuf::from(large_data));

    // Roundtrip through proto serialization
    {
        auto serialized = original.to_proto().get();
        auto deserialized = proto::example::io_buf_string_field::from_proto(
                              std::move(serialized))
                              .get();

        // Verify data is preserved
        EXPECT_EQ(original, deserialized);
    }

    // Roundtrip through json serialization
    {
        auto serialized = original.to_json().get();
        auto deserialized = proto::example::io_buf_string_field::from_json(
                              std::move(serialized))
                              .get();

        // Verify data is preserved
        EXPECT_EQ(original, deserialized);
    }
}
