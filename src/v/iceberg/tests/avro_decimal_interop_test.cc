/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "absl/numeric/int128.h"
#include "bytes/bytes.h"
#include "iceberg/avro_decimal.h"
#include "test_utils/runfiles.h"

#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

// 10^38 - 1, the largest in-spec magnitude for an Iceberg decimal with
// precision 38. ceil(log2(10^38)) == 127, so this still encodes as a full
// 16-byte payload — exercising the int128 boundary without exceeding spec.
constexpr absl::int128 k10Pow19{10'000'000'000'000'000'000ULL};
const absl::int128 kPrecision38Max = k10Pow19 * k10Pow19 - 1;

// Mirrors CASES in avro_decimal_interop_gen.py. Asserts that our decoder
// recovers each int128 from the bytes the reference Apache `avro` Python
// library produced.
const std::unordered_map<std::string, absl::int128>& expected_values() {
    static const auto& kCases
      = *new std::unordered_map<std::string, absl::int128>{
        {"zero", absl::int128(0)},
        {"one", absl::int128(1)},
        {"neg_one", absl::int128(-1)},
        {"127", absl::int128(127)},
        {"128", absl::int128(128)},
        {"neg_128", absl::int128(-128)},
        {"neg_129", absl::int128(-129)},
        {"12345", absl::int128(12345)},
        {"neg_12345", absl::int128(-12345)},
        {"65536", absl::int128(65536)},
        {"two_pow_40", absl::int128(1) << 40},
        {"neg_two_pow_40", -(absl::int128(1) << 40)},
        {"int64_max", absl::int128(std::numeric_limits<int64_t>::max())},
        {"int64_min", absl::int128(std::numeric_limits<int64_t>::min())},
        {"two_pow_64", absl::int128(1) << 64},
        {"precision_38_max", kPrecision38Max},
        {"neg_precision_38_max", -kPrecision38Max},
      };
    return kCases;
}

} // namespace

TEST(AvroDecimalInteropTest, ReferenceEncoderRoundTrips) {
    const auto path = test_utils::get_runfile_path(
      "src/v/iceberg/tests/decimal_cases.avro");
    avro::DataFileReader<avro::GenericDatum> reader(path.c_str());

    std::unordered_map<std::string, absl::int128> decoded;
    while (true) {
        avro::GenericDatum datum(reader.dataSchema());
        if (!reader.read(datum)) {
            break;
        }
        ASSERT_EQ(datum.type(), avro::AVRO_RECORD);
        const auto& record = datum.value<avro::GenericRecord>();

        const auto& name = record.field("name").value<std::string>();
        const auto& payload
          = record.field("payload").value<std::vector<uint8_t>>();

        bytes payload_bytes(payload.begin(), payload.end());
        auto value = iceberg::decode_avro_decimal(std::move(payload_bytes));
        auto [_, inserted] = decoded.emplace(name, value);
        ASSERT_TRUE(inserted) << "duplicate case name in fixture: " << name;
    }

    const auto& expected = expected_values();
    EXPECT_EQ(decoded.size(), expected.size())
      << "case count differs between fixture and expectations table";
    for (const auto& [name, exp] : expected) {
        auto it = decoded.find(name);
        ASSERT_NE(it, decoded.end()) << "case missing from fixture: " << name;
        EXPECT_EQ(it->second, exp) << "decoded mismatch for case: " << name;
    }
}
