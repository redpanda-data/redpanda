// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// crc::crc32c values are persisted (storage indices, snapshots, sst footers)
// and are part of the Kafka record batch wire format, so they can never
// change. These tests pin the absolute values against external vectors rather
// than against whatever a backing library computes, and they run over every
// basic_crc32c backend, so no backend -- and no change of the production
// backend -- can silently alter them.

#include "bytes/iobuf.h"
#include "hashing/crc32c.h"

#include <seastar/core/temporary_buffer.hh>

#include <base/seastarx.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <numeric>
#include <string>
#include <string_view>
#include <vector>

namespace {

template<typename CrcT>
uint32_t crc32c_of(std::string_view data) {
    CrcT crc;
    crc.extend(data.data(), data.size());
    return crc.value();
}

std::string_view as_view(const std::vector<uint8_t>& v) {
    // NOLINTNEXTLINE(*-reinterpret-cast)
    return {reinterpret_cast<const char*>(v.data()), v.size()};
}

} // namespace

template<typename CrcT>
class crc32c_test : public ::testing::Test {};

struct backend_names {
    template<typename CrcT>
    static std::string GetName(int) {
        if constexpr (
          std::is_same_v<CrcT, crc::basic_crc32c<crc::crc32c_google_backend>>) {
            return "google";
        } else {
            return "abseil";
        }
    }
};

using implementations = ::testing::Types<
  crc::basic_crc32c<crc::crc32c_google_backend>,
  crc::basic_crc32c<crc::crc32c_abseil_backend>>;
TYPED_TEST_SUITE(crc32c_test, implementations, backend_names);

// The four 32-byte vectors from RFC 3720 appendix B.4 (iSCSI), which is where
// the CRC32C polynomial, bit order and init/final xor are normatively defined.
TYPED_TEST(crc32c_test, rfc3720_vectors) {
    std::vector<uint8_t> zeros(32, 0x00);
    EXPECT_EQ(crc32c_of<TypeParam>(as_view(zeros)), 0x8a9136aaU);

    std::vector<uint8_t> ones(32, 0xff);
    EXPECT_EQ(crc32c_of<TypeParam>(as_view(ones)), 0x62a8ab43U);

    std::vector<uint8_t> incrementing(32);
    std::iota(incrementing.begin(), incrementing.end(), uint8_t{0});
    EXPECT_EQ(crc32c_of<TypeParam>(as_view(incrementing)), 0x46dd794eU);

    std::vector<uint8_t> decrementing(32);
    for (size_t i = 0; i < decrementing.size(); ++i) {
        decrementing[i] = static_cast<uint8_t>(31 - i);
    }
    EXPECT_EQ(crc32c_of<TypeParam>(as_view(decrementing)), 0x113fdb5cU);
}

// The CRC-32/ISCSI "check" value: the catalogued output for the ASCII string
// "123456789", used across implementations to identify the algorithm.
TYPED_TEST(crc32c_test, check_value) {
    EXPECT_EQ(crc32c_of<TypeParam>("123456789"), 0xe3069283U);
}

TYPED_TEST(crc32c_test, empty_is_zero) {
    TypeParam crc;
    EXPECT_EQ(crc.value(), 0U);
    crc.extend(static_cast<const char*>(nullptr), 0);
    EXPECT_EQ(crc.value(), 0U);
}

TYPED_TEST(crc32c_test, short_strings) {
    EXPECT_EQ(crc32c_of<TypeParam>("a"), 0xc1d04330U);
    EXPECT_EQ(crc32c_of<TypeParam>("foo"), 0xcfc4ae1dU);
    EXPECT_EQ(crc32c_of<TypeParam>("hello world"), 0xc99465aaU);
}

// Extending in pieces must equal extending in one call, at every split point.
// This is the property model::internal_header_only_crc and crc_extend_iobuf
// rely on, and the one most at risk from an implementation that dispatches on
// buffer length.
TYPED_TEST(crc32c_test, incremental_matches_contiguous) {
    std::vector<uint8_t> data(4096);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<uint8_t>(i * 31 + 7);
    }
    const auto whole = crc32c_of<TypeParam>(as_view(data));

    // Split points chosen to straddle the internal size thresholds of both
    // google/crc32c (1008, 4080) and abseil (64, 256, 2048).
    for (size_t split :
         {size_t{1},
          size_t{7},
          size_t{8},
          size_t{63},
          size_t{64},
          size_t{65},
          size_t{255},
          size_t{256},
          size_t{257},
          size_t{1007},
          size_t{1008},
          size_t{2047},
          size_t{2048},
          size_t{4032},
          size_t{4095}}) {
        TypeParam crc;
        // NOLINTNEXTLINE(*-reinterpret-cast)
        const auto* p = reinterpret_cast<const char*>(data.data());
        crc.extend(p, split);
        crc.extend(p + split, data.size() - split);
        EXPECT_EQ(crc.value(), whole) << "split at " << split;
    }
}

TYPED_TEST(crc32c_test, byte_at_a_time_matches_contiguous) {
    constexpr std::string_view data = "the quick brown fox jumps over the dog";
    TypeParam crc;
    for (char c : data) {
        crc.extend(&c, 1);
    }
    EXPECT_EQ(crc.value(), crc32c_of<TypeParam>(data));
}

// The integral overload hashes the object representation, so its result must
// match feeding the same bytes through the pointer overload.
TYPED_TEST(crc32c_test, integral_overload_matches_bytes) {
    constexpr uint64_t value = 0x0123456789abcdefULL;

    TypeParam from_integral;
    from_integral.extend(value);

    TypeParam from_bytes;
    // NOLINTNEXTLINE(*-reinterpret-cast)
    from_bytes.extend(reinterpret_cast<const char*>(&value), sizeof(value));

    EXPECT_EQ(from_integral.value(), from_bytes.value());
}

TYPED_TEST(crc32c_test, integral_overload_widths) {
    TypeParam crc;
    crc.extend(static_cast<int8_t>(-1));
    crc.extend(static_cast<int16_t>(-2));
    crc.extend(static_cast<int32_t>(-3));
    crc.extend(static_cast<int64_t>(-4));

    constexpr std::array<uint8_t, 15> expected{
      0xff,
      0xfe,
      0xff,
      0xfd,
      0xff,
      0xff,
      0xff,
      0xfc,
      0xff,
      0xff,
      0xff,
      0xff,
      0xff,
      0xff,
      0xff};
    TypeParam bytes;
    // NOLINTNEXTLINE(*-reinterpret-cast)
    bytes.extend(
      reinterpret_cast<const char*>(expected.data()), expected.size());

    EXPECT_EQ(crc.value(), bytes.value());
}

TYPED_TEST(crc32c_test, iobuf_matches_contiguous) {
    std::vector<uint8_t> data(8192);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<uint8_t>(i % 251);
    }
    const auto expected = crc32c_of<TypeParam>(as_view(data));

    for (size_t frag_size :
         {size_t{1},
          size_t{7},
          size_t{64},
          size_t{512},
          size_t{4096},
          size_t{8192}}) {
        iobuf buf;
        for (size_t off = 0; off < data.size(); off += frag_size) {
            auto sz = std::min(frag_size, data.size() - off);
            buf.append(
              std::make_unique<iobuf::fragment>(
                ss::temporary_buffer<char>(as_view(data).data() + off, sz)));
        }

        TypeParam crc;
        crc_extend_iobuf(crc, buf);
        EXPECT_EQ(crc.value(), expected) << "fragment size " << frag_size;
    }
}

TYPED_TEST(crc32c_test, iobuf_empty) {
    iobuf buf;
    TypeParam crc;
    crc_extend_iobuf(crc, buf);
    EXPECT_EQ(crc.value(), 0U);
}
