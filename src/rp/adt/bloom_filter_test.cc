#include <utility>

#include <gtest/gtest.h>

#include "roaring_bloom_filter.h"

TEST(bloom, basic) {
  rp::roaring_bloom_filter<> bf;
  ASSERT_FALSE(bf.contains("hello"));
  bf.add("hello");
  ASSERT_TRUE(bf.contains("hello"));
}

TEST(bloom, worst_case) {
  struct blhasher {
    std::array<uint32_t, 1>
    operator()(const uint32_t &data) const {
      std::array<uint32_t, 1> retval{data};
      return retval;
    }
  };

  rp::roaring_bloom_filter<uint32_t, 1, blhasher> bf;
  for (auto i = 0; i < 100000; ++i) {
    bf.add(i);
  }

  std::string buf = std::to_string(bf.positive_error_rate());
  buf.resize(8);
  ASSERT_EQ(buf, std::string("0.000047"));
  ASSERT_GT(bf.size_in_bytes(), 16000);
  ASSERT_LT(bf.size_in_bytes(), 17000);
}

TEST(bloom, string_items) {
  struct blhasher {
    rp::bloom_hasher<const char *, rp::kDefaultHashingLevels> hr;
    std::array<uint32_t, rp::kDefaultHashingLevels>
    operator()(const std::string &data) const {
      auto x = data.c_str();
      return hr(x);
    }
  };

  rp::roaring_bloom_filter<std::string, rp::kDefaultHashingLevels, blhasher> bf;
  for (auto i = 0; i < 10000; ++i) {
    bf.add(std::to_string(i * i));
  }

  std::string buf = std::to_string(bf.positive_error_rate());
  buf.resize(8);
  ASSERT_EQ(buf, std::string("0.000000"));

  // 10K distinct strings is expensive! 8KB
  ASSERT_GT(bf.size_in_bytes(), 8000);
  ASSERT_LT(bf.size_in_bytes(), 9000);
}

int
main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
