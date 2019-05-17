#include <iostream>
#include <utility>

#include <gtest/gtest.h>

// filesystem
#include "raft_client_cache.h"


TEST(raft_client_cache_bitflags, basic) {
  using bitflags = raft_client_cache::bitflags;
  bitflags b = bitflags::none;
  ASSERT_EQ(b, bitflags::none);
  b = b | bitflags::circuit_breaker | bitflags::reached_max_retries;
  ASSERT_EQ(b & bitflags::circuit_breaker, bitflags::circuit_breaker);
  ASSERT_EQ(b & bitflags::reached_max_retries, bitflags::reached_max_retries);

  b = b & ~bitflags::reached_max_retries;
  ASSERT_EQ(b & bitflags::reached_max_retries, bitflags::none);
  ASSERT_EQ(b & bitflags::circuit_breaker, bitflags::circuit_breaker);

  b = b & ~bitflags::circuit_breaker;
  ASSERT_EQ(b & bitflags::reached_max_retries, bitflags::none);
  ASSERT_EQ(b & bitflags::circuit_breaker, bitflags::none);
}

int
main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
