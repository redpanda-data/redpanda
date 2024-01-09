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

#pragma once

#include <cstddef>
#include <cstdint>

// adapted from original:
// https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.h

// bloom filter magic seed in bitcoin core
constexpr static const uint32_t kDefaultHashingSeed = 0xFBA4C795;

uint32_t murmurhash3_x86_32(
  const void* key, std::size_t len, uint32_t seed = kDefaultHashingSeed);

void murmurhash3_x86_128(
  const void* key,
  std::size_t len,
  void* out,
  uint32_t seed = kDefaultHashingSeed);

void murmurhash3_x64_128(
  const void* key,
  std::size_t len,
  void* out,
  uint32_t seed = kDefaultHashingSeed);

uint32_t murmur2(
  const void* key,
  std::size_t len,
  // Default Seed is the Kafka partition hashing seed. Since this is the main
  // intention for this hashing function, we make this the default value
  // https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L441
  uint32_t seed = 0x9747b28c);
