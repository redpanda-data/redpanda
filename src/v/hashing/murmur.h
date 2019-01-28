#pragma once

#include <cstddef>
#include <cstdint>

namespace v {
// adapted from original:
// https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.h

// bloom filter magic seed in bitcoin core
constexpr static const uint32_t kDefaultHashingSeed = 0xFBA4C795;

uint32_t murmurhash3_x86_32(const void *key, std::size_t len,
                            uint32_t seed = kDefaultHashingSeed);

void murmurhash3_x86_128(const void *key, std::size_t len, void *out,
                         uint32_t seed = kDefaultHashingSeed);

void murmurhash3_x64_128(const void *key, std::size_t len, void *out,
                         uint32_t seed = kDefaultHashingSeed);

}  // namespace v
