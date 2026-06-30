// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "hashing/murmur.h"

#include <bit>

// adapted from original:
// https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.h

namespace {

// MurmurHash3 reads each block as a little-endian word. Convert from
// little-endian to host order so the hash matches on big-endian platforms.
template<typename T>
inline __attribute__((always_inline)) T le_block_to_host(T k) {
    if constexpr (std::endian::native == std::endian::big) {
        return std::byteswap(k);
    }
    return k;
}
inline uint32_t rotl32(uint32_t x, int8_t r) {
    return (x << r) | (x >> (32 - r));
}
inline uint64_t rotl64(uint64_t x, int8_t r) {
    return (x << r) | (x >> (64 - r));
}

inline __attribute__((always_inline)) uint32_t fmix32(uint32_t h) {
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;

    return h;
}
inline __attribute__((always_inline)) uint64_t fmix64(uint64_t k) {
    k ^= k >> 33;
    k *= uint64_t(0xff51afd7ed558ccd);
    k ^= k >> 33;
    k *= uint64_t(0xc4ceb9fe1a85ec53);
    k ^= k >> 33;

    return k;
}

//-----------------------------------------------------------------------------
// Block read - if your platform needs to do endian-swapping or can only
// handle aligned reads, do the conversion here
inline __attribute__((always_inline)) uint32_t
getblock32(const uint32_t* p, int i) {
    uint32_t k;
    std::memcpy(&k, reinterpret_cast<const char*>(p + i), sizeof(k));
    return le_block_to_host(k);
}
inline __attribute__((always_inline)) uint64_t
getblock64(const uint64_t* p, int i) {
    uint64_t k;
    std::memcpy(&k, reinterpret_cast<const char*>(p + i), sizeof(k));
    return le_block_to_host(k);
}
namespace x86_32 {
const uint32_t c1 = 0xcc9e2d51;
const uint32_t c2 = 0x1b873593;

void consume_block(uint32_t& h1, uint32_t block) {
    block *= c1;
    block = rotl32(block, 15);
    block *= c2;

    h1 ^= block;
    h1 = rotl32(h1, 13);
    h1 = h1 * 5 + 0xe6546b64;
}

void consume_tail(uint32_t& h1, const uint8_t* tail, int len) {
    uint32_t k1 = 0;

    switch (len) {
    case 3:
        k1 ^= tail[2] << 16;
    case 2:
        k1 ^= tail[1] << 8;
    case 1:
        k1 ^= tail[0];
        k1 *= c1;
        k1 = rotl32(k1, 15);
        k1 *= c2;
        h1 ^= k1;
    };
}

void finalize(uint32_t& h1, int len) {
    h1 ^= len;
    h1 = fmix32(h1);
}
} // namespace x86_32
} // namespace

uint32_t murmurhash3_x86_32(const void* key, std::size_t len, uint32_t seed) {
    const uint8_t* data = (const uint8_t*)key;
    const int nblocks = len / 4;

    uint32_t h1 = seed;

    auto blocks = reinterpret_cast<const uint32_t*>(data + nblocks * 4);

    for (int i = -nblocks; i; i++) {
        uint32_t k1 = getblock32(blocks, i);
        x86_32::consume_block(h1, k1);
    }

    const uint8_t* tail = (const uint8_t*)(data + nblocks * 4);
    x86_32::consume_tail(h1, tail, len & 3);

    x86_32::finalize(h1, len);

    return h1;
}

uint32_t murmurhash3_x86_32(const iobuf& data, uint32_t seed) {
    uint32_t h1 = seed;

    uint32_t torn_block; // murmur block split between iobuf fragments
    auto torn_block_begin = reinterpret_cast<char*>(&torn_block);
    auto torn_block_end = torn_block_begin + 4;
    auto torn_block_data_end = torn_block_begin;

    for (const auto& fragment : data) {
        auto frag_begin = fragment.get();
        auto frag_size = fragment.size();
        auto frag_end = frag_begin + frag_size;

        // torn block
        size_t torn_remaining_capacity = torn_block_end - torn_block_data_end;
        if (fragment.size() < torn_remaining_capacity) {
            torn_block_data_end = std::copy(
              frag_begin, frag_end, torn_block_data_end);
            continue;
        }

        std::copy(
          frag_begin,
          frag_begin + torn_remaining_capacity,
          torn_block_data_end);
        // torn_block is assembled byte-wise; read it as a little-endian word to
        // match getblock32.
        x86_32::consume_block(h1, le_block_to_host(torn_block));

        // rest of full blocks in fragment
        auto blocks_begin = frag_begin + torn_remaining_capacity;
        const ssize_t blocks_size = fragment.size() - torn_remaining_capacity;
        auto nblocks = blocks_size / 4;
        auto blocks_end = blocks_begin + nblocks * 4;
        auto blocks = reinterpret_cast<const uint32_t*>(blocks_end);
        for (ssize_t i = -nblocks; i; i++) {
            uint32_t k1 = getblock32(blocks, i);
            x86_32::consume_block(h1, k1);
        }

        // next torn block
        torn_block_data_end = std::copy(blocks_end, frag_end, torn_block_begin);
    }

    auto tail = reinterpret_cast<const uint8_t*>(torn_block_begin);
    x86_32::consume_tail(
      h1, tail, static_cast<int>(torn_block_data_end - torn_block_begin));

    x86_32::finalize(h1, data.size_bytes());

    return h1;
}

//-----------------------------------------------------------------------------

void murmurhash3_x86_128(
  const void* key, std::size_t len, void* out, uint32_t seed) {
    const uint8_t* data = (const uint8_t*)key;
    const int nblocks = len / 16;

    uint32_t h1 = seed;
    uint32_t h2 = seed;
    uint32_t h3 = seed;
    uint32_t h4 = seed;

    const uint32_t c1 = 0x239b961b;
    const uint32_t c2 = 0xab0e9789;
    const uint32_t c3 = 0x38b34ae5;
    const uint32_t c4 = 0xa1e38b93;

    //----------
    // body

    auto blocks = reinterpret_cast<const uint32_t*>(data + nblocks * 16);

    for (int i = -nblocks; i; i++) {
        uint32_t k1 = getblock32(blocks, i * 4 + 0);
        uint32_t k2 = getblock32(blocks, i * 4 + 1);
        uint32_t k3 = getblock32(blocks, i * 4 + 2);
        uint32_t k4 = getblock32(blocks, i * 4 + 3);

        k1 *= c1;
        k1 = rotl32(k1, 15);
        k1 *= c2;
        h1 ^= k1;

        h1 = rotl32(h1, 19);
        h1 += h2;
        h1 = h1 * 5 + 0x561ccd1b;

        k2 *= c2;
        k2 = rotl32(k2, 16);
        k2 *= c3;
        h2 ^= k2;

        h2 = rotl32(h2, 17);
        h2 += h3;
        h2 = h2 * 5 + 0x0bcaa747;

        k3 *= c3;
        k3 = rotl32(k3, 17);
        k3 *= c4;
        h3 ^= k3;

        h3 = rotl32(h3, 15);
        h3 += h4;
        h3 = h3 * 5 + 0x96cd1c35;

        k4 *= c4;
        k4 = rotl32(k4, 18);
        k4 *= c1;
        h4 ^= k4;

        h4 = rotl32(h4, 13);
        h4 += h1;
        h4 = h4 * 5 + 0x32ac3b17;
    }

    //----------
    // tail

    const uint8_t* tail = (const uint8_t*)(data + nblocks * 16);

    uint32_t k1 = 0;
    uint32_t k2 = 0;
    uint32_t k3 = 0;
    uint32_t k4 = 0;

    switch (len & 15) {
    case 15:
        k4 ^= tail[14] << 16;
    case 14:
        k4 ^= tail[13] << 8;
    case 13:
        k4 ^= tail[12] << 0;
        k4 *= c4;
        k4 = rotl32(k4, 18);
        k4 *= c1;
        h4 ^= k4;

    case 12:
        k3 ^= tail[11] << 24;
    case 11:
        k3 ^= tail[10] << 16;
    case 10:
        k3 ^= tail[9] << 8;
    case 9:
        k3 ^= tail[8] << 0;
        k3 *= c3;
        k3 = rotl32(k3, 17);
        k3 *= c4;
        h3 ^= k3;

    case 8:
        k2 ^= tail[7] << 24;
    case 7:
        k2 ^= tail[6] << 16;
    case 6:
        k2 ^= tail[5] << 8;
    case 5:
        k2 ^= tail[4] << 0;
        k2 *= c2;
        k2 = rotl32(k2, 16);
        k2 *= c3;
        h2 ^= k2;

    case 4:
        k1 ^= tail[3] << 24;
    case 3:
        k1 ^= tail[2] << 16;
    case 2:
        k1 ^= tail[1] << 8;
    case 1:
        k1 ^= tail[0] << 0;
        k1 *= c1;
        k1 = rotl32(k1, 15);
        k1 *= c2;
        h1 ^= k1;
    };

    //----------
    // finalization

    h1 ^= len;
    h2 ^= len;
    h3 ^= len;
    h4 ^= len;

    h1 += h2;
    h1 += h3;
    h1 += h4;
    h2 += h1;
    h3 += h1;
    h4 += h1;

    h1 = fmix32(h1);
    h2 = fmix32(h2);
    h3 = fmix32(h3);
    h4 = fmix32(h4);

    h1 += h2;
    h1 += h3;
    h1 += h4;
    h2 += h1;
    h3 += h1;
    h4 += h1;

    ((uint32_t*)out)[0] = h1;
    ((uint32_t*)out)[1] = h2;
    ((uint32_t*)out)[2] = h3;
    ((uint32_t*)out)[3] = h4;
}

//-----------------------------------------------------------------------------

void murmurhash3_x64_128(
  const void* key, std::size_t len, void* out, uint32_t seed) {
    const uint8_t* data = (const uint8_t*)key;
    const int nblocks = len / 16;

    uint64_t h1 = seed;
    uint64_t h2 = seed;

    const uint64_t c1 = uint64_t(0x87c37b91114253d5);
    const uint64_t c2 = uint64_t(0x4cf5ad432745937f);

    //----------
    // body

    const uint64_t* blocks = (const uint64_t*)(data);

    for (int i = 0; i < nblocks; i++) {
        uint64_t k1 = getblock64(blocks, i * 2 + 0);
        uint64_t k2 = getblock64(blocks, i * 2 + 1);

        k1 *= c1;
        k1 = rotl64(k1, 31);
        k1 *= c2;
        h1 ^= k1;

        h1 = rotl64(h1, 27);
        h1 += h2;
        h1 = h1 * 5 + 0x52dce729;

        k2 *= c2;
        k2 = rotl64(k2, 33);
        k2 *= c1;
        h2 ^= k2;

        h2 = rotl64(h2, 31);
        h2 += h1;
        h2 = h2 * 5 + 0x38495ab5;
    }

    //----------
    // tail

    const uint8_t* tail = (const uint8_t*)(data + nblocks * 16);

    uint64_t k1 = 0;
    uint64_t k2 = 0;

    switch (len & 15) {
    case 15:
        k2 ^= ((uint64_t)tail[14]) << 48;
    case 14:
        k2 ^= ((uint64_t)tail[13]) << 40;
    case 13:
        k2 ^= ((uint64_t)tail[12]) << 32;
    case 12:
        k2 ^= ((uint64_t)tail[11]) << 24;
    case 11:
        k2 ^= ((uint64_t)tail[10]) << 16;
    case 10:
        k2 ^= ((uint64_t)tail[9]) << 8;
    case 9:
        k2 ^= ((uint64_t)tail[8]) << 0;
        k2 *= c2;
        k2 = rotl64(k2, 33);
        k2 *= c1;
        h2 ^= k2;

    case 8:
        k1 ^= ((uint64_t)tail[7]) << 56;
    case 7:
        k1 ^= ((uint64_t)tail[6]) << 48;
    case 6:
        k1 ^= ((uint64_t)tail[5]) << 40;
    case 5:
        k1 ^= ((uint64_t)tail[4]) << 32;
    case 4:
        k1 ^= ((uint64_t)tail[3]) << 24;
    case 3:
        k1 ^= ((uint64_t)tail[2]) << 16;
    case 2:
        k1 ^= ((uint64_t)tail[1]) << 8;
    case 1:
        k1 ^= ((uint64_t)tail[0]) << 0;
        k1 *= c1;
        k1 = rotl64(k1, 31);
        k1 *= c2;
        h1 ^= k1;
    };

    //----------
    // finalization

    h1 ^= len;
    h2 ^= len;

    h1 += h2;
    h2 += h1;

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 += h2;
    h2 += h1;

    ((uint64_t*)out)[0] = h1;
    ((uint64_t*)out)[1] = h2;
}

// https://github.com/aappleby/smhasher/blob/master/src/MurmurHash2.cpp#L37-L86
// murmur2 is under the public domain and we copied it from the murmur2 original
uint32_t murmur2(const void* key, std::size_t len, uint32_t seed) {
    // 'm' and 'r' are mixing constants generated offline.
    // They're not really 'magic', they just happen to work well.
    const uint32_t m = 0x5bd1e995;
    const int r = 24;

    // Initialize the hash to a 'random' value

    uint32_t h = seed ^ len;

    // Mix 4 bytes at a time into the hash

    const unsigned char* data = (const unsigned char*)key;

    while (len >= 4) {
        uint32_t k = *(uint32_t*)data;

        k *= m;
        k ^= k >> r;
        k *= m;

        h *= m;
        h ^= k;

        data += 4;
        len -= 4;
    }

    // Handle the last few bytes of the input array

    switch (len) {
    case 3:
        h ^= data[2] << 16;
    case 2:
        h ^= data[1] << 8;
    case 1:
        h ^= data[0];
        h *= m;
    };

    // Do a few final mixes of the hash to ensure the last few
    // bytes are well-incorporated.

    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;

    return h;
}
