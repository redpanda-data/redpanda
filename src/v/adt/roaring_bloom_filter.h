#pragma once
#include <array>
#include <cmath>
#include <cstring>

#include <roaring/roaring.hh>
#include <smf/macros.h>

#include "hashing/murmur.h"


constexpr static const std::size_t kDefaultHashingLevels = 2;

template <typename T, std::size_t Levels>
struct bloom_hasher {
  std::array<uint32_t, Levels> operator()(T t);
};

template <>
struct bloom_hasher<const char *, kDefaultHashingLevels> {
  std::array<uint32_t, kDefaultHashingLevels>
  operator()(const char *data) const {
    std::array<uint32_t, 4> arr{};
    murmurhash3_x64_128(data, std::strlen(data), arr.data());
    std::array<uint32_t, kDefaultHashingLevels> retval{};
    std::memcpy(retval.data(), arr.data(), kDefaultHashingLevels);
    return retval;
  }
};

template <typename T = const char *, std::size_t Levels = kDefaultHashingLevels,
          typename Hasher = bloom_hasher<T, Levels>>
class roaring_bloom_filter {
 public:
  roaring_bloom_filter() {}
  ~roaring_bloom_filter() = default;

  __attribute__((optimize("unroll-loops"))) bool
  contains(T t) const {
    auto arr = Hasher()(t);
    for (auto i = 0; i < Levels; ++i) {
      if (bmap_.contains(arr[i])) return true;
    }
    return false;
  }

  __attribute__((optimize("unroll-loops"))) void
  add(T t) {
    auto arr = Hasher()(t);
    for (auto i = 0; i < Levels; ++i) {
      bmap_.add(arr[i]);
    }
    ++elems_;
  }

  double
  positive_error_rate() const {
    // p ~= (1 - e^(-kn/m))^k
    //

    // k = hashing function levels
    // n = items in bloom filter
    // m = size of filter

    // Roaring bitmap adjusts up to the max of int32_t.
    // To compute the cardinality of the set is to give an
    // incorrect probably of positive error rate.
    //
    // const double m = bmap_.cardinality();

    const double m = std::numeric_limits<int32_t>::max();
    const double k = double(Levels);
    const double n = double(elems_);
    const double e_x = std::exp((-k * n) / m);
    const double rate = std::pow(1 - e_x, k);
    return rate;
  }

  inline uint32_t
  size_in_bytes() const {
    return bmap_.getSizeInBytes();
  }

  SMF_DISALLOW_COPY_AND_ASSIGN(roaring_bloom_filter);

 private:
  Roaring bmap_;
  uint32_t elems_{0};
};

