#pragma once

#include "bytes/iobuf.h"

#include <seastar/core/sstring.hh>
#include <seastar/core/unaligned.hh>

#include <array>
#include <cstdint>
#include <vector>

struct [[gnu::packed]] pod {
    ss::unaligned<int16_t> x = 1;
    // MISSING int16_t alignment here
    ss::unaligned<int32_t> y = 2;
    ss::unaligned<int64_t> z = 3;
};
struct [[gnu::packed]] very_packed_pod {
    ss::unaligned<int16_t> x = 1;
    ss::unaligned<int8_t> y = 2;
};
struct complex_custom {
    pod pit;
    iobuf oi;
};
struct pod_with_vector {
    pod pit;
    std::vector<int> v{1, 2, 3};
};
struct pod_with_array {
    pod pit;
    std::array<int, 3> v{1, 2, 3};
};
struct kv {
    ss::sstring k;
    iobuf v;
};
struct test_rpc_header {
    int32_t size = 42;
    uint64_t checksum = 66;
    std::vector<kv> hdrs;
};
