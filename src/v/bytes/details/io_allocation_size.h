#pragma once
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
namespace details {
class io_allocation_size {
public:
    static constexpr size_t max_chunk_size = 128 * 1024;
    static constexpr size_t default_chunk_size = 512;

public:
    // >>> x=512
    // >>> while x < int((1024*128)):
    // ...     print(x)
    // ...     x=int(((x*3)+1)/2)
    // ...     x=int(min(1024*128,x))
    // print(1024*128)
    static constexpr std::array<uint32_t, 15> alloc_table =
      // computed from a python script above
      {{512,
        768,
        1152,
        1728,
        2592,
        3888,
        5832,
        8748,
        13122,
        19683,
        29525,
        44288,
        66432,
        99648,
        131072}};
    static size_t next_allocation_size(size_t data_size);
};
//   - try to not exceed max_chunk_size
//   - must be enough for data_size
//   - uses folly::vector of 1.5 growth without using double conversions
inline size_t io_allocation_size::next_allocation_size(size_t data_size) {
    // size_t next_size = ((_next_alloc_sz * 3) + 1) / 2;
    if (data_size > alloc_table.back()) {
        return alloc_table.back();
    }
    for (auto x : alloc_table) {
        // should be <, because we want forward progress in the allocator
        // chunks. so if you call the allocator with 512, you'll get 768, and if
        // you call the allocator with 768, you'll get 1152, and so on.
        if (data_size < x) {
            return x;
        }
    }
    return alloc_table.back();
}

} // namespace details
