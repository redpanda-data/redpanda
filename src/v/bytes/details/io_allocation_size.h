#pragma once
#include <algorithm>
#include <cstddef>

namespace details {
class io_allocation_size {
public:
    static constexpr size_t max_chunk_size = 128 * 1024;
    static constexpr size_t default_chunk_size = 512;
    size_t next_allocation_size(size_t data_size);
    size_t current_allocation_size() const;
    void reset();

private:
    size_t _next_alloc_sz{default_chunk_size};
};
//   - try to not exceed max_chunk_size
//   - must be enough for data_size
//   - uses folly::vector of 1.5 growth without using double conversions
inline size_t io_allocation_size::next_allocation_size(size_t data_size) {
    size_t next_size = ((_next_alloc_sz * 3) + 1) / 2;
    next_size = std::min(next_size, max_chunk_size);
    return _next_alloc_sz = std::max(next_size, data_size);
}
inline size_t io_allocation_size::current_allocation_size() const {
    return _next_alloc_sz;
}
inline void io_allocation_size::reset() { _next_alloc_sz = default_chunk_size; }

} // namespace details
