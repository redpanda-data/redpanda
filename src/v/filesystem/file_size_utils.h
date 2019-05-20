#pragma once
#include <seastar/core/align.hh>

#include <cstdint>

/// \brief given a page alighment and an offset within the bounds of a page
/// returns the bottom offset of the page
inline int32_t offset_to_page(int64_t offset, int64_t page_alignment) {
    return static_cast<int32_t>(
      seastar::align_down(offset, page_alignment)
      >> __builtin_ctz(page_alignment));
}
