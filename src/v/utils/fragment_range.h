#pragma once

#include "utils/concepts-enabled.h"

// clang-format off
CONCEPT(
/// Fragmented buffer
///
/// Concept `FragmentedBuffer` is satisfied by any class that is a range of
/// fragments and provides a method `size_bytes()` which returns the total
/// size of the buffer. The interfaces accepting `FragmentedBuffer` will attempt
/// to avoid unnecessary linearization.
template<typename T>
concept bool FragmentRange = requires (T range) {
    typename T::fragment_type;
    requires std::is_convertible_v<typename T::fragment_type, bytes_view>;
    { *range.begin() } -> typename T::fragment_type;
    { *range.end() } -> typename T::fragment_type;
    { range.size_bytes() } -> size_t;
    { range.empty() } -> bool; // returns true iff size_bytes() == 0.
};
)

// clang-format on