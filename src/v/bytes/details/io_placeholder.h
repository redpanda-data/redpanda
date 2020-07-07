#pragma once

#include "bytes/details/io_fragment.h"
#include "bytes/details/out_of_range.h"
#include "utils/intrusive_list_helpers.h"

namespace details {
class io_placeholder {
public:
    using iterator
      = uncounted_intrusive_list<io_fragment, &io_fragment::hook>::iterator;

    io_placeholder() noexcept = default;

    io_placeholder(
      iterator iter, size_t initial_index, size_t max_size_to_write)
      : _iter(iter)
      , _byte_index(initial_index)
      , _remaining_size(max_size_to_write) {}

    [[gnu::always_inline]] void write(const char* src, size_t len) {
        details::check_out_of_range(len, _remaining_size);
        std::copy_n(src, len, mutable_index());
        _remaining_size -= len;
        _byte_index += len;
    }

    size_t remaining_size() const { return _remaining_size; }

    // the first byte of the _current_ iterator + offset
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    const char* index() const { return _iter->get() + _byte_index; }

    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    char* mutable_index() { return _iter->get_write() + _byte_index; }

private:
    iterator _iter;
    size_t _byte_index{0};
    size_t _remaining_size{0};
};

} // namespace details
