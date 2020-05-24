#pragma once

#include "storage/compacted_index_reader.h"

#include <seastar/core/file.hh>
#include <seastar/core/iostream.hh>

#include <optional>
namespace storage::internal {
using namespace storage; // NOLINT

/// \brief this class loads up slices in chunks tracking memory usage
///
class compacted_index_chunk_reader final : public compacted_index_reader::impl {
public:
    compacted_index_chunk_reader(
      ss::sstring name,
      ss::file,
      ss::io_priority_class,
      size_t max_chunk_memory) noexcept;

    ss::future<> close() final;

    ss::future<compacted_index::footer> load_footer() final;

    void print(std::ostream&) const final;

    bool is_end_of_stream() const final;

    ss::future<ss::circular_buffer<compacted_index::entry>>
      load_slice(model::timeout_clock::time_point) final;

private:
    ss::file _handle;
    ss::io_priority_class _iopc;
    size_t _max_chunk_memory{0};
    size_t _byte_index{0};
    std::optional<size_t> _file_size;
    compacted_index::footer _footer;
    bool _end_of_stream{false};
    std::optional<ss::input_stream<char>> _cursor;

    friend std::ostream&
    operator<<(std::ostream&, const compacted_index_chunk_reader&);
};

} // namespace storage::internal
