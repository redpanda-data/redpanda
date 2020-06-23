#include "storage/compacted_index_chunk_reader.h"

#include "bytes/iobuf.h"
#include "reflection/adl.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_reader.h"
#include "storage/logger.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>

#include <fmt/core.h>
#include <sys/stat.h>

#include <stdexcept>

namespace storage::internal {

compacted_index_chunk_reader::compacted_index_chunk_reader(
  ss::sstring name,
  ss::file in,
  ss::io_priority_class pc,
  size_t max_chunk_memory) noexcept
  : compacted_index_reader::impl(std::move(name))
  , _handle(std::move(in))
  , _iopc(pc)
  , _max_chunk_memory(max_chunk_memory) {}

ss::future<> compacted_index_chunk_reader::close() { return _handle.close(); }

static inline ss::future<compacted_index::footer>
footer_from_stream(ss::input_stream<char>& in) {
    return in.read_exactly(compacted_index::footer_size)
      .then([](ss::temporary_buffer<char> tmp) {
          if (tmp.size() != compacted_index::footer_size) {
              return ss::make_exception_future<compacted_index::footer>(
                std::runtime_error(fmt::format(
                  "could not read enough bytes to parse "
                  "footer. read:{}, expected:{}",
                  tmp.size(),
                  compacted_index::footer_size)));
          }
          iobuf b;
          b.append(std::move(tmp));
          iobuf_parser parser(std::move(b));
          auto footer
            = reflection::adl<storage::compacted_index::footer>{}.from(parser);
          return ss::make_ready_future<compacted_index::footer>(footer);
      });
}

ss::future<compacted_index::footer>
compacted_index_chunk_reader::load_footer() {
    if (_footer.crc != 0 && _file_size) {
        return ss::make_ready_future<compacted_index::footer>(_footer);
    }
    auto f = ss::now();
    if (!_file_size) {
        f = _handle.stat().then(
          [this](struct stat s) { _file_size = s.st_size; });
    }
    return f.then([this] {
        if (
          !_file_size || _file_size == 0
          || _file_size < compacted_index::footer_size) {
            return ss::make_exception_future<compacted_index::footer>(
              std::runtime_error(fmt::format(
                "Cannot read footer from empty file: {}", filename())));
        }
        ss::file_input_stream_options options;
        options.buffer_size = 4096;
        options.io_priority_class = _iopc;
        options.read_ahead = 0;
        return ss::do_with(
                 ss::make_file_input_stream(
                   _handle,
                   _file_size.value() - compacted_index::footer_size,
                   compacted_index::footer_size,
                   std::move(options)),
                 [](ss::input_stream<char>& in) {
                     return footer_from_stream(in);
                 })
          .then([this](compacted_index::footer f) { return _footer = f; });
    });
}

void compacted_index_chunk_reader::print(std::ostream& o) const { o << *this; }

bool compacted_index_chunk_reader::is_end_of_stream() const {
    return _end_of_stream || _byte_index == _footer.size
           || (_cursor && _cursor->eof());
}

ss::future<ss::circular_buffer<compacted_index::entry>>
compacted_index_chunk_reader::load_slice(model::timeout_clock::time_point) {
    using ret_t = ss::circular_buffer<compacted_index::entry>;
    if (unlikely(!_file_size)) {
        return ss::make_exception_future<ret_t>(
          std::runtime_error("Must call load_footer(), before loading slice to "
                             "verify file size correctness."));
    }
    if (!_cursor) {
        _cursor = ss::make_file_input_stream(_handle, 0, _footer.size);
    }

    return ss::do_with(
      ret_t{}, size_t(0), [this](ret_t& slice, size_t& mem_use) {
          return ss::do_until(
                   [&mem_use, this] {
                       // stop condition
                       return is_end_of_stream()
                              || mem_use >= _max_chunk_memory;
                   },
                   [&mem_use, &slice, this] {
                       return ::read_iobuf_exactly(*_cursor, sizeof(uint16_t))
                         .then([&mem_use, this](iobuf b) {
                             _byte_index += b.size_bytes();
                             iobuf_parser p(std::move(b));
                             const size_t entry_size
                               = reflection::adl<uint16_t>{}.from(p);
                             mem_use += entry_size;
                             return ::read_iobuf_exactly(*_cursor, entry_size);
                         })
                         .then([this, &slice](iobuf b) {
                             _byte_index += b.size_bytes();
                             iobuf_parser p(std::move(b));
                             auto type = reflection::adl<uint8_t>{}.from(p);
                             auto [offset, _1] = p.read_varlong();
                             auto [delta, _2] = p.read_varlong();
                             auto key = p.read_bytes(p.bytes_left());
                             slice.push_back(compacted_index::entry(
                               compacted_index::entry_type(type),
                               std::move(key),
                               model::offset(offset),
                               delta));
                         });
                   })
            .then([&slice] {
                return ss::make_ready_future<ret_t>(std::move(slice));
            });
      });
}

std::ostream&
operator<<(std::ostream& o, const compacted_index_chunk_reader& r) {
    fmt::print(
      o,
      "{{type:compacted_index_chunk_reader, _max_chunk_memory:{}, "
      "_file_size:{}, _footer:{}, active_cursor:{}, end_of_stream:{}, "
      "_byte_index:{}}}",
      r._max_chunk_memory,
      r._file_size.value(),
      r._footer,
      (r._cursor ? "yes" : "no"),
      r.is_end_of_stream(),
      r._byte_index);
    return o;
}

} // namespace storage::internal

namespace storage {
compacted_index_reader make_file_backed_compacted_reader(
  ss::sstring filename,
  ss::file f,
  ss::io_priority_class iopc,
  size_t step_chunk) {
    return compacted_index_reader(
      ss::make_shared<internal::compacted_index_chunk_reader>(
        std::move(filename), std::move(f), iopc, step_chunk));
}

} // namespace storage
