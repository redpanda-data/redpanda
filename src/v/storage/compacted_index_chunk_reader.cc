// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/compacted_index_chunk_reader.h"

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "hashing/crc32c.h"
#include "reflection/adl.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_reader.h"
#include "storage/logger.h"
#include "utils/to_string.h"

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
  segment_full_path path,
  ss::file in,
  ss::io_priority_class pc,
  size_t max_chunk_memory,
  ss::abort_source* as) noexcept
  : compacted_index_reader::impl(std::move(path))
  , _handle(std::move(in))
  , _iopc(pc)
  , _max_chunk_memory(max_chunk_memory)
  , _as(as) {}

ss::future<> compacted_index_chunk_reader::close() { return _handle.close(); }

ss::future<> compacted_index_chunk_reader::verify_integrity() {
    reset();
    return load_footer().then([this](compacted_index::footer) {
        // NOTE: these are *different* options from other methods in this class
        ss::file_input_stream_options options;
        options.buffer_size = 4096;
        options.io_priority_class = _iopc;
        options.read_ahead = 1;
        return ss::do_with(
                 int64_t(_footer->size),
                 crc::crc32c{},
                 ss::make_file_input_stream(
                   _handle, 0, _data_size.value(), std::move(options)),
                 [](
                   int64_t& max_bytes,
                   crc::crc32c& crc,
                   ss::input_stream<char>& in) {
                     return ss::do_until(
                              [&in, &max_bytes] {
                                  // stop condition
                                  return in.eof() || max_bytes <= 0;
                              },
                              [&crc, &in, &max_bytes] {
                                  return in.read().then(
                                    [&crc, &max_bytes](
                                      ss::temporary_buffer<char> buf) {
                                        if (buf.empty()) {
                                            max_bytes = 0;
                                            return;
                                        }
                                        max_bytes -= buf.size();
                                        crc.extend(buf.get(), buf.size());
                                    });
                              })
                       .then([&in, &crc] {
                           auto ret = crc.value();
                           return in.close().then([ret] { return ret; });
                       });
                 })
          .then([this](uint32_t crcsum) {
              if (_footer->crc != crcsum) {
                  return ss::make_exception_future<>(
                    std::runtime_error(fmt::format(
                      "Invalid file checksum. Expected: {}, but got:{} - {}",
                      _footer->crc,
                      crcsum,
                      *this)));
              }
              return ss::now();
          });
    });
}

bool compacted_index_chunk_reader::is_footer_loaded() const {
    return bool(_footer);
}

ss::future<compacted_index::footer>
compacted_index_chunk_reader::load_footer() {
    if (is_footer_loaded()) {
        co_return _footer.value();
    }

    auto stat = co_await _handle.stat();
    size_t file_size = stat.st_size;

    if (file_size < compacted_index::footer_v1::footer_size) {
        throw std::runtime_error(fmt::format(
          "Cannot read footer: file {} size is too small ({} bytes)",
          path(),
          file_size));
    }

    size_t footer_buf_size = std::min(
      file_size, compacted_index::footer::footer_size);

    ss::file_input_stream_options options;
    options.buffer_size = 4096;
    options.io_priority_class = _iopc;
    options.read_ahead = 0;
    auto in = ss::make_file_input_stream(
      _handle,
      file_size - footer_buf_size,
      footer_buf_size,
      std::move(options));
    iobuf buf = co_await read_iobuf_exactly(in, footer_buf_size);

    if (buf.size_bytes() != footer_buf_size) {
        throw std::runtime_error(fmt::format(
          "could not read enough bytes to parse footer. read:{}, expected:{}",
          buf.size_bytes(),
          footer_buf_size));
    }

    storage::compacted_index::footer footer;
    size_t data_size = 0;

    // To avoid rebuilding indices with good v1 footers, we first try parsing
    // the index suffix as v1 footer. If the version matches, use the parsed
    // values. Otherwise re-parse as v2 footer.
    iobuf_parser parser_v1(buf.share(
      footer_buf_size - storage::compacted_index::footer_v1::footer_size,
      storage::compacted_index::footer_v1::footer_size));
    auto footer_v1
      = reflection::adl<storage::compacted_index::footer_v1>{}.from(parser_v1);
    if (footer_v1.version == 1) {
        footer.size = footer_v1.size;
        footer.keys = footer_v1.keys;
        footer.size_deprecated = footer_v1.size;
        footer.keys_deprecated = footer_v1.keys;
        footer.flags = footer_v1.flags;
        footer.crc = footer_v1.crc;
        footer.version = footer_v1.version;

        data_size = file_size - compacted_index::footer_v1::footer_size;
    } else if (footer_v1.version == compacted_index::footer::current_version) {
        iobuf_parser parser(std::move(buf));
        footer = reflection::adl<storage::compacted_index::footer>{}.from(
          parser);

        data_size = file_size - compacted_index::footer::footer_size;
    } else {
        throw compacted_index::needs_rebuild_error{
          fmt::format("incompatible index version: {}", footer_v1.version)};
    }

    _footer = footer;
    _data_size = data_size;
    co_return _footer.value();
}

void compacted_index_chunk_reader::print(std::ostream& o) const { o << *this; }

bool compacted_index_chunk_reader::is_end_of_stream() const {
    return _end_of_stream || (_footer && _byte_index == _footer->size)
           || (_cursor && _cursor->eof());
}

void compacted_index_chunk_reader::reset() {
    _end_of_stream = false;
    _byte_index = 0;
    _data_size = std::nullopt;
    _footer = std::nullopt;
    _cursor = std::nullopt;
}

ss::future<ss::circular_buffer<compacted_index::entry>>
compacted_index_chunk_reader::load_slice(model::timeout_clock::time_point t) {
    using ret_t = ss::circular_buffer<compacted_index::entry>;
    if (unlikely(!is_footer_loaded())) {
        return load_footer().then(
          [this, t](compacted_index::footer) { return load_slice(t); });
    }
    if (!_cursor) {
        _cursor = ss::make_file_input_stream(_handle, 0, _footer->size);
    }
    if (_as) {
        _as->check();
    }

    return ss::do_with(ret_t{}, [this](ret_t& slice) {
        return ss::do_until(
                 [&slice, this] {
                     // stop condition
                     constexpr auto entry_size = sizeof(compacted_index::entry);
                     const auto mem_use = slice.size() * entry_size;
                     const auto next_mem_use = slice.size() == slice.capacity()
                                                 ? mem_use * 2
                                                 : mem_use + entry_size;

                     return is_end_of_stream()
                            || next_mem_use > _max_chunk_memory;
                 },
                 [&slice, this] {
                     return ::read_iobuf_exactly(*_cursor, sizeof(uint16_t))
                       .then([this](iobuf b) {
                           _byte_index += b.size_bytes();
                           iobuf_parser p(std::move(b));
                           const size_t entry_size
                             = reflection::adl<uint16_t>{}.from(p);
                           return ::read_iobuf_exactly(*_cursor, entry_size);
                       })
                       .then([this, &slice](iobuf b) {
                           _byte_index += b.size_bytes();
                           iobuf_parser p(std::move(b));
                           auto type = reflection::adl<uint8_t>{}.from(p);
                           auto [offset, _1] = p.read_varlong();
                           auto [delta, _2] = p.read_varlong();
                           auto bytes = p.read_bytes(p.bytes_left());
                           auto key = compaction_key(std::move(bytes));
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
      "_data_size:{}, _footer:{}, active_cursor:{}, end_of_stream:{}, "
      "_byte_index:{}}}",
      r._max_chunk_memory,
      r._data_size,
      r._footer,
      (r._cursor ? "yes" : "no"),
      r.is_end_of_stream(),
      r._byte_index);
    return o;
}

} // namespace storage::internal

namespace storage {
compacted_index_reader make_file_backed_compacted_reader(
  segment_full_path path,
  ss::file f,
  ss::io_priority_class iopc,
  size_t step_chunk,
  ss::abort_source* as) {
    return compacted_index_reader(
      ss::make_shared<internal::compacted_index_chunk_reader>(
        std::move(path), std::move(f), iopc, step_chunk, as));
}

} // namespace storage
