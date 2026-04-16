// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/sst/reader.h"

#include "hashing/crc32c.h"
#include "lsm/block/contents.h"
#include "lsm/block/filter.h"
#include "lsm/block/handle.h"
#include "lsm/block/reader.h"
#include "lsm/core/compression.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/two_level_iterator.h"
#include "lsm/io/persistence.h"
#include "lsm/io/readahead_file_reader.h"
#include "lsm/sst/footer.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>

#include <sys/uio.h>

#include <exception>

namespace lsm::sst {

namespace {

ss::future<ss::lw_shared_ptr<block::contents>>
read_block(io::random_access_file_reader* file, block::handle handle) {
    // File format contains a sequence of blocks where each block has:
    //    block_data: uint8[n]
    //    type: uint8
    //    crc: uint32
    static constexpr size_t block_footer_size
      = sizeof(compression_type) + sizeof(crc::crc32c::value_type);
    auto data = co_await file->read(
      handle.offset, handle.size + block_footer_size);
    auto compression = compression_type_from_raw(
      data[data.size() - block_footer_size]);
    uint32_t expected_crc = data.read_fixed32(
      data.size() - block_footer_size + sizeof(compression_type));
    crc::crc32c actual_crc;
    data.trim_back(sizeof(crc::crc32c::value_type));
    for (auto& chunk : data.as_iovec()) {
        actual_crc.extend(static_cast<char*>(chunk.iov_base), chunk.iov_len);
    }
    if (expected_crc != actual_crc.value()) {
        throw corruption_exception(
          "unexpected crc, got: {}, want: {} for handle {} and file {}",
          actual_crc.value(),
          expected_crc,
          handle,
          fmt::streamed(*file));
    }
    data.trim_back(sizeof(compression_type));
    if (compression != compression_type::none) {
        data = co_await uncompress(std::move(data), compression);
    }
    co_return ss::make_lw_shared<block::contents>(std::move(data));
}

ss::future<std::optional<block::filter_reader>> read_filter(
  io::random_access_file_reader* file, block::reader metaindex_block) {
    auto iter = metaindex_block.create_iterator();
    auto key = internal::key::encode(
      {.key = lsm::user_key_view("filter.RedpandaBloomV0")});
    co_await iter->seek(key);
    if (!iter->valid() || iter->key() != key) {
        co_return std::nullopt;
    }
    auto filter_handle = block::handle::from_iobuf(iter->value());
    auto filter_contents = co_await read_block(file, filter_handle);
    co_return block::filter_reader(std::move(filter_contents));
}

} // namespace

class reader::impl {
public:
    impl(
      internal::file_id id,
      block::reader index_block,
      std::unique_ptr<io::random_access_file_reader> file,
      size_t file_size,
      std::optional<block::filter_reader> filter,
      ss::lw_shared_ptr<block_cache> cache)
      : _id(id)
      , _file(std::move(file))
      , _file_size(file_size)
      , _index_block(std::move(index_block))
      , _filter(std::move(filter))
      , _cache(std::move(cache)) {}

    std::unique_ptr<internal::iterator>
    create_iterator(internal::iterator_options opts) {
        if (opts.readahead_size == 0) {
            return internal::create_two_level_iterator(
              _index_block.create_iterator(), [this](iobuf index_value) {
                  return block_reader(std::move(index_value), _file.get());
              });
        }
        auto ra = std::make_shared<io::readahead_file_reader>(
          _file.get(), _file_size, opts.readahead_size);
        return internal::create_two_level_iterator(
          _index_block.create_iterator(),
          [this, ra = std::move(ra)](iobuf index_value) {
              return block_reader(std::move(index_value), ra.get());
          });
    }

    ss::future<> internal_get(
      internal::key_view key,
      absl::FunctionRef<ss::future<>(internal::key_view, iobuf)> fn) {
        auto iiter = _index_block.create_iterator();
        co_await iiter->seek(key);
        if (!iiter->valid()) {
            co_return;
        }
        auto v = iiter->value();
        if (_filter) {
            auto handle = block::handle::from_iobuf(v.share());
            if (!_filter->key_may_match(handle.offset, key)) {
                // Bloom filter says it's certainly not there.
                co_return;
            }
        }
        auto block_iter = co_await block_reader(std::move(v), _file.get());
        co_await block_iter->seek(key);
        if (block_iter->valid()) {
            co_await fn(block_iter->key(), block_iter->value());
        }
    }

    ss::future<> close() { return _file->close(); }

private:
    ss::future<std::unique_ptr<internal::iterator>>
    block_reader(iobuf index_value, io::random_access_file_reader* file) {
        auto block_handle = block::handle::from_iobuf(std::move(index_value));
        auto cache_handle = co_await _cache->get(_id, block_handle);
        if (auto reader = cache_handle.get()) {
            co_return reader->create_iterator();
        }
        auto contents = co_await read_block(file, block_handle);
        auto rdr = block::reader(std::move(contents));
        auto it = rdr.create_iterator();
        cache_handle.insert(std::move(rdr));
        co_return it;
    }

    internal::file_id _id;
    std::unique_ptr<io::random_access_file_reader> _file;
    size_t _file_size;
    block::reader _index_block;
    std::optional<block::filter_reader> _filter;
    ss::lw_shared_ptr<block_cache> _cache;
};

reader::reader(std::unique_ptr<impl> impl)
  : _impl(std::move(impl)) {}

reader::~reader() = default;
reader::reader(reader&&) noexcept = default;
reader& reader::operator=(reader&&) noexcept = default;

ss::future<reader> reader::open(
  std::unique_ptr<io::random_access_file_reader> file,
  internal::file_id id,
  size_t file_size,
  ss::lw_shared_ptr<block_cache> block_cache) {
    std::exception_ptr ep;
    try {
        if (file_size < footer::encoded_length) {
            throw corruption_exception(
              "corruption: file is too short to be an sstable");
        }
        auto encoded_footer = co_await file->read(
          file_size - footer::encoded_length, footer::encoded_length);
        auto footer = footer::from_iobuf(encoded_footer.as_iobuf());
        auto index_block_contents = co_await read_block(
          file.get(), footer.index_handle);
        auto metaindex_block_contents = co_await read_block(
          file.get(), footer.metaindex_handle);
        block::reader index_block(std::move(index_block_contents));
        block::reader metaindex_block(std::move(metaindex_block_contents));
        auto filter = co_await read_filter(file.get(), metaindex_block);
        co_return reader(
          std::make_unique<impl>(
            id,
            std::move(index_block),
            std::move(file),
            file_size,
            std::move(filter),
            std::move(block_cache)));
    } catch (...) {
        ep = std::current_exception();
    }
    if (file) {
        co_await file->close();
    }
    std::rethrow_exception(ep);
}

std::unique_ptr<internal::iterator>
reader::create_iterator(internal::iterator_options opts) {
    return _impl->create_iterator(opts);
}

ss::future<> reader::internal_get(
  internal::key_view key,
  absl::FunctionRef<ss::future<>(internal::key_view, iobuf)> fn) {
    return _impl->internal_get(key, fn);
}

ss::future<> reader::close() { return _impl->close(); }

} // namespace lsm::sst
