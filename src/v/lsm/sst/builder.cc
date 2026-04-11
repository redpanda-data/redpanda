// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/sst/builder.h"

#include "hashing/crc32c.h"
#include "lsm/sst/footer.h"

#include <seastar/core/fstream.hh>

namespace lsm::sst {

builder::builder(std::unique_ptr<io::sequential_file_writer> w, options opts)
  : _writer(std::move(w))
  , _opts(opts) {
    if (_opts.filter_period > 0) {
        _filter.emplace(
          block::filter_builder::options{
            .filter_period = _opts.filter_period,
          });
    }
}

ss::future<> builder::add(internal::key key, iobuf value) {
    if (_pending_index_entry) {
        // TODO(lsm): We can compute shorter block boundaries for our index
        // here. For example: consider a block that ends with "the quick brown
        // fox" and the next block starts with "the who". In this case, we can
        // encode the index entry with "the r" because it's >= everything in the
        // previous block and < all entries in the next block.
        _index_block.add(_last_key, _pending_handle.as_iobuf());
        _pending_index_entry = false;
    }
    if (_filter) {
        _filter->add_key(key);
    }
    // TODO(lsm): It's a bummer we make so many copies of the key here
    // we only need it when we get to a block boundary (or at the end of the
    // stream). It might be better to only set the last key when we're about
    // to flush, otherwise we could decode the last key in the last _data_block
    // as well. For now, just copy leveldb and do the simple copy.
    _last_key = key;
    _data_block.add(std::move(key), std::move(value));
    ++_added_entries;
    if (_data_block.current_size_estimate() > _opts.block_size) {
        co_await flush();
    }
}

ss::future<> builder::flush() {
    if (_data_block.empty()) {
        co_return;
    }
    auto buf = _data_block.finish();
    _pending_handle = co_await write_raw_block(
      std::move(buf), _opts.compression);
    _pending_index_entry = true;
    if (_filter) {
        _filter->start_block(_written_bytes);
    }
}

ss::future<block::handle>
builder::write_raw_block(iobuf buf, compression_type comp_type) {
    if (comp_type != compression_type::none) {
        // TODO(lsm): only use compressed version if it actually saves enough
        // bytes.
        buf = co_await compress(std::move(buf), comp_type);
    }
    block::handle h = {.offset = _written_bytes, .size = buf.size_bytes()};
    // File format contains a sequence of blocks where each block has:
    //    block_data: uint8[n]
    //    type: uint8
    //    crc: uint32
    // Make sure the CRC covers the type
    buf.append(std::to_array({std::to_underlying(comp_type)}));
    crc::crc32c crc;
    crc_extend_iobuf(crc, buf);
    buf.append(
      std::bit_cast<std::array<uint8_t, sizeof(crc.value())>>(crc.value()));
    _written_bytes += buf.size_bytes();
    co_await _writer->append(std::move(buf));
    co_return h;
}

ss::future<> builder::finish() {
    co_await flush();

    block::handle filter_block_handle, metaindex_block_handle,
      index_block_handle;

    if (_filter) {
        filter_block_handle = co_await write_raw_block(
          _filter->finish(), compression_type::none);
    }

    // write metaindex block
    // NOTE: if we add more keys here they need to be added lexicographically
    block::builder meta_index_block;
    if (_filter) {
        auto key = internal::key::encode(
          {.key = lsm::user_key_view("filter.RedpandaBloomV0")});
        meta_index_block.add(std::move(key), filter_block_handle.as_iobuf());
    }
    metaindex_block_handle = co_await write_raw_block(
      meta_index_block.finish(), _opts.compression);

    if (_pending_index_entry) {
        // TODO(lsm): See the TODO in builder::add
        _index_block.add(_last_key, _pending_handle.as_iobuf());
        _pending_index_entry = false;
    }
    index_block_handle = co_await write_raw_block(
      _index_block.finish(), compression_type::none);

    // write footer
    footer foot{
      .metaindex_handle = metaindex_block_handle,
      .index_handle = index_block_handle,
    };
    iobuf encoded_footer = foot.as_iobuf();
    _written_bytes += encoded_footer.size_bytes();
    co_await _writer->append(std::move(encoded_footer));
}

size_t builder::num_entries() const { return _added_entries; }

size_t builder::file_size() const { return _written_bytes; }

ss::future<> builder::close() { return _writer->close(); }

} // namespace lsm::sst
