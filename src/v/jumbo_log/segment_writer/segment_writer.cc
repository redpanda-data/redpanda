// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "jumbo_log/segment_writer/segment_writer.h"

#include "base/likely.h"
#include "base/vassert.h"
#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "jumbo_log/segment/segment.h"
#include "jumbo_log/segment/sparse_index.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "serde/rw/rw.h"

#include <seastar/core/future.hh>

#include <stdexcept>
#include <stdint.h>
#include <utility>

namespace {
void write_ntp(iobuf& buf, const model::ntp& ntp) {
    serde::write(buf, ntp.ns);
    serde::write(buf, ntp.tp.topic);
    serde::write(buf, ntp.tp.partition);
}
} // namespace

namespace jumbo_log::segment_writer {

ss::future<> offset_tracking_output_stream::write(iobuf&& buf) {
    _offset += buf.size_bytes();
    return write_iobuf_to_output_stream(std::move(buf), *_out);
}

void iobuf_chunk_writer::write(model::ntp ntp, model::record_batch&& batch) {
    if (_buf.empty()) {
        _first = {ntp, batch.base_offset()};
        _last = {ntp, batch.last_offset()};
    }

    // Write NTP mark if we are writing the first batch or the NTP changed.
    if (_buf.empty() || _last.first != ntp) {
        if (unlikely(ntp < _last.first)) {
            throw std::runtime_error(
              "writer api invariant violation: ntps are out of order");
        }

        maybe_flush_batch_buffer();
        write_ntp(_buf, ntp);
    } else if (unlikely(_last.second >= batch.base_offset())) {
        throw std::runtime_error(
          "writer api invariant violation: offsets are out of order");
    }

    _last = {ntp, batch.last_offset()};
    serde::write(_batch_buf, std::move(batch));
}

iobuf iobuf_chunk_writer::advance() {
    maybe_flush_batch_buffer();
    return std::exchange(_buf, {});
}

void iobuf_chunk_writer::maybe_flush_batch_buffer() {
    if (_batch_buf.empty()) {
        return;
    }

    auto old = std::exchange(_batch_buf, {});

    serde::write(
      _buf,
      static_cast<jumbo_log::segment::chunk_data_batches_size_t>(
        old.size_bytes()));
    _buf.append(std::move(old));
}

size_t iobuf_chunk_writer::size_bytes() const {
    auto sz = _buf.size_bytes();
    if (_batch_buf.size_bytes() > 0) {
        sz += sizeof(jumbo_log::segment::chunk_data_batches_size_t)
              + _batch_buf.size_bytes();
    }
    return sz;
}

ss::future<> writer::write(model::ntp ntp, model::record_batch&& batch) {
    vassert(
      batch.header().type == model::record_batch_type::raft_data,
      "Only raft data expected in the jls writer");

    if (unlikely(_closed)) {
        throw std::runtime_error("writer is closed");
    }

    if (unlikely(!_did_write_header)) {
        co_await write_header();
    }

    _chunk_writer.write(ntp, std::move(batch));

    if (_chunk_writer.size_bytes() >= _opts.chunk_size_target) {
        co_await maybe_flush_chunk_writer();

        // TODO(nv): Checksum?
    }
}

ss::future<> writer::close() {
    if (unlikely(_closed)) {
        throw std::runtime_error("writer is already closed");
    }

    auto [last_ntp, last_offset] = _chunk_writer.last();
    co_await maybe_flush_chunk_writer();

    _index_builder.add_upper_limit(last_ntp, last_offset);

    co_await write_footer();
    co_await write_footer_epilogue();

    _closed = true;
}

ss::future<> writer::write_header() {
    co_await _out.write(
      jumbo_log::segment::RP2_MAGIC_SIGNATURE.data(),
      jumbo_log::segment::RP2_MAGIC_SIGNATURE.size());
    _did_write_header = true;
}

ss::future<> writer::maybe_flush_chunk_writer() {
    if (_chunk_writer.size_bytes() == 0) {
        co_return;
    }

    auto chunk_size = static_cast<jumbo_log::segment::chunk_size_t>(
      _chunk_writer.size_bytes());
    co_await _out.write(serde::to_iobuf(chunk_size));
    auto chunk_offset = _out.offset();

    auto [first_ntp, first_offset] = _chunk_writer.first();
    co_await _out.write(_chunk_writer.advance());

    _index_builder.add_entry(first_ntp, first_offset, chunk_offset, chunk_size);
}

ss::future<> writer::write_footer() {
    auto w = sparse_index_writer{};
    auto index = std::move(_index_builder).to_index();

    _sparse_index_size = co_await w(&_out, &index);
};

ss::future<> writer::write_footer_epilogue() {
    int64_t footer_size = jumbo_log::segment::RP2_MAGIC_SIGNATURE.size();
    footer_size += _sparse_index_size;
    footer_size += sizeof(footer_size);

    // TODO(nv): Entire footer size. Including metadata and sparse index.

    iobuf buf;

    auto size_before_epilogue = buf.size_bytes();
    serde::write(buf, footer_size);
    buf.append(
      jumbo_log::segment::RP2_MAGIC_SIGNATURE.data(),
      jumbo_log::segment::RP2_MAGIC_SIGNATURE.size());
    auto size_after_epilogue = buf.size_bytes();

    const auto epilogue_size = size_after_epilogue - size_before_epilogue;

    vassert(
      epilogue_size == jumbo_log::segment::FOOTER_EPILOGUE_SIZE_BYTES,
      "Footer epilogue size mismatch {} vs {}",
      epilogue_size,
      jumbo_log::segment::FOOTER_EPILOGUE_SIZE_BYTES);

    co_await _out.write(std::move(buf));
};

void sparse_index_accumulator::add_entry(
  model::ntp ntp,
  model::offset offset,
  size_t offset_bytes,
  size_t size_bytes) {
    _data.entries.push_back({
      .ntp = std::move(ntp),
      .offset = offset,
      .loc{
        .offset_bytes = offset_bytes,
        .size_bytes = size_bytes,
      },
    });
}

void sparse_index_accumulator::add_upper_limit(
  model::ntp ntp, model::offset offset) {
    _data.upper_limit.first = std::move(ntp);
    _data.upper_limit.second = offset;
}

ss::future<int64_t> sparse_index_writer::operator()(
  offset_tracking_output_stream* out, const segment::sparse_index* index) {
    iobuf buf{};

    // Write entries size so we know how many entries to read.
    serde::write(buf, static_cast<int64_t>(index->_data.entries.size()));

    // Write entries.
    for (const auto& entry : index->_data.entries) {
        write_ntp(buf, entry.ntp);
        serde::write(buf, entry.offset);
        serde::write(buf, entry.loc.offset_bytes);
        serde::write(buf, entry.loc.size_bytes);
    }

    // Write upper limit.
    write_ntp(buf, index->_data.upper_limit.first);
    serde::write(buf, index->_data.upper_limit.second);

    auto sz = buf.size_bytes();
    co_await out->write(std::move(buf));
    co_return sz;
}

} // namespace jumbo_log::segment_writer
