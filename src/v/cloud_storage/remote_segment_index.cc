/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/remote_segment_index.h"

#include "model/record_batch_types.h"
#include "serde/envelope.h"
#include "serde/serde.h"
#include "vlog.h"

namespace cloud_storage {

offset_index::offset_index(
  model::offset initial_rp,
  kafka::offset initial_kaf,
  int64_t initial_file_pos,
  int64_t file_pos_step)
  : _rp_offsets{}
  , _kaf_offsets{}
  , _file_offsets{}
  , _pos{}
  , _initial_rp(initial_rp)
  , _initial_kaf(initial_kaf)
  , _initial_file_pos(initial_file_pos)
  , _rp_index(initial_rp)
  , _kaf_index(initial_kaf)
  , _file_index(initial_file_pos, delta_delta_t(file_pos_step))
  , _min_file_pos_step(file_pos_step) {}

void offset_index::add(
  model::offset rp_offset, kafka::offset kaf_offset, int64_t file_offset) {
    auto ix = index_mask & _pos++;
    _rp_offsets.at(ix) = rp_offset();
    _kaf_offsets.at(ix) = kaf_offset();
    _file_offsets.at(ix) = file_offset;
    try {
        if ((_pos & index_mask) == 0) {
            _rp_index.add(_rp_offsets);
            _kaf_index.add(_kaf_offsets);
            _file_index.add(_file_offsets);
        }
    } catch (...) {
        // Get rid of the corrupted state in the encoders.
        // If the exception is thrown out of 'add' method
        // the invariant of the index is broken.
        _pos = 0;
        _rp_offsets = {};
        _kaf_offsets = {};
        _file_offsets = {};
        _rp_index = encoder_t(_initial_rp);
        _kaf_index = encoder_t(_initial_kaf);
        _file_index = foffset_encoder_t(
          _initial_file_pos, delta_delta_t(_min_file_pos_step));
        throw;
    }
}

std::
  variant<std::monostate, offset_index::index_value, offset_index::find_result>
  offset_index::maybe_find_offset(
    int64_t upper_bound,
    deltafor_encoder<int64_t>& encoder,
    const std::array<int64_t, buffer_depth>& write_buffer) {
    deltafor_decoder<int64_t> decoder(
      encoder.get_initial_value(), encoder.get_row_count(), encoder.share());
    auto max_index = encoder.get_row_count() * details::FOR_buffer_depth - 1;
    auto maybe_ix = _find_under(std::move(decoder), upper_bound);
    if (!maybe_ix || maybe_ix->ix == max_index) {
        auto ixend = _pos & index_mask;
        std::optional<find_result> candidate;
        for (size_t i = 0; i < ixend; i++) {
            if (write_buffer.at(i) < upper_bound) {
                candidate = find_result{
                  .rp_offset = model::offset(_rp_offsets.at(i)),
                  .kaf_offset = kafka::offset(_kaf_offsets.at(i)),
                  .file_pos = _file_offsets.at(i),
                };
            } else {
                break;
            }
        }
        // maybe_ix can point to the last element of the compressed
        // chunk if all elements inside it are less than the offset that
        // we're looking for. In this case we should use it even if
        // we can't find anything inside the buffer.
        // maybe_ix will be null if the compressed chunk is empty.
        if (candidate) {
            return *candidate;
        }
        if (!maybe_ix) {
            return std::monostate();
        }
    }
    // Invariant: maybe_ix here can't be nullopt
    return *maybe_ix;
}

std::optional<offset_index::find_result>
offset_index::find_rp_offset(model::offset upper_bound) {
    size_t ix = 0;
    find_result res{};

    auto search_result = maybe_find_offset(upper_bound, _rp_index, _rp_offsets);

    if (std::holds_alternative<std::monostate>(search_result)) {
        return std::nullopt;
    } else if (std::holds_alternative<find_result>(search_result)) {
        return std::get<find_result>(search_result);
    }
    auto maybe_ix = std::get<index_value>(search_result);

    // Invariant: maybe_ix here can't be nullopt
    ix = maybe_ix.ix;
    res.rp_offset = model::offset(maybe_ix.value);

    decoder_t kaf_dec(
      _kaf_index.get_initial_value(),
      _kaf_index.get_row_count(),
      _kaf_index.copy());
    auto kaf_offset = _fetch_ix(std::move(kaf_dec), ix);
    vassert(kaf_offset.has_value(), "Inconsistent index state");
    res.kaf_offset = kafka::offset(*kaf_offset);
    foffset_decoder_t file_dec(
      _file_index.get_initial_value(),
      _file_index.get_row_count(),
      _file_index.copy(),
      delta_delta_t(_min_file_pos_step));
    auto file_pos = _fetch_ix(std::move(file_dec), ix);
    res.file_pos = *file_pos;
    return res;
}

std::optional<offset_index::find_result>
offset_index::find_kaf_offset(kafka::offset upper_bound) {
    size_t ix = 0;
    find_result res{};

    auto search_result = maybe_find_offset(
      upper_bound, _kaf_index, _kaf_offsets);

    if (std::holds_alternative<std::monostate>(search_result)) {
        return std::nullopt;
    } else if (std::holds_alternative<find_result>(search_result)) {
        return std::get<find_result>(search_result);
    }
    auto maybe_ix = std::get<index_value>(search_result);

    // Invariant: maybe_ix here can't be nullopt
    ix = maybe_ix.ix;
    res.kaf_offset = kafka::offset(maybe_ix.value);

    decoder_t rp_dec(
      _rp_index.get_initial_value(),
      _rp_index.get_row_count(),
      _rp_index.copy());
    auto rp_offset = _fetch_ix(std::move(rp_dec), ix);
    vassert(rp_offset.has_value(), "Inconsistent index state");
    res.rp_offset = model::offset(*rp_offset);
    foffset_decoder_t file_dec(
      _file_index.get_initial_value(),
      _file_index.get_row_count(),
      _file_index.copy(),
      delta_delta_t(_min_file_pos_step));
    auto file_pos = _fetch_ix(std::move(file_dec), ix);
    res.file_pos = *file_pos;
    return res;
}

struct offset_index_header
  : serde::envelope<
      offset_index_header,
      serde::version<1>,
      serde::compat_version<1>> {
    int64_t min_file_pos_step;
    uint64_t num_elements;
    int64_t base_rp;
    int64_t last_rp;
    int64_t base_kaf;
    int64_t last_kaf;
    int64_t base_file;
    int64_t last_file;
    std::vector<int64_t> rp_write_buf;
    std::vector<int64_t> kaf_write_buf;
    std::vector<int64_t> file_write_buf;
    iobuf rp_index;
    iobuf kaf_index;
    iobuf file_index;
};

iobuf offset_index::to_iobuf() {
    offset_index_header hdr{
      .min_file_pos_step = _min_file_pos_step,
      .num_elements = _pos,
      .base_rp = _initial_rp,
      .last_rp = _rp_index.get_last_value(),
      .base_kaf = _initial_kaf,
      .last_kaf = _kaf_index.get_last_value(),
      .base_file = _initial_file_pos,
      .last_file = _file_index.get_last_value(),
      .rp_write_buf = std::vector<int64_t>(
        _rp_offsets.begin(), _rp_offsets.end()),
      .kaf_write_buf = std::vector<int64_t>(
        _kaf_offsets.begin(), _kaf_offsets.end()),
      .file_write_buf = std::vector<int64_t>(
        _file_offsets.begin(), _file_offsets.end()),
      .rp_index = _rp_index.copy(),
      .kaf_index = _kaf_index.copy(),
      .file_index = _file_index.copy(),
    };
    return serde::to_iobuf(std::move(hdr));
}

void offset_index::from_iobuf(iobuf b) {
    iobuf_parser parser(std::move(b));
    auto hdr = serde::read<offset_index_header>(parser);
    auto num_rows = hdr.num_elements / buffer_depth;
    _pos = hdr.num_elements;
    _initial_rp = model::offset(hdr.base_rp);
    _initial_kaf = kafka::offset(hdr.base_kaf);
    _initial_file_pos = hdr.base_file;
    std::copy(
      hdr.rp_write_buf.begin(), hdr.rp_write_buf.end(), _rp_offsets.begin());
    std::copy(
      hdr.kaf_write_buf.begin(), hdr.kaf_write_buf.end(), _kaf_offsets.begin());
    std::copy(
      hdr.file_write_buf.begin(),
      hdr.file_write_buf.end(),
      _file_offsets.begin());
    _rp_index = encoder_t(
      _initial_rp, num_rows, hdr.last_rp, std::move(hdr.rp_index));
    _kaf_index = encoder_t(
      _initial_kaf, num_rows, hdr.last_kaf, std::move(hdr.kaf_index));
    _file_index = foffset_encoder_t(
      _initial_file_pos,
      num_rows,
      hdr.last_file,
      std::move(hdr.file_index),
      delta_delta_t(_min_file_pos_step));
    _min_file_pos_step = hdr.min_file_pos_step;
}

std::optional<offset_index::index_value>
offset_index::_find_under(deltafor_decoder<int64_t> decoder, int64_t offset) {
    size_t ix = 0;
    std::array<int64_t, buffer_depth> rp_buf{};
    std::optional<index_value> candidate;
    while (decoder.read(rp_buf)) {
        for (auto o : rp_buf) {
            if (o >= offset) {
                return candidate;
            }
            candidate = {.ix = size_t(ix), .value = o};
            ix++;
        }
        rp_buf = {};
    }
    return candidate;
}

remote_segment_index_builder::remote_segment_index_builder(
  offset_index& ix, model::offset_delta initial_delta, size_t sampling_step)
  : _ix(ix)
  , _running_delta(initial_delta)
  , _sampling_step(sampling_step) {}

remote_segment_index_builder::consume_result
remote_segment_index_builder::accept_batch_start(
  const model::record_batch_header&) const {
    return consume_result::accept_batch;
}

void remote_segment_index_builder::consume_batch_start(
  model::record_batch_header hdr,
  size_t physical_base_offset,
  size_t size_on_disk) {
    if (
      hdr.type == model::record_batch_type::raft_configuration
      || hdr.type == model::record_batch_type::archival_metadata) {
        _running_delta += hdr.last_offset_delta + 1;
    } else {
        if (_window >= _sampling_step) {
            _ix.add(
              hdr.base_offset,
              hdr.base_offset - _running_delta,
              static_cast<int64_t>(physical_base_offset));
            _window = 0;
        }
    }
    _window += size_on_disk;
}

void remote_segment_index_builder::skip_batch_start(
  model::record_batch_header, size_t, size_t) {
    vassert(false, "no batches should be skipped by this consumer");
}

void remote_segment_index_builder::consume_records(iobuf&&) {}

remote_segment_index_builder::stop_parser
remote_segment_index_builder::consume_batch_end() {
    return stop_parser::no;
}

void remote_segment_index_builder::print(std::ostream& o) const {
    o << "remote_segment_index_builder";
}

} // namespace cloud_storage
