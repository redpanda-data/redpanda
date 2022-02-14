/*
 * Copyright 2022 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/remote_segment_index.h"

namespace cloud_storage {

offset_index::offset_index(
  model::offset initial_rp, model::offset initial_kaf, int64_t initial_file_pos)
  : _rp_offsets{}
  , _kaf_offsets{}
  , _file_offsets{}
  , _pos{}
  , _initial_rp(initial_rp)
  , _initial_kaf(initial_kaf)
  , _initial_file_pos(initial_file_pos)
  , _rp_encoder(initial_rp)
  , _kaf_encoder(initial_kaf)
  , _file_encoder(initial_file_pos) {}

void offset_index::add(
  model::offset rp_offset, model::offset kaf_offset, int64_t file_offset) {
    auto ix = index_mask & _pos++;
    _rp_offsets.at(ix) = rp_offset();
    _kaf_offsets.at(ix) = kaf_offset();
    _file_offsets.at(ix) = file_offset;
    try {
        if ((_pos & index_mask) == 0) {
            _rp_encoder.add(_rp_offsets);
            _kaf_encoder.add(_kaf_offsets);
            _file_encoder.add(_file_offsets);
        }
    } catch (...) {
        // Get rid of the corrupted state in the encoders.
        // If the exception is thrown out of 'add' method
        // the invariant of the index is broken.
        _pos = 0;
        _rp_offsets = {};
        _kaf_offsets = {};
        _file_offsets = {};
        _rp_encoder = deltafor_encoder<int64_t>(_initial_rp);
        _kaf_encoder = deltafor_encoder<int64_t>(_initial_kaf);
        _file_encoder = deltafor_encoder<int64_t>(_initial_file_pos);
        throw;
    }
}

std::
  variant<std::monostate, offset_index::index_value, offset_index::find_result>
  offset_index::maybe_find_offset(
    model::offset upper_bound,
    deltafor_encoder<int64_t>& encoder,
    const std::array<int64_t, buffer_depth>& write_buffer) {
    deltafor_decoder<int64_t> decoder(
      encoder.get_initial_value(), encoder.get_row_count(), encoder.share());
    auto max_index = encoder.get_row_count() * details::FOR_buffer_depth - 1;
    auto maybe_ix = _find_under(std::move(decoder), upper_bound());
    if (!maybe_ix || maybe_ix->ix == max_index) {
        auto ixend = _pos & index_mask;
        std::optional<find_result> candidate;
        for (size_t i = 0; i < ixend; i++) {
            if (write_buffer.at(i) < upper_bound) {
                candidate = find_result{
                  .rp_offset = model::offset(_rp_offsets.at(i)),
                  .kaf_offset = model::offset(_kaf_offsets.at(i)),
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

    auto search_result = maybe_find_offset(
      upper_bound, _rp_encoder, _rp_offsets);

    if (std::holds_alternative<std::monostate>(search_result)) {
        return std::nullopt;
    } else if (std::holds_alternative<find_result>(search_result)) {
        return std::get<find_result>(search_result);
    }
    auto maybe_ix = std::get<index_value>(search_result);

    // Invariant: maybe_ix here can't be nullopt
    ix = maybe_ix.ix;
    res.rp_offset = model::offset(maybe_ix.value);

    deltafor_decoder<int64_t> kaf_dec(
      _kaf_encoder.get_initial_value(),
      _kaf_encoder.get_row_count(),
      _kaf_encoder.copy());
    auto kaf_offset = _fetch_ix(std::move(kaf_dec), ix);
    vassert(kaf_offset.has_value(), "Inconsistent index state");
    res.kaf_offset = model::offset(*kaf_offset);
    deltafor_decoder<int64_t> file_dec(
      _file_encoder.get_initial_value(),
      _file_encoder.get_row_count(),
      _file_encoder.copy());
    auto file_pos = _fetch_ix(std::move(file_dec), ix);
    res.file_pos = *file_pos;
    return res;
}

std::optional<offset_index::find_result>
offset_index::find_kaf_offset(model::offset upper_bound) {
    size_t ix = 0;
    find_result res{};

    auto search_result = maybe_find_offset(
      upper_bound, _kaf_encoder, _kaf_offsets);

    if (std::holds_alternative<std::monostate>(search_result)) {
        return std::nullopt;
    } else if (std::holds_alternative<find_result>(search_result)) {
        return std::get<find_result>(search_result);
    }
    auto maybe_ix = std::get<index_value>(search_result);

    // Invariant: maybe_ix here can't be nullopt
    ix = maybe_ix.ix;
    res.kaf_offset = model::offset(maybe_ix.value);

    deltafor_decoder<int64_t> rp_dec(
      _rp_encoder.get_initial_value(),
      _rp_encoder.get_row_count(),
      _rp_encoder.copy());
    auto rp_offset = _fetch_ix(std::move(rp_dec), ix);
    vassert(rp_offset.has_value(), "Inconsistent index state");
    res.rp_offset = model::offset(*rp_offset);
    deltafor_decoder<int64_t> file_dec(
      _file_encoder.get_initial_value(),
      _file_encoder.get_row_count(),
      _file_encoder.copy());
    auto file_pos = _fetch_ix(std::move(file_dec), ix);
    res.file_pos = *file_pos;
    return res;
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

std::optional<int64_t>
offset_index::_fetch_ix(deltafor_decoder<int64_t> decoder, size_t target_ix) {
    size_t ix = 0;
    std::array<int64_t, buffer_depth> buffer{};
    while (decoder.read(buffer)) {
        for (auto o : buffer) {
            if (ix == target_ix) {
                return o;
            }
            ix++;
        }
        buffer = {};
    }
    return std::nullopt;
}

} // namespace cloud_storage
