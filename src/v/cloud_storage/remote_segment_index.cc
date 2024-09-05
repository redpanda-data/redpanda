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

#include "cloud_storage/logger.h"
#include "model/record_batch_types.h"
#include "raft/consensus.h"
#include "serde/rw/envelope.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/vector.h"

namespace cloud_storage {

offset_index::offset_index(
  model::offset initial_rp,
  kafka::offset initial_kaf,
  int64_t initial_file_pos,
  int64_t file_pos_step,
  model::timestamp initial_time)
  : _rp_offsets{}
  , _kaf_offsets{}
  , _file_offsets{}
  , _time_offsets{}
  , _pos{}
  , _initial_rp(initial_rp)
  , _initial_kaf(initial_kaf)
  , _initial_file_pos(initial_file_pos)
  , _initial_time(initial_time)
  , _rp_index(initial_rp)
  , _kaf_index(initial_kaf)
  , _file_index(initial_file_pos, delta_delta_t(file_pos_step))
  , _time_index(initial_time.value())
  , _min_file_pos_step(file_pos_step) {}

size_t offset_index::estimate_memory_use() const {
    return _file_index.mem_use() + _rp_index.mem_use() + _kaf_index.mem_use()
           + _time_index.mem_use();
}

void offset_index::add(
  model::offset rp_offset,
  kafka::offset kaf_offset,
  int64_t file_offset,
  model::timestamp max_timestamp) {
    auto ix = index_mask & _pos++;
    _rp_offsets.at(ix) = rp_offset();
    _kaf_offsets.at(ix) = kaf_offset();
    _file_offsets.at(ix) = file_offset;
    _time_offsets.at(ix) = max_timestamp.value();
    try {
        if ((_pos & index_mask) == 0) {
            _rp_index.add(_rp_offsets);
            _kaf_index.add(_kaf_offsets);
            _file_index.add(_file_offsets);
            _time_index.add(_time_offsets);
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
        _time_index = encoder_t(_initial_time.value());
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
    auto search_result = maybe_find_offset(upper_bound, _rp_index, _rp_offsets);

    return ss::visit(
      search_result,
      [](std::monostate) -> std::optional<find_result> { return std::nullopt; },
      [](find_result result) -> std::optional<find_result> { return result; },
      [this](index_value index_result) -> std::optional<find_result> {
          find_result res{};

          size_t ix = index_result.ix;
          res.rp_offset = model::offset(index_result.value);

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
      });
}

std::optional<offset_index::find_result>
offset_index::find_kaf_offset(kafka::offset upper_bound) {
    auto search_result = maybe_find_offset(
      upper_bound, _kaf_index, _kaf_offsets);
    return ss::visit(
      search_result,
      [](std::monostate) -> std::optional<find_result> { return std::nullopt; },
      [](find_result result) -> std::optional<find_result> { return result; },
      [this](index_value index_result) -> std::optional<find_result> {
          find_result res{};

          size_t ix = index_result.ix;
          res.kaf_offset = kafka::offset(index_result.value);

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
      });
}

std::optional<offset_index::find_result>
offset_index::find_timestamp(model::timestamp upper_bound) {
    if (_initial_time == model::timestamp::missing()) {
        // Bail out early if this is a version 1 index
        // that does not index timestamps.
        return std::nullopt;
    }

    auto search_result = maybe_find_offset(
      upper_bound.value(), _time_index, _time_offsets);

    return ss::visit(
      search_result,
      [](std::monostate) -> std::optional<find_result> { return std::nullopt; },
      [](find_result result) -> std::optional<find_result> { return result; },
      [this](index_value index_result) -> std::optional<find_result> {
          size_t ix = index_result.ix;

          // Decode all offset indices to build up the result.
          decoder_t rp_dec(
            _rp_index.get_initial_value(),
            _rp_index.get_row_count(),
            _rp_index.copy());
          auto rp_offset = _fetch_ix(std::move(rp_dec), ix);
          vassert(rp_offset.has_value(), "Inconsistent index state");

          decoder_t kaf_dec(
            _kaf_index.get_initial_value(),
            _kaf_index.get_row_count(),
            _kaf_index.copy());
          auto kaf_offset = _fetch_ix(std::move(kaf_dec), ix);
          vassert(kaf_offset.has_value(), "Inconsistent index state");

          foffset_decoder_t file_dec(
            _file_index.get_initial_value(),
            _file_index.get_row_count(),
            _file_index.copy(),
            delta_delta_t(_min_file_pos_step));
          auto file_pos = _fetch_ix(std::move(file_dec), ix);
          vassert(file_pos.has_value(), "Inconsistent index state");

          return offset_index::find_result{
            .rp_offset = model::offset(*rp_offset),
            .kaf_offset = kafka::offset(*kaf_offset),
            .file_pos = *file_pos};
      });
}

offset_index::coarse_index_t offset_index::build_coarse_index(
  uint64_t step_size, std::string_view index_path) const {
    vlog(
      cst_log.trace,
      "{}: building coarse index from file offset index with {} rows",
      index_path,
      _file_index.get_row_count());
    vassert(
      step_size > static_cast<uint64_t>(_min_file_pos_step),
      "{}: step size {} cannot be less than or equal to index step size {}",
      index_path,
      step_size,
      _min_file_pos_step);

    foffset_decoder_t file_dec(
      _file_index.get_initial_value(),
      _file_index.get_row_count(),
      _file_index.copy(),
      delta_delta_t(_min_file_pos_step));
    std::array<int64_t, buffer_depth> file_row{};

    decoder_t kaf_dec(
      _kaf_index.get_initial_value(),
      _kaf_index.get_row_count(),
      _kaf_index.copy());
    std::array<int64_t, buffer_depth> kafka_row{};

    coarse_index_t index;
    auto populate_index = [step_size, &index, index_path](
                            const auto& file_offsets,
                            const auto& kafka_offsets,
                            auto& span_start,
                            auto& span_end) {
        for (auto it = file_offsets.cbegin(), kit = kafka_offsets.cbegin();
             it != file_offsets.cend() && kit != kafka_offsets.cend();
             ++it, ++kit) {
            span_end = *it;
            auto delta = span_end - span_start + 1;
            if (span_end > span_start && delta >= step_size) {
                vlog(
                  cst_log.trace,
                  "{}: adding entry to coarse index, current file pos: {}, "
                  "step size: {}, span size: {}",
                  index_path,
                  span_end,
                  step_size,
                  delta);
                index[kafka::offset{*kit}] = span_end;
                span_start = span_end + 1;
            }
        }
    };

    size_t start{0};
    size_t end{0};
    while (file_dec.read(file_row) && kaf_dec.read(kafka_row)) {
        populate_index(file_row, kafka_row, start, end);
        file_row = {};
        kafka_row = {};
    }

    populate_index(_file_offsets, _kaf_offsets, start, end);
    return index;
}

struct offset_index_header
  : serde::envelope<
      offset_index_header,
      serde::version<2>,
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

    // Version 2 fields
    int64_t base_time{model::timestamp::missing().value()};
    int64_t last_time{model::timestamp::missing().value()};
    std::vector<int64_t> time_write_buf;
    iobuf time_index;

    auto serde_fields() {
        return std::tie(
          min_file_pos_step,
          num_elements,
          base_rp,
          last_rp,
          base_kaf,
          last_kaf,
          base_file,
          last_file,
          rp_write_buf,
          kaf_write_buf,
          file_write_buf,
          rp_index,
          kaf_index,
          file_index,
          base_time,
          last_time,
          time_write_buf,
          time_index);
    }
};

iobuf offset_index::to_iobuf() const {
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
      .base_time = _initial_time.value(),
      .last_time = _time_index.get_last_value(),
      .time_write_buf = std::vector<int64_t>(
        _time_offsets.begin(), _time_offsets.end()),
      .time_index = _time_index.copy()};
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

    _initial_time = model::timestamp(hdr.base_time);
    _time_index = encoder_t(
      _initial_time.value(),
      num_rows,
      hdr.last_time,
      std::move(hdr.time_index));
    std::copy(
      hdr.time_write_buf.begin(),
      hdr.time_write_buf.end(),
      _time_offsets.begin());
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
  const model::ntp& ntp,
  offset_index& ix,
  model::offset_delta initial_delta,
  size_t sampling_step,
  std::optional<std::reference_wrapper<segment_record_stats>> maybe_stats)
  : _ix(ix)
  , _running_delta(initial_delta)
  , _sampling_step(sampling_step)
  , _filter(raft::offset_translator_batch_types(ntp))
  , _stats(maybe_stats) {}

remote_segment_index_builder::consume_result
remote_segment_index_builder::accept_batch_start(
  const model::record_batch_header&) const {
    return consume_result::accept_batch;
}

void remote_segment_index_builder::consume_batch_start(
  model::record_batch_header hdr,
  size_t physical_base_offset,
  size_t size_on_disk) {
    auto it = std::find(_filter.begin(), _filter.end(), hdr.type);
    const auto is_config = it != _filter.end();
    auto delta = hdr.last_offset_delta + 1;
    if (is_config) {
        _running_delta += delta;
    } else {
        if (_window >= _sampling_step) {
            _ix.add(
              hdr.base_offset,
              hdr.base_offset - _running_delta,
              static_cast<int64_t>(physical_base_offset),
              hdr.max_timestamp);
            _window = 0;
        }
    }
    _window += size_on_disk;

    // Update stats
    if (_stats.has_value()) {
        if (is_config) {
            _stats->get().total_conf_records += delta;
        } else {
            _stats->get().total_data_records += delta;
        }
        if (_stats->get().base_rp_offset == model::offset{}) {
            _stats->get().base_rp_offset = hdr.base_offset;
        }
        _stats->get().last_rp_offset = hdr.last_offset();
        if (_stats->get().base_timestamp == model::timestamp{}) {
            _stats->get().base_timestamp = hdr.first_timestamp;
        }
        _stats->get().last_timestamp = hdr.max_timestamp;
        _stats->get().size_bytes += hdr.size_bytes;
    }
}

void remote_segment_index_builder::skip_batch_start(
  model::record_batch_header, size_t, size_t) {
    vassert(false, "no batches should be skipped by this consumer");
}

void remote_segment_index_builder::consume_records(iobuf&&) {}

ss::future<remote_segment_index_builder::stop_parser>
remote_segment_index_builder::consume_batch_end() {
    co_return stop_parser::no;
}

void remote_segment_index_builder::print(std::ostream& o) const {
    o << "remote_segment_index_builder";
}

} // namespace cloud_storage
