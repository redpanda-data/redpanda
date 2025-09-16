/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/frontend_reader/reader.h"

#include "cloud_topics/level_one/frontend_reader/reader.h"
#include "cloud_topics/level_one/metastore/replicated_metastore.h"
#include "cloud_topics/level_zero/frontend_reader/reader.h"
#include "cloud_topics/logger.h"
#include "cluster/partition.h"
#include "model/fundamental.h"

namespace cloud_topics {

cloud_topics_log_reader_impl::cloud_topics_log_reader_impl(
  cloud_topic_log_reader_config& cfg,
  model::ntp ntp,
  model::topic_id_partition tidp,
  ss::lw_shared_ptr<cluster::partition> partition,
  l1::frontend* l1_frontend,
  l1::io* io_interface,
  data_plane_api* data_plane_api,
  ctp_stm_api* stm_api)
  : _config(cfg)
  , _ntp(std::move(ntp))
  , _tidp(tidp)
  , _partition(std::move(partition))
  , _metastore(std::make_unique<l1::replicated_metastore>(*l1_frontend))
  , _io(io_interface)
  , _data_plane_api(data_plane_api)
  , _stm_api(stm_api) {}

bool cloud_topics_log_reader_impl::is_end_of_stream() const {
    return _state == state::end_of_stream;
}

ss::future<model::record_batch_reader::storage_t>
cloud_topics_log_reader_impl::do_load_slice(
  model::timeout_clock::time_point deadline) {
    // This simple loop and switch doesn't attempt to stitch L1 and L0 batches
    // together. When reaching the end of L1, the reader will return whatever
    // it gets and read from L0 on the next call to do_load_slice.
    while (true) {
        switch (_state) {
        case state::end_of_stream:
            co_return model::record_batch_reader::storage_t{};
        case state::uninitialized:
            co_await initialize_reader(deadline);
            vassert(
              _state != state::uninitialized, "reader should be initialized");
            break;
        case state::reading_l1:
            co_return co_await read_l1(deadline);
        case state::reading_l0:
            co_return co_await read_l0(deadline);
        }
    }
}

void cloud_topics_log_reader_impl::print(std::ostream& o) {
    o << "cloud_topics_log_reader";
}

ss::future<> cloud_topics_log_reader_impl::initialize_reader(
  model::timeout_clock::time_point /*deadline*/) {
    _lro = _stm_api->get_last_reconciled_offset();
    _original_max_offset = _config.max_offset;

    if (_config.start_offset <= _lro) {
        vlog(
          cd_log.debug,
          "Initializing reader in L1 for {} with start_offset {} and LRO {}",
          _ntp,
          _config.start_offset,
          _lro);

        _config.max_offset = std::min(_config.max_offset, _lro);

        _l1_reader = std::make_unique<level_one_log_reader_impl>(
          _config, _ntp, _tidp, _metastore.get(), _io);
        _state = state::reading_l1;
        co_return;
    }

    vlog(
      cd_log.debug,
      "Initializing reader in L0 for {} with start_offset {} and LRO {}",
      _ntp,
      _config.start_offset,
      _lro);

    _l0_reader = std::make_unique<level_zero_log_reader_impl>(
      _config, _partition, _data_plane_api);
    _state = state::reading_l0;
}

bool cloud_topics_log_reader_impl::should_transition_to_l0() const {
    if (_lro >= _original_max_offset) {
        return false;
    }

    if (
      (_config.strict_max_bytes || _config.bytes_consumed > 0)
      && _config.bytes_consumed >= _config.max_bytes) {
        return false;
    }

    return true;
}

ss::future<model::record_batch_reader::storage_t>
cloud_topics_log_reader_impl::read_l1(
  model::timeout_clock::time_point deadline) {
    vassert(_state == state::reading_l1, "reading L1 state mismatch");
    vassert(_l1_reader, "L1 reader should be initialized");
    vassert(!_l0_reader, "L0 reader should not be initialized");

    auto result = co_await _l1_reader->do_load_slice(deadline);

    if (_l1_reader->is_end_of_stream()) {
        if (should_transition_to_l0()) {
            _config.start_offset = kafka::next_offset(_lro);
            _config.max_offset = _original_max_offset;

            vlog(
              cd_log.debug,
              "Transitioning from L1 to L0 at offset {}",
              _config.start_offset);

            _l0_reader = std::make_unique<level_zero_log_reader_impl>(
              _config, _partition, _data_plane_api);
            _l1_reader.reset();
            _state = state::reading_l0;

            co_return result;
        }
        _state = state::end_of_stream;
    }

    co_return result;
}

ss::future<model::record_batch_reader::storage_t>
cloud_topics_log_reader_impl::read_l0(
  model::timeout_clock::time_point deadline) {
    vassert(_state == state::reading_l0, "reading L0 state mismatch");
    vassert(_l0_reader, "L0 reader should be initialized");
    vassert(!_l1_reader, "L1 reader should not be initialized");

    auto result = co_await _l0_reader->do_load_slice(deadline);

    if (_l0_reader->is_end_of_stream()) {
        _state = state::end_of_stream;
    }

    co_return result;
}

} // namespace cloud_topics
