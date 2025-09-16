/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "cloud_topics/log_reader_config.h"
#include "model/record_batch_reader.h"

namespace cluster {
class partition;
}

namespace cloud_topics {
class data_plane_api;

namespace l1 {
class frontend;
}

/*
 * This class implements a reader for a cloud topic. It reads from both L0 and
 * L1.
 *
 * The reader determines at initialization whether to start reading from L1 or
 * L0 based on the LRO, and if the L1 reader is exhausted but the reader expects
 * more batches, the reader transitions to reading from L0.
 */
class cloud_topics_log_reader_impl : public model::record_batch_reader::impl {
public:
    cloud_topics_log_reader_impl(
      cloud_topic_log_reader_config& cfg,
      model::ntp ntp,
      model::topic_id_partition tidp,
      ss::lw_shared_ptr<cluster::partition> partition,
      l1::frontend* l1_frontend,
      l1::io* io_interface,
      data_plane_api* data_plane_api,
      ctp_stm_api* stm_api);

    bool is_end_of_stream() const final;

    ss::future<model::record_batch_reader::storage_t>
      do_load_slice(model::timeout_clock::time_point) final;

    void print(std::ostream& o) final;

private:
    enum class state { uninitialized, reading_l1, reading_l0, end_of_stream };

    // Initialize the appropriate reader based on the LRO and min offset.
    ss::future<> initialize_reader(model::timeout_clock::time_point deadline);

    // Should the reader transition to L0, given that it has reached the end of
    // the L1 stream?
    bool should_transition_to_l0() const;

    ss::future<model::record_batch_reader::storage_t>
    read_l1(model::timeout_clock::time_point deadline);

    ss::future<model::record_batch_reader::storage_t>
    read_l0(model::timeout_clock::time_point deadline);

    state _state{state::uninitialized};

    // The last reconciled offset, determined at initialization.
    // Used as the boundary between L1 and L0.
    kafka::offset _lro{kafka::offset(-1)};

    // Original max_offset from the config, preserved for L0.
    kafka::offset _original_max_offset;

    std::unique_ptr<model::record_batch_reader::impl> _l1_reader;
    std::unique_ptr<model::record_batch_reader::impl> _l0_reader;

    cloud_topic_log_reader_config _config;
    model::ntp _ntp;
    model::topic_id_partition _tidp;
    ss::lw_shared_ptr<cluster::partition> _partition;
    std::unique_ptr<l1::metastore> _metastore;
    l1::io* _io;
    data_plane_api* _data_plane_api;
    ctp_stm_api* _stm_api;
};

} // namespace cloud_topics
