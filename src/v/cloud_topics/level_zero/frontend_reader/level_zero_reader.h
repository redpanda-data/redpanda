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

#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "cloud_topics/log_reader_config.h"
#include "model/record_batch_reader.h"
#include "utils/prefix_logger.h"

#include <expected>

namespace cluster {
class partition;
}

namespace storage {
struct local_log_reader_config;
}

namespace cloud_topics {
class data_plane_api;

/*
This class implements a record batch reader for cloud topics partitions.

The reader is a state machine. These are the states in which it can be
- empty_state, no metadata is cached, no data is materialized;
- ready_state, metadata is available but no data is materialized
- materialized_state, the reader contains materialized batches
- end of stream.

While in first two states the reader can consume data from the record
batch cache if the data is available.
             ┌───┐
             │EOS├───┤Terminate│
             └───┘
               ▲
               │
             ┌─┴───┐   ┌─────┐
│Init├──────►│empty├──►│ready│
             └─────┘   └──┬──┘
                 ▲        │
                 │        ▼
               ┌─┴──────────┐
               │materialized│
               └────────────┘

The reader starts with 'empty' state.

When in 'emtpy' state the reader initiates fetching of metadata from the
underlying partition. When the metadata is fetched it transitions to
'ready' state. If it's impossible to continue the reader transitions to
the 'EOS' state.

In 'ready' state the reader invokes the cloud topics api and asks to
materialize metadata. When the batches are materialized the reader transitions
to the 'materialized' state.

The data can be consumed while the reader is in the 'materialized' state.
When all materialized record batches are consumed the reader transitions
back to 'empty' state.
*/
class level_zero_log_reader_impl : public model::record_batch_reader::impl {
public:
    level_zero_log_reader_impl(
      const cloud_topic_log_reader_config& cfg,
      ss::lw_shared_ptr<cluster::partition> underlying,
      data_plane_api* ct_api);

    bool is_end_of_stream() const final;

    ss::future<model::record_batch_reader::storage_t>
      do_load_slice(model::timeout_clock::time_point) final;

    void print(std::ostream& o) final;

    // Register this reader with the STM - this is needed so that L0 doesn't GC
    // any active data during reads.
    void register_with_stm(ctp_stm_api*);

private:
    // A batch read from the local log, these can be either placeholder batches
    // with pointers to the actual data in cloud storage, or it can be control
    // batches from the local log (i.e. transaction markers). We need to
    // preserve transactional markers because clients expect to be able to read
    // them.
    struct local_log_batch {
        model::record_batch_header header;
        // For control batches, we preserve the batch record data, but for
        // placeholder batches we extract out the extent_meta to be hydrated
        // later.
        using payload = iobuf;

        // A cached_batch holds a fully-formed batch resolved from
        // the record batch cache during materialize_batches(), avoiding an
        // S3 download when a previous cache miss caused the fast cache path
        // to be skipped.
        struct cached_batch {
            model::record_batch batch;
        };
        std::variant<cloud_topics::extent_meta, payload, cached_batch> data;
    };

    bool cache_enabled() const;

    /// Look up the topic_id_partition for this reader's partition.
    /// Throws ss::abort_requested_exception if the topic config has been
    /// removed (e.g. topic deletion race).
    model::topic_id_partition require_topic_id_partition() const;

    // Prepare a local log reader configuration for reading placeholder and
    // other metadata batches from the CTP.
    storage::local_log_reader_config ctp_read_config() const;

    ss::future<model::record_batch_reader::storage_t>
      read_some(model::timeout_clock::time_point);

    ss::future<
      chunked_circular_buffer<level_zero_log_reader_impl::local_log_batch>>
    fetch_metadata(
      storage::local_log_reader_config cfg,
      model::timeout_clock::time_point deadline) const;
    ss::future<
      std::expected<chunked_circular_buffer<model::record_batch>, errc>>
    materialize_batches(
      chunked_circular_buffer<local_log_batch> unhydrated,
      model::timeout_clock::time_point deadline);
    // Return data from the record batch cache.
    // This method could change state of the reader to end_of_stream_state
    // when it reaches committed offset.
    chunked_circular_buffer<model::record_batch>
    maybe_read_batches_from_cache(kafka::offset committed_kafka);

    // If adding a batch of `size` would cause this to go over the bytes limit.
    bool is_over_limit_with_bytes(size_t size) const;

    // Data from the local log that is not yet hydrated from data in L0
    //
    // The data stored in this buffer is ascending order by offset.
    //
    // All batches in _unhydrated come after the _hydrated batches (in offset
    // ordering).
    chunked_circular_buffer<local_log_batch> _unhydrated;

    void set_end_of_stream();
    bool _end_of_stream{false};

    cloud_topic_log_reader_config _config;
    kafka::offset _next_offset;
    ss::lw_shared_ptr<cluster::partition> _ctp;
    data_plane_api* _ct_api;
    prefix_logger _log;
    size_t _bytes_consumed{0};
    // state that the STM tracks as to hold back prefix truncation and GC
    active_reader_state _state;
};

} // namespace cloud_topics
