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

#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/log_reader_config.h"
#include "model/record_batch_reader.h"
#include "utils/prefix_logger.h"

namespace cluster {
class partition;
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
      cloud_topic_log_reader_config& cfg,
      ss::lw_shared_ptr<cluster::partition> underlying,
      data_plane_api* ct_api);

    bool is_end_of_stream() const final;

    ss::future<model::record_batch_reader::storage_t>
      do_load_slice(model::timeout_clock::time_point) final;

    void print(std::ostream& o) final;

private:
    // States
    enum class state {
        empty_state,
        ready_state,
        materialized_state,
        end_of_stream_state,
    };

    bool cache_enabled() const;

    // Fetch L0 meta batches from the underlying partition
    ss::future<> fetch_metadata(model::timeout_clock::time_point deadline);
    ss::future<> materialize_batches(model::timeout_clock::time_point deadline);
    void consume_materialized_batches(
      chunked_circular_buffer<model::record_batch>* dest);
    // Return data from the record batch cache.
    // This method could change state of the reader to end_of_stream_state
    // when it reaches committed offset.
    std::optional<chunked_circular_buffer<model::record_batch>>
    maybe_load_slices_from_cache();

    // If adding a batch of `size` would cause this to go over the bytes limit.
    bool is_over_limit(size_t size) const;

    state _current{state::empty_state};

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
        std::variant<cloud_topics::extent_meta, payload> data;
    };

    // Data from the local log that is not yet hydrated from data in L0
    //
    // The data stored in this buffer is ascending order by offset.
    //
    // All batches in _unhydrated come after the _hydrated batches (in offset
    // ordering).
    chunked_circular_buffer<local_log_batch> _unhydrated;
    // Data that has been hydrated from L0 and is ready to be returned.
    //
    // The data stored in this buffer is ascending order by offset.
    chunked_circular_buffer<model::record_batch> _hydrated;

    cloud_topic_log_reader_config _config;
    ss::lw_shared_ptr<cluster::partition> _ctp;
    data_plane_api* _ct_api;
    prefix_logger _log;
};

} // namespace cloud_topics
