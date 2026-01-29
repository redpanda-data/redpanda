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
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/log_reader_config.h"
#include "model/record_batch_reader.h"
#include "utils/prefix_logger.h"

namespace cloud_topics {

/*
 * This class implements a record batch reader for level one.
 *
 * The reader is a state machine with the following states:
 * - empty: no metadata is cached, no data is materialized
 * - ready: metadata is available but no data is materialized
 * - materialized: the reader contains materialized batches
 * - end_of_stream: no more data to read
 *
 * The state transitions are:
 *              ┌───┐
 *              │EOS├───┤Terminate│
 *              └───┘
 *                ▲
 *                │
 *              ┌─┴───┐   ┌─────┐
 * │Init├──────►│empty├──►│ready│
 *              └─────┘   └──┬──┘
 *                  ▲        │
 *                  │        ▼
 *                ┌─┴──────────┐
 *                │materialized│
 *                └────────────┘
 *
 * The reader starts in the 'empty' state.
 *
 * In 'empty' state, the reader queries the metastore to find the L1 object
 * containing data at or after the requested offset. When metadata is found,
 * it transitions to 'ready' state. If no data is available, it transitions
 * to 'end_of_stream'.
 *
 * In 'ready' state, the reader reads the object footer to determine what
 * partition data to materialize, then fetches the required ranges from
 * object storage. When batches are materialized, it transitions to
 * 'materialized' state.
 *
 * In 'materialized' state, data can be consumed. When all materialized
 * batches are consumed, the reader transitions back to 'empty' state
 * and queries the metastore for the next L1 object partition that.
 */
class level_one_log_reader_impl : public model::record_batch_reader::impl {
public:
    level_one_log_reader_impl(
      const cloud_topic_log_reader_config& cfg,
      model::ntp ntp,
      model::topic_id_partition tidp,
      l1::metastore* metastore,
      l1::io* io_interface);

    bool is_end_of_stream() const final;

    ss::future<model::record_batch_reader::storage_t>
      do_load_slice(model::timeout_clock::time_point) final;

    void print(std::ostream& o) final;

private:
    struct object_info {
        l1::object_id oid;
        l1::footer footer;
        kafka::offset last_offset;
    };

    /*
     * Contacts the L1 metastore to retrieve metadata for an L1 object that
     * contains the target offset.
     */
    ss::future<std::optional<object_info>> lookup_object_for_offset(
      kafka::offset, model::timeout_clock::time_point deadline);

    /*
     * Materialize batches from the L1 object starting from the given offset.
     */
    ss::future<chunked_circular_buffer<model::record_batch>>
    materialize_batches_from_object_offset(
      const object_info&,
      kafka::offset,
      model::timeout_clock::time_point deadline);

    /*
     * Return batches from the reader's current position until the next
     * partition or the end of the object is reached. The set of batches
     * returned may further be limited by restrictions (e.g. byte limit)
     * imposed by the reader configuration.
     */
    ss::future<chunked_circular_buffer<model::record_batch>>
    read_batches(l1::object_reader& reader);

    ss::future<l1::footer>
    read_footer(l1::object_id oid, size_t footer_pos, size_t object_size);

    /*
     * Returns batches starting at next offset. It will continue to advance next
     * offset until batches are read or end-of-stream is reached.
     */
    ss::future<model::record_batch_reader::storage_t>
      read_some(model::timeout_clock::time_point);

    /*
     * Returns true if accepting the given number of bytes would cause the
     * reader to exceed its configured bytes limit.
     */
    bool is_over_limit_with_bytes(size_t size) const;

    ss::future<> close_reader_safe(l1::object_reader&);

    void set_end_of_stream();
    bool _end_of_stream{false};

    cloud_topic_log_reader_config _config;
    model::ntp _ntp;
    model::topic_id_partition _tidp;
    kafka::offset _next_offset;
    l1::metastore* _metastore;
    l1::io* _io;
    prefix_logger _log;
    size_t _bytes_consumed{0};
};

} // namespace cloud_topics
