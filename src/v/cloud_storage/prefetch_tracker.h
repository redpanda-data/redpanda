/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "config/property.h"

namespace cloud_storage {

/** Prefetch tracker for partition_record_batch_reader_impl.
 * The object of this class is created per reader. It is passed
 * to every remote_segment_batch_reader instance that partition
 * reader uses. When the partition is destroyed the instance of
 * the prefetch_tracker can still be stored inside the cached
 * remote_segment_batch_reader instance. Once the segment reader is completed
 * the partition reader have to extract the tracker out of it and
 * add it to the next segment reader.
 */
class prefetch_tracker {
public:
    static const prefetch_tracker disabled;
    /// Default c-tor will bind to config::shard_local_cfg()
    /// properties.
    prefetch_tracker();
    /// This c-tor can be used to pass custom bindings
    /// that could bind to something other than config::shard_local_cfg()
    prefetch_tracker(bool enabled, size_t threshold, size_t limit) noexcept;

    void on_new_segment();
    void set_segment_size(size_t sz);
    void on_bytes_consumed(size_t sz);
    bool operator()();

private:
    bool _segment_ready{false};
    size_t _segment_bytes{0};
    size_t _segment_size{0};
    size_t _total_bytes{0};
    bool _enabled;
    size_t _prefetch_threshold;
    size_t _prefetch_size;
};

} // namespace cloud_storage
