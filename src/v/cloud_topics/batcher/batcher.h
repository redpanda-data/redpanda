/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/outcome.h"
#include "base/seastarx.h"
#include "base/units.h"
#include "bytes/iobuf.h"
#include "cloud_topics/batcher/write_request.h"
#include "cloud_topics/types.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "utils/retry_chain_node.h"
#include "utils/uuid.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>

#include <absl/container/btree_map.h>

#include <chrono>

namespace cloud_io {
template<typename Clock>
class remote_api;
}

namespace experimental::cloud_topics {

struct batcher_result {
    uuid_t uuid;
    // Reader that contains placeholder batches. Batches
    // should map to original batches 1:1 but have different
    // content.
    std::unique_ptr<model::record_batch_reader> reader;
};

struct batcher_accessor;

/// The data path uploader
///
/// The batcher collects a list of write_request instances in
/// memory. Periodically, the data is uploaded to the cloud storage
/// and removed from memory.
template<class Clock = ss::lowres_clock>
class batcher {
    using clock_t = Clock;
    using timestamp_t = typename Clock::time_point;

    friend struct batcher_accessor;

public:
    explicit batcher(
      cloud_storage_clients::bucket_name bucket,
      cloud_io::remote_api<Clock>& remote_api);

    /// Upload data to the cloud storage
    ///
    /// Wait until the record batch is uploaded and return result.
    /// Always consumes everything from the reader.
    ss::future<result<model::record_batch_reader>> write_and_debounce(
      model::ntp ntp,
      model::record_batch_reader r,
      std::chrono::milliseconds timeout);

    ss::future<> start();
    ss::future<> stop();

private:
    /// Run one iteration of the background loop
    ///
    /// Single call
    /// - filters out timed out requests
    /// - aggregates requests to create one L0 object
    /// - uploads L0 object
    /// - generates placeholders and propagates them
    ///
    /// \returns false if the method should be called again, true otherwise
    ss::future<result<bool>> run_once() noexcept;

    /// Background fiber responsible for merging
    /// aggregated log data and sending it to the
    /// cloud storage
    ///
    /// The method should only be invoked on shard 0
    ss::future<> bg_controller_loop();

    /// List of write requests which are ready to be uploaded
    struct size_limited_write_req_list {
        /// Write requests list which are ready for
        /// upload or expired.
        details::write_request_list<Clock> ready;
        /// If the batcher contains more write requests which
        /// were not included because the size limit was reached
        /// this field will be set to false.
        bool complete{true};
        /// Total size of all listed write requests.
        size_t size_bytes{0};
    };

    /// Get write requests atomically.
    /// Return requests which are either ready to be uploaded or expired. Limit
    /// the total size of all returned write requests by 'max_bytes'.
    size_limited_write_req_list get_write_requests(size_t max_bytes);

    /// Find all timed out write requests and remove them from the list
    /// atomically.
    void remove_timed_out_write_requests();

    /// Wait until upload interval elapses or until
    /// enough bytes are accumulated
    ss::future<errc> wait_for_next_upload() noexcept;

    /// Upload L0 object based on placeholders
    ///
    /// Collect data from every shard and upload stream of data to S3.
    ///
    /// \return size of the uploaded object or error code
    ss::future<result<size_t>> upload_object(object_id id, iobuf payload);

    cloud_io::remote_api<Clock>& _remote;
    cloud_storage_clients::bucket_name _bucket;
    config::binding<std::chrono::milliseconds> _upload_timeout;
    config::binding<std::chrono::milliseconds> _upload_interval;

    ss::gate _gate;
    ss::abort_source _as;

    // List of new write requests
    details::write_request_list<Clock> _pending;

    static constexpr size_t max_buffer_size = 16_MiB;
    static constexpr size_t max_cardinality = 1000;

    details::batcher_req_index _index{0};
    size_t _current_size{0};
    basic_retry_chain_node<Clock> _rtc;
    basic_retry_chain_logger<Clock> _logger;

    ss::condition_variable _cv;
};
} // namespace experimental::cloud_topics
