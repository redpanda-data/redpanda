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

#include "cloud_storage/cache_service.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/partition_probe.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "storage/parser.h"
#include "storage/segment_reader.h"
#include "storage/translating_reader.h"
#include "storage/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/expiring_fifo.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>

namespace cloud_storage {

std::filesystem::path
generate_remote_index_path(const cloud_storage::remote_segment_path& p);

static constexpr size_t remote_segment_sampling_step_bytes = 64_KiB;

class download_exception : public std::exception {
public:
    explicit download_exception(download_result r, std::filesystem::path p);

    const char* what() const noexcept override;

    const download_result result;
    std::filesystem::path path;
};

class remote_segment_exception : public std::runtime_error {
public:
    explicit remote_segment_exception(const std::string& m)
      : std::runtime_error(m) {}
};

class remote_segment final {
public:
    remote_segment(
      remote& r,
      cache& cache,
      cloud_storage_clients::bucket_name bucket,
      const partition_manifest& m,
      model::offset base_offset,
      retry_chain_node& parent);

    remote_segment(const remote_segment&) = delete;
    remote_segment(remote_segment&&) = delete;
    remote_segment& operator=(const remote_segment&) = delete;
    remote_segment& operator=(remote_segment&&) = delete;
    ~remote_segment() = default;

    const model::ntp& get_ntp() const;

    const model::term_id get_term() const;

    /// Get max offset of the segment (redpada offset)
    const model::offset get_max_rp_offset() const;

    /// Number of non-data batches in all previous
    /// segments
    const model::offset_delta get_base_offset_delta() const;

    /// Get base offset of the segment (redpanda offset)
    const model::offset get_base_rp_offset() const;

    /// Get base offset of the segment (kafka offset)
    const kafka::offset get_base_kafka_offset() const;

    ss::future<> stop();

    /// create an input stream _sharing_ the underlying file handle
    /// starting at position @pos
    ss::future<storage::segment_reader_handle>
    data_stream(size_t pos, ss::io_priority_class);

    struct input_stream_with_offsets {
        ss::input_stream<char> stream;
        model::offset rp_offset;
        kafka::offset kafka_offset;
    };
    /// create an input stream _sharing_ the underlying file handle
    /// starting at position @pos
    ss::future<input_stream_with_offsets> offset_data_stream(
      kafka::offset kafka_offset,
      std::optional<model::timestamp>,
      ss::io_priority_class);

    /// Hydrate the segment
    ss::future<> hydrate();

    retry_chain_node* get_retry_chain_node() { return &_rtc; }

    bool download_in_progress() const noexcept { return !_wait_list.empty(); }

    /// Return aborted transactions metadata associated with the segment
    ///
    /// \param from start redpanda offset
    /// \param to end redpanda offset
    ss::future<std::vector<model::tx_range>>
    aborted_transactions(model::offset from, model::offset to);

    const remote_segment_path& get_segment_path() const noexcept {
        return _path;
    }

    bool is_stopped() const { return _stopped; }

private:
    /// get a file offset for the corresponding kafka offset
    /// if the index is available
    std::optional<offset_index::find_result>
    maybe_get_offsets(kafka::offset kafka_offset);

    /// Sets the results of the waiters of this segment as the given error.
    void set_waiter_errors(const std::exception_ptr& err);

    /// Run hydration loop. The method is supposed to be constantly running
    /// in the background. The background loop is triggered by the condition
    /// variable '_bg_cvar' and the promise list '_wait_list'. When the promise
    /// is waitning in the list and the cond-variable is triggered the loop
    /// wakes up and hydrates the segment if needed.
    ss::future<> run_hydrate_bg();

    /// Actually hydrate the segment. The method downloads the segment file
    /// to the cache dir and updates the segment index.
    ss::future<> do_hydrate_segment();

    /// Helper for do_hydrate_segment
    ss::future<uint64_t>
      put_segment_in_cache_and_create_index(uint64_t, ss::input_stream<char>);

    ss::future<uint64_t> put_segment_in_cache(uint64_t, ss::input_stream<char>);

    /// Hydrate tx manifest. Method downloads the manifest file to the cache
    /// dir.
    ss::future<> do_hydrate_txrange();

    ss::future<> do_hydrate_index();

    /// Materilize segment. Segment has to be hydrated beforehand. The
    /// 'materialization' process opens file handle and creates
    /// compressed segment index in memory.
    ss::future<bool> do_materialize_segment();
    ss::future<bool> do_materialize_txrange();

    /// Load segment index from file (if available)
    ss::future<bool> maybe_materialize_index();

    ss::gate _gate;
    remote& _api;
    cache& _cache;
    cloud_storage_clients::bucket_name _bucket;
    const model::ntp& _ntp;
    remote_segment_path _path;

    model::term_id _term;
    model::offset _base_rp_offset;
    model::offset_delta _base_offset_delta;
    model::offset _max_rp_offset;

    // The expected size according to the manifest entry for the segment
    size_t _size{0};

    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
    /// Notifies the background hydration fiber
    ss::condition_variable _bg_cvar;

    using expiry_handler = std::function<void(ss::promise<ss::file>&)>;

    /// List of fibers that wait for the segment to be hydrated
    ss::expiring_fifo<ss::promise<ss::file>, expiry_handler> _wait_list;

    ss::file _data_file;
    std::optional<offset_index> _index;

    using tx_range_vec = fragmented_vector<model::tx_range>;
    std::optional<tx_range_vec> _tx_range;

    // For backing off on apparent thrash/saturation of the local cache
    simple_time_jitter<ss::lowres_clock> _cache_backoff_jitter;

    bool _compacted{false};
    bool _stopped{false};

    segment_name_format _sname_format;
};

class remote_segment_batch_consumer;

/// The segment reader that can be used to fetch data from cloud storage
///
/// The reader invokes 'data_stream' method of the 'remote_segment'
/// which returns hydrated segment from disk.
///
/// The problem here is that shadow-indexing operates on sparse data.
/// It can't translate every offset. Only the base offsets of uploaded
/// segment. But it can also translate offsets as it scans the segment.
/// But this is all done internally, so caller have to proviede kafka
/// offsets. Mechanisms which require redpanda offset can use
/// '_cur_rp_offset' field. It's guaranteed to point to the same
/// record batch but the offset is not translated back to kafka. This is
/// useful since we can, for instance, compare it to committed_offset of
/// the uploaded segment to know that we scanned the whole segment.
///
/// The batches returned from the reader have offsets which are already
/// translated.
class remote_segment_batch_reader final {
    friend class remote_segment_batch_consumer;

public:
    remote_segment_batch_reader(
      ss::lw_shared_ptr<remote_segment>,
      const storage::log_reader_config& config,
      partition_probe& probe,
      ssx::semaphore_units) noexcept;

    // The following lines of code have different formatting on newer versions
    // of clang-format. In CI the clang-format version is 14 which expects
    // formatting as below.
    // clang-format off
    remote_segment_batch_reader(
      remote_segment_batch_reader&&) noexcept = delete;
    remote_segment_batch_reader&
    operator=(remote_segment_batch_reader&&) noexcept = delete;
    // clang-format on
    remote_segment_batch_reader(const remote_segment_batch_reader&) = delete;
    remote_segment_batch_reader& operator=(const remote_segment_batch_reader&)
      = delete;
    ~remote_segment_batch_reader() noexcept;

    ss::future<result<ss::circular_buffer<model::record_batch>>> read_some(
      model::timeout_clock::time_point, storage::offset_translator_state&);

    ss::future<> stop();

    const storage::log_reader_config& config() const { return _config; }
    storage::log_reader_config& config() { return _config; }

    /// Get max offset (redpanda offset)
    model::offset max_rp_offset() const { return _seg->get_max_rp_offset(); }

    /// Get base offset (redpanda offset)
    model::offset base_rp_offset() const { return _seg->get_base_rp_offset(); }

    kafka::offset base_kafka_offset() const {
        return _seg->get_base_kafka_offset();
    }

    bool is_eof() const { return _cur_rp_offset > _seg->get_max_rp_offset(); }

    void set_eof() {
        _cur_rp_offset = _seg->get_max_rp_offset() + model::offset{1};
    }

    model::offset current_rp_offset() const { return _cur_rp_offset; }
    kafka::offset current_kafka_offset() const {
        return _cur_rp_offset - _cur_delta;
    }

    model::offset_delta current_delta() const { return _cur_delta; }

    bool reads_from_segment(const remote_segment& segm) const {
        return &segm == _seg.get();
    }

    bool is_stopped() const { return _stopped; }

private:
    friend class single_record_consumer;
    ss::future<std::unique_ptr<storage::continuous_batch_parser>> init_parser();

    size_t produce(model::record_batch batch);

    ss::lw_shared_ptr<remote_segment> _seg;
    storage::log_reader_config _config;
    partition_probe& _probe;
    ss::circular_buffer<model::record_batch> _ringbuf;
    std::optional<std::reference_wrapper<storage::offset_translator_state>>
      _cur_ot_state;
    size_t _total_size{0};
    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
    std::unique_ptr<storage::continuous_batch_parser> _parser;
    model::term_id _term;
    model::offset _cur_rp_offset;
    model::offset_delta _cur_delta;
    size_t _bytes_consumed{0};
    ss::gate _gate;
    bool _stopped{false};

    /// Units for limiting concurrently-instantiated readers, they belong
    /// to materialized_segments.
    ssx::semaphore_units _units;
};

struct hydration_request {
    enum class kind {
        segment = 0,
        tx = 1,
        index = 2,
    };

    using hydrate_action_t = ss::noncopyable_function<ss::future<>()>;

    using materialize_action_t = ss::noncopyable_function<ss::future<bool>()>;

    std::filesystem::path path;
    cache_element_status current_status;
    bool was_cached{false};

    hydrate_action_t hydrate_action;
    materialize_action_t materialize_action;

    kind path_kind;
};

std::ostream& operator<<(std::ostream&, hydration_request::kind);

struct hydration_loop_state {
    using hydrate_action_t = hydration_request::hydrate_action_t;
    using materialize_action_t = hydration_request::materialize_action_t;

    explicit hydration_loop_state(
      cache& c, remote_segment_path root, retry_chain_logger& ctxlog);

    // Add request for hydration for a path. The actions supplied are used to
    // hydrate and materialize the path.
    void add_request(
      const std::filesystem::path& p,
      hydrate_action_t h,
      materialize_action_t m,
      hydration_request::kind path_kind);

    ss::future<> update_current_path_states();

    ss::future<bool> is_cache_thrashing();

    // Hydrate all registered paths. If any path is in progress an assertion is
    // triggered. If any path is already available it will be skipped.
    ss::future<> hydrate(size_t wait_list_size);

    // Call materialize actions for all registered paths. Called after
    // hydration. If there was an exception thrown during hydration,
    // materialization is skipped.
    ss::future<bool> materialize();

    // Returns the last error seen during hydration. Reset during each hydration
    // request.
    std::exception_ptr current_error();

private:
    cache& _cache;
    remote_segment_path _root;
    retry_chain_logger& _ctxlog;
    std::vector<hydration_request> _states;
    std::exception_ptr _current_error;
};

} // namespace cloud_storage
