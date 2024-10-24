/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "base/seastarx.h"
#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "container/chunked_hash_map.h"
#include "metrics/metrics.h"
#include "storage/fwd.h"
#include "storage/ntp_config.h"
#include "storage/parser.h"
#include "storage/segment_set.h"
#include "storage/snapshot.h"
#include "storage/types.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/timer.hh>

namespace storage {

/**
 * Durable key-value store.
 *
 * Manages a mapping between string and blob values. All mutating operates are
 * written to a write-ahead log and flushed before being applied in-memory.
 * Flushing is controlled by a commit interval configuration setting that allows
 * operations to be batched, amatorizing the cost of flushing to disk.
 *
 * Operation
 * =========
 *
 * The key-value store manages asynchronous mutation operations:
 *
 *   auto f = kvstore.operation(...);
 *
 * Operations are staged in an ordered in-memory container. After a commit
 * interval has elapsed the operations are serialized into a single blob and
 * flushed to disk. Once the flush is complete the operations are applied to the
 * in-memory cache, and the associated promise is resolved.
 *
 * Concurrency
 * ===========
 *
 * The caller must manage its own concurrency on a per-key basis. What this
 * means is that the key-value store provides no consistency guarantees for
 * concurrent reads and writes to the same key.
 *
 * This is sufficient for the initial use cases of storing voted_for metadata
 * in which access to the underlying file storing the metadata was already
 * controlled.
 *
 * Limitations
 * ===========
 *
 * The entire database is cached in memory, so users should not allow the set of
 * uniuqe keys to grow unbounded.  No backpressure is applied, so use
 * responsibly until this utility becomes more sophisticated. The initial set of
 * use cases--tracking raft voted-for and log's base offset--do not pose an
 * issue for either of these limitations since they exhibit a natural bound on
 * the set of unique keys and queue depth of at most a few bytes *
 * O(#-partitions-per-core).
 */
struct kvstore_config {
    size_t max_segment_size;
    config::binding<std::chrono::milliseconds> commit_interval;
    ss::sstring base_dir;
    std::optional<storage::file_sanitize_config> sanitizer_config;

    kvstore_config(
      size_t max_segment_size,
      config::binding<std::chrono::milliseconds> commit_interval,
      ss::sstring base_dir,
      std::optional<storage::file_sanitize_config> sanitizer_config)
      : max_segment_size(max_segment_size)
      , commit_interval(commit_interval)
      , base_dir(std::move(base_dir))
      , sanitizer_config(std::move(sanitizer_config)) {}
};

class kvstore {
public:
    using map_t = chunked_hash_map<bytes, iobuf>;

    enum class key_space : int8_t {
        testing = 0,
        consensus = 1,
        storage = 2,
        controller = 3,
        offset_translator = 4,
        usage = 5,
        stms = 6,
        shard_placement = 7,
        debug_bundle = 8,
        /* your sub-system here */
    };

    explicit kvstore(
      kvstore_config kv_conf,
      ss::shard_id shard,
      storage_resources&,
      ss::sharded<features::feature_table>& feature_table);
    ~kvstore() noexcept;

    ss::future<> start();
    ss::future<> stop();

    std::optional<iobuf> get(key_space ks, bytes_view key);
    ss::future<> put(key_space ks, bytes key, iobuf value);
    ss::future<> remove(key_space ks, bytes key);

    /// Iterate over all key-value pairs in a keyspace.
    /// NOTE: this will stall all updates, so use with a lot of caution.
    ss::future<> for_each(
      key_space ks,
      ss::noncopyable_function<void(bytes_view, const iobuf&)> visitor);

    bool empty() const {
        vassert(_started, "kvstore has not been started");
        return _db.empty();
    }

    /*
     * Return disk usage information about kvstore. Size information for any
     * segments are returned in the usage.data field. The kvstore doesn't
     * currently use indexing, and has no reclaimable space yes, so these fields
     * will be set to 0.
     */
    ss::future<usage_report> disk_usage() const;

private:
    kvstore_config _conf;
    storage_resources& _resources;
    ss::sharded<features::feature_table>& _feature_table;
    ntp_config _ntpc;
    ss::gate _gate;
    ss::abort_source _as;
    simple_snapshot_manager _snap;
    bool _started{false};

    /**
     * Database operation. A std::nullopt value is a deletion.
     */
    struct op {
        bytes key;
        std::optional<iobuf> value;
        ss::promise<> done;

        op(bytes&& key, std::optional<iobuf>&& value)
          : key(std::move(key))
          , value(std::move(value)) {}
    };

    /*
     * database operations are cached in `ops` and periodically flushed to the
     * current `segment` at position `next_offset` and then applied to `db`.
     * when the segment reaches a threshold size a snapshot is saved a new
     * segment is created.
     */
    std::vector<op> _ops;
    ss::timer<> _timer;
    ssx::semaphore _sem{0, "s/kvstore"};
    ss::lw_shared_ptr<segment> _segment;
    // Protect _db and _next_offset across asynchronous mutations.
    mutex _db_mut{"kvstore::db_mut"};
    model::offset _next_offset;
    map_t _db;
    std::optional<ntp_sanitizer_config> _ntp_sanitizer_config;

    ss::future<> put(key_space ks, bytes key, std::optional<iobuf> value);
    void apply_op(
      bytes key, std::optional<iobuf> value, const ssx::semaphore_units&);
    ss::future<> flush_and_apply_ops();
    ss::future<> roll();
    ss::future<> save_snapshot();

    /*
     * Recovery
     *
     * 1. load snapshot if found
     * 2. then recover from segments
     */
    ss::future<> recover();
    ss::future<> load_snapshot();
    ss::future<> load_snapshot_from_reader(snapshot_reader&);
    ss::future<> replay_segments(segment_set);

    /**
     * Replay batches against the key-value store.
     *
     * Used in recovery:
     *    segment -> parser -> replay_consumer -> db
     */
    class replay_consumer final : public batch_consumer {
    public:
        explicit replay_consumer(kvstore* store)
          : _store(store) {}

        consume_result
        accept_batch_start(const model::record_batch_header&) const override;
        void consume_batch_start(
          model::record_batch_header header, size_t, size_t) override;
        void skip_batch_start(
          model::record_batch_header header, size_t, size_t) override;
        void consume_records(iobuf&&) override;
        ss::future<stop_parser> consume_batch_end() override;
        void print(std::ostream&) const override;

    private:
        kvstore* _store;
        model::offset _last_offset;
        model::record_batch_header _header;
        iobuf _records;
    };

    friend replay_consumer;

    struct probe {
        void roll_segment() { ++segments_rolled; }
        void entry_fetched() { ++entries_fetched; }
        void entry_written() { ++entries_written; }
        void entry_removed() { ++entries_removed; }
        void add_cached_bytes(size_t count) { cached_bytes += count; }
        void dec_cached_bytes(size_t count) { cached_bytes -= count; }

        uint64_t segments_rolled{0};
        uint64_t entries_fetched{0};
        uint64_t entries_written{0};
        uint64_t entries_removed{0};
        size_t cached_bytes{0};

        metrics::internal_metric_groups metrics;
    };

    probe _probe;
};

} // namespace storage
