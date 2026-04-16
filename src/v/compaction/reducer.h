// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "compaction/types.h"
#include "model/compression.h"
#include "model/record.h"

#include <seastar/core/loop.hh>

#include <memory>
#include <ostream>
#include <utility>

namespace compaction {

// An implementation for the sliding window algorithm for compaction.
// Sliding window algorithm is performed in two steps:
// 1. A backward pass over the data source, in which the latest key-offset pair
// is indexed in a hash map of a finite size (determined by cluster config
// `storage_compaction_key_map_memory`) until all keys from the data source are
// indexed OR the hash map's size has reached its allocated capacity.
// 2. A forward pass over the data source, in which the data (i.e Kafka
// records) is filtered and rewritten.
// Filtering can be a removal of data due to:
// 1. De-duplication (the removal of data due to a newer key appearing in the
// data source).
// 2. Deletion (the removal of data due to other logic, i.e the removal of a
// tombstone record which has passed the time horizon set by
// `delete.retention.ms`)
// An example usage of this class is the following:
// auto src  = std::make_unique<storage::segment_source>(_segs);
// auto sink = std::make_unique<storage::segment_sink>();
// auto reducer = compaction::sliding_window_reducer(std::move(src),
//                std::move(sink));
// co_await std::move(reducer).run();
class sliding_window_reducer {
public:
    class source;

    // The sink for the data to be written by this round of compaction.
    // This class needs to implement three functions:
    // 1. `initialize(source&)`: This function occurs after the `source` has
    // performed its `map_building_iteration()` but before the
    // `deduplication_iteration()` stage has begun. It allows the `sink` an
    // opportunity to initialize some of its state, and to access whatever
    // shared state the `source` and `sink` may need to communicate to one
    // another. The boolean return value dictates whether or not the
    // `deduplication_iteration()` stage is required- for example, if the
    // `source` didn't index any dirty ranges from the log and there is no
    // meaningful compaction work to do, `initialize()` could return `false` to
    // skip compaction. `finalize()` will still be called regardless of the
    // return value here.
    // 2. `operator()(record_batch)`: This operator accepts a record batch
    // (which has already been determined to be written by a
    // `compaction::filter`) and is responsible for writing its contents to
    // whichever data format/store this `sink` represents.
    // 3. `finalize(success)`: perform any final steps required in the `sink`
    // layer, i.e flushing in progress writes, update final metadata, etc.
    // `success` is true if the deduplication pass completed without exception,
    // false if an exception was caught. Implementations may use this to discard
    // partially-written state rather than committing it.
    class sink {
    public:
        sink() noexcept = default;
        sink(sink&& o) noexcept = default;
        sink& operator=(sink&& o) noexcept = default;
        sink(const sink& o) = delete;
        sink& operator=(const sink& o) = delete;
        virtual ~sink() noexcept = default;

    public:
        virtual ss::future<bool> initialize(source&) = 0;
        virtual ss::future<ss::stop_iteration>
        operator()(model::record_batch, model::compression) = 0;
        virtual ss::future<> finalize(bool) = 0;
        virtual ss::future<> prepare_iteration(kafka::offset) = 0;
        virtual ss::future<> finish_iteration(kafka::offset, kafka::offset) = 0;
    };

    // The source of data for compaction.
    // This class needs to implement three functions:
    // 1. `initialize()`: This performs initial set-up (if required). For
    // example, the local storage source may need to collect the set of
    // `segment`s eligible for compaction, and perform self compaction on them
    // before proceeding to the sliding window algorithm.
    // 2. `map_building_iteration()`: This is the pass that reads from the
    // data source and indexes the latest key-offset pair in the contained map
    // within the provided `compaction_config` object. This should ideally be a
    // light-weight read over a portion of the log that avoids de-compression or
    // e.g. uncached reads from cloud storage.
    // 3. `deduplication_iteration(sink)`: This is the pass that reads from
    // the data source and provides the data to be written (determined by the
    // contents of the key-offset map and other configured parameters within
    // `cfg`) for this round of compaction to the sink object.
    class source {
    public:
        source() noexcept = default;
        source(source&& o) noexcept = default;
        source& operator=(source&& o) noexcept = default;
        source(const source& o) = delete;
        source& operator=(const source& o) = delete;
        virtual ~source() noexcept = default;

    public:
        virtual ss::future<> initialize() = 0;
        virtual ss::future<ss::stop_iteration> map_building_iteration() = 0;
        virtual ss::future<ss::stop_iteration>
        deduplication_iteration(sink&) = 0;
    };

public:
    explicit sliding_window_reducer(
      std::unique_ptr<source> src, std::unique_ptr<sink> sink) noexcept
      : _src(std::move(src))
      , _sink(std::move(sink)) {}
    sliding_window_reducer(const sliding_window_reducer&) = delete;
    sliding_window_reducer& operator=(const sliding_window_reducer&) = delete;
    sliding_window_reducer(sliding_window_reducer&&) noexcept = default;
    sliding_window_reducer&
    operator=(sliding_window_reducer&&) noexcept = default;
    ~sliding_window_reducer() noexcept = default;

    ss::future<> run() &&;

private:
    std::unique_ptr<source> _src;
    std::unique_ptr<sink> _sink;
};

} // namespace compaction
