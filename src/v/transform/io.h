/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/transform.h"

namespace transform {
namespace detail {

/**
 * A factory for sources and sinks.
 *
 * Use sink::factory or source::factory instead.
 */
template<typename T>
class factory {
public:
    factory() = default;
    factory(const factory&) = delete;
    factory& operator=(const factory&) = delete;
    factory(factory&&) = delete;
    factory& operator=(factory&&) = delete;
    virtual ~factory() = default;

    virtual std::optional<std::unique_ptr<T>> create(model::ntp) = 0;
};
} // namespace detail

/**
 * The output sink for Wasm transforms.
 */
class sink {
public:
    sink() = default;
    sink(const sink&) = delete;
    sink& operator=(const sink&) = delete;
    sink(sink&&) = delete;
    sink& operator=(sink&&) = delete;
    virtual ~sink() = default;

    virtual ss::future<> write(ss::chunked_fifo<model::record_batch>) = 0;

    using factory = detail::factory<sink>;
};

/**
 * The input source for Wasm transforms.
 */
class source {
public:
    source() = default;
    source(const source&) = delete;
    source& operator=(const source&) = delete;
    source(source&&) = delete;
    source& operator=(source&&) = delete;
    virtual ~source() = default;

    virtual ss::future<model::offset> load_latest_offset() = 0;
    virtual ss::future<model::record_batch_reader>
    read_batch(model::offset, ss::abort_source*) = 0;

    using factory = detail::factory<source>;
};
} // namespace transform
