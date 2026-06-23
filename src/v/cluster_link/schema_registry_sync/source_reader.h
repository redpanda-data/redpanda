/*
 * Copyright 2026 Redpanda Data, Inc.
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
#include "container/chunked_vector.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <cstdint>
#include <expected>

namespace cluster_link::schema_registry_sync {

namespace ppsr = pandaproxy::schema_registry;

/// Classifies why a source Schema Registry read failed.
enum class source_error_kind : uint8_t {
    /// The operation failed but the source is still reachable; the sync can
    /// continue with the next item.
    operation_failed,
    /// The source Schema Registry is unreachable; the whole sync should back
    /// off.
    source_unavailable,
};

struct source_error {
    source_error_kind kind;
    ss::sstring message;
};

template<typename T>
using source_result = std::expected<T, source_error>;

/// \brief Abstraction over a source Schema Registry, scoped to one link.
///
/// Reads are split into discovery (list subjects/versions) and fetch (read a
/// specific schema) so the sync can decide what to import before pulling
/// schema bodies. Production currently uses an unavailable reader (the real
/// HTTP-backed implementation is not wired yet); tests inject a fake.
class source_reader {
public:
    source_reader() = default;
    source_reader(const source_reader&) = delete;
    source_reader& operator=(const source_reader&) = delete;
    source_reader(source_reader&&) = delete;
    source_reader& operator=(source_reader&&) = delete;
    virtual ~source_reader() = default;

    virtual ss::future<source_result<chunked_vector<ppsr::context_subject>>>
    list_subjects(ppsr::context, ss::abort_source&) = 0;

    virtual ss::future<source_result<chunked_vector<ppsr::schema_version>>>
    list_subject_versions(ppsr::context_subject, ss::abort_source&) = 0;

    /// Reads a specific subject version's schema. Not used yet (the apply path
    /// is not implemented); kept here so the reader seam is stable.
    virtual ss::future<source_result<ppsr::stored_schema>> read_subject_version(
      ppsr::context_subject, ppsr::schema_version, ss::abort_source&) = 0;
};

/// \brief Creates one `source_reader` per link.
class source_reader_factory {
public:
    source_reader_factory() = default;
    source_reader_factory(const source_reader_factory&) = delete;
    source_reader_factory& operator=(const source_reader_factory&) = delete;
    source_reader_factory(source_reader_factory&&) = delete;
    source_reader_factory& operator=(source_reader_factory&&) = delete;
    virtual ~source_reader_factory() = default;

    virtual std::unique_ptr<source_reader> create() = 0;
};

} // namespace cluster_link::schema_registry_sync
