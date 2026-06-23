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

#include "cluster_link/schema_registry_sync/unavailable_source_reader.h"

#include <seastar/core/coroutine.hh>

namespace cluster_link::schema_registry_sync {

namespace {
source_error unavailable() {
    return source_error{
      .kind = source_error_kind::source_unavailable,
      .message = "Source Schema Registry reader is not implemented yet"};
}
} // namespace

ss::future<source_result<chunked_vector<ppsr::context_subject>>>
unavailable_source_reader::list_subjects(ppsr::context, ss::abort_source&) {
    co_return std::unexpected(unavailable());
}

ss::future<source_result<chunked_vector<ppsr::schema_version>>>
unavailable_source_reader::list_subject_versions(
  ppsr::context_subject, ss::abort_source&) {
    co_return std::unexpected(unavailable());
}

ss::future<source_result<ppsr::stored_schema>>
unavailable_source_reader::read_subject_version(
  ppsr::context_subject, ppsr::schema_version, ss::abort_source&) {
    co_return std::unexpected(unavailable());
}

std::unique_ptr<source_reader> unavailable_source_reader_factory::create() {
    return std::make_unique<unavailable_source_reader>();
}

} // namespace cluster_link::schema_registry_sync
