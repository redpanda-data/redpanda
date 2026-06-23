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

#include "cluster_link/schema_registry_sync/source_reader.h"

namespace cluster_link::schema_registry_sync {

/// \brief A `source_reader` whose every read reports the source as
/// unavailable.
///
/// Used in production until the HTTP-backed reader is wired. With this reader
/// the Schema Registry shadowing task parks the link in `link_unavailable`.
class unavailable_source_reader final : public source_reader {
public:
    ss::future<source_result<chunked_vector<ppsr::context_subject>>>
    list_subjects(ppsr::context, ss::abort_source&) override;

    ss::future<source_result<chunked_vector<ppsr::schema_version>>>
    list_subject_versions(ppsr::context_subject, ss::abort_source&) override;

    ss::future<source_result<ppsr::stored_schema>> read_subject_version(
      ppsr::context_subject, ppsr::schema_version, ss::abort_source&) override;
};

class unavailable_source_reader_factory final : public source_reader_factory {
public:
    std::unique_ptr<source_reader> create() override;
};

} // namespace cluster_link::schema_registry_sync
