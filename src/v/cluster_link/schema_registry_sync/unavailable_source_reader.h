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
#include "cluster_link/schema_registry_sync/source_reader.h"

#include <seastar/core/sstring.hh>

namespace cluster_link::schema_registry_sync {

/// \brief A `source_reader` whose every read reports the source as unavailable
/// with a fixed message.
///
/// Used when no usable HTTP reader can be built — e.g. Schema Registry API sync
/// is not configured, or the configured source URL is invalid. With this reader
/// the Schema Registry shadowing task parks the link in `link_unavailable`, and
/// the message surfaces as the link's last error, so it must name the actual
/// cause rather than a generic placeholder.
class unavailable_source_reader final : public source_reader {
public:
    /// \param message the error text every read reports; it becomes the link's
    ///        last error message, so pass a cause-specific string.
    explicit unavailable_source_reader(
      ss::sstring message = "Schema Registry API sync is not configured");

    ss::future<source_result<chunked_vector<ppsr::context>>>
    list_contexts(ss::abort_source&) override;

    ss::future<source_result<chunked_vector<ppsr::context_subject>>>
    list_subjects(ppsr::context, ss::abort_source&) override;

    ss::future<source_result<chunked_vector<ppsr::schema_version>>>
    list_subject_versions(
      ppsr::context_subject, ppsr::include_deleted, ss::abort_source&) override;

    ss::future<source_result<chunked_vector<ppsr::subject_version>>>
    list_schema_id_subject_versions(
      ppsr::schema_id, ppsr::context, ss::abort_source&) override;

    ss::future<source_result<ppsr::source_schema_read>> read_subject_version(
      ppsr::context_subject, ppsr::schema_version, ss::abort_source&) override;

    ss::future<source_result<std::optional<ppsr::mode>>>
    read_mode(ppsr::context_subject, ss::abort_source&) override;

    ss::future<source_result<source_config_read>>
    read_config(ppsr::context_subject, ss::abort_source&) override;

private:
    source_error unavailable() const;

    ss::sstring _message;
};

class unavailable_source_reader_factory final : public source_reader_factory {
public:
    std::unique_ptr<source_reader> create(
      const model::schema_registry_sync_config::shadow_schema_registry_api*)
      override;
};

} // namespace cluster_link::schema_registry_sync
