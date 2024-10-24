/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/seastarx.h"
#include "model/transform.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#pragma once

namespace transform::logging {

/**
 * Map a seastar log level to a SeverityNumber as specified in
 * https://opentelemetry.io/docs/specs/otel/logs/data-model/#field-severitynumber
 */
uint32_t log_level_to_severity(ss::log_level lvl);

/**
 * A singe log message emitted by some WASM data transform.
 *
 * Transforms emit log messages by writing to stdout/stderr, which
 * is intercepted at the WASI layer and forwarded to the transform
 * logging subsystem.
 *
 * That message is then packaged into an `event`, along with the source
 * node ID, a timestamp, and a log_level, the latter of which is determined
 * by whether the transform wrote to stdout (ss::log_level::info) or
 * stderr (ss::log_level::warn).
 *
 */
struct event {
    using clock_type = std::chrono::system_clock;
    event() = delete;
    explicit event(
      model::node_id source,
      clock_type::time_point ts,
      ss::log_level level,
      ss::sstring message);

    model::node_id source_id;
    clock_type::time_point ts;
    ss::log_level level;
    ss::sstring message;

    friend bool operator==(const event&, const event&) = default;

    /**
     * Serialize this event to JSON, with the result mapping directly to:
     * https://github.com/open-telemetry/opentelemetry-proto/blob/34d29fe5ad4689b5db0259d3750de2bfa195bc85/opentelemetry/proto/logs/v1/logs.proto#L134
     *
     * NOTE: We include a `name` for the attributes here, but the transform
     * name is omitted from the event itself to avoid duplicating it across
     * every log entry.
     */
    void to_json(model::transform_name_view name, iobuf&) const;
};
} // namespace transform::logging
