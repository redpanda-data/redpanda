/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/kafka_probe.h"
#include "model/batch_compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timestamp.h"

#include <seastar/util/noncopyable_function.hh>

#include <chrono>
#include <exception>
#include <expected>
#include <functional>
#include <optional>

namespace kafka {

struct error_code_and_msg {
    error_code err;
    ss::sstring msg;
    /// Index within the batch of the record that caused the rejection, when
    /// the failure can be attributed to a single record. Reported to clients
    /// as RecordErrors[].BatchIndex in produce responses at v8+ (KIP-467).
    std::optional<int32_t> batch_index;
};

struct validation_args {
    model::record_batch& batch;
    model::timestamp_type timestamp_type;
    std::chrono::milliseconds message_timestamp_before_max_ms;
    std::chrono::milliseconds message_timestamp_after_max_ms;
    kafka::kafka_probe& probe;
    const model::ntp& ntp;
    std::optional<std::string_view> client_id;
};

struct validation_result {
    std::optional<error_code_and_msg> error;

    // Set iff the batch had to be decompressed in order to validate it.
    // Callers that need the batch's uncompressed records (e.g. for
    // broker-side recompression) can reuse this payload to avoid a second
    // decompression.
    std::optional<iobuf> decompressed_payload;
};

// Entry point for batch validation.
ss::future<validation_result> validate_batch(const validation_args&);

} // namespace kafka
