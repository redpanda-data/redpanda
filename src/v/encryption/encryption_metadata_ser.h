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
#include "bytes/iobuf.h"
#include "encryption/types.h"
#include "model/record.h"

#include <seastar/core/future.hh>

#include <optional>
#include <string_view>

namespace encryption {

/// Header key used in Kafka record headers.
inline constexpr std::string_view encryption_header_key = "rp.encryption";

/// Serialize a dek_set into a Protobuf-encoded iobuf suitable for
/// use as a record header value.
ss::future<iobuf> serialize_encryption_metadata(const dek_set& deks);

/// Deserialize an rp.encryption header value back to a dek_set.
ss::future<dek_set> deserialize_encryption_metadata(iobuf buf);

/// Inject rp.encryption headers into a record batch.
/// First record gets the full header; subsequent records get a
/// sentinel (empty value) indicating the DEK should be inherited.
ss::future<model::record_batch>
inject_encryption_headers(model::record_batch batch, const dek_set& deks);

/// Extract the dek_set from a record batch by scanning for the
/// rp.encryption header (typically on the first record).
ss::future<std::optional<dek_set>>
extract_encryption_metadata(const model::record_batch& batch);

} // namespace encryption
