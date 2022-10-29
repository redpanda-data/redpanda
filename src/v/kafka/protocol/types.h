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
#include "absl/container/btree_map.h"
#include "bytes/bytes.h"
#include "kafka/protocol/fwd.h"
#include "kafka/types.h"
#include "utils/named_type.h"

namespace kafka {

/// Types for tags and tagged fields
/// These structures are used for encoding / decoding flexible requests
/// Additional metadata is allowed to be stored in this dynamic structure
using tag_id = named_type<uint32_t, struct tag_id_type>;

using tagged_fields
  = named_type<absl::btree_map<tag_id, bytes>, struct tagged_fields_type>;

/// Used to signify if a kafka request will never be interpreted as flexible.
/// Consumed by our generator and flexible method helpers.
///
/// The only request that is never flexible is sasl_handshake_request - 17.
/// Older versions of schemas may also contain values of 'none' that map to -1
static constexpr api_version never_flexible = api_version(-1);

template<typename T>
concept KafkaApi = requires(T request) {
    { T::name } -> std::convertible_to<const char*>;
    { T::key } -> std::convertible_to<const api_key&>;
    { T::min_flexible } -> std::convertible_to<const api_version&>;
};

} // namespace kafka
