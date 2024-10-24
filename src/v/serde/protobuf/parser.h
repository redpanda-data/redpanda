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

#pragma once

#include "bytes/hash.h"
#include "bytes/iobuf.h"
#include "container/chunked_hash_map.h"
#include "container/fragmented_vector.h"

#include <cstdint>

namespace google::protobuf {
class Descriptor;
} // namespace google::protobuf

namespace serde::pb {

namespace parsed {

struct message;

/**
 * A dynamic representation of a repeated field in a protobuf message.
 */
struct repeated {
    std::variant<
      chunked_vector<double>,
      chunked_vector<float>,
      chunked_vector<int32_t>, // Can be an enum value
      chunked_vector<int64_t>,
      chunked_vector<uint32_t>,
      chunked_vector<uint64_t>,
      chunked_vector<bool>,
      chunked_vector<iobuf>, // string or bytes
      chunked_vector<std::unique_ptr<message>>>
      elements;
};

/**
 * A dynamic representation of a proto3 map.
 */
struct map {
    using key = std::variant<
      std::monostate, // Can be unset
      double,
      float,
      int32_t,
      int64_t,
      uint32_t,
      uint64_t,
      bool,
      iobuf>;

    using value = std::variant<
      std::monostate, // Can be unset
      double,
      float,
      int32_t, // Can be an enum value
      int64_t,
      uint32_t,
      uint64_t,
      bool,
      iobuf, // string or bytes
      std::unique_ptr<message>>;

    // Technically every key and value here are the same type, but that would be
    // 72 different combinations of maps here, which seems a bit extreme.
    chunked_hash_map<key, value> entries;
};

/**
 * A dynamic representation of a message. Supports everything except deprecated
 * proto2 groups and the unknown fields that the offical protobuf library
 * supports.
 */
struct message {
    using field = std::variant<
      double,
      float,
      int32_t, // Can be an enum value
      int64_t,
      uint32_t,
      uint64_t,
      bool,
      iobuf, // string or bytes
      std::unique_ptr<message>,
      repeated,
      map>;

    // Fields of the message keyed by their field number.
    chunked_hash_map<int32_t, field> fields;
};

} // namespace parsed

/**
 * Parse a protobuf from the given bytes using the descriptor provided.
 *
 * The returned message is a dynamic representation of the protocol buffer, and
 * the descriptor will still be needed to fully discern the types of the
 * protobuf. The returned message in the case of strings or bytes fields will
 * hold a reference (via share) to the original input data iobuf.
 *
 * Messages must be fully accumulated (so no streaming parsing) due to last
 * field wins semantics within the protocol buffer specification.
 *
 * Throws if there are invalid messages.
 */
ss::future<std::unique_ptr<parsed::message>>
parse(iobuf, const google::protobuf::Descriptor&);

} // namespace serde::pb
