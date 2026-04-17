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

#include "encryption/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <avro/Schema.hh>

#include <memory>
#include <variant>
#include <vector>

namespace google::protobuf {
class Descriptor;
} // namespace google::protobuf

namespace encryption {

enum class schema_format { avro, protobuf, json };

/// Metadata about a field that should be encrypted.
struct tagged_field {
    std::vector<ss::sstring> path;
    ss::sstring tag;
    ss::sstring kek_name;
};

/// Handle to the compiled schema object needed for parsing.
using schema_handle = std::variant<
  std::shared_ptr<::avro::ValidSchema>,
  const google::protobuf::Descriptor*,
  std::monostate>;

/// Schema + encryption rules resolved for a subject.
struct encryption_schema {
    schema_format format;
    schema_handle handle;
    std::vector<tagged_field> tagged_fields;
};

class field_transformer {
public:
    field_transformer() = default;
    virtual ~field_transformer() noexcept = default;

    field_transformer(const field_transformer&) = delete;
    field_transformer& operator=(const field_transformer&) = delete;
    field_transformer(field_transformer&&) = default;
    field_transformer& operator=(field_transformer&&) = default;

    /// Transform a single record value: deserialize, encrypt tagged
    /// fields, re-serialize. Returns the modified value iobuf.
    virtual ss::future<iobuf> transform(
      iobuf value, const encryption_schema& schema, const dek_set& deks) = 0;
};

} // namespace encryption
