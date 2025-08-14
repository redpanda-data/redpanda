/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "bytes/bytes.h"
#include "container/chunked_vector.h"
#include "serde/envelope.h"
#include "serde/rw/bytes.h"

#include <seastar/core/sstring.hh>

#include <fmt/core.h>

namespace datalake::coordinator {

// Represents a file that exists in object storage.
struct data_file
  : serde::envelope<data_file, serde::version<1>, serde::compat_version<0>> {
    auto serde_fields() {
        return std::tie(
          remote_path,
          row_count,
          file_size_bytes,
          hour_deprecated,
          table_schema_id,
          partition_spec_id,
          partition_key);
    }
    ss::sstring remote_path = "";
    size_t row_count = 0;
    size_t file_size_bytes = 0;
    // After datalake_iceberg_ga feature flag is enabled, this field won't be
    // set. Use `partition_key` instead.
    int hour_deprecated = 0;

    int32_t table_schema_id = -1;
    int32_t partition_spec_id = -1;
    // Contains partition key fields serialized with Iceberg "binary
    // single-value serialization" (see iceberg/values_bytes.h).
    // Nulls are represented by std::nullopt.
    chunked_vector<std::optional<bytes>> partition_key;
    // TODO: add kafka schema id

    data_file copy() const {
        return {
          .remote_path = remote_path,
          .row_count = row_count,
          .file_size_bytes = file_size_bytes,
          .hour_deprecated = hour_deprecated,
          .table_schema_id = table_schema_id,
          .partition_spec_id = partition_spec_id,
          .partition_key = partition_key.copy(),
        };
    }

    friend bool operator==(const data_file&, const data_file&) = default;
};

std::ostream& operator<<(std::ostream& o, const data_file& f);

} // namespace datalake::coordinator
