/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/outcome.h"
#include "datalake/base_types.h"
#include "iceberg/partition_key.h"
namespace datalake {
/**
 * Error in partition key to path conversion
 */
class partition_key_error : public std::exception {
public:
    explicit partition_key_error(std::string msg) noexcept
      : msg_(std::move(msg)) {}

    const char* what() const noexcept final { return msg_.c_str(); }

private:
    std::string msg_;
};

/**
 * Converts a partition key value described by the partition spec to an object
 * store path.
 *
 * The value is converted to the string representation based on the transform
 * type and the key value. The path is constructed by concatenating the key
 * fields in the form of: <field_name>=<field_value> subsequent fields are
 * separated with '/'.
 *
 * The value to string conversion is not described explicitly in the Iceberg
 * spec. The conversion is based on the reference implementation:
 *
 * https://github.com/apache/iceberg/blob/fad0c1e6c68fbc7e48b5b17c02ed9c26a2693afb/api/src/main/java/org/apache/iceberg/PartitionSpec.java#L206
 *
 * The conversion is done based on the following rules:
 * - Identity and truncate transform result values are rendered based on the
 *   source field type
 * - Bucket transform value is rendered as an integer
 * - Time transform values are rendered as a string representing date/time
 *
 * Returned path elements are url encoded.
 */
checked<remote_path, partition_key_error> partition_key_to_path(
  const iceberg::partition_spec& spec, const iceberg::partition_key& key);
} // namespace datalake
