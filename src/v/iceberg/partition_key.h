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

#include "iceberg/partition.h"
#include "iceberg/struct_accessor.h"
#include "iceberg/values.h"

namespace iceberg {

// The struct value transformed from an original record by the transforms
// defined in a given partition spec. The high level relationship between the
// partition key and related abstractions is as follows:
//
//   schema   =>  struct_accessors
//   (fields)     (value getters)
//
//   struct_accessors + struct_val + partition_spec  => partition_key
//   (value getters)    (values)     (target fields)    (transformed values)
//                                   ( + transforms)
//
// The partition_key is expected to be serialized as an Avro record for field
// r102 of the manifest_entry.
struct partition_key {
    std::unique_ptr<struct_value> val;

    static partition_key create(
      const struct_value& source_struct,
      const struct_accessor::ids_accessor_map_t& accessors,
      const partition_spec& spec);

    partition_key copy() const;
};
bool operator==(const partition_key&, const partition_key&);

// Helper for legacy hourly partitioning. Assumes that the key contains an hour
// value.
int get_hour(const iceberg::partition_key&);

// Helper for daily partitioning. Assumes that the key contains a single
// `date_value` (days since epoch) as the first field.
int32_t get_day(const iceberg::partition_key&);

} // namespace iceberg

namespace std {

template<>
struct hash<iceberg::partition_key> {
    size_t operator()(const iceberg::partition_key& k) const {
        if (!k.val) {
            return 0;
        }
        return iceberg::value_hash(*k.val);
    }
};

} // namespace std
