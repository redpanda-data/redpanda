/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/client/types.h"
#include "model/fundamental.h"

namespace kafka::client {

class partitioner_impl {
public:
    partitioner_impl() = default;
    partitioner_impl(const partitioner_impl&) = delete;
    partitioner_impl(partitioner_impl&&) = default;
    partitioner_impl& operator=(const partitioner_impl&) = delete;
    partitioner_impl& operator=(partitioner_impl&&) = delete;
    virtual ~partitioner_impl() = default;

    virtual std::optional<model::partition_id>
    operator()(const record_essence&, size_t partition_count) = 0;
};

class partitioner {
public:
    partitioner() = default;
    explicit partitioner(std::unique_ptr<partitioner_impl> impl)
      : _impl(std::move(impl)) {}

    explicit operator bool() const { return bool(_impl); }

    std::optional<model::partition_id>
    operator()(const record_essence& rec, size_t partition_count) {
        return (*_impl)(rec, partition_count);
    }

private:
    std::unique_ptr<partitioner_impl> _impl;
};

/// \brief Returns the partition_id in the record
partitioner identity_partitioner();

/// \brief Returns the murmur2 hash of the key,
/// or nullopt if there is no key or the key is empty
partitioner murmur2_key_partitioner();

/// \brief Returns the partition_id in round-robin fashion, starting from
/// \ref initial
partitioner roundrobin_partitioner(model::partition_id initial);

/// \brief Returns the partition_id if one exists in the record, or,
/// returns the murmer2 hash of the key if there is one, or,
/// returns partition_id based on round-robin.
partitioner default_partitioner(model::partition_id initial);

} // namespace kafka::client
