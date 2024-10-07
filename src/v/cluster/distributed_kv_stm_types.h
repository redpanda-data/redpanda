/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/fwd.h"
#include "cluster/simple_batch_builder.h"
#include "model/fundamental.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/rw.h"

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>

#include <cstdint>

namespace cluster::distributed_kv_stm_types {

enum class record_type : int8_t {
    repartitioning = 0,
    coordinator_assignment = 1,
    kv_data = 2,
};

// Base record key for all records in data batches
// All record keys are prefixed with record_type that helps interpret
// the actual key and the value types.
// Factor in compaction overwrites when designing key data.
struct record_key {
    record_type type;
    iobuf key_data;
};

// A replication serializable wrapper struct with the actual
// data as serde. This is needed as record builder API only
// accepts KVs that are reflection compatible. So we just
struct record_value_wrapper {
    iobuf actual_value;
};

// --- repartitioning structs begin ---

// Record structure
//  - Record key: <record_type::repartition><empty key>
//  - Record value: repartitioning_record_data
// Key is empty as latest repartitioning record is what is needed.

struct repartitioning_record_data
  : serde::envelope<
      repartitioning_record_data,
      serde::version<0>,
      serde::compat_version<0>> {
    repartitioning_record_data() = default;
    explicit repartitioning_record_data(size_t partitions)
      : num_partitions(partitions) {}
    size_t num_partitions{};

    auto serde_fields() { return std::tie(num_partitions); }
};

inline simple_batch_builder make_repartitioning_batch(size_t num_partitions) {
    simple_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    iobuf val_data;
    serde::write(val_data, repartitioning_record_data{num_partitions});
    builder.add_kv(
      record_key{record_type::repartitioning, {}},
      record_value_wrapper{std::move(val_data)});
    return builder;
}

// --- repartitioning structs ---

// --- coordinator assignment structs begin ----

// Record structure
//  - record key :
//  <record_type::coordinator_assignment><coordinator_assignment_key>
//  - record value: <coordinator_assignment_data>]
// coordinator_assignment_key is a part of record key so that compaction ensures
// only keys with same key data can overwrite each other.

template<class Key>
struct coordinator_assignment_key
  : serde::envelope<
      coordinator_assignment_key<Key>,
      serde::version<0>,
      serde::compat_version<0>> {
    coordinator_assignment_key() = default;
    coordinator_assignment_key(Key k)
      : key(std::move(k)) {}

    Key key;

    auto serde_fields() { return std::tie(key); }
};

enum class coordinator_assignment_status : int8_t {
    assigned = 0,
    migration_in_progress = 1
};

struct coordinator_assignment_data
  : serde::envelope<
      coordinator_assignment_data,
      serde::version<0>,
      serde::compat_version<0>> {
    coordinator_assignment_data() = default;
    coordinator_assignment_data(
      model::partition_id p, coordinator_assignment_status s)
      : partition(p)
      , status(s) {}

    model::partition_id partition;
    coordinator_assignment_status status;

    auto serde_fields() { return std::tie(partition, status); }
};

template<class Key>
static simple_batch_builder make_coordinator_assignment_batch(
  Key k,
  model::partition_id coordinator,
  coordinator_assignment_status status) {
    simple_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));

    coordinator_assignment_key key{k};
    iobuf key_data;
    serde::write(key_data, std::move(key));
    coordinator_assignment_data val{coordinator, status};
    iobuf val_data;
    serde::write(val_data, val);

    builder.add_kv(
      record_key{record_type::coordinator_assignment, std::move(key_data)},
      record_value_wrapper{std::move(val_data)});

    return builder;
}

// --- coordinator assignment structs ----

// --- kv_data structs begin ---

// Record structure
//  - Record key: <record_type::kv_data><kv_data_key>
//  - Record value: <kv_data_value>
// This key structure ensures that during compaction only keys with same key
// data can overwrite each other.

template<class Key>
struct kv_data_key
  : serde::
      envelope<kv_data_key<Key>, serde::version<0>, serde::compat_version<0>> {
    kv_data_key() = default;
    explicit kv_data_key(Key k)
      : key(std::move(k)) {}

    Key key;
    auto serde_fields() { return std::tie(key); }
};

template<class Value>
struct kv_data_value
  : serde::envelope<
      kv_data_value<Value>,
      serde::version<0>,
      serde::compat_version<0>> {
    kv_data_value() = default;
    explicit kv_data_value(Value v)
      : value(std::move(v)) {}

    std::optional<Value> value;
    auto serde_fields() { return std::tie(value); }
};

template<class Key, class Value>
static simple_batch_builder
make_kv_data_batch(absl::btree_map<Key, Value> kvs) {
    simple_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    for (auto& [k, v] : kvs) {
        iobuf key_buf;
        serde::write(key_buf, kv_data_key<Key>{k});
        iobuf value_buf;
        serde::write(value_buf, kv_data_value<Value>{v});

        builder.add_kv(
          record_key{record_type::kv_data, std::move(key_buf)},
          record_value_wrapper{std::move(value_buf)});
    }
    return builder;
}

template<class Key, class Value>
static simple_batch_builder
make_kv_data_batch_remove_all(absl::btree_set<Key> ks) {
    simple_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    for (auto& k : ks) {
        iobuf key_buf;
        serde::write(key_buf, kv_data_key<Key>{k});
        iobuf value_buf;
        serde::write(value_buf, kv_data_value<Value>{});

        builder.add_kv(
          record_key{record_type::kv_data, std::move(key_buf)},
          record_value_wrapper{std::move(value_buf)});
    }
    return builder;
}

// --- kv_data structs ---
} // namespace cluster::distributed_kv_stm_types
