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

#include <ankerl/unordered_dense.h>
#include <utils/fragmented_vector.h>

/**
 * @brief A hash map that uses a chunked vector as the underlying storage.
 *
 * Use when the hash map is expected to have a large number of elements (e.g.:
 * scales with partitions or topics). Performance wise it's equal to the abseil
 * hashmaps.
 *
 * NB: References and iterators are not stable across insertions and deletions.
 *
 * For more info please see
 * https://github.com/martinus/unordered_dense/?tab=readme-ov-file#1-overview
 */
template<
  typename Key,
  typename Value,
  typename Hash = ankerl::unordered_dense::hash<Key>,
  typename EqualTo = std::equal_to<Key>>
using chunked_hash_map = ankerl::unordered_dense::segmented_map<
  Key,
  Value,
  Hash,
  EqualTo,
  chunked_vector<std::pair<Key, Value>>,
  ankerl::unordered_dense::bucket_type::standard,
  chunked_vector<ankerl::unordered_dense::bucket_type::standard>>;
