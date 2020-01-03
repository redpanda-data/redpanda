#pragma once

#include "model/fundamental.h"
#include "model/timestamp.h"
#include "storage2/vassert.h"

namespace storage {

/**
 * A generic type that denotes an inclusive range of
 * offsets of a certain key type (i.e. model::offset or timestamps).
 */
template<typename KeyType>
struct key_bounds {
    KeyType first;
    KeyType last;
};

/**
 * This type is returned after each partition.append() operation.
 * It specifies the inclusive range of offsets assigned to the batch and its
 * records.
 */
using append_result = key_bounds<model::offset>;

/**
 * This type is returned after each partition.trim operation,
 * It describes the inclusive range of offsets that were removed.
 */
using trim_result = key_bounds<model::offset>;

/**
 * This type represents the raw byte offset within a file.
 */
using file_offset = named_type<int64_t, struct file_offset_tag>;

/**
 * This type is returned after partition::flush operation.
 * It tells what was the last dirty offset before the flush
 * and what is the last committed offset after the flush.
 * The number of flushed records is (dirty - committed).
 */
using flush_result = key_bounds<model::offset>;

template<typename>
class segment_index;

template<typename...>
class indices;

using offset_index = segment_index<model::offset>;
using timestamp_index = segment_index<model::timestamp>;

/**
 * This type defines the list of all indexers that are enabled for segments
 * and automatically for partitions. This list is used quite extensively
 * throughout the codebase when performaing index-related operations. For
 * example when we want to add a batch to the index type is used to collectively
 * index it on all enabled/active indices. The index at position 0 has a special
 * status of being the primary index. The primary index is used for deciding on
 * the filename, the append_result types, flush_results, etc.
 */
using segment_indices
  = indices<segment_index<model::offset>, segment_index<model::timestamp>>;

/**
 * The base exception type for all storage related errors.
 */
class storage_error : public std::exception {
public:
    storage_error(sstring msg)
      : _msg(msg) {}
    const char* what() const noexcept { return _msg.data(); }

private:
    sstring _msg;
};

class segment_error : public storage_error {
public:
    segment_error(sstring m)
      : storage_error(std::move(m)) {}
};

class record_batch_error : public storage_error {
public:
    record_batch_error(sstring m)
      : storage_error(std::move(m)) {}
};

} // namespace storage