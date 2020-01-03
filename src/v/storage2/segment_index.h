#pragma once

#include "detail/index.h"
#include "model/record.h"
#include "storage2/common.h"
#include "utils/concepts-enabled.h"
#include "utils/named_type.h"

#include <seastar/core/file.hh>
#include <seastar/util/tuple_utils.hh>

namespace storage {

// fwds:
class segment;
template<typename...>
class indices;

template<typename KeyType>
class segment_index {
public:
    /**
     * A less-than comparable type that is used for indexing
     * data within the log segment. examples include: model::offset,
     * model::timestamp, etc.
     */
    using key_type = KeyType;
    using bounds_type = key_bounds<key_type>;

    /**
     * A short string identifier for the index type.
     * This string is used wherever a human-readable form of
     * the index is needed, like file extension, logging, etc.
     * This identifier mus be unique across all active indices.
     */
    static const char* string_id();

    /**
     * Returns the values assigned to the first and last record in the segment.
     */
    bounds_type inclusive_range() const;

    /**
     * Returns the byte offset from the begining of the segment file
     * to the batch header that matches the search key.
     *
     * The returned file offset is used as an argument to position the
     * input stream returned from data_stream() on the segment.
     *
     * If the key is not found, then as a fallback it will return zero,
     * and it will open a stream to the segment file from its begining
     * and the sequential scan logic will kick in.
     */
    file_offset find(key_type&&) const;

    /**
     * Tells the index that a batch is being appended to the segment file
     * at a given file offset. The index may chose to include that batch
     * in its list of indexed batches.
     */
    bounds_type index(const model::record_batch&, file_offset);

    /**
     * Synchronizes changes to the index to the disk.
     *
     * This call will may also create a compacted version
     * of the index and write it as the last record if
     * any trim/delete operations happened.
     */
    future<append_result> flush();

    /**
     * Implicit flush + close file handle + delete index memory.
     * Any further calls to find() or index() will
     * throw an exception
     */
    future<append_result> close();

private:
    /**
     * Asynchronously opens a given type of index for a segment.
     * This static function is invoked by the segment class during
     * its initialization. End users of the API should not create instances
     * of the index directly, instead access them through the segment class.
     */
    static future<segment_index<key_type>> open(file);

private:
    class impl;
    shared_ptr<impl> _impl;
    segment_index(shared_ptr<impl>);
    template<typename...>
    friend class indices;
};

} // namespace storage