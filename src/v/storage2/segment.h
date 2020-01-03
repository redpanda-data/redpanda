#pragma once

#include "model/record.h"
#include "storage2/common.h"
#include "storage2/fileset.h"
#include "utils/named_type.h"

#include <seastar/core/file.hh>
#include <seastar/core/iostream.hh>

namespace storage {

/**
 * Provides low-level access to log segment files.
 *
 * This class exposes reader and writer interfaces to the
 * underlying segment file on a byte level.
 */
class segment {
public:
    /**
     * Given an index type, this function will return the
     * inclusive first and inclusive last stored record key.
     * currently its offsets, timestamps, and file_offset.
     *
     * So for example:
     *  - file size = bounds<file_offset>().last,
     *  - base offset: bounds<model::offset>().first
     *  - max inclusive offset = bounds<model::offset>().last
     *  - base timestamp = bounds<model::timestamp>().first
     *  - last timestamp = bounds<model::timestamp().last
     *
     * This method is cheap because it uses the in-memory index for
     * resolving this range.
     */
    template<typename Key>
    key_bounds<Key> inclusive_range() const;

    /**
     * Returns an input stream positioned at wherever the Key type
     * resolves from the index. The semantics of resolving the key
     * are similar to std::multiset::lower_bound().
     *
     * Possible values of Key are:
     *
     *  - file_offset, positions the open stream at the exact byte
     *    offset from the beginning of the segment file. This is the
     *    base of all other read(...) calls, as they all will translate
     *    the index key to a file offset and then return the stream.
     *
     *  - model::offset, positions the open stream at the record batch
     *    with the given offset. If the offset happens to be in the middle
     *    of a batch, then the stream will be positioned at the beginning
     *    of the batch that contains that offset.
     *
     *  - model::timestamp, positions the input stream at the first record
     *    batch that is not less then the given timestamp (see
     *    multiset::lower_bound())
     */
    template<typename Key>
    seastar::input_stream<char> read(Key key) const;

    /**
     * Writes the record batch at the end of the segment file.
     *
     * Along with writing to the log file, this method will also
     * include the record batch in all enabled indices.
     */
    future<append_result> append(model::record_batch&& batch);

    /**
     * Fsyncs the segment and all its indices to disk.
     */
    future<flush_result> flush();

    /**
     * If a segment file is sealed, it won't accept any further
     * appends, and any append attempt will result in an exception.
     *
     * All segments are sealed except the current active segment.
     */
    bool is_sealed() const;

    /**
     * Gets the current size of file including data pending flush.
     *
     * For sealed segments, this value is retrieved during construction
     * and never changes. For the active segment, this value is retrieved from
     * the segment appender.
     */
    file_offset size_bytes() const;

    /**
     * Closes all open file handles to the segment log file and
     * its indices. For sealed segments this is only closes the segment
     * log file, because the index files are already closed during construction
     * after loading the index in memory.
     */
    future<> close();

private:
    /**
     * Called on an active segments and closes it for writing.
     * After this method is invoked no further appends are possible on this
     * segments in the future.
     */
    future<flush_result> seal();

    /**
     * Returns a read-only view of all the indexing types that are supported
     * by this segment.
     */
    const segment_indices& indices() const;

    /**
     * This function is used for existing old segments and segments instantiated
     * using this function are always open in sealed mode, meaning no new
     * records can be appended to them. It is expected that the segment file and
     * its indices exist on disk.
     */
    static future<segment> open(closed_fileset, io_priority_class);

    /**
     * This function is used for creating new segments in non-sealed/active
     * modes. Segments in this mode accept new records and their indices change
     * as new entries are added to the log. It is expected that the segment file
     * and its indices do not exist on disk prior to calling this function.
     */
    static future<segment> create(closed_fileset, io_priority_class);

private:
    class impl;
    shared_ptr<impl> _impl;
    segment(shared_ptr<impl>);
    friend class partition;
};

} // namespace storage
