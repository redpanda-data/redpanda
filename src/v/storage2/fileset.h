#pragma once

#include "storage2/common.h"
#include "storage2/detail/index.h"
#include "storage2/indices.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/log.hh>

namespace storage {

/**
 * The directory layout of a log folder:
 *
 *  default/topic-one/0:
 *    - state.log           \
 *    - log.offset_ix     | -> fileset
 *    - log.timestamp_ix /
 *
 *    - 0-0-v1.log           \
 *    - 0-0-v1.offset_ix     | -> fileset
 *    - 0-0-v1.timestamp_ix /
 *
 *    - 1-0-v1.log           \
 *    - 1-0-v1.offsets_ix    | -> fileset
 *    - 1-1-v1.timestamp_ix /
 *    .
 *    .
 *    .
 *    - <sequential_id>-<term_id>-<format_version>.log
 *    - <sequential_id>-<term_id>-<format_version>.offset_ix
 *    - <sequential_id>-<term_id>-<format_version>.timestamp_ix
 *    - <sequential_id>-<term_id>-<format_version>.other_ix
 *
 * The concept of a fileset denotes the set of files that together make up
 * a segment file, which is the data log file + its indices (see segment_indices
 * in common.h).
 *
 * They are almost always opened and manipulated together as a unit, so this
 * header defines types that make it easier to access them as a unit.
 */

namespace detail { // helpers
template<typename T>
using ix_type = as_array_of<segment_indices, T>;
} // namespace detail

/**
 * This type represents a fileset with "live" open handles to
 * the underlying files. It cannot be instantiated directly, instead
 * it has to be created from a closed fileset. It's not safe to discard
 * this type without first invoking and completing close().
 */
class open_fileset {
public:
    using ix_type = detail::ix_type<file>;
    using ix_type_iterator = ix_type::iterator;
    using ix_type_const_iterator = ix_type::const_iterator;

private:
    /**
     * Don't use this, use closed_fileset::open instead to obtain
     * an instance of open_fileset.
     */
    template<typename It>
    open_fileset(file logfile, It auxb, It auxe);

public:
    open_fileset(open_fileset&& other) noexcept;
    ~open_fileset() noexcept;

    /**
     * A file handle to the data log file.
     */
    file& logfile();

    /**
     * A pair of iterators over the index files for the data log file.
     * These index files are in the same order as the segment_indices type.
     */
    ix_type_iterator begin();
    ix_type_iterator end();

    ix_type_const_iterator begin() const;
    ix_type_const_iterator end() const;

    /**
     * Asynchronously closes the log file and all its supporting index files.
     * Close involves an implicit flush for all files.
     */
    future<> close();

    /**
     * Used only in non-sealed segments, this method syncs buffers with disk.
     * Sync only the data log file not the aux indices.
     */
    future<> flush_logfile();

    /**
     * Used only in non-sealed segments, this method syncs buffers with disk.
     * Sync only the aux indices.
     */
    future<> flush_aux();

    /**
     * Used only in non-sealed segments, this method syncs buffers with disk.
     */
    future<> flush_all();

private:
    bool _closed{false};
    file _logfile;
    ix_type _ixfiles;
    friend class closed_fileset;
};

/**
 * This type describes information about a bundle of files that make a fileset.
 * This type uses no OS resources and is safe to discard. It holds a list of
 * paths that describe a log file and its indices. This type can be used to
 * create an instance of an open fileset.
 *
 * After construction this type is meant to be immutable.
 */
class closed_fileset {
public:
    using ix_type = detail::ix_type<std::filesystem::path>;
    using ix_iterator = ix_type::iterator;
    using ix_const_iterator = ix_type::const_iterator;

public:
    /**
     * Creates a closed_fileset instance for a segment file.
     * This constructor will also synthesize the file paths to all aux index
     * files for the segment files. The list of aux index files is generated
     * from the segment_indices type, that defines all active indices in the
     * system.
     */
    closed_fileset(std::filesystem::path);

    /**
     * Gets the file path of the main segment file of this fileset.
     */
    const std::filesystem::path& logfile() const;

    /**
     * A pair of iterators to the list of filenames of the indices associated
     * with the segment file.
     */
    ix_const_iterator begin() const;
    ix_const_iterator end() const;

    /**
     * Opens the fileset for reading or writing.
     *
     * This function will open the main segment file and all its aux index
     * files, create "live" handles and bundle them in an open_fileset type.
     */
    future<open_fileset> open() const;

    bool operator<(const closed_fileset& other) const;

private:
    std::filesystem::path _logfile;
    ix_type _ixfiles;
};

} // namespace storage
