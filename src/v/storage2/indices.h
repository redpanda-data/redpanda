#pragma once

#include "storage2/common.h"
#include "storage2/segment_index.h"

#include <seastar/core/future-util.hh>

namespace storage {

class open_fileset;

template<typename... Is>
class indices {
public:
    using indices_type = std::tuple<Is...>;
    using bounds_type = std::tuple<typename Is::bounds_type...>;
    using primary_index_type = std::tuple_element_t<0, indices_type>;

public:
    bounds_type inclusive_range() const {
        return seastar::tuple_map(
          instances(), [](auto& index) { return index.inclusive_range(); });
    }

    template<typename KeyType>
    auto inclusive_range() const {
        using bounds_type_for_key = key_bounds<KeyType>;
        return std::get<bounds_type_for_key>(inclusive_range());
    }

    /**
     * Given a record batch, it instructs all underlying
     * active indexers to consider adding it to the index
     * at segment file-offset pos.
     */
    bounds_type index(const model::record_batch& batch, file_offset pos) {
        return detail::tuple_map(instances(), [&batch, pos](auto& ix) mutable {
            return ix.index(batch, pos);
        });
    }

    /**
     * Closes all underlying index writers for this segment.
     *
     * Only writers need to be closed before destructing the
     * index, because readers are static in-memory objects
     * that are safe to just delete once loaded from disk.
     */
    future<> close() {
        std::vector<future<>> work;
        std::apply(
          [&work](auto&&... index) {
              ((work.emplace_back(index.writer.close()), ...));
          },
          instances());
        return when_all_succeed(work.begin(), work.end());
    }

    /**
     * Given a positioning type this function will pick the index type
     * that indexes by that type and return an input_stream that points at the
     * beginning of the record_batch that matches that position.
     */
    template<
      typename KeyType,
      typename = detail::is_indexable<KeyType, indices>>
    file_offset find(KeyType&& pos) const {
        return detail::index_by_key<KeyType>(instances())
          .find(std::forward<KeyType>(pos));
    }

    /**
     * Access to the underlying collection of individual indexers instances.
     */
    indices_type& instances() { return instances_; }
    const indices_type& instances() const { return instances_; }

    /**
     * A compile-time constant that species the number of indices in the current
     * indices collection. This is most often used to get the number of active
     * indices.
     */
    static constexpr size_t count() { return std::tuple_size_v<indices_type>; }

    /**
     * Returns a collection of all string ids of indices in the current indices
     * collection. This is most often used to synthesize an array of file
     * extensions for active indices.
     */
    static constexpr std::array<const char*, count()> index_names() {
        std::array<const char*, count()> output{Is::string_id()...};
        return output;
    }

    /**
     * Given a file path, returns a list of all supporting index files.
     *
     * This function is used to synthesize a list of filenames for a given
     * segment file that point to each of the defined indices. It is used for
     * example to get "a.offsets_idx" and "a.timestamps_idx" for "a.log".
     *
     * The returned list of file paths are in the same order as the index
     * definition in this indices collection.
     *
     * Todo: consider replacing stack-allocated array with vector
     * for better move semantics. Observe use cases first, atm array size == 2
     * && array elements are moved anyway, vector would be on the heap.
     */
    static auto files_for(std::filesystem::path file) {
        std::array<std::filesystem::path, count()> output{
          file.replace_extension(Is::string_id())...};
        return output;
    }

private:
    /**
     * For a given log segment, this method will create pairs
     * of index readers and writers for each known active
     * index type.
     *
     * This is where the initial IO happens for constructing
     * readers. Readers should not use the IO beyond this point.
     */
    static future<indices> open(open_fileset&);

private:
    indices(Is... instances);

private:
    indices_type instances_;
    friend class segment; // calls open(..) only a segment can open its index.
};

/**
 * This should be specialized further for more exotic indices
 * if they will be used as a primary key - quite unlikely for now.
 */
template<typename>
class index_key_traits;

template<>
class index_key_traits<model::offset> {
public:
    using key_type = model::offset;

public:
    /**
     * This is intentionally implemented temporarily now as an external
     * function, because incrementing the index needs to be handled more
     * generically and is not always possible. It is possible for monolithically
     * increasing continuos indices like model::offset, but not possible for
     * discrete indices like model::timestamp.
     */
    static key_type inline increment(const key_type& current) {
        return current + key_type(1);
    }

    /**
     * This is used to get the logical zero (first valid) value of a key.
     * Its easy for offsets, but each index has its own logic for defining
     * what a zero is.
     */
    static key_type inline zero() { return key_type(0); }
};

} // namespace storage