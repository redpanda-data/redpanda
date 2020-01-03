#include "model/fundamental.h"
#include "storage2/common.h"
#include "storage2/indices.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/util/log.hh>

#include <algorithm>
#include <stdexcept>

namespace storage {

static logger slog("s/offsets");

struct offset_ix_entry {
    file_offset pos;
    offset_index::bounds_type bounds;
};

template<>
class offset_index::impl {
public:
    using container_type = std::vector<offset_ix_entry>;
    using entries_iter = container_type::const_iterator;

public:
    impl(file f)
      : _ixfile(std::move(f))
      , _committed(_entries.end()) {}

    offset_index::bounds_type
    index(const model::record_batch& batch, file_offset fileoffset) {
        _entries.emplace_back(offset_ix_entry{
          .pos = fileoffset,
          .bounds = offset_index::bounds_type{
            .first = next_offset(),
            .last = next_offset() + model::offset(batch.size() - 1)}});
        return _entries.back().bounds;
    }

    /**
     * Given a logical record offset, this method will do an in-memory
     * binary search for the position of the record batch that contains
     * a record with the requested offset. It will throw if the offset
     * is not within the range of offsets held by this segment.
     */
    file_offset find(model::offset offset) const {
        auto const range = inclusive_range();
        if (offset < range.first || offset > range.last) {
            throw std::out_of_range("offset out of range");
        }

        // custom less-than comparision function for the binary search.
        // compares the requested offset with the base offset of each batch.
        auto comp = [](const offset_ix_entry& ix, const model::offset& o) {
            return ix.bounds.first < o;
        };

        // find the first index entry that marks a batch
        // with a base offset equal or greater than the
        // target offset.
        auto index_iter = std::lower_bound(
          _entries.begin(), _entries.end(), offset, comp);

        // we've found the first greater than, now scan backwards until the
        // target batch is found. This is done because offset may refer to a
        // record within a batch, so we are going to return the batch that
        // contains it. This loop will in most circumstances run for zero or
        // one iterations only.
        while (index_iter != _entries.begin()) {
            if (
              index_iter->bounds.first <= offset
              && index_iter->bounds.last >= offset) {
                break; // found the containing batch, stop.
            }
            --index_iter; // otherwise, keep scanning backwards
        }
        return index_iter->pos;
    }

    offset_index::bounds_type inclusive_range() const {
        return offset_index::bounds_type{.first = base_offset(),
                                         .last = last_offset()};
    }

private:
    model::offset base_offset() const { return _entries.front().bounds.first; }
    model::offset last_offset() const { return _entries.back().bounds.last; }
    model::offset next_offset() const {
        if (_entries.empty()) {
            return model::offset(0);
        } else {
            return last_offset() + model::offset(1);
        }
    }

private:
    file _ixfile;
    model::offset _nextoffset;
    container_type _entries;
    entries_iter _committed;
};

template<>
offset_index::segment_index(shared_ptr<offset_index::impl> ptr)
  : _impl(ptr) { }

template<>
offset_index::bounds_type offset_index::inclusive_range() const {
    return _impl->inclusive_range();
}

template<>
offset_index::bounds_type
offset_index::index(const model::record_batch& batch, file_offset fileoffset) {
    slog.trace("indexing batch at file offset {}", fileoffset);
    return _impl->index(batch, fileoffset);
}

template<>
file_offset offset_index::find(model::offset&& offset) const {
    return _impl->find(offset);
}

template<>
const char* offset_index::string_id() {
    return "offset_ix";
}

template<>
future<offset_index> offset_index::open(file ixfile) {
    return make_ready_future<offset_index>(offset_index(
      seastar::make_shared<offset_index::impl>(std::move(ixfile))));
}

} // namespace storage
