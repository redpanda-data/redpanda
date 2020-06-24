#pragma once

#include "storage/compacted_index.h"
#include "storage/compacted_index_writer.h"
#include "storage/logger.h"
#include "units.h"

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <fmt/core.h>
#include <roaring/roaring.hh>

namespace storage::internal {

struct compaction_reducer {};

class truncation_offset_reducer : public compaction_reducer {
public:
    // offset to natural index mapping
    using underlying_t = absl::btree_map<model::offset, uint32_t>;

    ss::future<ss::stop_iteration> operator()(compacted_index::entry&&);
    Roaring end_of_stream();

private:
    uint32_t _natural_index = 0;
    underlying_t _indices;
};

class compaction_key_reducer : public compaction_reducer {
public:
    static constexpr const size_t default_max_memory_usage = 5_MiB;
    struct value_type {
        value_type(model::offset o, uint32_t i)
          : offset(o)
          , natural_index(i) {}
        model::offset offset;
        uint32_t natural_index;
    };
    using underlying_t
      = absl::flat_hash_map<bytes, value_type, bytes_type_hash, bytes_type_eq>;

    explicit compaction_key_reducer(
      // truncation_reduced are the entries *to keep* if nullopt - keep all
      // and do key-reduction
      std::optional<Roaring> truncation_reduced,
      size_t max_mem = default_max_memory_usage)
      : _to_keep(std::move(truncation_reduced))
      , _max_mem(max_mem) {}

    ss::future<ss::stop_iteration> operator()(compacted_index::entry&&);
    Roaring end_of_stream();

private:
    std::optional<Roaring> _to_keep;
    Roaring _inverted;
    underlying_t _indices;
    size_t _mem_usage{0};
    size_t _max_mem{0};
    uint32_t _natural_index{0};
};

/// This class copies the input reader into the writer consulting the bitmap of
/// wether ot keep the entry or not
class index_filtered_copy_reducer {
public:
    index_filtered_copy_reducer(Roaring b, compacted_index_writer& w)
      : _bm(std::move(b))
      , _writer(&w) {}

    ss::future<ss::stop_iteration> operator()(compacted_index::entry&& e) {
        using stop_t = ss::stop_iteration;
        const bool should_add = _bm.contains(_i);
        ++_i;
        if (should_add) {
            bytes_view bv = e.key;
            return _writer->index(bv, e.offset, e.delta)
              .then([k = std::move(e.key)] {
                  return ss::make_ready_future<stop_t>(stop_t::no);
              });
        }
        return ss::make_ready_future<stop_t>(stop_t::no);
    }
    void end_of_stream() {}

private:
    uint32_t _i = 0;
    Roaring _bm;
    compacted_index_writer* _writer;
};

} // namespace storage::internal
