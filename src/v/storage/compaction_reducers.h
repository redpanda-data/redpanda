#pragma once

#include "model/record_batch_reader.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_writer.h"
#include "storage/compacted_offset_list.h"
#include "storage/index_state.h"
#include "storage/logger.h"
#include "storage/segment_appender.h"
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
class index_filtered_copy_reducer : public compaction_reducer {
public:
    index_filtered_copy_reducer(Roaring b, compacted_index_writer& w)
      : _bm(std::move(b))
      , _writer(&w) {}

    ss::future<ss::stop_iteration> operator()(compacted_index::entry&&);
    void end_of_stream() {}

private:
    uint32_t _natural_index = 0;
    Roaring _bm;
    compacted_index_writer* _writer;
};

class compacted_offset_list_reducer : public compaction_reducer {
public:
    explicit compacted_offset_list_reducer(model::offset base)
      : _list(base, Roaring{}) {}

    ss::future<ss::stop_iteration> operator()(compacted_index::entry&&);
    compacted_offset_list end_of_stream() { return std::move(_list); }

private:
    compacted_offset_list _list;
};

class copy_data_segment_reducer : public compaction_reducer {
public:
    copy_data_segment_reducer(compacted_offset_list l, segment_appender* a)
      : _list(std::move(l))
      , _appender(a) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch&&);
    storage::index_state end_of_stream() { return std::move(_idx); }

private:
    ss::future<ss::stop_iteration> do_compaction(model::record_batch&&);

    bool should_keep(model::offset base, int32_t delta) const {
        const auto o = base + model::offset(delta);
        return _list.contains(o);
    }
    std::optional<model::record_batch> filter(model::record_batch&&);

    compacted_offset_list _list;
    segment_appender* _appender;
    index_state _idx;
    size_t _acc{0};
};

class index_rebuilder_reducer : public compaction_reducer {
public:
    explicit index_rebuilder_reducer(compacted_index_writer* w) noexcept
      : _w(w) {}
    ss::future<ss::stop_iteration> operator()(model::record_batch&&);
    void end_of_stream() {}

private:
    ss::future<> do_index(model::record_batch&&);

    compacted_index_writer* _w;
};

} // namespace storage::internal
