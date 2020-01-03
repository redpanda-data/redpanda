#include "model/timestamp.h"
#include "storage2/common.h"
#include "storage2/indices.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/util/log.hh>

namespace storage {

static logger slog("s/timestamps");

template<>
class timestamp_index::impl {
public:
    impl(file f)
      : _ixfile(std::move(f)) {}

private:
    file _ixfile;
};

template<>
timestamp_index::segment_index(shared_ptr<timestamp_index::impl> ptr)
  : _impl(ptr) {}

template<>
timestamp_index::bounds_type timestamp_index::inclusive_range() const {
    return timestamp_index::bounds_type{};
}

template<>
timestamp_index::bounds_type timestamp_index::index(
  const model::record_batch& batch, file_offset fileoffset) {
    slog.trace("indexing batch at file offset {}", fileoffset);
    return timestamp_index::bounds_type{.first = batch.first_timestamp(),
                                        .last = batch.max_timestamp()};
}

template<>
file_offset timestamp_index::find(model::timestamp&& ts) const {
    throw std::logic_error("not implemented");
}

template<>
const char* timestamp_index::string_id() {
    return "timestamp_ix";
}

template<>
future<timestamp_index> timestamp_index::open(file ixfile) {
    return make_ready_future<timestamp_index>(timestamp_index(
      seastar::make_shared<timestamp_index::impl>(std::move(ixfile))));
}

} // namespace storage
