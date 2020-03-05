#include "storage/lock_manager.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/rwlock.hh>

#include <stdexcept>

namespace storage {

static ss::future<std::unique_ptr<lock_manager::lease>>
range(segment_set::underlying_t segs) {
    auto ctx = std::make_unique<lock_manager::lease>(
      segment_set(std::move(segs)));
    auto raw = ctx.get();
    std::vector<ss::future<ss::rwlock::holder>> dispatch;
    dispatch.reserve(ctx->range.size());
    for (auto& s : ctx->range) {
        dispatch.emplace_back(s->read_lock());
    }
    return ss::when_all_succeed(dispatch.begin(), dispatch.end())
      .then(
        [ctx = std::move(ctx)](std::vector<ss::rwlock::holder> lks) mutable {
            ctx->locks = std::move(lks);
            return std::move(ctx);
        });
}

template<typename Iterator>
segment_set::underlying_t
inclusive_copy(Iterator begin, Iterator end, Iterator real_end) {
    segment_set::underlying_t tmp;
    // inclusive
    if (end != real_end) {
        end = std::next(end);
    }
    std::copy(begin, end, std::back_inserter(tmp));
    return tmp;
}

ss::future<std::unique_ptr<lock_manager::lease>>
lock_manager::range_lock(const timequery_config& cfg) {
    segment_set::underlying_t tmp;
    auto begin = _set.lower_bound(cfg.time);
    auto end = _set.lower_bound(cfg.max_offset);
    return range(inclusive_copy(begin, end, _set.end()));
}

ss::future<std::unique_ptr<lock_manager::lease>>
lock_manager::range_lock(const log_reader_config& cfg) {
    auto begin = _set.lower_bound(cfg.start_offset);
    auto end = _set.lower_bound(cfg.max_offset);
    return range(inclusive_copy(begin, end, _set.end()));
}

} // namespace storage
