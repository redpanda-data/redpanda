#include "storage/lock_manager.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/shared_ptr.hh>

#include <stdexcept>

namespace storage {

static ss::future<std::unique_ptr<lock_manager::lease>>
range(segment_set::underlying_t segs) {
    auto ctx = std::make_unique<lock_manager::lease>(
      segment_set(std::move(segs)));
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

ss::future<std::unique_ptr<lock_manager::lease>>
lock_manager::range_lock(const timequery_config& cfg) {
    segment_set::underlying_t tmp;
    std::copy_if(
      _set.lower_bound(cfg.time),
      _set.end(),
      std::back_inserter(tmp),
      [&cfg](ss::lw_shared_ptr<segment>& s) {
          // must be base offset
          return s->reader().base_offset() <= cfg.max_offset;
      });
    return range(std::move(tmp));
}

ss::future<std::unique_ptr<lock_manager::lease>>
lock_manager::range_lock(const log_reader_config& cfg) {
    segment_set::underlying_t tmp;
    std::copy_if(
      _set.lower_bound(cfg.start_offset),
      _set.end(),
      std::back_inserter(tmp),
      [&cfg](ss::lw_shared_ptr<segment>& s) {
          // must be base offset
          return s->reader().base_offset() <= cfg.max_offset;
      });
    return range(std::move(tmp));
}

std::ostream& operator<<(std::ostream& o, const lock_manager::lease& l) {
    fmt::print(o, "({})", l.range);
    return o;
}

} // namespace storage
