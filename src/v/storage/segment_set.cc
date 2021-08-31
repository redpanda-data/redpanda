// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment_set.h"

#include "storage/fs_utils.h"
#include "storage/log_replayer.h"
#include "storage/logger.h"
#include "utils/directory_walker.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>

#include <fmt/format.h>

#include <exception>

namespace storage {
struct segment_ordering {
    using type = ss::lw_shared_ptr<segment>;
    bool operator()(const type& seg1, const type& seg2) const {
        return seg1->offsets().base_offset < seg2->offsets().base_offset;
    }
    bool operator()(const type& seg, model::offset value) const {
        return seg->offsets().dirty_offset < value;
    }
    bool operator()(const type& seg, model::timestamp value) const {
        return seg->index().max_timestamp() < value;
    }
};

segment_set::segment_set(segment_set::underlying_t segs)
  : _handles(std::move(segs)) {
    std::sort(_handles.begin(), _handles.end(), segment_ordering{});
}

void segment_set::add(ss::lw_shared_ptr<segment> h) {
    if (!_handles.empty()) {
        vassert(
          h->offsets().base_offset > _handles.back()->offsets().dirty_offset,
          "New segments must be monotonically increasing. Assertion failure: "
          "({} > {}) Got:{} - Current:{}",
          h->offsets().base_offset,
          _handles.back()->offsets().dirty_offset,
          *h,
          *this);
    }
    _handles.emplace_back(std::move(h));
}

void segment_set::pop_back() { _handles.pop_back(); }
void segment_set::pop_front() { _handles.pop_front(); }
void segment_set::erase(iterator begin, iterator end) {
    _handles.erase(begin, end);
}

template<typename Iterator>
struct needle_in_range {
    bool operator()(Iterator ptr, model::offset o) {
        auto& s = **ptr;
        if (s.empty()) {
            return false;
        }
        // must use max_offset
        return o <= s.offsets().dirty_offset && o >= s.offsets().base_offset;
    }

    bool operator()(Iterator ptr, model::timestamp t) {
        auto& s = **ptr;
        if (s.empty()) {
            return false;
        }
        // must use max_offset
        return t <= s.index().max_timestamp()
               && t >= s.index().base_timestamp();
    }
};

template<typename Iterator, typename Needle>
Iterator segments_lower_bound(Iterator begin, Iterator end, Needle needle) {
    if (std::distance(begin, end) == 0) {
        return end;
    }
    auto it = std::lower_bound(begin, end, needle, segment_ordering{});
    if (it == end) {
        it = std::prev(it);
    }
    if (needle_in_range<Iterator>()(it, needle)) {
        return it;
    }
    if (std::distance(begin, it) > 0) {
        it = std::prev(it);
    }
    if (needle_in_range<Iterator>()(it, needle)) {
        return it;
    }
    return end;
}

/// lower_bound returns the element that is _strictly_ greater than or equal
/// to bucket->dirty_offset() - see comparator above because our offsets are
/// _inclusive_ we must check the previous iterator in the case that we are at
/// the end, we also check the last element.
segment_set::iterator segment_set::lower_bound(model::offset offset) {
    return segments_lower_bound(
      std::begin(_handles), std::end(_handles), offset);
}

segment_set::const_iterator
segment_set::lower_bound(model::offset offset) const {
    return segments_lower_bound(
      std::cbegin(_handles), std::cend(_handles), offset);
}
// Lower bound for timestamp based indexing
//
// From KIP-33:
//
// When searching by timestamp, broker will start from the earliest log segment
// and check the last time index entry. If the timestamp of the last time index
// entry is greater than the target timestamp, the broker will do binary search
// on that time index to find the closest index entry and scan the log from
// there. Otherwise it will move on to the next log segment.
segment_set::iterator segment_set::lower_bound(model::timestamp needle) {
    return std::lower_bound(
      std::begin(_handles), std::end(_handles), needle, segment_ordering{});
}

segment_set::const_iterator
segment_set::lower_bound(model::timestamp needle) const {
    return segments_lower_bound(
      std::cbegin(_handles), std::cend(_handles), needle);
}

std::ostream& operator<<(std::ostream& o, const segment_set& s) {
    o << "{size: " << s.size() << ", [";
    for (auto& p : s) {
        o << p;
    }
    return o << "]}";
}

// Recover the last segment. Whenever we close a segment, we will likely
// open a new one to which we will direct new writes. That new segment
// might be empty. To optimize log replay, implement #140.
static ss::future<segment_set>
unsafe_do_recover(segment_set&& segments, ss::abort_source& as) {
    return ss::async([segments = std::move(segments), &as]() mutable {
        if (segments.empty() || as.abort_requested()) {
            return std::move(segments);
        }
        segment_set::underlying_t good = std::move(segments).release();
        segment_set::underlying_t to_recover;
        to_recover.push_back(std::move(good.back()));
        good.pop_back(); // always recover last segment
        // keep segments sorted
        auto good_end = std::stable_partition(
          good.begin(), good.end(), [](ss::lw_shared_ptr<segment>& ss) {
              auto& s = *ss;
              try {
                  // use the segment materialize instead of going through
                  // the index directly to hydrate the max_offset state
                  return s.materialize_index().get0();
              } catch (...) {
                  vlog(
                    stlog.info,
                    "Error materializing index:{}. Recovering parent "
                    "segment:{}. Details:{}",
                    s.index().filename(),
                    s.reader().filename(),
                    std::current_exception());
              }
              return false;
          });
        std::move(
          std::move_iterator(good_end),
          std::move_iterator(good.end()),
          std::back_inserter(to_recover));
        good.erase(
          good_end,
          good.end()); // remove all the ones we copied into recover

        // Validate that the segments' indices do not claim to
        // have overlapping offset ranges.  This is a form of corruption
        // that can result from bugs when writing the segments+indices.
        std::optional<segment_set::type> prev_seg = std::nullopt;
        for (auto& seg : good) {
            if (prev_seg.has_value()) {
                auto prev = prev_seg.value();
                if (seg->index().base_offset() <= prev->index().max_offset()) {
                    vlog(
                      stlog.error,
                      "Index range conflict.  Indices: \n  {}\n  {}",
                      prev->index(),
                      seg->index());
                }
            }
            prev_seg = seg;
        }

        // remove empty segments
        auto non_empty_end = std::stable_partition(
          to_recover.begin(),
          to_recover.end(),
          [](ss::lw_shared_ptr<segment>& segment) {
              auto stat = segment->reader().stat().get0();
              if (stat.st_size != 0) {
                  return true;
              }
              vlog(stlog.info, "Removing empty segment: {}", segment);
              segment->close().get();
              ss::remove_file(segment->reader().filename()).get();
              ss::remove_file(segment->index().filename()).get();
              return false;
          });
        // remove empty from to recover set
        to_recover.erase(non_empty_end, to_recover.end());
        // we left with nothing to recover, take the last good segment if
        // available
        if (to_recover.empty() && !good.empty()) {
            to_recover.push_back(std::move(good.back()));
            good.pop_back();
        }

        for (auto& s : to_recover) {
            // check for abort
            if (unlikely(as.abort_requested())) {
                return segment_set(std::move(good));
            }
            auto replayer = log_replayer(*s);
            auto recovered = replayer.recover_in_thread(
              ss::default_priority_class());
            if (!recovered) {
                vlog(stlog.info, "Unable to recover segment: {}", s);
                s->close().get();
                ss::rename_file(
                  s->reader().filename(),
                  s->reader().filename() + ".cannotrecover")
                  .get();
                continue;
            }
            s->truncate(
               recovered.last_offset.value(),
               recovered.truncate_file_pos.value())
              .get();
            // persist index
            s->index().flush().get();
            vlog(stlog.info, "Recovered: {}", s);
            good.emplace_back(std::move(s));
        }
        return segment_set(std::move(good));
    });
}

static ss::future<segment_set>
do_recover(segment_set&& segments, ss::abort_source& as) {
    // light-weight copy used for clean-up if recovery fails
    segment_set::underlying_t copy;
    copy.reserve(segments.size());
    std::copy(segments.cbegin(), segments.cend(), std::back_inserter(copy));

    // if an exception occurs during recovery close all the segments that are
    // still open and then return the original exception. the issue we want to
    // avoid is destroying a segment without first closing it because if there
    // are any pending io operations on a file associated with the segment
    // at the time of destruction seastar will complain about the file handle
    // being destroyed with pending ops.
    return unsafe_do_recover(std::move(segments), as)
      .handle_exception(
        [copy = std::move(copy)](const std::exception_ptr& ex) mutable {
            return ss::do_with(
              std::move(copy), [ex](segment_set::underlying_t& segments) {
                  return ss::parallel_for_each(
                           segments,
                           [](segment_set::type& segment) {
                               if (segment && !segment->is_closed()) {
                                   return segment->close();
                               }
                               return ss::now();
                           })
                    .then([ex] {
                        return ss::make_exception_future<segment_set>(ex);
                    });
              });
        });
}

/**
 * \brief Open all segments in a directory.
 *
 * Returns an exceptional future if any error occured opening a
 * segment. Otherwise all open segment readers are returned.
 */
static ss::future<segment_set::underlying_t> open_segments(
  ss::sstring dir,
  debug_sanitize_files sanitize_fileops,
  std::function<std::optional<batch_cache_index>()> cache_factory,
  ss::abort_source& as) {
    using segs_type = segment_set::underlying_t;
    return ss::do_with(
      segs_type{},
      [&as, cache_factory, sanitize_fileops, dir = std::move(dir)](
        segs_type& segs) {
          auto f = directory_walker::walk(
            dir,
            [&as, cache_factory, dir, sanitize_fileops, &segs](
              ss::directory_entry seg) {
                // abort if requested
                if (as.abort_requested()) {
                    return ss::now();
                }
                /*
                 * Skip non-regular files (including links)
                 */
                if (
                  !seg.type || *seg.type != ss::directory_entry_type::regular) {
                    return ss::make_ready_future<>();
                }
                auto path = std::filesystem::path(
                  fmt::format("{}/{}", dir, seg.name));
                try {
                    auto is_valid = segment_path::parse_segment_filename(
                      path.filename().string());
                    if (!is_valid) {
                        return ss::make_ready_future<>();
                    }
                } catch (...) {
                    // not a reader filename
                    return ss::make_ready_future<>();
                }
                return open_segment(path, sanitize_fileops, cache_factory())
                  .then([&segs](ss::lw_shared_ptr<segment> p) {
                      segs.push_back(std::move(p));
                  });
            });
          /*
           * if the directory walker returns an exceptional future then all
           * the segment readers that were created are cleaned up by
           * ss::do_with.
           */
          return f.then([&segs]() mutable {
              return ss::make_ready_future<segs_type>(std::move(segs));
          });
      });
}

ss::future<segment_set> recover_segments(
  std::filesystem::path path,
  debug_sanitize_files sanitize_fileops,
  bool is_compaction_enabled,
  std::function<std::optional<batch_cache_index>()> cache_factory,
  ss::abort_source& as) {
    return ss::recursive_touch_directory(path.string())
      .then([&as, cache_factory, sanitize_fileops, path = std::move(path)] {
          return open_segments(
            path.string(), sanitize_fileops, cache_factory, as);
      })
      .then([&as, is_compaction_enabled](segment_set::underlying_t segs) {
          auto segments = segment_set(std::move(segs));
          // we have to mark compacted segments before recovery to allow reading
          // gaps introduced by compaction
          if (is_compaction_enabled) {
              for (auto& s : segments) {
                  s->mark_as_compacted_segment();
              }
          }
          return do_recover(std::move(segments), as);
      });
}

} // namespace storage
