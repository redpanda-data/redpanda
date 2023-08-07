// Copyright 2020 Redpanda Data, Inc.
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
#include "storage/segment.h"
#include "utils/directory_walker.h"
#include "utils/filtered_lower_bound.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>

#include <absl/container/btree_set.h>
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

    bool operator()(const type& seg, model::term_id value) const {
        return seg->offsets().term < value;
    }
    bool operator()(model::term_id value, const type& seg) const {
        return value < seg->offsets().term;
    }
};

segment_set::segment_set(segment_set::underlying_t segs)
  : _handles(std::move(segs)) {
    std::sort(_handles.begin(), _handles.end(), segment_ordering{});
}

segment_set::~segment_set() noexcept = default;

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
    // Note that we exclude the segments that only contain configuration batches
    // from our search, as their timestamps may be wildly different from the
    // user provided timestamps.
    return filtered_lower_bound(
      _handles.begin(),
      _handles.end(),
      needle,
      segment_ordering{},
      [](const auto& segment) {
          return segment->index().non_data_timestamps() == false;
      });
}

segment_set::iterator segment_set::upper_bound(model::term_id term) {
    return std::upper_bound(
      _handles.begin(), _handles.end(), term, segment_ordering{});
}

segment_set::const_iterator
segment_set::upper_bound(model::term_id term) const {
    return std::upper_bound(
      _handles.cbegin(), _handles.cend(), term, segment_ordering{});
}

std::ostream& operator<<(std::ostream& o, const segment_set& s) {
    o << "{size: " << s.size() << ", [";
    static constexpr size_t max_to_log = 8;
    static constexpr size_t halved = max_to_log / 2;
    if (s.size() <= max_to_log) {
        for (auto& p : s) {
            o << p;
        }
    } else {
        for (size_t i = 0; i < halved; i++) {
            o << s[i];
        }
        o << "...";
        for (size_t i = s.size() - halved; i < s.size(); i++) {
            o << s[i];
        }
    }
    return o << "]}";
}

static bool
is_last_segment(segment* s, std::optional<ss::sstring> last_clean_segment) {
    return last_clean_segment
           && std::filesystem::path(s->filename()).filename().string()
                == std::string(last_clean_segment.value());
}

// Recover the last segment. Whenever we close a segment, we will likely
// open a new one to which we will direct new writes. That new segment
// might be empty. To optimize log replay, implement #140.
static ss::future<segment_set> unsafe_do_recover(
  segment_set&& segments,
  std::optional<ss::sstring> last_clean_segment,
  ss::abort_source& as) {
    return ss::async([segments = std::move(segments),
                      last_clean_segment = std::move(last_clean_segment),
                      &as]() mutable {
        if (segments.empty() || as.abort_requested()) {
            return std::move(segments);
        }
        segment_set::underlying_t good = std::move(segments).release();
        absl::btree_set<segment*> to_recover_set;
        for (size_t i = 0; i < good.size(); ++i) {
            auto& s = *good[i];
            if (i > 0) {
                auto& prev = *good[i - 1];

                if (prev.offsets().dirty_offset >= s.offsets().base_offset) {
                    vlog(
                      stlog.warn,
                      "looks like segment index for segment {} is corrupted: "
                      "dirty offset {} is >= than the next base offset: {}, "
                      "will recover it",
                      prev,
                      prev.offsets().dirty_offset,
                      s.offsets().base_offset);
                    to_recover_set.insert(&prev);
                }
            }

            try {
                // use the segment materialize instead of going through
                // the index directly to hydrate the max_offset state
                if (s.materialize_index().get()) {
                    vassert(
                      s.offsets().dirty_offset == s.index().max_offset(),
                      "dirty_offset and index max_offset must be equal for "
                      "segment {}",
                      s);
                } else {
                    if (is_last_segment(&s, last_clean_segment)) {
                        // skipping last_clean_segment is an optimization for
                        // happy case; here we explicitly know that there is a
                        // problem with index; skipping the optimization
                        last_clean_segment = {};
                    }

                    to_recover_set.insert(&s);
                }
            } catch (...) {
                vlog(
                  stlog.info,
                  "Error materializing index:{}. Recovering parent "
                  "segment:{}. Details:{}",
                  s.index().path(),
                  s.filename(),
                  std::current_exception());

                if (is_last_segment(&s, last_clean_segment)) {
                    // skipping last_clean_segment is an optimization for
                    // happy case; here we explicitly know that there is a
                    // problem with index; skipping the optimization
                    last_clean_segment = {};
                }

                to_recover_set.insert(&s);
            }
        }
        segment_set::underlying_t to_recover;
        // keep segments sorted
        auto good_end = std::stable_partition(
          good.begin(),
          good.end(),
          [&to_recover_set](ss::lw_shared_ptr<segment>& ss) {
              return !to_recover_set.contains(ss.get());
          });
        std::move(
          std::move_iterator(good_end),
          std::move_iterator(good.end()),
          std::back_inserter(to_recover));
        good.erase(
          good_end,
          good.end()); // remove all the ones we copied into recover

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
              ss::remove_file(segment->reader().path().string()).get();
              try {
                  ss::remove_file(segment->index().path().string()).get();
              } catch (const std::filesystem::filesystem_error& e) {
                  // Ignore ENOENT on deletion: segments are allowed to
                  // exist without an index if redpanda shutdown without
                  // a flush.
                  if (e.code() != std::errc::no_such_file_or_directory) {
                      throw;
                  }
              }
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

            if (is_last_segment(s.get(), last_clean_segment)) {
                vlog(
                  stlog.debug,
                  "Skipping recovery of {}, it is marked clean",
                  s);
                good.emplace_back(std::move(s));
                continue;
            }

            // Check if the segment was marked clean on shutdown
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
               recovered.truncate_file_pos.value(),
               recovered.last_max_timestamp.value())
              .get();
            // persist index
            s->index().flush().get();
            vlog(stlog.info, "Recovered: {}", s);
            good.emplace_back(std::move(s));
        }
        return segment_set(std::move(good));
    });
}

static ss::future<segment_set> do_recover(
  segment_set&& segments,
  std::optional<ss::sstring> last_clean_segment,
  ss::abort_source& as) {
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
    return unsafe_do_recover(std::move(segments), last_clean_segment, as)
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
  partition_path ppath,
  std::function<std::optional<batch_cache_index>()> cache_factory,
  ss::abort_source& as,
  size_t buf_size,
  unsigned read_ahead,
  storage_resources& resources,
  ss::sharded<features::feature_table>& feature_table,
  const std::optional<ntp_sanitizer_config>& ntp_sanitizer_config) {
    using segs_type = segment_set::underlying_t;
    return ss::do_with(
      segs_type{},
      [&as,
       ppath,
       cache_factory,
       ntp_sanitizer_config,
       buf_size,
       read_ahead,
       &resources,
       &feature_table](segs_type& segs) {
          auto f = directory_walker::walk(
            ss::sstring(ppath),
            [&as,
             ppath,
             cache_factory,
             san_cfg = ntp_sanitizer_config,
             &segs,
             buf_size,
             read_ahead,
             &resources,
             &feature_table](ss::directory_entry seg) {
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

                auto path = segment_full_path::parse(ppath, seg.name);
                if (!path) {
                    // This is normal, we skip non-log files like indices
                    return ss::make_ready_future<>();
                }

                return open_segment(
                         *path,
                         cache_factory(),
                         buf_size,
                         read_ahead,
                         resources,
                         feature_table,
                         san_cfg)
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
  partition_path path,
  bool is_compaction_enabled,
  std::function<std::optional<batch_cache_index>()> cache_factory,
  ss::abort_source& as,
  size_t read_buf_size,
  unsigned read_readahead_count,
  std::optional<ss::sstring> last_clean_segment,
  storage_resources& resources,
  ss::sharded<features::feature_table>& feature_table,
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config) {
    return ss::recursive_touch_directory(ss::sstring(path))
      .then([&as,
             path,
             cache_factory,
             ntp_sanitizer_config,
             read_buf_size,
             read_readahead_count,
             &resources,
             &feature_table] {
          return open_segments(
            path,
            cache_factory,
            as,
            read_buf_size,
            read_readahead_count,
            resources,
            feature_table,
            ntp_sanitizer_config);
      })
      .then([&as,
             is_compaction_enabled,
             last_clean_segment = std::move(last_clean_segment)](
              segment_set::underlying_t segs) {
          auto segments = segment_set(std::move(segs));
          // we have to mark compacted segments before recovery to allow reading
          // gaps introduced by compaction
          if (is_compaction_enabled) {
              for (auto& s : segments) {
                  s->mark_as_compacted_segment();
              }
          }
          return do_recover(std::move(segments), last_clean_segment, as);
      });
}

} // namespace storage
