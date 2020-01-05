#include "storage2/log.h"

#include "model/fundamental.h"
#include "model/record.h"
#include "seastarx.h"
#include "storage2/common.h"
#include "storage2/detail/index.h"
#include "storage2/fileset.h"
#include "storage2/fold_log.h"
#include "storage2/indices.h"
#include "storage2/log_manager.h"
#include "storage2/segment.h"
#include "storage2/segment_index.h"
#include "utils/directory_walker.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>

#include <gsl/span>

static logger slog("s/log");

namespace storage {

/**
 * At the implementation level a log is a folder with several types of
 * files in it:
 *
 *  1. state.log
 *     It is a log file of all the actions that happened to the log at
 *     the segment level, like creating a segment, sealing a segment, truncating
 *     a segment or deleting a segment. Folding all this list of events creates
 *     a snapshot of the state of the log. This log file is not aware of
 *     any indices or offsets within segments. Offset information is stored
 *     index files.
 *
 *  2. log.offset_idx / log.timestamp_idx
 *     A collection of index files that store the range of index values per
 *     segment. They are used to identify the segment holding a given offset.
 *     So things like first_offset, last_offset are here. This is the first
 *     level of indexing.
 *
 *  3. <N>-<term>-v1.log
 *     Here is where record batches are stored. N is a sequential identifier of
 *     the log file, <term> is the raft term during which this segment was
 *     created. The data format of this file mimics the kafka record batch
 *    format so that serving requests can be done without any copies.
 *
 *  4. <N>-<term>-v1.offset_idx / <N>-<term>-v1.offset_idx
 *     This set of files index .log files and enable fast lookup of the byte
 *     offset of record batches within the segment data file. This is the
 *     second level of indexing.
 */
class log::impl {
public:
    using primary_index = segment_indices::primary_index_type;
    using pkey_type = primary_index::key_type;
    using segment_ref = std::reference_wrapper<segment>;

public:
    /**
     * Constructing a log object is an IO heavy operation and involves
     * lots of async work, That's why the constructor is made private and public
     * initialization happens through the open() function that does all the
     * heavylifting asynchronously and returns a constructed object in a
     * future.
     *
     * For the log object to be usable, it needs a list of all the
     * segments it manages - which is stored in the state.log file,
     * and the log indices for each active index.
     */
    impl(
      std::vector<segment> segs,
      model::ntp id,
      io_priority_class iopc,
      const log_manager& mgr)
      : _myntp(std::move(id))
      , _segments(std::move(segs))
      , _io_priority(std::move(iopc))
      , _mgr(mgr)
      , _termid(0) {}

    /**
     * The model identifier of the current log.
     */
    const model::ntp& ntp() const { return _myntp; }

    template<typename Key>
    seastar::input_stream<char> read(Key key) const {
        return input_stream<char>();
    }

    /**
     * Locates the current active segment and forwards the append call to it.
     */
    future<append_result> append(model::record_batch&& batch) {
        if (__builtin_expect(batch.size() == 0, false)) {
            return make_exception_future<append_result>(
              record_batch_error("empty batches are not supported"));
        }
        return active_segment().then(
          [batch = std::move(batch)](segment_ref active_seg) mutable {
              return active_seg.get().append(std::move(batch));
          });
    }
    future<std::vector<append_result>>
    append(model::record_batch_reader&& reader) {
        struct consumer {
            explicit consumer(log::impl* p)
              : ptr(p) {}
            future<stop_iteration> operator()(model::record_batch batch) {
                return ptr->append(std::move(batch))
                  .then([this](append_result r) {
                      ret.push_back(r);
                      return stop_iteration::no;
                  });
            }
            std::vector<append_result> end_of_stream() { return ret; }
            log::impl* ptr;
            std::vector<append_result> ret;
        };
        // FIXME(agallego) - once we add per write options
        return reader.consume(consumer(this), model::no_timeout);
    }
    /**
     * Closes all log resources and all its underlying segments.
     *
     * During this operation this is what gets closed:
     *  - state log file, log indices
     *  - segment log files, segment indices
     */
    future<> close() {
        return parallel_for_each(
          _segments.begin(), _segments.end(), [](segment& s) {
              return s.close();
          });
    }

private:
    /**
     * True of this log has no data stored in it yet.
     * Otherwise false.
     */
    bool empty() const { return _segments.empty(); }

    /**
     * This function returns true in case a new segment needs to be created.
     * This includes, current segment being corrupt, maxed out or having
     * no segments yet.
     */
    bool needs_new_segment() const {
        return empty()
               || _segments.back().size_bytes()
                    >= file_offset(_mgr.configuration().max_segment_size)
               || _segments.back().is_sealed();
    }

    /**
     * This function closes the current segment and sets it in sealed state.
     * Sealing a segment involves flushing it and all its indices, marking it
     * in the changelog and returning the flush result.
     */
    future<flush_result> seal_segment() {
        if (empty()) {
            // nothing to be sealed, so the flush result is the zero on
            // the primary offset index.
            return make_ready_future<flush_result>(flush_result{
              .first = model::offset(0), .last = model::offset(0)});
        } else {
            return _segments.back().flush().then([this](flush_result fr) {
                return _segments.back().seal().then([fr, this](flush_result) {
                    return make_ready_future<flush_result>(fr);
                });
            });
        }
    }

    pkey_type next_pkey() const {
        if (empty()) {
            return index_key_traits<pkey_type>::zero();
        } else {
            pkey_type maxpkey = std::accumulate(
              _segments.begin(),
              _segments.end(),
              index_key_traits<pkey_type>::zero(),
              [](pkey_type acc, const segment& seg) {
                  return acc + seg.inclusive_range<pkey_type>().last;
              });
            return index_key_traits<pkey_type>::increment(maxpkey);
        }
    }

    /**
     * Given a base offset (or any currently defined primary key) and
     * the current term id, this function will synthesize a filename
     * of a segment file in this ntp.
     */
    std::filesystem::path
    seg_filename(pkey_type base_pkey, model::term_id term) const {
        auto ntppath = std::filesystem::path(ntp().path());
        auto basedir = _mgr.working_directory() / ntppath;
        auto segbase = std::to_string(base_pkey());
        auto filename = fmt::format("{}-{}-v1.log", segbase, term());
        return basedir / filename;
    }

    // todo: seal last segment.
    // todo: acquire next ix from index
    // todo: create index entry about new segment
    future<segment_ref> create_segment(model::term_id term) {
        return seal_segment().then([this, term](flush_result r) {
            closed_fileset cfs(seg_filename(next_pkey(), term));
            return segment::create(std::move(cfs), _io_priority)
              .then([this](segment seg) {
                  _segments.emplace_back(std::move(seg));
                  auto segref = std::ref(_segments.back());
                  return make_ready_future<segment_ref>(segref);
              });
        });
    }

    /**
     * Possible scenarios:
     *  1. does not exist (empty log), create one and return.
     *  2. available and not reached max size, return a reference to what we
     *     have.
     *  3. available and reached max size, roll to new segment and return.
     */
    future<segment_ref> active_segment() {
        if (needs_new_segment()) {
            return create_segment(_termid);
        }

        // getting here, we're guaranteed that there is an active
        // segment that is ready for new writes.
        return make_ready_future<segment_ref>(std::ref(_segments.back()));
    }

private:
    model::ntp _myntp;
    model::term_id _termid;
    log_manager const& _mgr;
    std::vector<segment> _segments;
    io_priority_class _io_priority;
};

future<std::vector<closed_fileset>>
discover_filesets(std::filesystem::path dir) {
    return do_with(
      std::vector<closed_fileset>(),
      [dir](std::vector<closed_fileset>& filesets) {
          return directory_walker::walk(
                   dir.string(),
                   [dir, &filesets](directory_entry de) {
                       if (de.type == directory_entry_type::regular) {
                           auto path = dir / de.name.c_str();
                           if (path.extension() == ".log") {
                               filesets.emplace_back(path);
                           }
                       }
                       return make_ready_future<>();
                   })
            .then([&filesets] {
                std::sort(filesets.begin(), filesets.end());
                return filesets;
            });
      });
}

log::log(shared_ptr<impl> impl)
  : _impl(std::move(impl)) {}

future<log>
log::open(const log_manager& mgr, model::ntp id, io_priority_class iopc) {
    auto path = mgr.working_directory() / id.path().c_str();
    auto log_log = path / "state.log";

    return discover_filesets(path).then([iopc, id, &mgr](
                                          std::vector<closed_fileset> cfss) {
        std::vector<future<segment>> segments;
        for (auto& cfs : cfss) {
            segments.emplace_back(segment::open(cfs, iopc));
        }
        return when_all_succeed(segments.begin(), segments.end())
          .then([id, iopc, &mgr](auto vals) {
              return make_ready_future<log>(log(seastar::make_shared<log::impl>(
                std::move(vals), id, iopc, mgr)));
          });
    });
}

/**
 * The log unique identifier using model types.
 */
const model::ntp& log::ntp() const { return _impl->ntp(); }

/**
 * Appends a batch of records to the log.
 */
future<std::vector<append_result>>
log::append(model::record_batch_reader&& reader) {
    return _impl->append(std::move(reader));
}

future<flush_result> log::flush() {
    return make_ready_future<flush_result>(
      flush_result{model::offset(0), model::offset(0)});
}

future<flush_result> log::close() {
    return flush().then([this](flush_result result) {
        return _impl->close().then([result = std::move(result)]() {
            return make_ready_future<flush_result>(std::move(result));
        });
    });
}

template<>
seastar::input_stream<char> log::read(model::offset key) const {
    return _impl->read(key);
}

template<>
seastar::input_stream<char> log::read(model::timestamp key) const {
    return _impl->read(key);
}

} // namespace storage
