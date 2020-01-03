#include "segment.h"

#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "storage2/common.h"
#include "storage2/fileset.h"
#include "storage2/indices.h"
#include "storage2/segment.h"
#include "storage2/segment_appender.h"
#include "storage2/segment_writer.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/log.hh>

#include <exception>
#include <mutex>
#include <optional>

static logger slog("s/segment");

namespace storage {

class segment::impl {
public:
    using opt_appender = std::optional<segment_appender>;
    using primary_index = segment_indices::primary_index_type; // model::offset
    using pkey_type = primary_index::key_type;

public:
    impl(
      open_fileset ofs,
      segment_indices ixs,
      io_priority_class priority,
      bool sealed)
      : _files(std::move(ofs))
      , _indices(std::move(ixs))
      , _appender(maybe_create_appender(sealed, _files.logfile(), priority)) {}

    /**
     * True if the segment is read-only, otherwise true if the
     * segment accepts new appends.
     */
    bool is_sealed() const { return !_appender.has_value(); }

    future<> close() {
        if (!is_sealed()) {
            return _appender->close().then([this] { return _files.close(); });
        }
        return _files.close();
    }

    future<append_result> append(model::record_batch&& batch) {
        return ensure_not_sealed().then([this, b = std::move(batch)]() mutable {
            // first add this batch to the index. The index is responsible
            // for assigning offsets and other index keys to batches. Here
            // we're also telling the index where the batch will be written
            // on disk
            file_offset foffset(_appender->file_byte_offset());
            auto assigned = _indices.index(b, foffset);

            // for the purpose of communicating append results, the
            // primary index is used as the unique identifier, which is
            // the model::offset key.
            auto primary = std::get<primary_index::bounds_type>(assigned);

            // write the assigned base offset to the batch header, so it
            // gets persisted on disk.
            b.set_base_offset(primary.first);
            append_result result{.first = primary.first, .last = primary.last};
            slog.trace(
              "writing batch [{}-{}], bytes: {}, fpos: {}",
              result.first,
              result.last,
              b.size_bytes(),
              foffset);

            // actually write to segment appender
            return write_to(_appender.value(), std::move(b)).then([result]() {
                return result;
            });
        });
    }

    file_offset size_bytes() const {
        if (!is_sealed()) {
            return file_offset(_appender->file_byte_offset());
        }

        vassert(
          false,
          "getting segment file size on a sealed "
          "segment is not yet supported.");
        return file_offset(0);
    }

public:
    /**
     * Holds the initialization logic of segment::open() and
     * segment::close() minus the file existence check. Its declared here so
     * it could have access to the private constructor of the segment class
     * without exposing it through the public api.
     */
    static future<segment> initialize_segment(
      closed_fileset cfs, io_priority_class iopc, bool sealed) {
        return cfs.open().then([sealed, iopc](open_fileset fs) {
            return do_with(std::move(fs), [sealed, iopc](open_fileset& ofs) {
                return segment_indices::open(ofs).then(
                  [&ofs, sealed, iopc](segment_indices ixs) {
                      return segment(seastar::make_shared<segment::impl>(
                        std::move(ofs), std::move(ixs), iopc, sealed));
                  });
            });
        });
    }

    future<flush_result> seal() {
        return flush().then([this](flush_result fr) {
            return _files.flush_aux().then([this, fr] {
                return _appender->close().then([this, fr] {
                    _appender.reset();
                    return fr;
                });
            });
        });
    }

    future<flush_result> flush() {
        return ensure_not_sealed().then([this] {
            return _appender->flush().then([this] {
                return _files.flush_logfile().then([this] {
                    auto segrange = _indices.inclusive_range<pkey_type>();
                    return flush_result{.first = segrange.first,
                                        .last = segrange.last};
                });
            });
        });
    }

    input_stream<char> read(file_offset fpos) {
        return seastar::make_file_input_stream(_files.logfile(), fpos());
    }

    const segment_indices& indices() const { return _indices; }

private:
    /**
     * Used in all write operations (append/truncate/flush) to ensure that
     * they're called on an active segment
     */
    future<> ensure_not_sealed() {
        if (__builtin_expect(is_sealed(), false)) {
            return make_exception_future<>(
              segment_error("attempt to write to a sealed segment"));
        }
        return make_ready_future<>();
    }

    /**
     * Used in the constructor, for non-sealed segments this will
     * instantiate a non-empty optional of log_segment_appender for the
     * logfile.
     */
    static opt_appender
    maybe_create_appender(bool sealed, file& logfile, io_priority_class iopc) {
        if (sealed) {
            return opt_appender(std::nullopt);
        } else {
            // here the file handle is duplicated because it will be used not
            // only for reading from the file segment, but in case of a
            // non-sealed segment there is an appender that will write to the
            // file. Two file handles are needed because when an active segment
            // is sealed, only its appender is closed (along with its file
            // handle), but not the reader file handle.
            return opt_appender(segment_appender(
              file(logfile.dup()), segment_appender::options(iopc)));
        }
    }

private:
    open_fileset _files;
    segment_indices _indices;
    opt_appender _appender;
};

/**
 * Verifies if the log segment that is about to be opened or created exists
 * or not. Used in open (with expectation=true) and create (with
 * expect=false).
 */
future<closed_fileset>
ensure_existence(closed_fileset cfs, bool expectation, const char* error) {
    return seastar::file_exists(cfs.logfile().string())
      .then([cfs = std::move(cfs), error, expectation](bool exists) {
          if (exists != expectation) {
              return make_exception_future<closed_fileset>(
                segment_error(error));
          }
          return make_ready_future<closed_fileset>(std::move(cfs));
      });
}

future<segment> segment::open(closed_fileset cfs, io_priority_class iopc) {
    slog.info("opening segment {}", cfs.logfile());
    const char* error = "attempt to open non-existing segment";
    return ensure_existence(std::move(cfs), true, error)
      .then([iopc](closed_fileset cfs) {
          return impl::initialize_segment(std::move(cfs), iopc, true);
      });
}

future<segment> segment::create(closed_fileset cfs, io_priority_class iopc) {
    slog.info("creating segment {}", cfs.logfile());
    const char* error = "attempt to overwrite an existing segment";
    return ensure_existence(std::move(cfs), false, error)
      .then([iopc](closed_fileset cfs) {
          return impl::initialize_segment(std::move(cfs), iopc, false);
      });
}

segment::segment(shared_ptr<segment::impl> impl)
  : _impl(impl) {}

bool segment::is_sealed() const { return _impl->is_sealed(); }
future<> segment::close() { return _impl->close(); }
future<flush_result> segment::seal() { return _impl->seal(); }
future<flush_result> segment::flush() { return _impl->flush(); }
file_offset segment::size_bytes() const { return _impl->size_bytes(); }
const segment_indices& segment::indices() const { return _impl->indices(); }

template<>
key_bounds<model::offset> segment::inclusive_range<model::offset>() const {
    return _impl->indices().inclusive_range<model::offset>();
}

template<>
seastar::input_stream<char> segment::read(file_offset pos) const {
    return _impl->read(pos);
}
template<>
seastar::input_stream<char> segment::read(model::offset pos) const {
    // obtain the file offset from the index, then open a reader
    // positioned at that offset.
    return _impl->read(_impl->indices().find(std::move(pos)));
}

template<>
seastar::input_stream<char> segment::read(model::timestamp pos) const {
    // obtain the file offset from the index, then open a reader
    // positioned at that offset.
    return _impl->read(_impl->indices().find(std::move(pos)));
}

future<append_result> segment::append(model::record_batch&& batch) {
    return _impl->append(std::move(batch));
}

} // namespace storage
