#pragma once

#include "filesystem/wal_generated.h"
#include "filesystem/wal_opts.h"
#include "filesystem/wal_requests.h"
#include "filesystem/wal_segment.h"
#include "filesystem/wal_writer_utils.h"
#include "seastarx.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/noncopyable_function.hh>

#include <smf/time_utils.h>

struct wal_writer_node_opts {
    /// \brief callback that users can opt-in
    /// gets called every time we roll a log segment
    ///
    using notify_create_log_handle_t
      = noncopyable_function<future<>(sstring)>;

    /// \brief callback that users can opt-in to listen to
    /// the name of the file being written to and the latest size flushed
    /// to disk. This callback is only triggered *AFTER* flush() succeeds
    ///
    using notify_size_change_handle_t = noncopyable_function<
      future<>(sstring, int64_t)>;

    SMF_DISALLOW_COPY_AND_ASSIGN(wal_writer_node_opts);

    wal_writer_node_opts(
      const wal_opts& op,
      int64_t term_id,
      int64_t epoch,
      const wal_topic_create_request* create_props,
      const sstring& writer_dir,
      const io_priority_class& prio,
      notify_create_log_handle_t cfunc,
      notify_size_change_handle_t sfunc)
      : wopts(op)
      , tprops(THROW_IFNULL(create_props))
      , writer_directory(writer_dir)
      , pclass(prio)
      , epoch(epoch)
      , term(term_id)
      , log_segment_create_notify(std::move(cfunc))
      , log_segment_size_notify(std::move(sfunc)) {
    }

    wal_writer_node_opts(wal_writer_node_opts&& o) noexcept
      : wopts(o.wopts)
      , tprops(o.tprops)
      , writer_directory(o.writer_directory)
      , pclass(o.pclass)
      , epoch(o.epoch)
      , term(o.term)
      , log_segment_create_notify(std::move(o.log_segment_create_notify))
      , log_segment_size_notify(std::move(o.log_segment_size_notify)) {
    }

    const wal_opts& wopts;
    const wal_topic_create_request* tprops;
    const sstring& writer_directory;
    const io_priority_class& pclass;

    int64_t epoch;
    int64_t term;

    notify_create_log_handle_t log_segment_create_notify;
    notify_size_change_handle_t log_segment_size_notify;
};

/// \brief - given a prefix and an epoch (monotinically increasing counter)
/// wal_writer_node will continue writing records in file_size multiples
/// which by default is 64MB. It will compress the buffers that are bigger
/// than min_compression_size
///
/// Note: the file closes happen concurrently, they simply get scheduled,
/// however, the _fstream.close() is called after the file has been flushed
/// to disk, so even during crash we are safe
///
class wal_writer_node {
public:
    explicit wal_writer_node(wal_writer_node_opts opts);
    SMF_DISALLOW_COPY_AND_ASSIGN(wal_writer_node);

    /// \brief writes the projection to disk ensuring file size capacity
    future<std::unique_ptr<wal_write_reply>> append(wal_write_request);

    /// \brief/// \brief forces the log_segment to rotate and start a new
    /// log segment with <epoch>.<term>.log
    future<> set_term(int64_t term);

    /// \brief flushes the file before closing
    future<> close();
    /// \brief opens the file w/ open_flags::rw | open_flags::create |
    ///                          open_flags::truncate
    /// the file should fail if it exists. It should not exist on disk, as
    /// we'll truncate them
    future<> open();

    sstring filename() const {
        if (!_lease) {
            return "";
        }
        return _lease->filename;
    }
    ~wal_writer_node();

    int64_t space_left() const {
        return _opts.wopts.max_log_segment_size - current_size_;
    }
    int64_t current_offset() const {
        return _opts.epoch + current_size_;
    }

private:
    future<> rotate_fstream();
    /// \brief 0-copy append to buffer
    /// https://github.com/apache/kafka/blob/fb21209b5ad30001eeace56b3c8ab060e0ceb021/core/src/main/scala/kafka/log/Log.scala
    /// do append has a similar logic as the kafka log.
    /// effectively just check if there is enough space, if not rotate and then
    /// write.
    future<> do_append(const wal_binary_record* f);
    future<> disk_write(const wal_binary_record* f);

private:
    wal_writer_node_opts _opts;
    int64_t current_size_ = 0;
    // the lease has to be a lw_shared_ptr because the object
    // may go immediately out of existence, before we get a chance to close the
    // file it needs to exist in the background fiber that closes the
    // underlying file
    //
    lw_shared_ptr<wal_segment> _lease = nullptr;
    semaphore serialize_writes_{1};
    timer<> flush_timeout_;
    bool is_closed_{false};
};
