#include "wal_writer_node.h"

#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"
#include "hbadger/hbadger.h"
#include "filesystem/wal_segment.h"
#include "filesystem/wal_writer_utils.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>

#include <smf/log.h>
#include <smf/macros.h>

#include <sys/sdt.h>

#include <utility>

inline int64_t wal_write_request_size(const wal_write_request& r) {
    return std::accumulate(
      r.begin(),
      r.end(),
      int64_t(0),
      [](int64_t acc, const wal_binary_record* x) {
          return acc + x->data()->size();
      });
}

wal_writer_node::wal_writer_node(wal_writer_node_opts opts)
  : _opts(std::move(opts)) {
    flush_timeout_.set_callback([this] {
        if (is_closed_)
            return;
        if (_lease->current_size() == 0)
            return;
        if (is_closed_)
            return;

        // timer<>.set_callback is void - dispatch in background
        DTRACE_PROBE(rp, wal_writer_node_periodic_flush);
        _lease->flush().then(
          [this, sz = _lease->current_size(), name = _lease->filename] {
              return _opts.log_segment_size_notify(name, sz);
          });
    });
    flush_timeout_.arm_periodic(_opts.wopts.writer_flush_period);
}
wal_writer_node::~wal_writer_node() {
    // done at close() - no need to re cancel:
    // flush_timeout_.cancel();
}

seastar::future<> wal_writer_node::open() {
    HBADGER(filesystem, wal_writer_node::open);
    const auto name = wal_file_name(
      _opts.writer_directory, _opts.epoch, _opts.term);
    DLOG_TRACE("Rolling log: {}", name);
    LOG_THROW_IF(!!_lease, "opening new file. Previous file is unclosed");
    _lease = seastar::make_lw_shared<wal_segment>(
      name,
      _opts.pclass,
      _opts.wopts.max_log_segment_size,
      _opts.wopts.max_bytes_in_writer_cache);
    return _lease->open().then(
      [this, name] { return _opts.log_segment_create_notify(name); });
}
seastar::future<> wal_writer_node::disk_write(const wal_binary_record* f) {
    HBADGER(filesystem, wal_writer_node::disk_write);
    current_size_ += f->data()->size();
    return _lease->append((const char*)f->data()->Data(), f->data()->size());
}

seastar::future<std::unique_ptr<wal_write_reply>>
wal_writer_node::append(wal_write_request req) {
    DTRACE_PROBE(rp, wal_writer_node_append);
    HBADGER(filesystem, wal_writer_node::append);
    return seastar::with_semaphore(
      serialize_writes_, 1, [this, req = std::move(req)]() mutable {
          const int64_t start_offset = current_offset();
          const int64_t write_size = wal_write_request_size(req);
          const int32_t partition = req.partition;
          const int32_t put_count = req.data.size();
          const int64_t ns = req.req->ns();
          const int64_t topic = req.req->topic();
          return seastar::do_with(
                   std::move(req),
                   [this](auto& r) mutable {
                       return seastar::do_for_each(
                         r.begin(), r.end(), [this](auto i) mutable {
                             return this->do_append(i);
                         });
                   })
            .then([ns,
                   topic,
                   write_size,
                   start_offset,
                   partition,
                   this,
                   put_count] {
                LOG_THROW_IF(
                  start_offset + write_size != current_offset(),
                  "Invalid offset accounting: start_offset:{}, "
                  "write_size:{}, current_offset(): {}, "
                  "total_writes_in_batch: {}",
                  start_offset,
                  write_size,
                  current_offset(),
                  put_count);
                auto ret = std::make_unique<wal_write_reply>(ns, topic);
                ret->set_reply_partition_tuple(
                  ns,
                  topic,
                  partition,
                  start_offset,
                  start_offset + write_size);
                return seastar::make_ready_future<decltype(ret)>(
                  std::move(ret));
            });
      });
}

seastar::future<> wal_writer_node::do_append(const wal_binary_record* f) {
    if (SMF_LIKELY(f->data()->size() <= space_left())) {
        return disk_write(f);
    }
    return rotate_fstream().then([this, f]() mutable { return disk_write(f); });
}

seastar::future<> wal_writer_node::close() {
    is_closed_ = true;
    flush_timeout_.cancel();
    // need to make sure the file is not closed in the midst of a write
    //
    return seastar::with_semaphore(serialize_writes_, 1, [l = _lease] {
        return l->flush().then([l] { return l->close(); }).finally([l] {});
    });
}

seastar::future<> wal_writer_node::set_term(int64_t term) {
    LOG_THROW_IF(
      term >= _opts.term,
      "Invalid log term. Logic error. Existing term:{}, but wanting "
      "to set term: {}",
      _opts.term,
      term);
    DLOG_TRACE("Rotating fstream due to set_term()");
    _opts.term = term;
    return rotate_fstream();
}
seastar::future<> wal_writer_node::rotate_fstream() {
    DTRACE_PROBE(rp, wal_writer_node_rotation);
    DLOG_INFO("rotating fstream");
    HBADGER(filesystem, wal_writer_node::rotate_fstream);
    // Although close() does similar work, it will deadlock the fiber
    // if you call close here. Close ensures that there is no other ongoing
    // operations and it is a public method which needs to serialize access to
    // the internal file.
    auto l = _lease;
    return l->flush()
      .then([l] { return l->close(); })
      .then([this] {
          _lease = nullptr;
          _opts.epoch += current_size_;
          current_size_ = 0;
          return open();
      })
      .finally([l] {});
}
