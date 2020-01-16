#include "storage/disk_log_impl.h"

#include "storage/log_manager.h"
#include "storage/log_writer.h"
#include "storage/offset_assignment.h"
#include "storage/version.h"

#include <seastar/core/reactor.hh>

#include <fmt/format.h>

namespace storage {

disk_log_impl::disk_log_impl(
  model::ntp ntp, ss::sstring workdir, log_manager& manager, log_set segs)
  : log::impl(std::move(ntp), std::move(workdir))
  , _manager(manager)
  , _segs(std::move(segs)) {
    _probe.setup_metrics(this->ntp());
    if (_segs.size()) {
        _tracker.update_committed_offset(_segs.last()->max_offset());
        _tracker.update_dirty_offset(_segs.last()->max_offset());
        _term = _segs.last()->term();
    } else {
        _term = model::term_id(0);
    }
}

ss::future<> disk_log_impl::close() {
    auto active = ss::make_ready_future<>();
    if (_appender) {
        // flush + truncate + close
        active = _appender->close();
    }
    return active.then([this] {
        return ss::parallel_for_each(
          _segs, [](segment_reader_ptr& seg) { return seg->close(); });
    });
}

ss::future<> disk_log_impl::new_segment(
  model::offset o, model::term_id term, const ss::io_priority_class& pc) {
    return _manager.make_log_segment(ntp(), o, term, pc)
      .then([this, pc](log_manager::log_handles handles) {
          _active_segment = std::move(handles.reader);
          _appender = std::move(handles.appender);
          _segs.add(_active_segment);
          _probe.segment_created();
      });
}

ss::future<append_result> disk_log_impl::do_append(
  model::record_batch_reader&& reader, log_append_config config) {
    auto f = ss::make_ready_future<>();
    if (__builtin_expect(!_active_segment, false)) {
        // FIXME: We need to persist the last offset somewhere.
        _term = config.term;
        auto offset = _segs.size() > 0
                        ? _segs.last()->max_offset() + model::offset(1)
                        : model::offset(0);
        f = new_segment(offset, _term, config.io_priority);
    }
    if (_term != config.term) {
        _term = config.term;
        f = f.then([this] { return do_roll(); });
    }
    return f.then(
      [this, reader = std::move(reader), config = std::move(config)]() mutable {
          return ss::do_with(
            std::move(reader),
            [this,
             config = std::move(config)](model::record_batch_reader& reader) {
                auto now = log_clock::now();
                auto base = _tracker.dirty_offset() >= model::offset(0)
                              ? _tracker.dirty_offset() + model::offset(1)
                              : model::offset(0);
                auto writer = log_writer(
                  std::make_unique<default_log_writer>(*this));
                return reader
                  .consume(
                    wrap_with_offset_assignment(std::move(writer), base),
                    config.timeout)
                  .then([this, config = std::move(config), now, base](
                          model::offset last_offset) {
                      _tracker.update_dirty_offset(last_offset);
                      _active_segment->set_last_written_offset(last_offset);
                      auto f = ss::make_ready_future<>();
                      /// fsync, means we fsync _every_ record_batch
                      /// most API's will want to batch the fsync, at least
                      /// to the record_batch_reader level
                      if (config.should_fsync) {
                          f = flush();
                      }
                      return f.then([this, now, base, last_offset] {
                          return append_result{now, base, last_offset};
                      });
                  });
            });
      });
}
ss::future<> disk_log_impl::flush() {
    if (!_appender) {
        return ss::make_ready_future<>();
    }
    return _appender->flush().then([this] {
        _tracker.update_committed_offset(_tracker.dirty_offset());
        _active_segment->set_last_written_offset(_tracker.committed_offset());
        _active_segment->set_last_visible_byte_offset(
          _appender->file_byte_offset());
    });
}
ss::future<> disk_log_impl::do_roll() {
    return flush().then([this] { return _appender->close(); }).then([this] {
        auto offset = _tracker.committed_offset() + model::offset(1);
        stlog.trace("Rolling log segment offset {}, term {}", offset, _term);
        return new_segment(offset, _term, _appender->priority_class());
    });
}
ss::future<> disk_log_impl::maybe_roll(model::offset current_offset) {
    if (_appender->file_byte_offset() < _manager.max_segment_size()) {
        return ss::make_ready_future<>();
    }
    _tracker.update_dirty_offset(current_offset);
    return do_roll();
}

model::record_batch_reader
disk_log_impl::make_reader(log_reader_config config) {
    return model::make_record_batch_reader<log_reader>(
      _segs, _tracker, std::move(config), _probe);
}

ss::future<> disk_log_impl::do_truncate(model::offset o, model::term_id term) {
    // 1. update metadata
    // 2. get a list of segments to drop
    // 3. perform drop in background for all
    // 4. synchronize dir-entry
    // 5. translate offset into disk/filename
    // 6. truncate the last segment
    // 7. roll

    // 1.
    _term = term;
    _tracker.update_dirty_offset(o);
    _tracker.update_committed_offset(o);

    // 2.
    std::vector<ss::sstring> names_to_delete;
    for (auto s : _segs) {
        if (s->term() > term) {
            stlog.info("do_truncate() full file:{}", s->get_filename());
            names_to_delete.push_back(s->get_filename());
        }
    }

    //  3.
    auto erased = _segs.remove(std::move(names_to_delete));
    auto f = ss::make_ready_future<>();
    // do not roll when we do not have segment opened
    if (!_appender) {
        return f;
    }
    // 4.
    f = parallel_for_each(erased, [](segment_reader_ptr i) {
        return i->close().then([i] { return remove_file(i->get_filename()); });
    });

    // 5.
    f = f.then([d = _manager.config().base_dir] { return sync_directory(d); });

    // 6.
    // FIXME
    // missing, find offset in offset_index and truncate at size
    // _segs.back().truncate(offset_index.get(o))
    stlog.error("We cannot truncate a logical offset without an index. rolling "
                "last segment");

    // 7.
    f = f.then([this] { return do_roll(); });
    return f;
}

log make_disk_backed_log(model::ntp ntp, log_manager& manager, log_set segs) {
    auto workdir = fmt::format("{}/{}", manager.config().base_dir, ntp.path());
    auto ptr = ss::make_shared<disk_log_impl>(
      std::move(ntp), std::move(workdir), manager, std::move(segs));
    return log(ptr);
}

} // namespace storage
