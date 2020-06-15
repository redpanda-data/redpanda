#include "storage/kvstore.h"

#include "bytes/iobuf.h"
#include "cluster/namespace.h"
#include "raft/types.h"
#include "reflection/adl.h"
#include "storage/record_batch_builder.h"
#include "storage/segment_set.h"
#include "vlog.h"

#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

static ss::logger lg("kvstore");

namespace storage {

kvstore::kvstore(kvstore_config kv_conf, storage::log_config log_conf)
  : _conf(kv_conf)
  , _mgr(log_conf)
  , _ntpc(cluster::kvstore_ntp(ss::this_shard_id()), _mgr.config().base_dir)
  , _snap(
      std::filesystem::path(_ntpc.work_directory()),
      ss::default_priority_class())
  , _timer([this] { _sem.signal(); }) {}

ss::future<> kvstore::start() {
    vlog(lg.info, "Starting kvstore: dir {}", _ntpc.work_directory());

    return recover().then([this] {
        _started = true;

        // below, ss::with_gate enters the gate synchronously, but since its
        // called within the recover().then(...) continuation it's subject to
        // scheduling and does race with stop() + gate::close.
        if (_gate.is_closed()) {
            return;
        }

        // Flushing background fiber
        (void)ss::with_gate(_gate, [this] {
            return ss::do_until(
              [this] { return _gate.is_closed(); },
              [this] {
                  // semaphore used here instead of condition variable so that
                  // we don't lose wake-ups if they occur while flushing.
                  return _sem.wait(_sem.available_units()).then([this] {
                      return roll().then(
                        [this] { return flush_and_apply_ops(); });
                  });
              });
        });
    });
}

ss::future<> kvstore::stop() {
    vlog(lg.info, "Stopping kvstore: dir {}", _ntpc.work_directory());

    // prevent new ops, signal flusher to exit
    auto f = _gate.close();
    _sem.signal();

    // it's ok for the flusher to run concurrently with stop() because the
    // flusher only operates on a snapshot of the pending ops that it takes when
    // it starts these ops begin cancelled would be ops that arrived between the
    // start of a flush and this service being stopped.
    for (auto& op : _ops) {
        op.done.set_exception(ss::gate_closed_exception());
    }
    _ops.clear();

    return f.then([this] {
        // wait until the flusher exists--it might create _segment
        if (_segment) {
            return _segment->flush().then([this] { return _segment->close(); });
        }
        return ss::now();
    });
}

/*
 * Return a key prefixed by a key-space
 */
static inline bytes make_spaced_key(kvstore::key_space ks, bytes_view key) {
    auto ks_native
      = static_cast<std::underlying_type<kvstore::key_space>::type>(ks);
    auto ks_le = ss::cpu_to_le(ks_native);
    auto spaced_key = ss::uninitialized_string<bytes>(
      sizeof(ks_le) + key.size());
    auto out = spaced_key.begin();
    out = std::copy_n(
      reinterpret_cast<const char*>(&ks_le), sizeof(ks_le), out);
    std::copy_n(key.begin(), key.size(), out);
    return spaced_key;
}

std::optional<iobuf> kvstore::get(key_space ks, bytes_view key) {
    vassert(_started, "kvstore has not been started");

    // do not re-assign to string_view -> temporary
    auto kkey = make_spaced_key(ks, key);
    if (auto it = _db.find(kkey); it != _db.end()) {
        return it->second.copy();
    }
    return std::nullopt;
}

ss::future<> kvstore::put(key_space ks, bytes key, iobuf value) {
    return put(ks, std::move(key), std::make_optional<iobuf>(std::move(value)));
}

ss::future<> kvstore::remove(key_space ks, bytes key) {
    return put(ks, std::move(key), std::nullopt);
}

ss::future<> kvstore::put(key_space ks, bytes key, std::optional<iobuf> value) {
    vassert(_started, "kvstore has not been started");

    key = make_spaced_key(ks, key);
    return ss::with_gate(
      _gate, [this, key = std::move(key), value = std::move(value)]() mutable {
          auto& w = _ops.emplace_back(std::move(key), std::move(value));
          if (!_timer.armed()) {
              _timer.arm(_conf.commit_interval);
          }
          return w.done.get_future();
      });
}

void kvstore::apply_op(bytes key, std::optional<iobuf> value) {
    if (value) {
        auto res = _db.insert_or_assign(std::move(key), std::move(*value));
        vlog(
          lg.trace,
          "Apply op: {}: key={} value={}",
          (res.second ? "insert" : "update"),
          res.first->first,
          res.first->second);
    } else {
        auto it = _db.find(key);
        if (it == _db.end()) {
            vlog(lg.trace, "Apply op: delete: key={} not found", key);
        } else {
            vlog(lg.trace, "Apply op: delete: key={}", key);
            _db.erase(it);
        }
    }
}

ss::future<> kvstore::flush_and_apply_ops() {
    if (_gate.is_closed()) {
        // early out on shutdown
        return ss::now();
    }

    if (_ops.empty()) {
        return ss::now();
    }

    // flush and apply whatever happens to be queued up
    auto ops = std::exchange(_ops, {});

    // build the operation batch to be logged
    storage::record_batch_builder builder(kvstore_batch_type, _next_offset);
    for (auto& op : ops) {
        std::optional<iobuf> value;
        if (op.value) {
            value = op.value->share(0, op.value->size_bytes());
        }
        builder.add_raw_kv(
          bytes_to_iobuf(op.key), reflection::to_iobuf(std::move(value)));
    }
    auto batch = std::move(builder).build();
    auto last_offset = batch.last_offset();

    /*
     * 1. write batch
     * 2. flush to disk
     * 3. apply db ops
     * 4. notify waiters
     */
    return _segment->append(std::move(batch))
      .then([this](append_result) { return _segment->flush(); })
      .then([this, last_offset, ops = std::move(ops)]() mutable {
          for (auto& op : ops) {
              apply_op(std::move(op.key), std::move(op.value));
              op.done.set_value();
          }
          _next_offset = last_offset + model::offset(1);
      });
}

ss::future<> kvstore::roll() {
    if (!_segment) {
        return _mgr
          .do_make_log_segment(
            _ntpc,
            model::offset(_next_offset),
            model::term_id(0),
            ss::default_priority_class(),
            record_version_type::v1,
            default_read_buffer_size)
          .then([this](ss::lw_shared_ptr<segment> seg) { _segment = seg; });
    }

    if (_segment->appender().file_byte_offset() > _conf.max_segment_size) {
        vlog(
          lg.debug,
          "Rolling segment with base offset {} size {}",
          _segment->offsets().base_offset,
          _segment->appender().file_byte_offset());
        return _segment->close()
          .then([this] { return save_snapshot(); })
          .then([this] {
              vlog(
                lg.debug,
                "Removing old segment with base offset {}",
                _segment->offsets().base_offset);
              return ss::remove_file(_segment->reader().filename())
                .then([this] {
                    return ss::remove_file(_segment->index().filename());
                    _segment = nullptr;
                });
          })
          .then([this] {
              return _mgr
                .do_make_log_segment(
                  _ntpc,
                  model::offset(_next_offset),
                  model::term_id(0),
                  ss::default_priority_class(),
                  record_version_type::v1,
                  default_read_buffer_size)
                .then(
                  [this](ss::lw_shared_ptr<segment> seg) { _segment = seg; });
          });
    }

    return ss::now();
}

ss::future<> kvstore::save_snapshot() {
    _gate.check(); // early out on shutdown

    vassert(
      _next_offset >= model::offset(0),
      "Unexpected next offset {}",
      _next_offset);

    // no operations have been applied to the db
    if (_next_offset == model::offset(0)) {
        return ss::now();
    }

    vlog(
      lg.debug,
      "Creating snapshot at offset {}",
      _next_offset - model::offset(1));

    // package up the db into a batch
    storage::record_batch_builder builder(kvstore_batch_type, model::offset(0));
    for (auto& entry : _db) {
        builder.add_raw_kv(
          bytes_to_iobuf(entry.first),
          entry.second.share(0, entry.second.size_bytes()));
    }
    auto batch = std::move(builder).build();

    // serialize batch: size_prefix + batch
    iobuf data;
    auto ph = data.reserve(sizeof(int32_t));
    reflection::serialize(data, std::move(batch));
    auto size = ss::cpu_to_le(int32_t(data.size_bytes() - sizeof(int32_t)));
    ph.write((const char*)&size, sizeof(size));

    return _snap.start_snapshot().then(
      [this, data = std::move(data)](snapshot_writer writer) mutable {
          return ss::do_with(
            std::move(writer),
            [this, data = std::move(data)](snapshot_writer& wr) mutable {
                // the last log offset represented in the snapshot
                snapshot_metadata meta;
                meta.last_included_index = _next_offset - model::offset(1);

                return wr.write_metadata(meta)
                  .then([&wr, data = std::move(data)]() mutable {
                      auto& os = wr.output(); // kept alive by do_with above
                      return write_iobuf_to_output_stream(std::move(data), os);
                  })
                  .then([&wr] { return wr.close(); })
                  .then([this, &wr]() {
                      vlog(lg.debug, "Finishing snapshot creation");
                      return _snap.finish_snapshot(wr);
                  });
            });
      });
}

ss::future<> kvstore::recover() {
    return ss::async([this] {
        /*
         * after loading _next_offset will be set to either zero if no snapshot
         * is found, or the offset immediately following the snapshot offset.
         */
        load_snapshot_in_thread();

        auto dir = std::filesystem::path(_ntpc.work_directory());
        auto segments = _mgr.recover_segments(std::move(dir)).get0();

        replay_segments_in_thread(std::move(segments));
    });
}

void kvstore::load_snapshot_in_thread() {
    _gate.check(); // early out on shutdown

    // open snapshot reader, if a snapshot exists
    auto reader = _snap.open_snapshot().get0();
    if (!reader) {
        vlog(lg.debug, "Load snapshot: no snapshot found");
        _next_offset = model::offset(0);
        return;
    }
    auto close_reader = ss::defer([&reader] { return reader->close().get(); });

    // the snapshot metadata contains the last offset represented
    auto snap_meta = reader->read_metadata().get0();
    vlog(
      lg.debug,
      "Load snapshot: loading snapshot with last offset {}",
      snap_meta.last_included_index);

    // read and restore db from snapshot
    auto buf = read_iobuf_exactly(reader->input(), sizeof(int32_t)).get0();
    if (buf.size_bytes() != sizeof(int32_t)) {
        throw std::runtime_error(fmt::format(
          "Failed to read snapshot size. Wanted {} bytes != {}",
          sizeof(int32_t),
          buf.size_bytes()));
    }
    auto size = reflection::from_iobuf<int32_t>(std::move(buf));

    buf = read_iobuf_exactly(reader->input(), size).get0();
    if ((int32_t)buf.size_bytes() != size) {
        throw std::runtime_error(fmt::format(
          "Failed to read snapshot data. Wanted {} bytes != {}",
          size,
          buf.size_bytes()));
    }

    auto batch = reflection::from_iobuf<model::record_batch>(std::move(buf));

    auto batch_crc = model::crc_record_batch(batch);
    if (batch.header().crc != batch_crc) {
        throw std::runtime_error(fmt::format(
          "Snapshot batch failed crc {} != {}", batch_crc, batch.header().crc));
    }

    auto header_crc = model::internal_header_only_crc(batch.header());
    if (batch.header().header_crc != header_crc) {
        throw std::runtime_error(fmt::format(
          "Snapshot batch header failed crc {} != {}",
          header_crc,
          batch.header().header_crc));
    }

    for (auto& r : batch) {
        auto key = iobuf_to_bytes(r.release_key());
        auto res = _db.emplace(std::move(key), r.release_value());
        vassert(
          res.second, "Snapshot contained duplicate key {}", res.first->first);
        vlog(
          lg.trace,
          "Load snapshot: restoring key={} value={}",
          res.first->first,
          res.first->second);
    }

    _next_offset = snap_meta.last_included_index + model::offset(1);
}

void kvstore::replay_segments_in_thread(segment_set segs) {
    vlog(
      lg.info,
      "Replaying {} segments from offset {}",
      segs.size(),
      _next_offset);

    if (segs.empty()) {
        return;
    }

    // find segment that starts at _next_offset
    const auto match = std::find_if(
      segs.begin(), segs.end(), [this](ss::lw_shared_ptr<segment> seg) {
          return seg->offsets().base_offset == _next_offset;
      });

    // we didn't find an exact match, and the last segment starts after
    // _next_offset. this is unrecoverable. it's effectively a hole in the log.
    if (
      match == segs.end()
      && segs.back()->offsets().base_offset > _next_offset) {
        throw std::runtime_error(
          fmt::format("Segment starting at offset {} not found", _next_offset));
    }

    // if no exact match was found (match == segs.end()) then all the segments
    // are old and can be deleted. the recovery loop below will be skipped, and
    // we'll immediately gc the old segments.

    for (auto it = match; it != segs.end(); it++) {
        auto seg = *it;
        vlog(
          lg.info,
          "Replaying segment with base offset {}",
          seg->offsets().base_offset);
        vassert(
          seg->offsets().base_offset == _next_offset,
          "Segment base offset {} != expected next offset {}",
          seg->offsets().base_offset,
          _next_offset);

        auto input = seg->reader().data_stream(0, ss::default_priority_class());
        auto parser = std::make_unique<continuous_batch_parser>(
          std::make_unique<replay_consumer>(this), std::move(input));
        auto p = parser.get();
        p->consume()
          .discard_result()
          .finally([parser = std::move(parser)] {})
          .get();

        // early out on shutdown. parser will exit fast, but cleanly. here we
        // ensure the entire recovery process is halted.
        _gate.check();
    }

    // garbage collect range: [segs.begin(), match)
    for (auto it = segs.begin(); it != match; it++) {
        auto seg = *it;
        vlog(
          lg.info,
          "Removing old segment with base offset {}",
          seg->offsets().base_offset);
        seg->close().get();
        ss::remove_file(seg->reader().filename()).get();
        ss::remove_file(seg->index().filename()).get();
    }

    // close the rest
    for (auto it = match; it != segs.end(); it++) {
        (*it)->close().get();
    }

    // saving a snapshot right after recovery during start-up prevents an
    // accumulation of segments in cases where the system restarts many times
    // without ever filling up a segment and snapshotting when rolling. they'll
    // be removed on the next startup.
    save_snapshot().get();
}

batch_consumer::consume_result kvstore::replay_consumer::consume_batch_start(
  model::record_batch_header header, size_t, size_t) {
    if (_store->_gate.is_closed()) {
        // early out on shutdown
        return stop_parser::yes;
    }
    vassert(header.record_count > 0, "Unexpected empty batch");
    vassert(
      header.base_offset == _store->_next_offset,
      "Replaying from offset {} expected {}",
      header.base_offset,
      _store->_next_offset);
    _last_offset = header.last_offset();
    return skip_batch::no;
}

batch_consumer::consume_result
kvstore::replay_consumer::consume_record(model::record r) {
    auto key = iobuf_to_bytes(r.release_key());
    auto value = reflection::from_iobuf<std::optional<iobuf>>(
      r.release_value());
    _store->apply_op(std::move(key), std::move(value));
    _store->_next_offset += model::offset(1);
    return skip_batch::no;
}

void kvstore::replay_consumer::consume_compressed_records(iobuf&&) {
    vassert(false, "Key-value store does not support compressed records");
}

batch_consumer::stop_parser kvstore::replay_consumer::consume_batch_end() {
    const auto next_batch_offset = _last_offset + model::offset(1);
    vassert(
      _store->_next_offset == next_batch_offset,
      "Unexpected next offset {} expected {}",
      _store->_next_offset,
      next_batch_offset);
    return stop_parser::no;
}

} // namespace storage
