// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "likely.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "seastarx.h"
#include "storage/log.h"
#include "storage/logger.h"
#include "storage/types.h"
#include "vlog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/rwlock.hh>

#include <boost/container/flat_map.hpp>
#include <boost/intrusive/list_hook.hpp>
namespace storage {
struct entries_ordering {
    bool operator()(
      const model::record_batch& e1, const model::record_batch& e2) const {
        return e1.base_offset() < e2.base_offset();
    }

    bool operator()(const model::record_batch& e1, model::term_id term) const {
        return e1.term() < term;
    }

    bool operator()(const model::record_batch& e1, model::offset value) const {
        return e1.header().last_offset() < value;
    }
    bool
    operator()(const model::record_batch& e1, model::timestamp value) const {
        return e1.header().first_timestamp < value;
    }
};

struct mem_probe {
    void add_bytes_written(size_t sz) { partition_bytes += sz; }
    void remove_bytes_written(size_t sz) { partition_bytes -= sz; }
    size_t partition_bytes{0};
};

struct mem_log_impl;
// makes a copy of every batch starting at some iterator
class mem_iter_reader final
  : public model::record_batch_reader::impl
  , public boost::intrusive::list_base_hook<> {
public:
    using data_t = model::record_batch_reader::data_t;
    using foreign_data_t = model::record_batch_reader::foreign_data_t;
    using storage_t = model::record_batch_reader::storage_t;
    using underlying_t = std::deque<model::record_batch>;
    using iterator = typename underlying_t::iterator;
    mem_iter_reader(
      boost::intrusive::list<mem_iter_reader>& hook,
      iterator begin,
      iterator end,
      model::offset eof)
      : _hook(hook)
      , _cur(begin)
      , _end(end)
      , _endoffset(eof) {
        _hook.get().push_back(*this);
    }
    mem_iter_reader(const mem_iter_reader&) = delete;
    mem_iter_reader& operator=(const mem_iter_reader&) = delete;
    mem_iter_reader(mem_iter_reader&&) noexcept = default;
    mem_iter_reader& operator=(mem_iter_reader&&) noexcept = default;
    ~mem_iter_reader() final {
        _hook.get().erase(_hook.get().iterator_to(*this));
    }

    bool is_end_of_stream() const final {
        return _end_of_stream || _cur == _end
               || _cur->base_offset() > _endoffset;
    }

    ss::future<storage_t>
    do_load_slice(model::timeout_clock::time_point) final {
        data_t ret;
        if (!is_end_of_stream()) {
            ret.push_back(_cur->share());
            _cur = std::next(_cur);
        } else {
            _end_of_stream = true;
        }
        return ss::make_ready_future<storage_t>(std::move(ret));
    }

    void print(std::ostream& os) final {
        fmt::print(os, "{memory record reader}");
    }

    model::offset end_offset() { return _endoffset; }

    iterator end() { return _end; }

    void invalidate() { _end_of_stream = true; }

private:
    bool _end_of_stream = false;
    std::reference_wrapper<boost::intrusive::list<mem_iter_reader>> _hook;
    std::optional<model::record_batch> _data;
    iterator _cur;
    iterator _end;
    model::offset _endoffset;
};

class mem_log_appender final : public log_appender::impl {
public:
    explicit mem_log_appender(
      mem_log_impl& log, model::offset min_offset) noexcept
      : _log(log)
      , _min_offset(min_offset)
      , _cur_offset(min_offset) {}

    inline ss::future<ss::stop_iteration>
    operator()(model::record_batch&) final;

    inline ss::future<append_result> end_of_stream() final;

private:
    mem_log_impl& _log;
    model::offset _min_offset;
    model::offset _cur_offset;
    size_t _byte_size{0};
};

struct mem_log_impl final : log::impl {
    using underlying_t = std::deque<model::record_batch>;
    // forward ctor
    explicit mem_log_impl(ntp_config cfg)
      : log::impl(std::move(cfg)) {}
    ~mem_log_impl() override = default;
    mem_log_impl(const mem_log_impl&) = delete;
    mem_log_impl& operator=(const mem_log_impl&) = delete;
    mem_log_impl(mem_log_impl&&) noexcept = default;
    mem_log_impl& operator=(mem_log_impl&&) noexcept = delete;
    ss::future<std::optional<ss::sstring>> close() final {
        if (_eviction_monitor) {
            _eviction_monitor->promise.set_exception(
              std::runtime_error("log closed"));
        }
        return ss::make_ready_future<std::optional<ss ::sstring>>(std::nullopt);
    }
    ss::future<> remove() final { return ss::make_ready_future<>(); }
    ss::future<> flush() final { return ss::make_ready_future<>(); }
    ss::future<> compact(compaction_config cfg) final {
        return gc(cfg.eviction_time, cfg.max_bytes);
    }

    ss::future<> apply_segment_ms() final override { return ss::now(); }
    std::ostream& print(std::ostream& o) const final {
        fmt::print(o, "{{mem_log_impl:{}}}", offsets());
        return o;
    }
    ss::future<std::optional<timequery_result>>
    timequery(timequery_config cfg) final {
        using ret_t = std::optional<timequery_result>;
        if (cfg.time != model::timestamp{}) {
            auto it = std::lower_bound(
              std::cbegin(_data),
              std::cend(_data),
              cfg.time,
              entries_ordering{});
            if (
              it != _data.end() && it->header().base_offset <= cfg.max_offset) {
                return ss::make_ready_future<ret_t>(timequery_result(
                  it->header().base_offset, it->header().first_timestamp));
            }
        }
        return ss::make_ready_future<ret_t>();
    }
    ss::future<> truncate_prefix(truncate_prefix_config cfg) final {
        stlog.debug("PREFIX Truncating {} log at {}", config().ntp(), cfg);
        if (cfg.start_offset <= _start_offset) {
            return ss::make_ready_future<>();
        }
        for (auto& reader : _readers) {
            reader.invalidate();
        }
        auto it = std::lower_bound(
          std::begin(_data),
          std::end(_data),
          cfg.start_offset,
          entries_ordering{});

        _probe.remove_bytes_written(std::accumulate(
          _data.begin(),
          it,
          size_t(0),
          [](size_t acc, const model::record_batch& b) {
              return acc += b.size_bytes();
          }));
        _data.erase(_data.begin(), it);
        _start_offset = cfg.start_offset;
        return ss::make_ready_future<>();
    }
    ss::future<> truncate(truncate_config cfg) final {
        stlog.debug("Truncating {} log at {}", config().ntp(), cfg);
        if (unlikely(cfg.base_offset < model::offset(0))) {
            throw std::invalid_argument("cannot truncate at negative offset");
        }
        for (auto& reader : _readers) {
            reader.invalidate();
        }
        auto it = std::lower_bound(
          std::begin(_data),
          std::end(_data),
          cfg.base_offset,
          entries_ordering{});
        if (it != _data.end()) {
            if (it->base_offset() != cfg.base_offset) {
                throw std::invalid_argument{fmt::format(
                  "ntp {}: trying to truncate at offset {} which is not at "
                  "batch base (containing batch base offset: {})",
                  config().ntp(),
                  cfg.base_offset,
                  it->base_offset())};
            }

            _probe.remove_bytes_written(std::accumulate(
              it,
              _data.end(),
              size_t(0),
              [](size_t acc, const model::record_batch& b) {
                  return acc += b.size_bytes();
              }));
            _data.erase(it, _data.end());
        }
        return ss::make_ready_future<>();
    }

    ss::future<> gc(
      model::timestamp eviction_time,
      std::optional<size_t> max_partition_retention_size) {
        const size_t max = max_partition_retention_size.value_or(
          std::numeric_limits<size_t>::max());
        size_t reclaimed = 0;
        model::offset max_offset;
        for (const model::record_batch& b : _data) {
            if (
              b.header().max_timestamp <= eviction_time
              || (_probe.partition_bytes - reclaimed) > max) {
                max_offset = b.last_offset();
                reclaimed += b.size_bytes();
                continue;
            }

            break;
        }
        if (_eviction_monitor) {
            _eviction_monitor->promise.set_value(max_offset);
            _eviction_monitor.reset();
        }
        if (max_offset > _max_collectible_offset) {
            return ss::now();
        }

        auto it = _data.begin();
        while (it != _data.end() && it->last_offset() <= max_offset) {
            _probe.remove_bytes_written(it->size_bytes());
            it++;
        }

        if (it != _data.begin()) {
            _data.erase(_data.begin(), it);
            _data.shrink_to_fit();
        }
        return ss::now();
    }

    ss::future<model::offset> monitor_eviction(ss::abort_source& as) final {
        if (_eviction_monitor) {
            throw std::logic_error("Eviction promise already registered. "
                                   "Eviction can not be monitored twice.");
        }

        auto opt_sub = as.subscribe([this]() noexcept {
            _eviction_monitor->promise.set_exception(
              ss::abort_requested_exception());
        });

        // already aborted
        if (!opt_sub) {
            return ss::make_exception_future<model::offset>(
              ss::abort_requested_exception());
        }

        return _eviction_monitor
          .emplace(
            eviction_monitor{ss::promise<model::offset>{}, std::move(*opt_sub)})
          .promise.get_future();
    }

    void set_collectible_offset(model::offset o) final {
        _max_collectible_offset = std::max(_max_collectible_offset, o);
    }

    ss::future<> update_configuration(ntp_config::default_overrides o) final {
        mutable_config().set_overrides(o);
        return ss::now();
    }

    int64_t compaction_backlog() const final { return 0; }

    ss::future<model::record_batch_reader>
    make_reader(log_reader_config cfg) final {
        if (cfg.start_offset < _start_offset) {
            return ss::make_exception_future<model::record_batch_reader>(
              std::runtime_error(fmt::format(
                "Reader cannot read before start of the log {} < {}",
                cfg.start_offset,
                _start_offset)));
        }

        auto it = std::lower_bound(
          std::begin(_data),
          std::end(_data),
          cfg.start_offset,
          entries_ordering{});

        auto reader = model::record_batch_reader(
          std::make_unique<mem_iter_reader>(
            _readers, it, _data.end(), cfg.max_offset));
        return ss::make_ready_future<model::record_batch_reader>(
          std::move(reader));
    }

    log_appender make_appender(log_append_config) final {
        auto ofs = offsets();
        auto next_offset = ofs.dirty_offset;
        if (next_offset() >= 0) {
            // non-empty log, start appending at (dirty + 1)
            next_offset++;
        } else {
            // log is empty, start appending at _start_offset
            next_offset = ofs.start_offset;

            // but, in the case of a brand new log, no starting offset has been
            // explicitly set, so it is defined implicitly to be 0.
            if (next_offset() < 0) {
                next_offset = model::offset(0);
            }
        }

        return log_appender(
          std::make_unique<mem_log_appender>(*this, next_offset));
    }

    std::optional<model::term_id> get_term(model::offset o) const final {
        if (o != model::offset{}) {
            auto it = std::lower_bound(
              std::cbegin(_data), std::cend(_data), o, entries_ordering{});
            if (it != _data.end() && o >= _start_offset) {
                return it->term();
            }
        }

        return std::nullopt;
    }

    std::optional<model::offset>
    get_term_last_offset(model::term_id term) const final {
        for (auto it = _data.rbegin(); it != _data.rend(); ++it) {
            if (it->term() == term) {
                return it->last_offset();
            }
        }
        return std::nullopt;
    }

    size_t segment_count() const final { return 1; }

    storage::offset_stats offsets() const final {
        if (_data.empty()) {
            offset_stats ret;
            ret.start_offset = _start_offset;
            if (ret.start_offset > model::offset(0)) {
                ret.dirty_offset = ret.start_offset - model::offset(1);
                ret.committed_offset = ret.dirty_offset;
            }
            return ret;
        }
        auto& b = _data.front();
        auto start_offset = _start_offset() >= 0 ? _start_offset
                                                 : b.base_offset();

        auto& e = _data.back();

        return storage::offset_stats{
          .start_offset = start_offset,
          .committed_offset = e.last_offset(),
          .committed_offset_term = e.term(),
          .dirty_offset = e.last_offset(),
          .dirty_offset_term = e.term()};
    }

    model::offset find_last_term_start_offset() const final {
        auto& e = _data.back();
        auto it = std::lower_bound(
          std::cbegin(_data), std::cend(_data), e.term(), entries_ordering{});
        return it->base_offset();
    }

    model::timestamp start_timestamp() const final {
        if (_data.size()) {
            return _data.begin()->header().first_timestamp;
        } else {
            return model::timestamp{};
        }
    }

    size_t size_bytes() const override {
        return std::accumulate(
          _data.cbegin(),
          _data.cend(),
          size_t(0),
          [](size_t acc, const model::record_batch& b) {
              return acc + b.size_bytes();
          });
    }

    struct eviction_monitor {
        ss::promise<model::offset> promise;
        ss::abort_source::subscription subscription;
    };
    boost::intrusive::list<mem_iter_reader> _readers;
    underlying_t _data;
    model::offset _start_offset;
    std::optional<eviction_monitor> _eviction_monitor;
    ss::rwlock _eviction_lock;
    mem_probe _probe;
    model::offset _max_collectible_offset;
};

ss::future<ss::stop_iteration>
mem_log_appender::operator()(model::record_batch& batch) {
    batch.header().base_offset = _cur_offset;
    _byte_size += batch.header().size_bytes;
    vlog(
      stlog.trace,
      "Writing to {}, batch size {} bytes, records offsets [{},{}], term {}",
      _log.config().ntp(),
      batch.header().size_bytes,
      batch.base_offset(),
      batch.last_offset(),
      batch.term());
    _cur_offset = batch.last_offset() + model::offset(1);
    _log._probe.add_bytes_written(batch.size_bytes());
    _log._data.emplace_back(std::move(batch));
    return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::no);
}

ss::future<append_result> mem_log_appender::end_of_stream() {
    append_result ret{
      .append_time = ss::lowres_clock::now(),
      .base_offset = _min_offset,
      .last_offset = _cur_offset - model::offset(1),
      .byte_size = _byte_size,
      .last_term = _log._data.back().term()};
    return ss::make_ready_future<append_result>(ret);
}

log make_memory_backed_log(ntp_config cfg) {
    auto ptr = ss::make_shared<mem_log_impl>(std::move(cfg));
    return storage::log(ptr);
}
} // namespace storage
