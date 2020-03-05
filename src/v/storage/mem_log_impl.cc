#include "likely.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "seastarx.h"
#include "storage/log.h"
#include "storage/logger.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

#include <boost/container/flat_map.hpp>
#include <boost/intrusive/list_hook.hpp>
namespace storage {
struct entries_ordering {
    bool operator()(
      const model::record_batch& e1, const model::record_batch& e2) const {
        return e1.base_offset() < e2.base_offset();
    }
    bool operator()(const model::record_batch& e1, model::offset value) const {
        return e1.header().last_offset() < value;
    }
    bool
    operator()(const model::record_batch& e1, model::timestamp value) const {
        return e1.header().first_timestamp < value;
    }
};

struct mem_log_impl;
// makes a copy of every batch starting at some iterator
class mem_iter_reader final
  : public model::record_batch_reader::impl
  , public boost::intrusive::list_base_hook<> {
public:
    using underlying_t = std::vector<model::record_batch>;
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

    bool end_of_stream() const final {
        return _end_of_stream || _cur == _end
               || _cur->base_offset() > _endoffset;
    }

    ss::future<ss::circular_buffer<model::record_batch>>
    do_load_slice(model::timeout_clock::time_point) final {
        ss::circular_buffer<model::record_batch> ret;
        if (!end_of_stream()) {
            ret.push_back(_cur->share());
            _cur = std::next(_cur);
        } else {
            _end_of_stream = true;
        }
        return ss::make_ready_future<ss::circular_buffer<model::record_batch>>(
          std::move(ret));
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

    ss::future<> initialize() final { return ss::make_ready_future<>(); }

    inline ss::future<ss::stop_iteration>
    operator()(model::record_batch&&) final;

    inline ss::future<append_result> end_of_stream() final;

private:
    mem_log_impl& _log;
    model::offset _min_offset;
    model::offset _cur_offset;
    size_t _byte_size{0};
};

struct mem_log_impl final : log::impl {
    using underlying_t = std::vector<model::record_batch>;
    // forward ctor
    explicit mem_log_impl(model::ntp n, ss::sstring workdir)
      : log::impl(std::move(n), std::move(workdir)) {}
    ~mem_log_impl() override = default;
    mem_log_impl(const mem_log_impl&) = delete;
    mem_log_impl& operator=(const mem_log_impl&) = delete;
    mem_log_impl(mem_log_impl&&) noexcept = default;
    mem_log_impl& operator=(mem_log_impl&&) noexcept = default;
    ss::future<> close() final { return ss::make_ready_future<>(); }
    ss::future<> flush() final { return ss::make_ready_future<>(); }

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

    ss::future<> truncate(model::offset offset) final {
        stlog.debug("Truncating {} log at {}", ntp(), offset);
        if (unlikely(offset < model::offset(0))) {
            throw std::invalid_argument("cannot truncate at negative offset");
        }
        for (auto& reader : _readers) {
            reader.invalidate();
        }

        auto it = std::lower_bound(
          std::begin(_data), std::end(_data), offset, entries_ordering{});

        _data.erase(it, _data.end());
        return ss::make_ready_future<>();
    }

    ss::future<model::record_batch_reader>
    make_reader(log_reader_config cfg) final {
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
        auto o = max_offset();
        if (o() < 0) {
            o = model::offset(0);
        } else {
            o = o + model::offset(1);
        }
        return log_appender(std::make_unique<mem_log_appender>(*this, o));
    }

    std::optional<model::term_id> get_term(model::offset o) const final {
        if (o != model::offset{}) {
            auto it = std::lower_bound(
              std::cbegin(_data), std::cend(_data), o, entries_ordering{});
            if (it != _data.end()) {
                return it->term();
            }
        }

        return std::nullopt;
    }

    size_t segment_count() const final { return 1; }

    model::offset start_offset() const final {
        // default value
        if (_data.empty()) {
            return model::offset{};
        }
        return _data.begin()->base_offset();
    }
    model::offset max_offset() const final {
        // default value
        if (_data.empty()) {
            return model::offset{};
        }
        auto it = _data.end();
        return std::prev(it)->last_offset();
    }

    model::offset committed_offset() const final { return max_offset(); }
    boost::intrusive::list<mem_iter_reader> _readers;
    underlying_t _data;
};

ss::future<ss::stop_iteration>
mem_log_appender::operator()(model::record_batch&& batch) {
    batch.header().base_offset = _cur_offset;
    _byte_size += batch.header().size_bytes;
    stlog.trace(
      "Wrting to {} batch of {} records offsets [{},{}], term {}",
      _log.ntp(),
      batch.header().size_bytes,
      batch.base_offset(),
      batch.last_offset(),
      batch.term());
    auto offset = _cur_offset;
    _cur_offset = batch.last_offset() + model::offset(1);
    _log._data.emplace_back(std::move(batch));
    return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::no);
}

ss::future<append_result> mem_log_appender::end_of_stream() {
    append_result ret{.append_time = ss::lowres_clock::now(),
                      .base_offset = _min_offset,
                      .last_offset = _cur_offset - model::offset(1),
                      .byte_size = _byte_size,
                      .last_term = _log._data.rbegin()->term()};
    return ss::make_ready_future<append_result>(ret);
}

log make_memory_backed_log(model::ntp ntp, ss::sstring workdir) {
    auto ptr = ss::make_shared<mem_log_impl>(
      std::move(ntp), std::move(workdir));
    return storage::log(ptr);
}
} // namespace storage
