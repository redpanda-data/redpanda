#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "seastarx.h"
#include "storage/log.h"

#include <seastar/core/future-util.hh>

namespace storage {
struct mem_log_impl;

// makes a copy of every batch starting at some iterator
class mem_iter_reader final : public model::record_batch_reader::impl {
public:
    using map_t = std::map<model::offset, model::record_batch>;
    using iterator = typename map_t::iterator;
    mem_iter_reader(iterator begin, iterator end, model::offset eof)
      : _cur(begin)
      , _end(end)
      , _endoffset(eof) {}
    mem_iter_reader(const mem_iter_reader&) = delete;
    mem_iter_reader& operator=(const mem_iter_reader&) = delete;
    mem_iter_reader(mem_iter_reader&&) noexcept = default;
    mem_iter_reader& operator=(mem_iter_reader&&) noexcept = default;

    ss::future<span> do_load_slice(model::timeout_clock::time_point) final {
        std::next(_cur);
        if (_cur == _end || _cur->first > _endoffset) {
            _end_of_stream = true;
            return ss::make_ready_future<span>();
        }
        _data = _cur->second.share();
        model::record_batch* x = &_data.value();
        return ss::make_ready_future<span>(span(x, 1));
    }

private:
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

    virtual ss::future<> initialize() override {
        return ss::make_ready_future<>();
    }

    virtual inline ss::future<ss::stop_iteration>
    operator()(model::record_batch&&) override;

    virtual inline ss::future<append_result> end_of_stream() override;

private:
    mem_log_impl& _log;
    model::offset _min_offset;
    model::offset _cur_offset;
};

struct mem_log_impl final : log::impl {
    // forward ctor
    explicit mem_log_impl(model::ntp n, ss::sstring workdir)
      : log::impl(std::move(n), std::move(workdir)) {}
    ~mem_log_impl() override = default;
    ss::future<> close() final { return ss::make_ready_future<>(); }
    ss::future<> flush() final { return ss::make_ready_future<>(); }

    ss::future<> truncate(model::offset offset) final {
        auto it = _data.find(offset);
        _data.erase(it, _data.end());
        return ss::make_ready_future<>();
    }

    model::record_batch_reader make_reader(log_reader_config cfg) final {
        auto it = _data.find(cfg.start_offset);
        return model::record_batch_reader(
          std::make_unique<mem_iter_reader>(it, _data.end(), cfg.max_offset));
    }

    log_appender make_appender(log_append_config cfg) final {
        auto o = max_offset();
        if (o() < 0) {
            o = model::offset(0);
        } else {
            o = o + model::offset(1);
        }
        return log_appender(std::make_unique<mem_log_appender>(*this, o));
    }

    size_t segment_count() const final { return 1; }

    model::offset start_offset() const final {
        // default value
        if (_data.empty()) {
            return model::offset{};
        }
        return _data.begin()->first;
    }
    model::offset max_offset() const final {
        // default value
        if (_data.empty()) {
            return model::offset{};
        }
        auto it = _data.end();
        return std::prev(it)->first;
    }

    model::offset committed_offset() const final { return max_offset(); }

    std::map<model::offset, model::record_batch> _data;
};

ss::future<ss::stop_iteration> mem_log_appender::
operator()(model::record_batch&& batch) {
    model::offset cur = _cur_offset;
    _cur_offset = _cur_offset + model::offset(1);
    _log._data.emplace(cur, std::move(batch));
    return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::no);
}

ss::future<append_result> mem_log_appender::end_of_stream() {
    // TODO(agallego) missing .append_time
    append_result ret{
      .base_offset = _min_offset,
      .last_offset = _cur_offset - model::offset(1),
    };
    return ss::make_ready_future<append_result>(ret);
}

log make_memory_backed_log(model::ntp ntp, ss::sstring workdir) {
    auto ptr = ss::make_shared<mem_log_impl>(
      std::move(ntp), std::move(workdir));
    return storage::log(ptr);
}
} // namespace storage
