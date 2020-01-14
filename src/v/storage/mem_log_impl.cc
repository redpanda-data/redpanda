#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "seastarx.h"
#include "storage/log.h"

#include <seastar/core/future-util.hh>

namespace storage {
struct mem_log_impl;

class memory_map_consumer {
public:
    // expexts the 'next' valid offset
    explicit memory_map_consumer(model::offset o)
      : _idx(o) {}
    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        model::offset cur = _idx;
        _idx = _idx + model::offset(1);
        _result.emplace(cur, std::move(b));
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }
    std::map<model::offset, model::record_batch>&& end_of_stream() {
        return std::move(_result);
    }

private:
    model::offset _idx;
    std::map<model::offset, model::record_batch> _result;
};

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

struct mem_log_impl final : log::impl {
    // forward ctor
    explicit mem_log_impl(model::ntp n, ss::sstring workdir)
      : log::impl(std::move(n), std::move(workdir)) {}
    ~mem_log_impl() override = default;
    ss::future<> close() final { return ss::make_ready_future<>(); }
    ss::future<> flush() final { return ss::make_ready_future<>(); }

    ss::future<> truncate(model::offset offset, model::term_id) final {
        auto it = _data.find(offset);
        _data.erase(it, _data.end());
        return ss::make_ready_future<>();
    }

    model::record_batch_reader make_reader(log_reader_config cfg) final {
        auto it = _data.find(cfg.start_offset);
        return model::record_batch_reader(
          std::make_unique<mem_iter_reader>(it, _data.end(), cfg.max_offset));
    }

    ss::future<append_result>
    append(model::record_batch_reader&& r, log_append_config) final {
        auto o = max_offset();
        if (o() < 0) {
            o = model::offset(0);
        } else {
            o = o + model::offset(1);
        }
        return ss::do_with(
                 std::move(r),
                 [o](model::record_batch_reader& r) {
                     return r.consume(
                       memory_map_consumer(o), model::no_timeout);
                 })
          .then([this](std::map<model::offset, model::record_batch> m) {
              // TODO(agallego) missing .append_time
              append_result ret{
                .base_offset = m.begin()->first,
                .last_offset = m.rbegin()->first,
              };
              std::move(
                std::move_iterator(m.begin()),
                std::move_iterator(m.end()),
                std::inserter(_data, _data.end()));
              return ret;
          });
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
log make_memory_backed_log(model::ntp ntp, ss::sstring workdir) {
    auto ptr = ss::make_shared<mem_log_impl>(
      std::move(ntp), std::move(workdir));
    return storage::log(ptr);
}
} // namespace storage
