#pragma once

#include "model/record.h"
#include "model/timeout_clock.h"
#include "seastarx.h"
#include "utils/concepts-enabled.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/util/optimized_optional.hh>

#include <gsl/span>

#include <memory>
#include <vector>

namespace model {

// clang-format off
CONCEPT(template<typename Consumer> concept bool BatchReaderConsumer() {
    return requires(Consumer c, record_batch b) {
        { c(std::move(b)) } -> future<stop_iteration>;
        c.end_of_stream();
    };
})
// clang-format on

// A stream of `model::record_batch`s, consumed one-by-one but preloaded
// in slices.
class record_batch_reader final {
public:
    class impl {
    protected:
        // FIXME: In C++20, use std::span.
        using span = gsl::span<record_batch>;

        virtual future<span> do_load_slice(timeout_clock::time_point) = 0;

    public:
        virtual ~impl() {}

        bool end_of_stream() const { return _end_of_stream; }

        bool is_slice_empty() const { return _current == _slice.end(); }

        virtual future<> load_slice(timeout_clock::time_point timeout) {
            return do_load_slice(timeout).then([this](span s) {
                _slice = std::move(s);
                _current = _slice.begin();
            });
        }

        std::vector<record_batch> release_buffered_batches() {
            std::vector<record_batch> retval;
            retval.reserve(std::distance(_current, _slice.end()));
            while (!is_slice_empty()) {
                retval.push_back(pop_batch());
            }
            return retval;
        }

        future<record_batch_opt> operator()(timeout_clock::time_point timeout) {
            if (!is_slice_empty()) {
                return make_ready_future<record_batch_opt>(pop_batch());
            }
            if (end_of_stream()) {
                return make_ready_future<record_batch_opt>();
            }
            return load_slice(timeout).then(
              [this, timeout] { return operator()(timeout); });
        }

        record_batch pop_batch() {
            auto& batch = *_current;
            _current++;
            return std::move(batch);
        }

        const record_batch& peek_batch() const { return *_current; }

        template<typename Consumer>
        auto consume(Consumer consumer, timeout_clock::time_point timeout) {
            return do_with(
              std::move(consumer), [this, timeout](Consumer& consumer) {
                  return repeat([this, timeout, &consumer] {
                             if (end_of_stream() && is_slice_empty()) {
                                 return make_ready_future<stop_iteration>(
                                   stop_iteration::yes);
                             }
                             if (is_slice_empty()) {
                                 return load_slice(timeout).then(
                                   [] { return stop_iteration::no; });
                             }
                             return consumer(pop_batch());
                         })
                    .then([&consumer] { return consumer.end_of_stream(); });
              });
        }

    protected:
        bool _end_of_stream = false;
        span _slice = span();
        span::iterator _current = _slice.end();
    };

public:
    explicit record_batch_reader(std::unique_ptr<impl> impl) noexcept
      : _impl(std::move(impl)) {}
    record_batch_reader(const record_batch_reader&) = delete;
    record_batch_reader& operator=(const record_batch_reader&) = delete;
    record_batch_reader(record_batch_reader&&) noexcept = default;
    record_batch_reader& operator=(record_batch_reader&&) noexcept = default;

    future<record_batch_opt> operator()(timeout_clock::time_point timeout) {
        return _impl->operator()(timeout);
    }

    // Can only be called if !should_load_slice().
    record_batch pop_batch() { return _impl->pop_batch(); }

    // Can only be called if !should_load_slice().
    const record_batch& peek_batch() const { return _impl->peek_batch(); }

    future<> load_slice(timeout_clock::time_point timeout) {
        return _impl->load_slice(timeout);
    }

    bool end_of_stream() const {
        return _impl->is_slice_empty() && _impl->end_of_stream();
    }

    bool should_load_slice() const {
        return _impl->is_slice_empty() && !_impl->end_of_stream();
    }

    std::vector<record_batch> release_buffered_batches() {
        return _impl->release_buffered_batches();
    }
    // Stops when consumer returns stop_iteration::yes or end of stream is
    // reached. Next call will start from the next mutation_fragment in the
    // stream.
    template<typename Consumer>
    CONCEPT(requires BatchReaderConsumer<Consumer>())
    auto consume(Consumer consumer, timeout_clock::time_point timeout) {
        return _impl->consume(std::move(consumer), timeout);
    }

private:
    std::unique_ptr<impl> _impl;

    record_batch_reader() = default;
    explicit operator bool() const noexcept { return bool(_impl); }
    friend class optimized_optional<record_batch_reader>;
};

using record_batch_reader_opt = optimized_optional<record_batch_reader>;

template<typename Impl, typename... Args>
record_batch_reader make_record_batch_reader(Args&&... args) {
    return record_batch_reader(
      std::make_unique<Impl>(std::forward<Args>(args)...));
}

record_batch_reader
  make_memory_record_batch_reader(std::vector<model::record_batch>);

inline record_batch_reader
make_memory_record_batch_reader(model::record_batch b) {
    std::vector<model::record_batch> batches;
    batches.reserve(1);
    batches.push_back(std::move(b));
    return make_memory_record_batch_reader(std::move(batches));
}
// clang-format off
record_batch_reader
make_generating_record_batch_reader(noncopyable_function<future<record_batch_opt>()>);
// clang-format on

} // namespace model
