#pragma once

#include "likely.h"
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

/**
 * The BatchReaderConsumerInitialize is a batch reader consumer that contains an
 * initialization life cycle hook. The hook is called before the first call to
 * operator() and can be used to do things like initialize data on disk as is
 * the case fo the log_writer.
 */
// clang-format off
CONCEPT(template<typename Consumer> concept bool BatchReaderConsumer() {
    return requires(Consumer c, record_batch b) {
        { c(std::move(b)) } -> ss::future<ss::stop_iteration>;
        c.end_of_stream();
    };
})
CONCEPT(template<typename Consumer> concept bool BatchReaderConsumerInitialize() {
    return requires(Consumer c, record_batch b) {
        { c(std::move(b)) } -> ss::future<ss::stop_iteration>;
        { c.initialize() } -> ss::future<>;
        c.end_of_stream();
    };
})
// clang-format on

// A stream of `model::record_batch`s, consumed one-by-one but preloaded
// in slices.
class record_batch_reader final {
public:
    class impl {
        template<typename C, typename = int>
        struct has_initialize : std::false_type {};

        template<typename C>
        struct has_initialize<C, decltype(&C::initialize, 0)>
          : std::true_type {};

    protected:
        // FIXME: In C++20, use std::span.
        using span = gsl::span<record_batch>;

        virtual ss::future<span> do_load_slice(timeout_clock::time_point) = 0;

    public:
        virtual ~impl() = default;

        bool end_of_stream() const { return _end_of_stream; }

        bool is_slice_empty() const { return _current == _slice.end(); }

        virtual ss::future<> load_slice(timeout_clock::time_point timeout) {
            return do_load_slice(timeout).then([this](span s) {
                _slice = s;
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

        ss::future<record_batch_opt>
        operator()(timeout_clock::time_point timeout) {
            if (!is_slice_empty()) {
                return ss::make_ready_future<record_batch_opt>(pop_batch());
            }
            if (end_of_stream()) {
                return ss::make_ready_future<record_batch_opt>();
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

        template<
          typename Consumer,
          typename std::enable_if_t<has_initialize<Consumer>::value, int> = 0>
        auto consume(Consumer consumer, timeout_clock::time_point timeout) {
            return ss::do_with(
              std::move(consumer), [this, timeout](Consumer& consumer) {
                  return consumer.initialize().then([this, timeout, &consumer] {
                      return do_consume(consumer, timeout);
                  });
              });
        }

        template<
          typename Consumer,
          typename std::enable_if_t<!has_initialize<Consumer>::value, int> = 0>
        auto consume(Consumer consumer, timeout_clock::time_point timeout) {
            return ss::do_with(
              std::move(consumer), [this, timeout](Consumer& consumer) {
                  return do_consume(consumer, timeout);
              });
        }

    protected:
        bool _end_of_stream = false;
        span _slice = span();
        span::iterator _current = _slice.end();

    private:
        template<typename Consumer>
        auto do_consume(Consumer& consumer, timeout_clock::time_point timeout) {
            return ss::repeat([this, timeout, &consumer] {
                       if (likely(!is_slice_empty())) {
                           return consumer(pop_batch());
                       }
                       if (end_of_stream()) {
                           return ss::make_ready_future<ss::stop_iteration>(
                             ss::stop_iteration::yes);
                       }
                       return load_slice(timeout).then(
                         [] { return ss::stop_iteration::no; });
                   })
              .then([&consumer] { return consumer.end_of_stream(); });
        }
    };

public:
    explicit record_batch_reader(std::unique_ptr<impl> impl) noexcept
      : _impl(std::move(impl)) {}
    record_batch_reader(const record_batch_reader&) = delete;
    record_batch_reader& operator=(const record_batch_reader&) = delete;
    record_batch_reader(record_batch_reader&&) noexcept = default;
    record_batch_reader& operator=(record_batch_reader&&) noexcept = default;

    ss::future<record_batch_opt> operator()(timeout_clock::time_point timeout) {
        return _impl->operator()(timeout);
    }

    // Can only be called if !should_load_slice().
    record_batch pop_batch() { return _impl->pop_batch(); }

    // Can only be called if !should_load_slice().
    const record_batch& peek_batch() const { return _impl->peek_batch(); }

    ss::future<> load_slice(timeout_clock::time_point timeout) {
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
    CONCEPT(
      requires BatchReaderConsumer<Consumer>()
      || BatchReaderConsumerInitialize<Consumer>())
    auto consume(Consumer consumer, timeout_clock::time_point timeout) & {
        return _impl->consume(std::move(consumer), timeout);
    }

    /**
     * A common pattern is
     *
     *    auto reader = make_reader(...);
     *    return reader.consume(writer(), ..);
     *
     * which means that we need a way to deal with the common case of
     * reader.consume returning a future and reader going out of scope. that is
     * what this r-value ref qualified version is for. instead, write
     *
     *    return std::move(reader).consume(writer(), ..);
     *
     * and the internal impl will be released and held onto for the lifetime of
     * the consume method.
     */
    template<typename Consumer>
    CONCEPT(
      requires BatchReaderConsumer<Consumer>()
      || BatchReaderConsumerInitialize<Consumer>())
    auto consume(Consumer consumer, timeout_clock::time_point timeout) && {
        /*
         * ideally what we would do here is:
         *
         *    ss::shared_ptr p(std::move(_impl));
         *
         * but ss::shared_ptr has no such constructor, and we cannot use
         * ss::make_shared<impl>(_impl.release()) since impl is abstract. so
         * this would appear to be an example of a valid case where manual
         * memory management is necessary.
         */
        auto i = _impl.release();
        return i->consume(std::move(consumer), timeout).finally([i] {
            delete i; // NOLINT
        });
    }

private:
    std::unique_ptr<impl> _impl;

    record_batch_reader() = default;
    explicit operator bool() const noexcept { return bool(_impl); }
    friend class ss::optimized_optional<record_batch_reader>;
};

using record_batch_reader_opt = ss::optimized_optional<record_batch_reader>;

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
make_generating_record_batch_reader(ss::noncopyable_function<ss::future<record_batch_opt>()>);
// clang-format on

} // namespace model
