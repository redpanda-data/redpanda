#pragma once

#include "likely.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "seastarx.h"
#include "utils/concepts-enabled.h"

#include <seastar/core/circular_buffer.hh>
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
        { c(std::move(b)) } -> ss::future<ss::stop_iteration>;
        c.end_of_stream();
    };
})
// clang-format on

// A stream of `model::record_batch`s, consumed one-by-one but preloaded
// in slices.
class record_batch_reader final {
public:
    using storage_t = ss::circular_buffer<model::record_batch>;

    class impl {
    public:
        impl() noexcept = default;
        impl(impl&& o) noexcept = default;
        impl& operator=(impl&& o) noexcept = default;
        impl(const impl& o) = delete;
        impl& operator=(const impl& o) = delete;
        virtual ~impl() noexcept = default;

        virtual bool is_end_of_stream() const = 0;

        virtual ss::future<storage_t>
          do_load_slice(timeout_clock::time_point) = 0;

        virtual void print(std::ostream&) = 0;

        bool is_slice_empty() const { return _slice.empty(); }

        template<typename Consumer>
        auto consume(Consumer consumer, timeout_clock::time_point timeout) {
            return ss::do_with(
              std::move(consumer), [this, timeout](Consumer& consumer) {
                  return do_consume(consumer, timeout);
              });
        }

    private:
        record_batch pop_batch() {
            record_batch batch = std::move(_slice.front());
            _slice.pop_front();
            return batch;
        }

        ss::future<> load_slice(timeout_clock::time_point timeout) {
            return do_load_slice(timeout).then(
              [this](storage_t s) { _slice = std::move(s); });
        }

        template<typename Consumer>
        auto do_consume(Consumer& consumer, timeout_clock::time_point timeout) {
            return ss::repeat([this, timeout, &consumer] {
                       if (likely(!is_slice_empty())) {
                           return consumer(pop_batch());
                       }
                       if (is_end_of_stream()) {
                           return ss::make_ready_future<ss::stop_iteration>(
                             ss::stop_iteration::yes);
                       }
                       return load_slice(timeout).then(
                         [] { return ss::stop_iteration::no; });
                   })
              .then([&consumer] { return consumer.end_of_stream(); });
        }

        storage_t _slice;
    };

public:
    explicit record_batch_reader(std::unique_ptr<impl> impl) noexcept
      : _impl(std::move(impl)) {}
    record_batch_reader(const record_batch_reader&) = delete;
    record_batch_reader& operator=(const record_batch_reader&) = delete;
    record_batch_reader(record_batch_reader&&) noexcept = default;
    record_batch_reader& operator=(record_batch_reader&&) noexcept = default;
    ~record_batch_reader() noexcept = default;


    bool is_end_of_stream() const {
        return _impl->is_slice_empty() && _impl->is_end_of_stream();
    }

    // Stops when consumer returns stop_iteration::yes or end of stream is
    // reached. Next call will start from the next mutation_fragment in the
    // stream.
    template<typename Consumer>
    CONCEPT(requires BatchReaderConsumer<Consumer>())
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
    CONCEPT(requires BatchReaderConsumer<Consumer>())
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
        auto raw = _impl.get();
        return raw->consume(std::move(consumer), timeout)
          .finally([i = std::move(_impl)] {});
    }

    std::unique_ptr<impl> release() && { return std::move(_impl); }

private:
    std::unique_ptr<impl> _impl;

    record_batch_reader() = default;
    explicit operator bool() const noexcept { return bool(_impl); }
    friend class ss::optimized_optional<record_batch_reader>;

    friend std::ostream&
    operator<<(std::ostream& os, const record_batch_reader& r);
};

using record_batch_reader_opt = ss::optimized_optional<record_batch_reader>;

template<typename Impl, typename... Args>
record_batch_reader make_record_batch_reader(Args&&... args) {
    return record_batch_reader(
      std::make_unique<Impl>(std::forward<Args>(args)...));
}

record_batch_reader
  make_memory_record_batch_reader(record_batch_reader::storage_t);

inline record_batch_reader
make_memory_record_batch_reader(model::record_batch b) {
    record_batch_reader::storage_t batches;
    batches.push_back(std::move(b));
    return make_memory_record_batch_reader(std::move(batches));
}
record_batch_reader make_generating_record_batch_reader(
  ss::noncopyable_function<ss::future<record_batch_opt>()>);

ss::future<record_batch_reader::storage_t> consume_reader_to_memory(
  record_batch_reader, timeout_clock::time_point timeout);

/// \brief wraps a reader into a foreign_ptr<unique_ptr>
record_batch_reader make_foreign_record_batch_reader(record_batch_reader&&);
std::ostream& operator<<(std::ostream& os, const record_batch_reader& r);

} // namespace model
