/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "likely.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "seastarx.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/util/optimized_optional.hh>
#include <seastar/util/variant_utils.hh>

#include <memory>
#include <variant>

namespace model {

template<typename Consumer>
concept BatchReaderConsumer = requires(Consumer c, record_batch&& b) {
    { c(std::move(b)) } -> std::same_as<ss::future<ss::stop_iteration>>;
    c.end_of_stream();
};

template<typename ReferenceConsumer>
concept ReferenceBatchReaderConsumer
  = requires(ReferenceConsumer c, record_batch& b) {
    { c(b) } -> std::same_as<ss::future<ss::stop_iteration>>;
    c.end_of_stream();
};

class record_batch_reader final {
public:
    using data_t = ss::circular_buffer<model::record_batch>;
    struct foreign_data_t {
        ss::foreign_ptr<std::unique_ptr<data_t>> buffer;
        size_t index{0};
    };
    using storage_t = std::variant<data_t, foreign_data_t>;

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

        bool is_slice_empty() const {
            return ss::visit(
              _slice,
              [](const data_t& d) {
                  // circular buffer is the default
                  return d.empty();
              },
              [](const foreign_data_t& d) {
                  return d.index >= d.buffer->size();
              });
        }

        virtual ss::future<> finally() noexcept { return ss::now(); }

        /// Meant for non-owning iteration of the data. If you need to own the
        /// batches, please use consume() below
        template<typename ReferenceConsumer>
        auto for_each_ref(ReferenceConsumer c, timeout_clock::time_point tm) {
            return ss::do_with(std::move(c), [this, tm](ReferenceConsumer& c) {
                return do_for_each_ref(c, tm);
            });
        }
        template<typename Consumer>
        auto consume(Consumer consumer, timeout_clock::time_point timeout) {
            return ss::do_with(
              std::move(consumer), [this, timeout](Consumer& consumer) {
                  return do_consume(consumer, timeout);
              });
        }

    private:
        record_batch pop_batch() {
            return ss::visit(
              _slice,
              [](data_t& d) {
                  record_batch batch = std::move(d.front());
                  d.pop_front();
                  return batch;
              },
              [](foreign_data_t& d) {
                  // cannot have a move-only type from a remote core
                  // we must make a copy. for iteration use for_each_ref
                  return (*d.buffer)[d.index++].copy();
              });
        }
        ss::future<> load_slice(timeout_clock::time_point timeout) {
            return do_load_slice(timeout).then([this](storage_t s) {
                // reassign the local cache
                _slice = std::move(s);
            });
        }
        template<typename ReferenceConsumer>
        auto do_for_each_ref(
          ReferenceConsumer& refc, timeout_clock::time_point timeout) {
            return do_action(refc, timeout, [this](ReferenceConsumer& c) {
                return ss::visit(
                  _slice,
                  [&c](data_t& d) {
                      return c(d.front()).finally([&d] { d.pop_front(); });
                  },
                  [&c](foreign_data_t& d) {
                      // for remote core, next simply means advancing the
                      // pointer, we need to release the batches wholesale
                      return c((*d.buffer)[d.index++]);
                  });
            });
        }
        template<typename Consumer>
        auto do_consume(Consumer& consumer, timeout_clock::time_point timeout) {
            return do_action(consumer, timeout, [this](Consumer& c) {
                return c(pop_batch());
            });
        }
        template<typename ConsumerType, typename ActionFn>
        auto do_action(
          ConsumerType& consumer,
          timeout_clock::time_point timeout,
          ActionFn&& fn) {
            return ss::repeat([this,
                               timeout,
                               &consumer,
                               fn = std::forward<ActionFn>(fn)] {
                       if (likely(!is_slice_empty())) {
                           return fn(consumer);
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

    /// \brief Intended for non-owning iteration of the data
    /// if you need to own the data, please use consume() below
    /// Stops when consumer returns stop_iteration::yes or end of stream
    template<typename ReferenceConsumer>
    requires ReferenceBatchReaderConsumer<ReferenceConsumer>
    auto for_each_ref(
      ReferenceConsumer consumer, timeout_clock::time_point timeout) & {
        return _impl->for_each_ref(std::move(consumer), timeout);
    }
    /// \brief Intended for non-owning iteration of the data
    /// if you need to own the data, please use consume() below
    /// Stops when consumer returns stop_iteration::yes or end of stream
    ///
    /// r-value version so you can do std::move(reader).do_for_each_ref();
    ///
    template<typename ReferenceConsumer>
    requires ReferenceBatchReaderConsumer<ReferenceConsumer>
    auto for_each_ref(
      ReferenceConsumer consumer, timeout_clock::time_point timeout) && {
        auto raw = _impl.get();
        return raw->for_each_ref(std::move(consumer), timeout)
          .finally([raw, i = std::move(_impl)]() mutable {
              return raw->finally().finally([i = std::move(i)] {});
          });
    }

    // Stops when consumer returns stop_iteration::yes or end of stream is
    // reached. Next call will start from the next mutation_fragment in the
    // stream.
    template<typename Consumer>
    requires BatchReaderConsumer<Consumer>
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
    requires BatchReaderConsumer<Consumer>
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
          .finally([raw, i = std::move(_impl)]() mutable {
              return raw->finally().finally([i = std::move(i)] {});
          });
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

template<typename Impl, typename... Args>
record_batch_reader make_record_batch_reader(Args&&... args) {
    return record_batch_reader(
      std::make_unique<Impl>(std::forward<Args>(args)...));
}

record_batch_reader
  make_memory_record_batch_reader(record_batch_reader::storage_t);

inline record_batch_reader
make_memory_record_batch_reader(model::record_batch b) {
    record_batch_reader::data_t batches;
    batches.reserve(1);
    batches.push_back(std::move(b));
    return make_memory_record_batch_reader(std::move(batches));
}

template<typename Func>
requires requires(Func f, model::record_batch&& batch) {
    { f(std::move(batch)) } -> std::same_as<model::record_batch>;
}
ss::future<record_batch_reader::data_t> transform_reader_to_memory(
  record_batch_reader reader, timeout_clock::time_point timeout, Func&& f) {
    using data_t = record_batch_reader::data_t;
    class consumer {
    public:
        explicit consumer(Func f)
          : _func(std::move(f)) {}

        ss::future<ss::stop_iteration> operator()(model::record_batch b) {
            _result.push_back(_func(std::move(b)));
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        }
        record_batch_reader::data_t end_of_stream() {
            return std::move(_result);
        }

    private:
        data_t _result;
        Func _func;
    };
    return std::move(reader).consume(consumer(std::forward<Func>(f)), timeout);
}

record_batch_reader make_foreign_memory_record_batch_reader(record_batch);

record_batch_reader
  make_foreign_memory_record_batch_reader(record_batch_reader::data_t);

record_batch_reader make_generating_record_batch_reader(
  ss::noncopyable_function<ss::future<record_batch_reader::data_t>()>);

ss::future<record_batch_reader::data_t> consume_reader_to_memory(
  record_batch_reader, timeout_clock::time_point timeout);

/// \brief wraps a reader into a foreign_ptr<unique_ptr>
record_batch_reader make_foreign_record_batch_reader(record_batch_reader&&);

} // namespace model
