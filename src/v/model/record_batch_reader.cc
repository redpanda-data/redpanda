// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/record_batch_reader.h"

#include "container/chunked_vector.h"
#include "model/record.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/util/variant_utils.hh>

#include <exception>
#include <memory>
#include <sstream>
#include <utility>

namespace model {
using data_t = record_batch_reader::data_t;
using foreign_data_t = record_batch_reader::foreign_data_t;
using storage_t = record_batch_reader::storage_t;

/// \brief wraps a reader into a foreign_ptr<unique_ptr>
record_batch_reader make_foreign_record_batch_reader(record_batch_reader&& r) {
    class foreign_reader final : public record_batch_reader::impl {
    public:
        explicit foreign_reader(std::unique_ptr<record_batch_reader::impl> i)
          : impl(needs_finally::yes)
          , _ptr(std::move(i)) {}
        foreign_reader(const foreign_reader&) = delete;
        foreign_reader& operator=(const foreign_reader&) = delete;
        foreign_reader(foreign_reader&&) = delete;
        foreign_reader& operator=(foreign_reader&&) = delete;
        ~foreign_reader() override = default;

        bool is_end_of_stream() const final {
            // ok to copy a bool
            return _ptr->is_end_of_stream();
        }

        fmt::iterator format_to(fmt::iterator it) const final {
            it = fmt::format_to(
              it,
              "foreign_record_batch_reader. remote_core:{} - proxy for:",
              _ptr.get_owner_shard());
            return _ptr->format_to(it);
        }

        ss::future<storage_t> do_load_slice(timeout_clock::time_point t) final {
            auto shard = _ptr.get_owner_shard();
            if (shard == ss::this_shard_id()) {
                return _ptr->do_load_slice(t);
            }
            // TODO: this function should take an SMP group
            return ss::smp::submit_to(shard, [this, t] {
                return _ptr->do_load_slice(t).then(
                  [](storage_t recs) { return recs; });
            });
        }

        ss::future<> do_finally() noexcept final {
            auto shard = _ptr.get_owner_shard();
            if (shard == ss::this_shard_id()) {
                return _ptr->finally();
            }
            return ss::smp::submit_to(
              shard, [this] { return _ptr->finally(); });
        }

    private:
        ss::foreign_ptr<std::unique_ptr<record_batch_reader::impl>> _ptr;
    };
    auto frn = std::make_unique<foreign_reader>(std::move(r).release());
    return record_batch_reader(std::move(frn));
}

record_batch_reader make_memory_record_batch_reader(storage_t batches) {
    class reader final : public record_batch_reader::impl {
    public:
        explicit reader(storage_t batches)
          : impl(needs_finally::no)
          , _batches(std::move(batches)) {}

        bool is_end_of_stream() const final {
            return ss::visit(
              _batches,
              [](const data_t& d) { return d.empty(); },
              [](const foreign_data_t& d) {
                  return d.index >= d.buffer->size();
              });
        }

        fmt::iterator format_to(fmt::iterator it) const final {
            auto size = ss::visit(
              _batches,
              [](const data_t& d) { return d.size(); },
              [](const foreign_data_t& d) { return d.buffer->size(); });
            return fmt::format_to(it, "memory reader {} batches", size);
        }

    protected:
        ss::future<record_batch_reader::storage_t>
        do_load_slice(timeout_clock::time_point) final {
            return ss::make_ready_future<record_batch_reader::storage_t>(
              std::exchange(_batches, {}));
        }

    private:
        storage_t _batches;
    };

    return make_record_batch_reader<reader>(std::move(batches));
}

record_batch_reader
make_foreign_memory_record_batch_reader(record_batch_reader::data_t data) {
    return make_memory_record_batch_reader(std::move(data));
}

record_batch_reader make_foreign_memory_record_batch_reader(record_batch b) {
    record_batch_reader::data_t data;
    data.push_back(std::move(b));
    return make_foreign_memory_record_batch_reader(std::move(data));
}

record_batch_reader make_empty_record_batch_reader() {
    class reader final : public record_batch_reader::impl {
    public:
        reader() noexcept
          : impl(needs_finally::no) {}

        bool is_end_of_stream() const final { return true; }

        ss::future<storage_t> do_load_slice(timeout_clock::time_point) final {
            co_return data_t{};
        }

        fmt::iterator format_to(fmt::iterator it) const final {
            return fmt::format_to(it, "{{empty_record_batch_reader}}");
        }
    };
    return make_record_batch_reader<reader>();
}

record_batch_reader make_generating_record_batch_reader(
  ss::noncopyable_function<ss::future<record_batch_reader::data_t>()> gen) {
    class reader final : public record_batch_reader::impl {
    public:
        explicit reader(
          ss::noncopyable_function<ss::future<record_batch_reader::data_t>()>
            gen)
          : impl(needs_finally::no)
          , _gen(std::move(gen)) {}

        bool is_end_of_stream() const final { return _end_of_stream; }

        fmt::iterator format_to(fmt::iterator it) const final {
            return fmt::format_to(it, "{{generating batch reader}}");
        }

    protected:
        ss::future<record_batch_reader::storage_t>
        do_load_slice(timeout_clock::time_point) final {
            return _gen().then([this](record_batch_reader::data_t data) {
                if (data.empty()) {
                    _end_of_stream = true;
                    return storage_t();
                }
                return storage_t(std::move(data));
            });
        }

    private:
        bool _end_of_stream{false};
        ss::noncopyable_function<ss::future<record_batch_reader::data_t>()>
          _gen;
    };

    return make_record_batch_reader<reader>(std::move(gen));
}

/// Creates a readahead wrapper around a record_batch_reader that issues one
/// read ahead of the consumer. When do_load_slice() is called, it returns
/// any buffered future from the previous readahead and immediately issues a
/// new read for the next call. This amortizes the latency of the underlying
/// reader across calls, improving throughput for sequential reads.
///
/// Note: Due to the record_batch_reader::impl interface not supporting
/// concurrent do_load_slice() calls, the readahead depth is fixed at 1.
record_batch_reader
make_readahead_record_batch_reader(record_batch_reader&& reader) {
    class readahead_reader final : public record_batch_reader::impl {
    public:
        explicit readahead_reader(std::unique_ptr<impl> underlying)
          : impl(needs_finally::yes)
          , _underlying(std::move(underlying)) {}

        bool is_end_of_stream() const final {
            return _underlying->is_end_of_stream()
                   && !_readahead_future.has_value();
        }

        fmt::iterator format_to(fmt::iterator it) const final {
            it = fmt::format_to(
              it,
              "readahead_reader(buffered={}) wrapping: ",
              _readahead_future.has_value() ? 1 : 0);
            return _underlying->format_to(it);
        }

        ss::future<storage_t>
        do_load_slice(timeout_clock::time_point timeout) final {
            ss::future<storage_t> fut
              = _readahead_future
                  ? std::exchange(_readahead_future, std::nullopt).value()
                  : _underlying->do_load_slice(timeout);
            fut = co_await ss::coroutine::as_future(std::move(fut));
            if (fut.failed()) {
                // Don't issue readahead if the future fails, this allows the
                // caller to decide what should happen.
                co_await ss::coroutine::return_exception_ptr(
                  fut.get_exception());
            }
            auto slice = std::move(fut.get());
            if (!_underlying->is_end_of_stream()) {
                _readahead_future = _underlying->do_load_slice(timeout);
            }
            co_return slice;
        }

        ss::future<> do_finally() noexcept final {
            if (!_readahead_future.has_value()) {
                return _underlying->finally();
            }
            // Drain the buffered future before calling underlying finally
            return std::exchange(_readahead_future, std::nullopt)
              .value()
              .then_wrapped([this](ss::future<storage_t> f) {
                  f.ignore_ready_future();
                  return _underlying->finally();
              });
        }

    private:
        std::unique_ptr<impl> _underlying;
        std::optional<ss::future<storage_t>> _readahead_future;
    };

    auto rdr = std::make_unique<readahead_reader>(std::move(reader).release());
    return record_batch_reader(std::move(rdr));
}

namespace {
record_batch_reader make_chunked_memory_record_batch_reader(
  std::vector<record_batch_reader::storage_t> data) {
    class reader final : public record_batch_reader::impl {
    public:
        explicit reader(std::vector<storage_t> data)
          : impl(needs_finally::no)
          , _data(std::move(data)) {}

        bool is_end_of_stream() const final { return _index >= _data.size(); }

        fmt::iterator format_to(fmt::iterator it) const final {
            return fmt::format_to(
              it,
              "fragmented memory reader {} batches of batches",
              _data.size());
        }

    protected:
        ss::future<storage_t> do_load_slice(timeout_clock::time_point) final {
            if (is_end_of_stream()) {
                return ss::make_ready_future<storage_t>(storage_t(data_t{}));
            }
            return ss::make_ready_future<storage_t>(std::move(_data[_index++]));
        }

    private:
        std::vector<storage_t> _data;
        size_t _index = 0;
    };
    return make_record_batch_reader<reader>(std::move(data));
}

template<typename Container>
std::vector<record_batch_reader::storage_t>
make_chunked_memory_storage_batches(Container batches) {
    std::vector<record_batch_reader::storage_t> data;
    size_t elements_per_fragment
      = chunked_vector<model::record_batch>::elements_per_fragment();
    data.reserve(batches.size() / elements_per_fragment);
    record_batch_reader::data_t data_chunk;
    size_t i = 0;
    for (auto it = batches.begin(); it != batches.end(); ++i, ++it) {
        if (!data_chunk.empty() && i % elements_per_fragment == 0) {
            data.emplace_back(std::exchange(data_chunk, {}));
        }
        auto& b = *it;
        data_chunk.push_back(std::move(b));
    }
    if (!data_chunk.empty()) {
        data.emplace_back(std::move(data_chunk));
    }
    return data;
}

template<typename Container>
ss::future<Container> consume_reader_to_fragmented_memory(
  record_batch_reader reader, timeout_clock::time_point timeout) {
    class fragmented_memory_batch_consumer {
    public:
        ss::future<ss::stop_iteration> operator()(model::record_batch b) {
            _result.push_back(std::move(b));
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        }
        Container end_of_stream() { return std::move(_result); }

    private:
        Container _result;
    };
    return std::move(reader).consume(
      fragmented_memory_batch_consumer{}, timeout);
}
} // namespace

record_batch_reader make_chunked_memory_record_batch_reader(
  chunked_vector<model::record_batch> batches) {
    return make_chunked_memory_record_batch_reader(
      make_chunked_memory_storage_batches<chunked_vector<model::record_batch>>(
        std::move(batches)));
}

record_batch_reader make_foreign_chunked_memory_record_batch_reader(
  chunked_vector<model::record_batch> batches) {
    return make_chunked_memory_record_batch_reader(
      make_chunked_memory_storage_batches<chunked_vector<model::record_batch>>(
        std::move(batches)));
}

record_batch_reader make_chunked_fragmented_memory_record_batch_reader(
  ss::chunked_fifo<model::record_batch> batches) {
    return make_chunked_memory_record_batch_reader(
      make_chunked_memory_storage_batches<
        ss::chunked_fifo<model::record_batch>>(std::move(batches)));
}

record_batch_reader make_chunked_memory_record_batch_reader(
  ss::chunked_fifo<model::record_batch> batches) {
    return make_chunked_memory_record_batch_reader(
      make_chunked_memory_storage_batches<
        ss::chunked_fifo<model::record_batch>>(std::move(batches)));
}

ss::future<record_batch_reader::data_t> consume_reader_to_memory(
  record_batch_reader reader, timeout_clock::time_point timeout) {
    class memory_batch_consumer {
    public:
        ss::future<ss::stop_iteration> operator()(model::record_batch b) {
            _result.push_back(std::move(b));
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        }
        data_t end_of_stream() { return std::move(_result); }

    private:
        data_t _result;
    };
    return std::move(reader).consume(memory_batch_consumer{}, timeout);
}

ss::future<chunked_vector<model::record_batch>>
consume_reader_to_fragmented_memory(
  record_batch_reader reader, timeout_clock::time_point timeout) {
    return consume_reader_to_fragmented_memory<
      chunked_vector<model::record_batch>>(std::move(reader), timeout);
}

ss::future<chunked_vector<model::record_batch>>
consume_reader_to_chunked_vector(
  record_batch_reader reader, timeout_clock::time_point timeout) {
    return consume_reader_to_fragmented_memory<
      chunked_vector<model::record_batch>>(std::move(reader), timeout);
}

fmt::iterator record_batch_reader::format_to(fmt::iterator it) const {
    return _impl->format_to(it);
}

} // namespace model
