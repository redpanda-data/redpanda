// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/record_batch_reader.h"

#include "model/record.h"
#include "model/record_batch_types.h"
#include "utils/fragmented_vector.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/variant_utils.hh>

#include <exception>
#include <memory>
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
          : _ptr(std::move(i)) {}
        foreign_reader(const foreign_reader&) = delete;
        foreign_reader& operator=(const foreign_reader&) = delete;
        foreign_reader(foreign_reader&&) = delete;
        foreign_reader& operator=(foreign_reader&&) = delete;
        ~foreign_reader() override = default;

        bool is_end_of_stream() const final {
            // ok to copy a bool
            return _ptr->is_end_of_stream();
        }

        void print(std::ostream& os) final {
            fmt::print(
              os,
              "foreign_record_batch_reader. remote_core:{} - proxy for:",
              _ptr.get_owner_shard());
            _ptr->print(os);
        }

        ss::future<storage_t> do_load_slice(timeout_clock::time_point t) final {
            auto shard = _ptr.get_owner_shard();
            if (shard == ss::this_shard_id()) {
                return _ptr->do_load_slice(t);
            }
            // TODO: this function should take an SMP group
            return ss::smp::submit_to(shard, [this, t] {
                return _ptr->do_load_slice(t).then([](storage_t recs) {
                    if (likely(std::holds_alternative<data_t>(recs))) {
                        auto& d = std::get<data_t>(recs);
                        auto p = std::make_unique<data_t>(std::move(d));
                        return storage_t(foreign_data_t{
                          .buffer = ss::make_foreign(std::move(p)),
                          .index = 0});
                    }
                    return recs;
                });
            });
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
          : _batches(std::move(batches)) {}

        bool is_end_of_stream() const final {
            return ss::visit(
              _batches,
              [](const data_t& d) { return d.empty(); },
              [](const foreign_data_t& d) {
                  return d.index >= d.buffer->size();
              });
        }

        void print(std::ostream& os) final {
            auto size = ss::visit(
              _batches,
              [](const data_t& d) { return d.size(); },
              [](const foreign_data_t& d) { return d.buffer->size(); });
            fmt::print(os, "memory reader {} batches", size);
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
    auto batches = std::make_unique<record_batch_reader::data_t>(
      std::move(data));
    return make_memory_record_batch_reader(record_batch_reader::foreign_data_t{
      .buffer = ss::make_foreign(std::move(batches)),
      .index = 0,
    });
}

record_batch_reader make_foreign_memory_record_batch_reader(record_batch b) {
    record_batch_reader::data_t data;
    data.reserve(1);
    data.push_back(std::move(b));
    return make_foreign_memory_record_batch_reader(std::move(data));
}

record_batch_reader make_generating_record_batch_reader(
  ss::noncopyable_function<ss::future<record_batch_reader::data_t>()> gen) {
    class reader final : public record_batch_reader::impl {
    public:
        explicit reader(
          ss::noncopyable_function<ss::future<record_batch_reader::data_t>()>
            gen)
          : _gen(std::move(gen)) {}

        bool is_end_of_stream() const final { return _end_of_stream; }

        void print(std::ostream& os) final {
            fmt::print(os, "{generating batch reader}");
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

namespace {
record_batch_reader make_fragmented_memory_record_batch_reader(
  std::vector<record_batch_reader::storage_t> data) {
    class reader final : public record_batch_reader::impl {
    public:
        explicit reader(std::vector<storage_t> data)
          : _data(std::move(data)) {}

        bool is_end_of_stream() const final { return _index >= _data.size(); }

        void print(std::ostream& os) final {
            fmt::print(
              os,
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

template<bool is_foreign, typename Container>
std::vector<record_batch_reader::storage_t>
make_fragmented_memory_storage_batches(Container batches) {
    std::vector<record_batch_reader::storage_t> data;
    size_t elements_per_fragment
      = fragmented_vector<model::record_batch>::elements_per_fragment();
    data.reserve(batches.size() / elements_per_fragment);
    record_batch_reader::data_t data_chunk;
    data_chunk.reserve(std::min(elements_per_fragment, batches.size()));
    size_t i = 0;
    for (auto it = batches.begin(); it != batches.end(); ++i, ++it) {
        if (!data_chunk.empty() && i % elements_per_fragment == 0) {
            if (is_foreign) {
                data.emplace_back(record_batch_reader::foreign_data_t{
                  .buffer = ss::make_foreign(
                    std::make_unique<record_batch_reader::data_t>(
                      std::exchange(data_chunk, {}))),
                  .index = 0});
            } else {
                data.emplace_back(std::exchange(data_chunk, {}));
            }
        }
        auto& b = *it;
        data_chunk.push_back(std::move(b));
    }
    if (!data_chunk.empty()) {
        if (is_foreign) {
            data.emplace_back(record_batch_reader::foreign_data_t{
              .buffer = ss::make_foreign(
                std::make_unique<record_batch_reader::data_t>(
                  std::exchange(data_chunk, {}))),
              .index = 0});
        } else {
            data.emplace_back(std::move(data_chunk));
        }
    }
    return data;
}
} // namespace

record_batch_reader make_fragmented_memory_record_batch_reader(
  fragmented_vector<model::record_batch> batches) {
    return make_fragmented_memory_record_batch_reader(
      make_fragmented_memory_storage_batches<
        false,
        fragmented_vector<model::record_batch>>(std::move(batches)));
}

record_batch_reader make_foreign_fragmented_memory_record_batch_reader(
  fragmented_vector<model::record_batch> batches) {
    return make_fragmented_memory_record_batch_reader(
      make_fragmented_memory_storage_batches<
        true,
        fragmented_vector<model::record_batch>>(std::move(batches)));
}

record_batch_reader make_foreign_fragmented_memory_record_batch_reader(
  ss::chunked_fifo<model::record_batch> batches) {
    return make_fragmented_memory_record_batch_reader(
      make_fragmented_memory_storage_batches<
        true,
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

ss::future<fragmented_vector<model::record_batch>>
consume_reader_to_fragmented_memory(
  record_batch_reader reader, timeout_clock::time_point timeout) {
    class fragmented_memory_batch_consumer {
    public:
        ss::future<ss::stop_iteration> operator()(model::record_batch b) {
            _result.push_back(std::move(b));
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        }
        fragmented_vector<model::record_batch> end_of_stream() {
            return std::move(_result);
        }

    private:
        fragmented_vector<model::record_batch> _result;
    };
    return std::move(reader).consume(
      fragmented_memory_batch_consumer{}, timeout);
}

std::ostream& operator<<(std::ostream& os, const record_batch_reader& r) {
    r._impl->print(os);
    return os;
}

} // namespace model
