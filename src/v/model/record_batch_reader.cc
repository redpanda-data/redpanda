#include "model/record_batch_reader.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include <memory>

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
  ss::noncopyable_function<ss::future<record_batch_opt>()> gen) {
    class reader final : public record_batch_reader::impl {
    public:
        explicit reader(
          ss::noncopyable_function<ss::future<record_batch_opt>()> gen)
          : _gen(std::move(gen)) {}

        bool is_end_of_stream() const final { return _end_of_stream; }

        void print(std::ostream& os) final {
            fmt::print(os, "{generating batch reader}");
        }

    protected:
        ss::future<record_batch_reader::storage_t>
        do_load_slice(timeout_clock::time_point) final {
            return _gen().then([this](record_batch_opt batch) {
                data_t ret;
                if (!batch) {
                    _end_of_stream = true;
                } else {
                    ret.reserve(1);
                    ret.push_back(std::move(*batch));
                }
                return storage_t(std::move(ret));
            });
        }

    private:
        bool _end_of_stream{false};
        ss::noncopyable_function<ss::future<record_batch_opt>()> _gen;
    };

    return make_record_batch_reader<reader>(std::move(gen));
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

std::ostream& operator<<(std::ostream& os, const record_batch_reader& r) {
    r._impl->print(os);
    return os;
}

} // namespace model
