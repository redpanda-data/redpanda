#include "model/record_batch_reader.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include <memory>

namespace model {
/// \brief wraps a reader into a foreign_ptr<unique_ptr>
record_batch_reader make_foreign_record_batch_reader(record_batch_reader&& r) {
    class foreign_reader final : public record_batch_reader::impl {
    public:
        using storage_t = record_batch_reader::storage_t;
        using remote_recs = ss::foreign_ptr<std::unique_ptr<storage_t>>;

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

        ss::future<storage_t> do_load_slice(timeout_clock::time_point t) final {
            // TODO: this function should take an SMP group
            return ss::smp::submit_to(_ptr.get_owner_shard(), [this, t] {
                // must convert it to a foreign remote
                return _ptr->do_load_slice(t)
                  .then([](storage_t recs) {
                      auto p = std::make_unique<storage_t>(std::move(recs));
                      return ss::make_foreign(std::move(p));
                  })
                  .then([](remote_recs recs) {
                      // FIXME: copy them for now
                      storage_t ret;
                      ret.reserve(recs->size());
                      for (auto& b : *recs) {
                          ret.push_back(b.copy());
                      }
                      return ret;
                  });
            });
        }

    private:
        ss::foreign_ptr<std::unique_ptr<record_batch_reader::impl>> _ptr;
    };
    auto frn = std::make_unique<foreign_reader>(std::move(r).release());
    return record_batch_reader(std::move(frn));
}

record_batch_reader
make_memory_record_batch_reader(record_batch_reader::storage_t batches) {
    class reader final : public record_batch_reader::impl {
    public:
        explicit reader(record_batch_reader::storage_t batches)
          : _batches(std::move(batches)) {}

        bool is_end_of_stream() const final { return _batches.empty(); }

    protected:
        ss::future<record_batch_reader::storage_t>
        do_load_slice(timeout_clock::time_point) final {
            return ss::make_ready_future<record_batch_reader::storage_t>(
              std::exchange(_batches, {}));
        }

    private:
        record_batch_reader::storage_t _batches;
    };

    return make_record_batch_reader<reader>(std::move(batches));
}

record_batch_reader make_generating_record_batch_reader(
  ss::noncopyable_function<ss::future<record_batch_opt>()> gen) {
    class reader final : public record_batch_reader::impl {
    public:
        explicit reader(
          ss::noncopyable_function<ss::future<record_batch_opt>()> gen)
          : _gen(std::move(gen)) {}

        bool is_end_of_stream() const final { return _end_of_stream; }

    protected:
        ss::future<record_batch_reader::storage_t>
        do_load_slice(timeout_clock::time_point) final {
            return _gen().then([this](record_batch_opt batch) {
                record_batch_reader::storage_t ret;
                if (!batch) {
                    _end_of_stream = true;
                } else {
                    ret.reserve(1);
                    ret.push_back(std::move(*batch));
                }
                return ret;
            });
        }

    private:
        bool _end_of_stream{false};
        ss::noncopyable_function<ss::future<record_batch_opt>()> _gen;
    };

    return make_record_batch_reader<reader>(std::move(gen));
}

ss::future<record_batch_reader::storage_t> consume_reader_to_memory(
  record_batch_reader reader, timeout_clock::time_point timeout) {
    class memory_batch_consumer {
    public:
        ss::future<ss::stop_iteration> operator()(model::record_batch b) {
            _result.push_back(std::move(b));
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        }
        ss::circular_buffer<model::record_batch> end_of_stream() {
            return std::move(_result);
        }

    private:
        ss::circular_buffer<model::record_batch> _result;
    };
    return std::move(reader).consume(memory_batch_consumer{}, timeout);
}

} // namespace model
