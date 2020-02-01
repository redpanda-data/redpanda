#include "model/record_batch_reader.h"

namespace model {

record_batch_reader
make_memory_record_batch_reader(record_batch_reader::storage_t batches) {
    class reader final : public record_batch_reader::impl {
    public:
        explicit reader(record_batch_reader::storage_t batches)
          : _batches(std::move(batches)) {}

        bool end_of_stream() const final { return _batches.empty(); }

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

        bool end_of_stream() const final { return _end_of_stream; }

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
