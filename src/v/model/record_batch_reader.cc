#include "model/record_batch_reader.h"

namespace model {

record_batch_reader
make_memory_record_batch_reader(std::vector<model::record_batch> batches) {
    class reader final : public record_batch_reader::impl {
    public:
        explicit reader(std::vector<model::record_batch> batches)
          : _batches(std::move(batches)) {
        }

    protected:
        virtual future<span> do_load_slice(timeout_clock::time_point) override {
            _end_of_stream = true;
            return make_ready_future<span>(_batches);
        }

    private:
        std::vector<model::record_batch> _batches;
    };

    return make_record_batch_reader<reader>(std::move(batches));
}

record_batch_reader make_generating_record_batch_reader(
  noncopyable_function<future<record_batch_opt>()> gen) {
    class reader final : public record_batch_reader::impl {
    public:
        explicit reader(noncopyable_function<future<record_batch_opt>()> gen)
          : _gen(std::move(gen)) {
        }

    protected:
        virtual future<span> do_load_slice(timeout_clock::time_point) override {
            return _gen().then([this](record_batch_opt batch) {
                _current_batch = std::move(batch);
                if (_current_batch) {
                    return span(&*_current_batch, 1);
                }
                _end_of_stream = true;
                return span();
            });
        }

    private:
        noncopyable_function<future<record_batch_opt>()> _gen;
        model::record_batch_opt _current_batch;
    };

    return make_record_batch_reader<reader>(std::move(gen));
}

} // namespace model
