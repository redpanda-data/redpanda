
#pragma once
#include "model/record.h"
#include "model/record_batch_reader.h"

namespace storage {
template<typename Consumer>
CONCEPT(requires model::BatchReaderConsumer<Consumer>())
class assigning_consumer {
public:
    assigning_consumer(Consumer consumer, model::offset offset)
      : _c(std::move(consumer))
      , _offset(offset) {}

    future<stop_iteration> operator()(model::record_batch&& batch) {
        batch.set_base_offset(_offset);
        _offset = batch.last_offset() + 1;
        return _c(std::move(batch));
    }

    auto end_of_stream() { return _c.end_of_stream(); }

private:
    Consumer _c;
    model::offset _offset;
};

template<typename Consumer>
CONCEPT(requires model::BatchReaderConsumer<Consumer>())
assigning_consumer<Consumer> wrap_with_offset_assignment(
  Consumer&& consumer, model::offset offset) {
    return assigning_consumer<Consumer>(
      std::forward<Consumer>(consumer), offset);
}
} // namespace storage