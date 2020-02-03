#pragma once

#include "model/record.h"

#include <seastar/core/circular_buffer.hh>

namespace storage::test {

model::record_batch
make_random_batch(model::offset o, int num_records, bool allow_compression);

model::record_batch
make_random_batch(model::offset o, bool allow_compression = true);

ss::circular_buffer<model::record_batch>
make_random_batches(model::offset o, int count, bool allow_compression = true);

ss::circular_buffer<model::record_batch>
make_random_batches(model::offset o = model::offset(0));

} // namespace storage::test
