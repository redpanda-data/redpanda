#pragma once

#include "bytes/iobuf_parser.h"
#include "model/record.h"

namespace storage::internal {

/// \brief batch decompression
ss::future<model::record_batch> decompress_batch(model::record_batch&&);
/// \brief batch decompression
ss::future<model::record_batch> decompress_batch(const model::record_batch&);

/// \brief batch compression
ss::future<model::record_batch>
compress_batch(model::compression, model::record_batch&&);
/// \brief batch compression
ss::future<model::record_batch>
compress_batch(model::compression, const model::record_batch&);

/// \brief resets the size, header crc and payload crc
void reset_size_checksum_metadata(model::record_batch_header&, const iobuf&);

} // namespace storage::internal
