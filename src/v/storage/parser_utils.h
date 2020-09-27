#pragma once

#include "bytes/iobuf_parser.h"
#include "model/record.h"

namespace storage::internal {

std::vector<model::record_header> parse_record_headers(iobuf_parser& parser);

model::record parse_one_record_from_buffer(iobuf_parser& parser);

/// \brief appends the record @r to buffer @a
void append_record_using_kafka_format(iobuf& a, const model::record& r);

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
void reset_size_checksum_metadata(model::record_batch&);

} // namespace storage::internal
