#pragma once

#include "bytes/iobuf_parser.h"
#include "model/record.h"

namespace storage::internal {

std::vector<model::record_header> parse_record_headers(iobuf_parser& parser);

/// \brief uses internal on-disk format
model::record parse_one_record_from_buffer(iobuf_parser& parser);

/// \brief uses the canonical kafka format
model::record
parse_one_record_from_buffer_using_kafka_format(iobuf_parser& parser);

void append_record_using_kafka_format(iobuf& a, const model::record& r);

} // namespace storage::internal
