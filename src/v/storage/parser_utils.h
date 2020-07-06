#pragma once

#include "bytes/iobuf_parser.h"
#include "model/record.h"

namespace storage::internal {

std::vector<model::record_header> parse_record_headers(iobuf_parser& parser);

model::record parse_one_record_from_buffer(iobuf_parser& parser);

} // namespace storage::internal
