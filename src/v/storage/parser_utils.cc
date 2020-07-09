#include "storage/parser_utils.h"

#include "model/record_utils.h"
#include "reflection/adl.h"

namespace storage::internal {
std::vector<model::record_header> parse_record_headers(iobuf_parser& parser) {
    std::vector<model::record_header> headers;
    auto [header_count, _] = parser.read_varlong();
    headers.reserve(header_count);
    for (int i = 0; i < header_count; ++i) {
        auto [key_length, kv] = parser.read_varlong();
        iobuf key;
        if (key_length > 0) {
            key = parser.share(key_length);
        }
        auto [value_length, vv] = parser.read_varlong();
        iobuf value;
        if (value_length > 0) {
            value = parser.share(value_length);
        }
        headers.emplace_back(model::record_header(
          key_length, std::move(key), value_length, std::move(value)));
    }
    return headers;
}

model::record parse_one_record_from_buffer(iobuf_parser& parser) {
    auto [record_size, rv] = parser.read_varlong();
    auto attr = reflection::adl<model::record_attributes::type>{}.from(parser);
    auto [timestamp_delta, tv] = parser.read_varlong();
    auto [offset_delta, ov] = parser.read_varlong();
    auto [key_length, kv] = parser.read_varlong();
    iobuf key;
    if (key_length > 0) {
        key = parser.share(key_length);
    }
    auto [value_length, vv] = parser.read_varlong();
    iobuf value;
    if (value_length > 0) {
        value = parser.share(value_length);
    }
    auto headers = parse_record_headers(parser);
    return model::record(
      record_size,
      model::record_attributes(attr),
      static_cast<int32_t>(timestamp_delta),
      static_cast<int32_t>(offset_delta),
      key_length,
      std::move(key),
      value_length,
      std::move(value),
      std::move(headers));
}
} // namespace storage::internal
