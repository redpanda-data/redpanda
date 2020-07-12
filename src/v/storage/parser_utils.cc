#include "storage/parser_utils.h"

#include "model/record.h"
#include "model/record_utils.h"
#include "reflection/adl.h"

#include <seastar/core/byteorder.hh>

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

model::record do_parse_one_record_from_buffer(
  iobuf_parser& parser, int32_t record_size, model::record_attributes::type attr) {
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

model::record parse_one_record_from_buffer(iobuf_parser& parser) {
    auto [record_size, rv] = parser.read_varlong();
    auto attr = reflection::adl<model::record_attributes::type>{}.from(parser);
    return do_parse_one_record_from_buffer(parser, record_size, attr);
}

model::record
parse_one_record_from_buffer_using_kafka_format(iobuf_parser& parser) {
    auto [record_size, rv] = parser.read_varlong();
    // NOTE: this is the main difference between our batch format and kafka
    // at the record level. At the batch level we have many differences
    auto attr = parser.consume_be_type<model::record_attributes::type>();
    return do_parse_one_record_from_buffer(parser, record_size, attr);
}

static inline void append_vint_to_iobuf(iobuf& b, vint::value_type v) {
    auto vb = vint::to_bytes(v);
    b.append(vb.data(), vb.size());
}
void append_record_using_kafka_format(iobuf& a, const model::record& r) {
    a.reserve_memory(vint::max_length * 6);
    append_vint_to_iobuf(a, r.size_bytes());

    const auto attrs = ss::cpu_to_be(r.attributes().value());
    // NOLINTNEXTLINE
    a.append(reinterpret_cast<const char*>(&attrs), sizeof(attrs));

    append_vint_to_iobuf(a, r.timestamp_delta());
    append_vint_to_iobuf(a, r.offset_delta());

    a.reserve_memory(r.key_size() + r.value_size());
    append_vint_to_iobuf(a, r.key_size());
    if (r.key_size() > 0) {
        for (auto& f : r.key()) {
            a.append(f.get(), f.size());
        }
    }
    append_vint_to_iobuf(a, r.value_size());
    if (r.value_size() > 0) {
        for (auto& f : r.value()) {
            a.append(f.get(), f.size());
        }
    }

    auto& hdrs = r.headers();
    append_vint_to_iobuf(a, hdrs.size());
    for (auto& h : hdrs) {
        append_vint_to_iobuf(a, h.key_size());
        a.reserve_memory(h.memory_usage());
        if (h.key_size() > 0) {
            for (auto& f : h.key()) {
                a.append(f.get(), f.size());
            }
        }
        append_vint_to_iobuf(a, h.value_size());
        if (h.value_size() > 0) {
            for (auto& f : h.value()) {
                a.append(f.get(), f.size());
            }
        }
    }
}

} // namespace storage::internal
