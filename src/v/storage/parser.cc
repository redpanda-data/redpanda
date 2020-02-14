#include "storage/parser.h"

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "likely.h"
#include "model/record.h"
#include "reflection/adl.h"
#include "storage/logger.h"
#include "storage/parser.h"

#include <seastar/util/variant_utils.hh>

#include <bits/stdint-uintn.h>
#include <fmt/format.h>

namespace storage {
using stop_parser = batch_consumer::stop_parser;
using skip_batch = batch_consumer::skip_batch;
model::record_batch_header header_from_iobuf(iobuf b) {
    iobuf_parser parser(std::move(b));
    auto sz = ss::le_to_cpu(reflection::adl<int32_t>{}.from(parser));
    using offset_t = model::offset::type;
    auto off = model::offset(
      ss::le_to_cpu(reflection::adl<offset_t>{}.from(parser)));
    auto type = reflection::adl<model::record_batch_type>{}.from(parser);
    auto crc = ss::le_to_cpu(reflection::adl<int32_t>{}.from(parser));
    using attr_t = model::record_batch_attributes::type;
    auto attrs = model::record_batch_attributes(
      ss::le_to_cpu(reflection::adl<attr_t>{}.from(parser)));
    auto delta = ss::le_to_cpu(reflection::adl<int32_t>{}.from(parser));
    using tmstmp_t = model::timestamp::type;
    auto first = model::timestamp(
      ss::le_to_cpu(reflection::adl<tmstmp_t>{}.from(parser)));
    auto max = model::timestamp(
      ss::le_to_cpu(reflection::adl<tmstmp_t>{}.from(parser)));
    auto producer_id = ss::le_to_cpu(parser.consume_type<int64_t>());
    auto producer_epoch = ss::le_to_cpu(parser.consume_type<int16_t>());
    auto base_sequence = ss::le_to_cpu(parser.consume_type<int32_t>());
    auto record_count = ss::le_to_cpu(parser.consume_type<int32_t>());
    vassert(
      parser.bytes_consumed() == model::packed_record_batch_header_size,
      "Error in header parsing. Must consume:{} bytes, but consumed:{}",
      model::packed_record_batch_header_size,
      parser.bytes_consumed());
    return model::record_batch_header{sz,
                                      off,
                                      type,
                                      crc,
                                      attrs,
                                      delta,
                                      first,
                                      max,
                                      producer_id,
                                      producer_epoch,
                                      base_sequence,
                                      record_count};
}

static inline ss::future<std::optional<iobuf>> verify_read_iobuf(
  ss::input_stream<char>& in, size_t expected, ss::sstring msg) {
    return read_iobuf_exactly(in, expected)
      .then([msg = std::move(msg), expected](iobuf b) {
          if (likely(b.size_bytes() == expected)) {
              return ss::make_ready_future<std::optional<iobuf>>(std::move(b));
          }
          stlog.error(
            "Cannot continue parsing. recived size:{} bytes, expected:{} "
            "bytes. context:{}",
            b.size_bytes(),
            expected,
            msg);
          return ss::make_ready_future<std::optional<iobuf>>();
      });
}

using opt_hdr = std::optional<model::record_batch_header>;

ss::future<stop_parser> continuous_batch_parser::consume_header() {
    return read_iobuf_exactly(_input, model::packed_record_batch_header_size)
      .then([this](iobuf b) -> std::optional<iobuf> {
          if (b.empty()) {
              // expected when end of stream
              return std::nullopt;
          }
          if (b.size_bytes() != model::packed_record_batch_header_size) {
              stlog.error(
                "Could not parse header. Expected:{}, but Got:{}",
                model::packed_record_batch_header_size,
                b.size_bytes());
              return std::nullopt;
          }
          return std::move(b);
      })
      .then([this](std::optional<iobuf> b) {
          if (!b) {
              return ss::make_ready_future<opt_hdr>();
          }
          return ss::make_ready_future<opt_hdr>(
            header_from_iobuf(std::move(b.value())));
      })
      .then([this](opt_hdr o) {
          if (!o) {
              return ss::make_ready_future<stop_parser>(stop_parser::yes);
          }
          _header = o.value();
          const auto size_on_disk = _header.size_bytes;
          auto ret = _consumer->consume_batch_start(
            _header, _physical_base_offset, size_on_disk);
          _physical_base_offset += size_on_disk;
          if (std::holds_alternative<skip_batch>(ret)) {
              auto s = std::get<skip_batch>(ret);
              if (unlikely(bool(s))) {
                  auto remaining = _header.size_bytes
                                   - model::packed_record_batch_header_size;
                  return verify_read_iobuf(
                           _input, remaining, "parser::skip_batch")
                    .then([this](std::optional<iobuf> b) {
                        if (!b) {
                            return ss::make_ready_future<stop_parser>(
                              stop_parser::yes);
                        }
                        // start again
                        add_bytes_and_reset();
                        return consume_header();
                    });
              }
              return ss::make_ready_future<stop_parser>(stop_parser::no);
          }
          auto s = std::get<stop_parser>(ret);
          if (unlikely(bool(s))) {
              return ss::make_ready_future<stop_parser>(stop_parser::yes);
          }
          return ss::make_ready_future<stop_parser>(stop_parser::no);
      });
}

bool continuous_batch_parser::is_compressed_payload() const {
    return _header.attrs.compression() != model::compression::none;
}
ss::future<stop_parser> continuous_batch_parser::consume_one() {
    return consume_header().then([this](stop_parser st) {
        if (st) {
            return ss::make_ready_future<stop_parser>(st);
        }
        if (is_compressed_payload()) {
            return consume_compressed_records();
        }
        return consume_records();
    });
}
static std::vector<model::record_header>
parse_record_headers(iobuf_parser& parser) {
    std::vector<model::record_header> headers;
    auto [header_count, _] = parser.read_varlong();
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

ss::future<stop_parser> continuous_batch_parser::consume_records() {
    auto sz = _header.size_bytes - model::packed_record_batch_header_size;
    return verify_read_iobuf(_input, sz, "parser::consume_records")
      .then([this, sz](std::optional<iobuf> b) {
          if (!b) {
              return stop_parser::yes;
          }
          iobuf_parser parser(std::move(b.value()));
          for (int i = 0; i < _header.record_count; ++i) {
              auto [record_size, rv] = parser.read_varlong();
              auto attr = parser.consume_type<model::record_attributes::type>();
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
              auto ret = _consumer->consume_record(model::record(
                record_size,
                model::record_attributes(attr),
                static_cast<int32_t>(timestamp_delta),
                static_cast<int32_t>(offset_delta),
                key_length,
                std::move(key),
                value_length,
                std::move(value),
                std::move(headers)));
              if (std::holds_alternative<skip_batch>(ret)) {
                  if (std::get<skip_batch>(ret)) {
                      return stop_parser::no;
                  }
              } else if (std::holds_alternative<stop_parser>(ret)) {
                  if (std::get<stop_parser>(ret)) {
                      return stop_parser::yes;
                  }
              }
          }
          vassert(
            parser.bytes_left() == 0,
            "{} bytes left in to parse, but reached end",
            parser.bytes_left());
          return _consumer->consume_batch_end();
      });
}

size_t continuous_batch_parser::consumed_batch_bytes() const {
    return _header.size_bytes;
}

void continuous_batch_parser::add_bytes_and_reset() {
    _bytes_consumed += consumed_batch_bytes();
    _header = {}; // reset
}
ss::future<stop_parser> continuous_batch_parser::consume_compressed_records() {
    auto sz = _header.size_bytes - model::packed_record_batch_header_size;
    return verify_read_iobuf(_input, sz, "parser::consume_compressed_records")
      .then([this](std::optional<iobuf> record) {
          if (!record) {
              return stop_parser::yes;
          }
          _consumer->consume_compressed_records(std::move(record.value()));
          return _consumer->consume_batch_end();
      });
}

ss::future<size_t> continuous_batch_parser::consume() {
    return ss::repeat([this] {
               return consume_one().then([this](stop_parser s) {
                   add_bytes_and_reset();
                   return s || _input.eof() ? ss::stop_iteration::yes
                                            : ss::stop_iteration::no;
               });
           })
      .then_wrapped([this](ss::future<> f) {
          try {
              f.get();
              return _input.close();
          } catch (...) {
              return _input.close().then([e = std::current_exception()] {
                  return ss::make_exception_future<>(e);
              });
          }
      })
      .then([this] { return _bytes_consumed; });
}
} // namespace storage
