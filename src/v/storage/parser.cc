#include "storage/parser.h"

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "model/record.h"
#include "reflection/adl.h"
#include "storage/constants.h"
#include "storage/logger.h"
#include "storage/parser.h"

#include <seastar/util/variant_utils.hh>

#include <bits/stdint-uintn.h>
#include <fmt/format.h>

namespace storage {
static constexpr size_t disk_header_size = packed_header_size + 4;
using stop_parser = batch_consumer::stop_parser;
using skip_batch = batch_consumer::skip_batch;
std::pair<model::record_batch_header, uint32_t> header_from_iobuf(iobuf b) {
    iobuf_parser parser(std::move(b));
    auto sz = reflection::adl<uint32_t>{}.from(parser);
    auto off = reflection::adl<model::offset>{}.from(parser);
    auto type = reflection::adl<model::record_batch_type>{}.from(parser);
    auto crc = reflection::adl<int32_t>{}.from(parser);
    using attr_t = model::record_batch_attributes::type;
    auto attrs = model::record_batch_attributes(
      reflection::adl<attr_t>{}.from(parser));
    auto delta = reflection::adl<int32_t>{}.from(parser);
    using tmstmp_t = model::timestamp::type;
    auto first = model::timestamp(reflection::adl<tmstmp_t>{}.from(parser));
    auto max = model::timestamp(reflection::adl<tmstmp_t>{}.from(parser));
    auto record_count = parser.consume_type<uint32_t>();
    return {
      model::record_batch_header{sz, off, type, crc, attrs, delta, first, max},
      record_count};
}

static inline ss::future<std::optional<iobuf>> verify_read_iobuf(
  ss::input_stream<char>& in, size_t expected, ss::sstring msg) {
    return read_iobuf_exactly(in, expected)
      .then([msg = std::move(msg), expected](iobuf b) {
          if (__builtin_expect(b.size_bytes() == expected, true)) {
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

using opt_hdr = std::optional<std::pair<model::record_batch_header, uint32_t>>;

ss::future<stop_parser> continuous_batch_parser::consume_header() {
    return verify_read_iobuf(_input, disk_header_size, "parser::header")
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
          auto& p = o.value();
          _header = p.first;
          auto num_records = p.second;
          const auto size_on_disk = _header.size_bytes + 4;
          auto ret = _consumer->consume_batch_start(
            _header, num_records, _physical_base_offset, size_on_disk);
          if (std::holds_alternative<skip_batch>(ret)) {
              auto s = std::get<skip_batch>(ret);
              if (__builtin_expect(bool(s), false)) {
                  auto remaining = _header.size_bytes - packed_header_size;
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
ss::future<stop_parser> continuous_batch_parser::consume_records() {
    auto sz = _header.size_bytes - packed_header_size;
    return verify_read_iobuf(_input, sz, "parser::consume_records")
      .then([this, sz](std::optional<iobuf> b) {
          if (!b) {
              return stop_parser::yes;
          }
          iobuf_parser parser(std::move(b.value()));
          while (parser.bytes_left()) {
              const auto start = parser.bytes_consumed();
              auto [record_size, rv] = parser.read_varlong();
              auto attr = parser.consume_type<model::record_attributes::type>();
              auto [timestamp_delta, tv] = parser.read_varlong();
              auto [offset_delta, ov] = parser.read_varlong();
              auto [key_length, kv] = parser.read_varlong();
              auto key = parser.share(key_length);
              auto value_length = record_size - key_length - kv - ov - tv
                                  - sizeof(model::record_attributes::type);
              stlog.info(
                "DOS:KeySize={},ValueSize{}", key_length, value_length);
              auto value = parser.share(value_length);
              auto ret = _consumer->consume_record(
                record_size,
                model::record_attributes(attr),
                static_cast<int32_t>(timestamp_delta),
                static_cast<int32_t>(offset_delta),
                std::move(key),
                std::move(value));
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
          return _consumer->consume_batch_end();
      });
}

size_t continuous_batch_parser::consumed_batch_bytes() const {
    return _header.size_bytes +  4 /*batch size*/;
}

void continuous_batch_parser::add_bytes_and_reset() {
    _bytes_consumed += consumed_batch_bytes();
    _header = {}; // reset
}
ss::future<stop_parser> continuous_batch_parser::consume_compressed_records() {
    auto sz = _header.size_bytes - packed_header_size;
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
                   return s ? ss::stop_iteration::yes : ss::stop_iteration::no;
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
