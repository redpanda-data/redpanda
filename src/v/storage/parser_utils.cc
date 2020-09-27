#include "storage/parser_utils.h"

#include "compression/compression.h"
#include "model/compression.h"
#include "model/record.h"
#include "model/record_utils.h"
#include "reflection/adl.h"
#include "storage/logger.h"
#include "vlog.h"

#include <seastar/core/byteorder.hh>

namespace storage::internal {

ss::future<model::record_batch> decompress_batch(model::record_batch&& b) {
    if (!b.compressed()) {
        return ss::make_ready_future<model::record_batch>(std::move(b));
    }
    return decompress_batch(b);
}

ss::future<model::record_batch> decompress_batch(const model::record_batch& b) {
    using recs_t = model::record_batch::uncompressed_records;
    if (unlikely(!b.compressed())) {
        return ss::make_exception_future<model::record_batch>(
          std::runtime_error(fmt_with_ctx(
            fmt::format,
            "Asked to decompressed a non-compressed batch:{}",
            b.header())));
    }
    auto h = b.header();
    iobuf body_buf = compression::compressor::uncompress(
      b.get_compressed_records(), b.header().attrs.compression());
    return ss::do_with(
      iobuf_parser(std::move(body_buf)),
      recs_t{},
      [h](iobuf_parser& parser, recs_t& recs) {
          const auto r = boost::irange(0, h.record_count);
          return ss::do_for_each(
                   r,
                   [&recs, &parser, h](int32_t i) {
                       try {
                           recs.emplace_back(
                             model::parse_one_record_from_buffer(parser));
                       } catch (...) {
                           auto str = fmt_with_ctx(
                             fmt::format,
                             "Could not decode record:{}, header:{}, error:{}, "
                             "parser state:{}",
                             i,
                             h,
                             std::current_exception(),
                             parser);
                           vlog(stlog.error, "{}", str);
                           throw std::runtime_error(str);
                       }
                   })
            .then([h, &parser, &recs] {
                if (
                  parser.bytes_left()
                  || recs.size() != size_t(h.record_count)) {
                    auto err = fmt_with_ctx(
                      fmt::format,
                      "Partial parsing of records {}/{}: {} bytes left to "
                      "parse - Header:{}",
                      recs.size(),
                      h.record_count,
                      parser,
                      h);
                    throw std::runtime_error(err);
                }
            })
            .then([h, &recs] {
                auto b = model::record_batch(h, std::move(recs));
                auto& hdr = b.header();
                // must remove compression first!
                hdr.attrs.remove_compression();
                reset_size_checksum_metadata(b);
                return b;
            });
      });
}

ss::future<model::record_batch>
compress_batch(model::compression c, model::record_batch&& b) {
    if (c == model::compression::none) {
        vassert(
          b.header().attrs.compression() == model::compression::none,
          "Asked to compress a batch with `none` compression, but header "
          "metadata is incorrect: {}",
          b.header());
        return ss::make_ready_future<model::record_batch>(std::move(b));
    }
    return ss::do_with(std::move(b), [c](model::record_batch& b) {
        return compress_batch(c, b);
    });
}
ss::future<model::record_batch>
compress_batch(model::compression c, const model::record_batch& b) {
    vassert(
      c != model::compression::none,
      "Asked to compress a batch with type `none`: {} - {}",
      c,
      b.header());
    return ss::do_with(
             iobuf{},
             [c, &b](iobuf& buf) {
                 return ss::do_for_each(
                          b,
                          [&buf](const model::record& r) {
                              model::append_record_to_buffer(buf, r);
                          })
                   .then([c, &buf] {
                       return compression::compressor::compress(buf, c);
                   });
             })
      .then([c, &b](model::record_batch::records_type&& payload) {
          auto ret = model::record_batch(b.header(), std::move(payload));
          auto& hdr = ret.header();
          // compression bit must be set first!
          hdr.attrs |= c;
          reset_size_checksum_metadata(ret);
          return ret;
      });
}

/// \brief resets the size, header crc and payload crc
void reset_size_checksum_metadata(model::record_batch& ret) {
    auto& hdr = ret.header();
    hdr.size_bytes = model::recompute_record_batch_size(ret);
    hdr.crc = model::crc_record_batch(ret);
    hdr.header_crc = model::internal_header_only_crc(hdr);
}

} // namespace storage::internal
