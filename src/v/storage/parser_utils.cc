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
    if (unlikely(!b.compressed())) {
        return ss::make_exception_future<model::record_batch>(
          std::runtime_error(fmt_with_ctx(
            fmt::format,
            "Asked to decompressed a non-compressed batch:{}",
            b.header())));
    }
    iobuf body_buf = compression::compressor::uncompress(
      b.data(), b.header().attrs.compression());
    // must remove compression first!
    auto h = b.header();
    h.attrs.remove_compression();
    auto batch = model::record_batch(
      h, std::move(body_buf), model::record_batch::tag_ctor_ng{});
    reset_size_checksum_metadata(batch);
    return ss::make_ready_future<model::record_batch>(std::move(batch));
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
                 return model::for_each_record(
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
