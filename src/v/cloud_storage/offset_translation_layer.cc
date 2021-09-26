/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/offset_translation_layer.h"

#include "cloud_storage/logger.h"
#include "cloud_storage/manifest.h"
#include "cloud_storage/types.h"
#include "ssx/sformat.h"
#include "storage/parser.h"
#include "utils/retry_chain_node.h"

#include <seastar/util/log.hh>

#include <exception>

namespace cloud_storage {

void offset_translator::update(const manifest& m) { _manifest = std::cref(m); }

ss::future<uint64_t> offset_translator::copy_stream(
  remote_segment_path path,
  ss::input_stream<char> src,
  ss::output_stream<char> dst,
  retry_chain_node& fib) const {
    retry_chain_logger ctxlog(cst_log, fib);
    auto smeta = _manifest->get().get(path);
    auto removed = smeta->delta_offset;
    vassert(
      removed != model::offset::min(),
      "Can't copy segment which isn't in the manifest");
    auto pred = [&removed, &ctxlog](model::record_batch_header& hdr) {
        if (hdr.type == model::record_batch_type::raft_configuration) {
            vlog(ctxlog.debug, "skipping batch {}", hdr);
            removed++;
            return storage::batch_consumer::consume_result::skip_batch;
        }
        auto old_offset = hdr.base_offset;
        hdr.base_offset = hdr.base_offset - removed;
        vlog(
          ctxlog.trace,
          "writing batch {}, old base-offset: {}, new base-offset: {}",
          hdr,
          old_offset,
          hdr.base_offset);
        vlog(
          ctxlog.debug,
          "writing batch, old base-offset: {}, new base-offset: {}",
          old_offset,
          hdr.base_offset);
        return storage::batch_consumer::consume_result::accept_batch;
    };
    auto len = co_await storage::transform_stream(
      std::move(src), std::move(dst), pred);
    if (len.has_error()) {
        throw std::system_error(len.error());
    }
    co_return len.value();
}

ss::input_stream<char> offset_translator::patch_stream(
  ss::input_stream<char> src, retry_chain_node&) const {
    // TBD: implement
    return src;
}

remote_segment_path offset_translator::get_adjusted_segment_name(
  const remote_segment_path& s, retry_chain_node& fib) const {
    auto smeta = _manifest->get().get(s);
    vassert(
      smeta != nullptr,
      "Can't adjust name of the segment which isn't in the manifest {}",
      s);
    auto res = get_adjusted_segment_name(s(), *smeta, fib);
    return remote_segment_path(res.string());
}

segment_name offset_translator::get_adjusted_segment_name(
  const segment_name& s, retry_chain_node& fib) const {
    auto smeta = _manifest->get().get(s);
    vassert(
      smeta != nullptr,
      "Can't adjust name of the segment which isn't in the manifest {}",
      s);
    auto res = get_adjusted_segment_name(
      std::filesystem::path(s()), *smeta, fib);
    return segment_name(res.string());
}

std::filesystem::path offset_translator::get_adjusted_segment_name(
  std::filesystem::path path,
  const manifest::segment_meta& smeta,
  retry_chain_node& fib) const {
    retry_chain_logger ctxlog(cst_log, fib);
    auto [base_offset, term_id, success] = parse_segment_name(path);
    vassert(success, "Invalid path {}", path);
    auto delta = smeta.delta_offset;
    auto new_base = base_offset - delta;
    auto new_fname = std::filesystem::path(
      ssx::sformat("{}-{}-v1.log", new_base(), term_id()));
    path.replace_filename(new_fname);
    vlog(
      ctxlog.debug,
      "Segment path: {}, base-offset: {}, term-id: {}, "
      "adjusted-base-offset: {}, adjuste-path: {}",
      path,
      base_offset,
      term_id,
      new_base,
      path);
    return path;
}

} // namespace cloud_storage
