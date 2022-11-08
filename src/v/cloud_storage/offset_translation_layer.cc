/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/offset_translation_layer.h"

#include "cloud_storage/logger.h"
#include "cloud_storage/types.h"
#include "ssx/sformat.h"
#include "storage/parser.h"
#include "utils/retry_chain_node.h"

#include <seastar/util/log.hh>

#include <exception>

namespace cloud_storage {

ss::future<stream_stats> offset_translator::copy_stream(
  ss::input_stream<char> src,
  ss::output_stream<char> dst,
  retry_chain_node& fib) const {
    retry_chain_logger ctxlog(cst_log, fib);
    auto removed = _initial_delta;
    vassert(
      removed != model::offset_delta::min(),
      "Can't copy segment with initial delta {}",
      removed);
    model::offset min_offset = model::offset::max();
    model::offset max_offset = model::offset::min();
    auto pred = [&removed, &ctxlog, &min_offset, &max_offset](
                  model::record_batch_header& hdr) {
        if (
          hdr.type == model::record_batch_type::raft_configuration
          || hdr.type == model::record_batch_type::archival_metadata) {
            vlog(ctxlog.debug, "skipping batch {}", hdr);
            removed += hdr.last_offset_delta + 1;
            return storage::batch_consumer::consume_result::skip_batch;
        }
        auto old_offset = hdr.base_offset;
        hdr.base_offset = kafka::offset_cast(hdr.base_offset - removed);

        min_offset = std::min(min_offset, hdr.base_offset);
        max_offset = std::max(max_offset, hdr.last_offset());

        if (cst_log.is_enabled(ss::log_level::trace)) {
            vlog(
              ctxlog.trace,
              "writing batch {}, old base-offset: {}, new base-offset: {}",
              hdr,
              old_offset,
              hdr.base_offset);
        } else {
            vlog(
              ctxlog.debug,
              "writing batch, old base-offset: {}, new base-offset: {}",
              old_offset,
              hdr.base_offset);
        }
        return storage::batch_consumer::consume_result::accept_batch;
    };
    auto len = co_await storage::transform_stream(
      std::move(src), std::move(dst), pred, _as);
    if (len.has_error()) {
        throw std::system_error(len.error());
    }
    co_return stream_stats{
      .min_offset = min_offset,
      .max_offset = max_offset,
      .size_bytes = len.value(),
    };
}

segment_name offset_translator::get_adjusted_segment_name(
  const segment_meta& meta, retry_chain_node& fib) const {
    retry_chain_logger ctxlog(cst_log, fib);
    auto base_offset = meta.base_offset;
    auto term_id = meta.segment_term;
    auto new_base = base_offset - _initial_delta;
    auto new_name = segment_name{
      ssx::sformat("{}-{}-v1.log", new_base(), term_id())};
    vlog(
      ctxlog.debug,
      "Segment: {}-{}-v1.log, base-offset: {}, term-id: {}, "
      "adjusted-base-offset: {}, adjusted-name: {}",
      base_offset,
      term_id,
      base_offset,
      term_id,
      new_base,
      new_name);
    return new_name;
}

} // namespace cloud_storage
