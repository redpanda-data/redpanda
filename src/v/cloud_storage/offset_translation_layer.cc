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

ss::future<uint64_t> offset_translator::copy_stream(
  ss::input_stream<char> src,
  ss::output_stream<char> dst,
  retry_chain_node& fib) const {
    retry_chain_logger ctxlog(cst_log, fib);
    auto removed = _initial_delta;
    auto pred = [&removed, &ctxlog](model::record_batch_header& hdr) {
        if (
          hdr.type == model::record_batch_type::raft_configuration
          || hdr.type == model::record_batch_type::archival_metadata) {
            vlog(ctxlog.debug, "skipping batch {}", hdr);
            removed += hdr.last_offset_delta + 1;
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

segment_name offset_translator::get_adjusted_segment_name(
  const manifest::key& key, retry_chain_node& fib) const {
    retry_chain_logger ctxlog(cst_log, fib);
    auto base_offset = key.base_offset;
    auto term_id = key.term;
    auto new_base = base_offset - _initial_delta;
    auto new_name = segment_name{
      ssx::sformat("{}-{}-v1.log", new_base(), term_id())};
    vlog(
      ctxlog.debug,
      "Segment: {}-{}-v1.log, base-offset: {}, term-id: {}, "
      "adjusted-base-offset: {}, adjusted-name: {}",
      key.base_offset,
      key.term,
      base_offset,
      term_id,
      new_base,
      new_name);
    return new_name;
}

} // namespace cloud_storage
