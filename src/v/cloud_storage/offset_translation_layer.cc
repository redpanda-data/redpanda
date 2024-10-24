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
#include "model/record_batch_types.h"
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
    model::offset min_offset = model::offset::max();
    model::offset max_offset = model::offset::min();

    auto pred =
      [this, &min_offset, &max_offset](model::record_batch_header& hdr) {
          static const auto types = model::offset_translator_batch_types();
          auto n = std::count(types.begin(), types.end(), hdr.type);
          if (n > 0) {
              _ot_state->add_gap(hdr.base_offset, hdr.last_offset());
          }
          min_offset = std::min(min_offset, hdr.base_offset);
          max_offset = std::max(max_offset, hdr.last_offset());
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

} // namespace cloud_storage
