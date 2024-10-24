/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "log_gap_analysis.h"

#include "model/fundamental.h"
#include "storage/parser_utils.h"

static ss::logger slog{"log_gap_test"};
namespace storage {

log_gap_analysis make_log_gap_analysis(
  model::record_batch_reader&& reader,
  std::optional<model::offset> expected_start) {
    std::optional<model::offset> last_offset;
    log_gap_analysis ga;

    auto mem_log = model::consume_reader_to_memory(
                     std::move(reader), model::no_timeout)
                     .get();

    auto record_consumer =
      [&](const model::record& r, const model::record_batch& b) {
          model::offset msg_offset = model::offset(r.offset_delta())
                                     + b.base_offset();
          if (expected_start && !last_offset) {
              last_offset = *expected_start - model::offset(1);
              slog.info("gap_analysis: starting at {}", *last_offset + 1L);
          }
          if (last_offset) {
              model::offset expected = *last_offset + model::offset(1);
              if (msg_offset != expected) {
                  ga.num_gaps++;
                  if (ga.first_gap_start < model::offset(0)) {
                      ga.first_gap_start = expected;
                  }
                  ga.last_gap_end = msg_offset - model::offset(1);
                  slog.info(
                    "gap_analysis: at moffs {} expected {}, gaps {}",
                    msg_offset,
                    expected,
                    ga);
              } else {
                  slog.debug(
                    "gap_analysis: *OK*: at moffs {} = expected {}, gaps {}",
                    msg_offset,
                    expected,
                    ga);
              }
          }
          last_offset = msg_offset;
      };

    auto batch_consumer = [&](model::record_batch& b) {
        b.for_each_record(
          [&](const model::record& r) { record_consumer(r, b); });
    };

    for (auto&& rb : mem_log) {
        rb = storage::internal::decompress_batch(std::move(rb)).get();
        batch_consumer(rb);
    }
    return ga;
}

std::ostream& operator<<(std::ostream& o, const log_gap_analysis& a) {
    fmt::print(
      o,
      "{{first_gap_start:{}, last_gap_end:{}, num_gaps:{}}}",
      a.first_gap_start,
      a.last_gap_end,
      a.num_gaps);
    return o;
}

} // namespace storage
