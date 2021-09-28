/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/script_context_frontend.h"

#include "cluster/partition.h"
#include "coproc/exception.h"
#include "coproc/logger.h"
#include "coproc/reference_window_consumer.hpp"
#include "model/fundamental.h"
#include "storage/parser_utils.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

namespace coproc {
static std::size_t max_batch_size() {
    return config::shard_local_cfg().coproc_max_batch_size.value();
}

class high_offset_tracker {
public:
    struct batch_info {
        model::offset last{};
        std::size_t size{0};
    };
    ss::future<ss::stop_iteration> operator()(const model::record_batch& rb) {
        _info.last = rb.last_offset();
        _info.size += rb.size_bytes();
        co_return ss::stop_iteration::no;
    }

    batch_info end_of_stream() { return _info; }

private:
    batch_info _info;
};

storage::log_reader_config get_reader(
  ss::abort_source& abort_src, read_context& rctx, const write_context& wctx) {
    auto offsets = wctx.unique_offsets();
    model::offset start, end;
    if (offsets.empty()) {
        start = rctx.absolute_start;
        end = rctx.input->last_stable_offset();
    } else if (offsets.size() == 1) {
        start = *offsets.begin();
        end = rctx.input->last_stable_offset();
    } else {
        /// The fact that this case was entered means a retry is to be performed
        /// since there is a difference in where all output topics stand.
        start = *offsets.begin();
        /// Limit the read to process only up until the next output topic. This
        /// way \ref last_read isn't ahead of any logs which would perform a
        /// promotion after the response is processed in
        /// script_context_backend.cc
        end = *(++offsets.begin()) - model::offset{1};
    }
    /// The 'high watermark' of the current read. All output topics have
    /// processed all inputs below this value. When the offsets map for all
    /// outputs has values equivlent to this, it means all outputs are up to
    /// date and a new 'high watermark' can be promoted due to read progressing
    /// forward.
    vassert(start <= end, "Offset logic error start: {} end: {}", start, end);
    rctx.last_acked = start;
    return storage::log_reader_config(
      start,
      end,
      1,
      max_batch_size(),
      ss::default_priority_class(),
      model::record_batch_type::raft_data,
      std::nullopt,
      abort_src);
}

ss::future<std::optional<process_batch_request::data>>
read_ntp(input_read_args args, ss::lw_shared_ptr<source> ctx) {
    storage::log_reader_config cfg = get_reader(
      args.abort_src, ctx->rctx, ctx->wctx);
    try {
        auto rbr = co_await ctx->rctx.input->make_reader(cfg);
        auto read_result = co_await std::move(rbr).for_each_ref(
          coproc::reference_window_consumer(
            high_offset_tracker(),
            storage::internal::decompress_batch_consumer()),
          model::no_timeout);
        auto& [info, nrbr] = read_result;
        if (info.size > 0) {
            ctx->rctx.last_read = info.last + model::offset{1};
            co_return process_batch_request::data{
              .ids = std::vector<script_id>{args.id},
              .ntp = ctx->rctx.input->ntp(),
              .reader = std::move(nrbr)};
        }
    } catch (const ss::gate_closed_exception&) {
        throw partition_shutdown_exception(
          ctx->rctx.input->ntp(),
          fmt::format(
            "Partition {} shutdown while script {} was attempting to read",
            ctx->rctx.input->ntp(),
            args.id));
    }
    co_return std::nullopt;
}

ss::future<std::vector<process_batch_request::data>>
read_from_inputs(input_read_args args) {
    std::vector<process_batch_request::data> requests;
    requests.reserve(args.inputs.size());
    auto read_all = [args, &requests](const routes_t::value_type& p) {
        return ss::with_semaphore(
                 args.read_sem,
                 max_batch_size(),
                 [args, ctx = p.second]() { return read_ntp(args, ctx); })
          .then([&requests](auto request) {
              if (request) {
                  requests.push_back(std::move(*request));
              }
          });
    };
    co_await ss::parallel_for_each(args.inputs, std::move(read_all));
    co_return requests;
}

} // namespace coproc
