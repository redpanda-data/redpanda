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
#include "coproc/logger.h"
#include "coproc/ntp_context.h"
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
  script_id id,
  ss::abort_source& abort_src,
  const ss::lw_shared_ptr<ntp_context>& ntp_ctx) {
    auto found = ntp_ctx->offsets.find(id);
    vassert(
      found != ntp_ctx->offsets.end(),
      "script_id must exist: {} for ntp: {}",
      id,
      ntp_ctx->ntp());
    const ntp_context::offset_pair& cp_offsets = found->second;
    const model::offset next_read
      = (unlikely(cp_offsets.last_acked == model::offset{}))
          ? model::offset(0)
          : cp_offsets.last_acked + model::offset(1);
    if (next_read <= cp_offsets.last_acked) {
        vlog(
          coproclog.info,
          "Replaying read on ntp: {} at offset: {}",
          ntp_ctx->ntp(),
          cp_offsets.last_read);
    }
    return storage::log_reader_config(
      next_read,
      ntp_ctx->partition->last_stable_offset(),
      1,
      max_batch_size(),
      ss::default_priority_class(),
      model::record_batch_type::raft_data,
      std::nullopt,
      abort_src);
}

ss::future<std::optional<process_batch_request::data>>
read_ntp(input_read_args args, ss::lw_shared_ptr<ntp_context> ntp_ctx) {
    storage::log_reader_config cfg = get_reader(
      args.id, args.abort_src, ntp_ctx);
    auto rbr = co_await ntp_ctx->partition->make_reader(cfg);
    auto read_result = co_await std::move(rbr).for_each_ref(
      coproc::reference_window_consumer(
        high_offset_tracker(), storage::internal::decompress_batch_consumer()),
      model::no_timeout);
    auto& [info, nrbr] = read_result;
    if (info.size == 0) {
        co_return std::nullopt;
    }
    ntp_ctx->offsets[args.id].last_read = info.last;
    co_return process_batch_request::data{
      .ids = std::vector<script_id>{args.id},
      .ntp = ntp_ctx->ntp(),
      .reader = std::move(nrbr)};
}

ss::future<std::vector<process_batch_request::data>>
read_from_inputs(input_read_args args) {
    std::vector<process_batch_request::data> requests;
    requests.reserve(args.inputs.size());
    auto read_all = [args, &requests](const ntp_context_cache::value_type& p) {
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
