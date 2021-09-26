// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/id_allocator_stm.h"

#include "cluster/logger.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "raft/consensus.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/future.hh>

namespace cluster {

template<typename T>
static model::record_batch serialize_cmd(T t, model::record_batch_type type) {
    storage::record_batch_builder b(type, model::offset(0));
    iobuf key_buf;
    reflection::adl<uint8_t>{}.to(key_buf, T::record_key);
    iobuf v_buf;
    reflection::adl<T>{}.to(v_buf, std::move(t));
    b.add_raw_kv(std::move(key_buf), std::move(v_buf));
    return std::move(b).build();
}

id_allocator_stm::id_allocator_stm(
  ss::logger& logger, raft::consensus* c, config::configuration&)
  : raft::state_machine(c, logger, ss::default_priority_class()) {}

ss::future<id_allocator_stm::stm_allocation_result>
id_allocator_stm::allocate_id(model::timeout_clock::duration) {
    return ss::make_ready_future<stm_allocation_result>(
      stm_allocation_result{-1, raft::errc::timeout});
}

ss::future<> id_allocator_stm::apply(model::record_batch) { return ss::now(); }

} // namespace cluster
