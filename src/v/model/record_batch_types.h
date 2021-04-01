/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "utils/named_type.h"

namespace model {

using record_batch_type = named_type<int8_t, struct model_record_batch_type>;

constexpr std::array<record_batch_type, 14> well_known_record_batch_types{
  record_batch_type(),   // unknown - used for debugging
  record_batch_type(1),  // raft::data
  record_batch_type(2),  // raft::configuration
  record_batch_type(3),  // controller::*
  record_batch_type(4),  // kvstore::*
  record_batch_type(5),  // checkpoint - used to achieve linearizable reads
  record_batch_type(6),  // controller topic command batch type
  record_batch_type(7),  // ghost - used to fill gaps in raft recovery
  record_batch_type(8),  // id_allocator_stm::*
  record_batch_type(9),  // tx_prepare_batch_type
  record_batch_type(10), // tx_fence_batch_type
  record_batch_type(11), // tm_update_batch_type
  record_batch_type(12), // controller user management command batch type
  record_batch_type(13), // controller acl management command batch type
};
} // namespace model
