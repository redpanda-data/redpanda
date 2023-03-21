/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/types.h"
#include "model/metadata.h"
#include "seastarx.h"

namespace cluster {

class rm_group_proxy {
public:
    virtual ~rm_group_proxy() = default;

    // Timeout from begin_group_tx is not
    // used inside handler for begin_group_tx.
    // So we can pass usefull tx timeout
    // to store it in group service for expire
    // old txs.
    virtual ss::future<begin_group_tx_reply> begin_group_tx(
      kafka::group_id,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration,
      model::partition_id)
      = 0;

    virtual ss::future<begin_group_tx_reply>
      begin_group_tx_locally(begin_group_tx_request) = 0;

    virtual ss::future<prepare_group_tx_reply> prepare_group_tx(
      kafka::group_id,
      model::term_id,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration)
      = 0;

    virtual ss::future<prepare_group_tx_reply>
      prepare_group_tx_locally(prepare_group_tx_request) = 0;

    virtual ss::future<commit_group_tx_reply> commit_group_tx(
      kafka::group_id,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration)
      = 0;

    virtual ss::future<commit_group_tx_reply>
      commit_group_tx_locally(commit_group_tx_request) = 0;

    virtual ss::future<abort_group_tx_reply> abort_group_tx(
      kafka::group_id,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration)
      = 0;

    virtual ss::future<abort_group_tx_reply>
      abort_group_tx_locally(abort_group_tx_request) = 0;
};
} // namespace cluster
