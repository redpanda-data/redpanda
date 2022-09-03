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
#pragma once

#include "cluster/types.h"
#include "compat/generator.h"
#include "model/timeout_clock.h"
#include "test_utils/randoms.h"

namespace compat {

template<>
struct instance_generator<cluster::try_abort_request> {
    static cluster::try_abort_request random() {
        return cluster::try_abort_request(
          tests::random_named_int<model::partition_id>(),
          model::random_producer_identity(),
          tests::random_named_int<model::tx_seq>(),
          tests::random_duration<model::timeout_clock::duration>());
    }
    static std::vector<cluster::try_abort_request> limits() { return {}; }
};

template<>
struct instance_generator<cluster::try_abort_reply> {
    static cluster::try_abort_reply random() {
        return cluster::try_abort_reply(
          cluster::try_abort_reply::committed_type(tests::random_bool()),
          cluster::try_abort_reply::aborted_type(tests::random_bool()),
          cluster::tx_errc(random_generators::get_int<int>(
            static_cast<int>(cluster::tx_errc::none),
            static_cast<int>(cluster::tx_errc::invalid_txn_state))));
    }
    static std::vector<cluster::try_abort_reply> limits() { return {}; }
};

} // namespace compat
