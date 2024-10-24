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
#include "model/tests/randoms.h"
#include "test_utils/randoms.h"

namespace compat {

template<>
struct instance_generator<cluster::begin_tx_request> {
    static cluster::begin_tx_request random() {
        return cluster::begin_tx_request(
          model::random_ntp(),
          model::random_producer_identity(),
          tests::random_named_int<model::tx_seq>(),
          tests::random_duration_ms(),
          tests::random_named_int<model::partition_id>());
    }
    static std::vector<cluster::begin_tx_request> limits() { return {}; }
};

template<>
struct instance_generator<cluster::begin_tx_reply> {
    static cluster::begin_tx_reply random() {
        return cluster::begin_tx_reply(
          model::random_ntp(),
          tests::random_named_int<model::term_id>(),
          cluster::tx::errc(random_generators::get_int<int>(
            static_cast<int>(cluster::tx::errc::none),
            static_cast<int>(cluster::tx::errc::invalid_txn_state))));
    }
    static std::vector<cluster::begin_tx_reply> limits() { return {}; }
};

} // namespace compat
