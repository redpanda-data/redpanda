// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "cluster/commands.h"
#include "cluster/fwd.h"
#include "model/fundamental.h"

#include <seastar/core/sharded.hh>

namespace cluster::client_quota {

class backend final {
public:
    explicit backend(ss::sharded<store>& quotas)
      : _quotas(quotas) {};

    static constexpr auto commands
      = make_commands_list<alter_quotas_delta_cmd>();

    ss::future<std::error_code> apply_update(model::record_batch);

    bool is_batch_applicable(const model::record_batch& batch) const {
        return batch.header().type == model::record_batch_type::client_quota;
    }

    ss::future<> fill_snapshot(controller_snapshot&) const;
    ss::future<> apply_snapshot(model::offset, const controller_snapshot&);

private:
    ss::sharded<store>& _quotas;
};

} // namespace cluster::client_quota
