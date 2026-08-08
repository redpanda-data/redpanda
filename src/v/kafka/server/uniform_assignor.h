/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "kafka/server/consumer_group_assignor.h"

namespace kafka {

/// The built-in `uniform` assignor: one owner per partition, and that owner
/// subscribes to the partition's topic.
///
/// It spreads load as evenly as the group's subscriptions allow, then keeps as
/// much of the current assignment as that evenness leaves room for. Evenness
/// wins where the two conflict, so a member holding too many partitions gives
/// some up. A group that is already balanced comes back unchanged.
class uniform_assignor final : public assignor {
public:
    static constexpr std::string_view assignor_name = "uniform";

    std::string_view name() const final { return assignor_name; }

    assignment_result
    assign(const group_spec&, const topic_describer&) const final;
};

} // namespace kafka
