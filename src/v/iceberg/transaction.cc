// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/transaction.h"

#include "iceberg/merge_append_action.h"
#include "iceberg/schema.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update_applier.h"
#include "iceberg/update_schema_action.h"

namespace iceberg {

ss::future<transaction::txn_outcome>
transaction::apply(std::unique_ptr<action> a) {
    if (error_.has_value()) {
        co_return error_.value();
    }
    auto u = co_await std::move(*a).build_updates();
    if (u.has_error()) {
        error_ = u.error();
        co_return u.error();
    }
    co_return update_metadata(std::move(u.value()));
}

transaction::txn_outcome transaction::update_metadata(updates_and_reqs ur) {
    auto& new_reqs = ur.requirements;
    // Drop new requirements that share a type with an existing requirement.
    //
    // E.g. we wouldn't assert the current schema has two different schema IDs;
    // we would assert the first schema ID, and expect that subsequent applied
    // actions put the table metadata in a state that subsequent schema
    // requirements pass.
    chunked_hash_set<size_t> existing_requirement_idxs;
    for (const auto& existing_req : updates_.requirements) {
        existing_requirement_idxs.emplace(existing_req.index());
    }
    for (const auto& new_req : new_reqs) {
        if (!existing_requirement_idxs.contains(new_req.index())) {
            updates_.requirements.emplace_back(new_req);
        }
    }
    for (auto& new_update : ur.updates) {
        auto outcome = table_update::apply(new_update, table_);
        switch (outcome) {
            using enum table_update::outcome;
        case success:
            break;
        case unexpected_state:
            auto ret = action::errc::unexpected_state;
            error_ = ret;
            return ret;
        }
        updates_.updates.emplace_back(std::move(new_update));
    }
    return std::nullopt;
}

ss::future<transaction::txn_outcome> transaction::set_schema(schema s) {
    auto a = std::make_unique<update_schema_action>(table_, std::move(s));
    co_return co_await apply(std::move(a));
}

ss::future<transaction::txn_outcome>
transaction::merge_append(chunked_vector<data_file> files) {
    auto a = std::make_unique<merge_append_action>(
      io_, table_, std::move(files));
    co_return co_await apply(std::move(a));
}

} // namespace iceberg
