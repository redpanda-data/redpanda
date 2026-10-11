/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/transaction.h"

#include "iceberg/merge_append_action.h"
#include "iceberg/remove_snapshots_action.h"
#include "iceberg/row_delta_action.h"
#include "iceberg/schema.h"
#include "iceberg/table_update_applier.h"
#include "iceberg/update_partition_spec_action.h"
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
transaction::set_partition_spec(unresolved_partition_spec pspec) {
    auto a = std::make_unique<update_partition_spec_action>(
      table_, std::move(pspec));
    co_return co_await apply(std::move(a));
}

ss::future<transaction::txn_outcome> transaction::merge_append(
  manifest_io& io,
  chunked_vector<file_to_append> files,
  chunked_vector<std::pair<ss::sstring, ss::sstring>> snapshot_props,
  std::optional<ss::sstring> tag_name,
  std::optional<int64_t> tag_expiration_ms) {
    auto a = std::make_unique<merge_append_action>(
      io,
      table_,
      std::move(files),
      std::move(snapshot_props),
      std::move(tag_name),
      tag_expiration_ms);
    co_return co_await apply(std::move(a));
}

ss::future<transaction::txn_outcome> transaction::row_delta(
  manifest_io& io,
  chunked_vector<file_to_append> data_files,
  chunked_vector<file_to_delete> delete_files,
  chunked_vector<std::pair<ss::sstring, ss::sstring>> snapshot_props,
  std::optional<ss::sstring> tag_name,
  std::optional<int64_t> tag_expiration_ms) {
    auto a = std::make_unique<row_delta_action>(
      io,
      table_,
      std::move(data_files),
      std::move(delete_files),
      std::move(snapshot_props),
      std::move(tag_name),
      tag_expiration_ms);
    co_return co_await apply(std::move(a));
}

ss::future<transaction::txn_outcome>
transaction::remove_expired_snapshots(model::timestamp now) {
    auto a = std::make_unique<remove_snapshots_action>(table_, now);
    co_return co_await apply(std::move(a));
}

} // namespace iceberg
