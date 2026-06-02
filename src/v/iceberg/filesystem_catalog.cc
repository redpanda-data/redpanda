/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/filesystem_catalog.h"

#include "iceberg/manifest_entry.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_update_applier.h"
#include "model/timestamp.h"

namespace iceberg {

namespace {
catalog::errc to_catalog_errc(metadata_io::errc e) {
    switch (e) {
    case metadata_io::errc::shutting_down:
        return catalog::errc::shutting_down;
    case metadata_io::errc::timedout:
        return catalog::errc::timedout;
    case metadata_io::errc::failed:
        return catalog::errc::io_error;
    case metadata_io::errc::invalid_uri:
        return catalog::errc::unexpected_state;
    }
}
constexpr std::string_view vhint_filename = "version-hint.text";
ss::sstring tmeta_filename(int version) {
    return fmt::format("v{}.metadata.json", version);
}
} // namespace

ss::future<checked<table_metadata, catalog::errc>>
filesystem_catalog::create_table(
  const table_identifier& table_ident,
  const schema& s,
  const partition_spec& spec) {
    // TODO: at some level (maybe the caller), we need to manually re-assign
    // determininstic, unique field IDs, and re-assign the partition ids. The
    // Java library and the REST catalog will reassign these on their end, and
    // we need to follow suit, lest we end up with a mismatch.

    auto new_schema = s.copy();
    new_schema.schema_id = schema::id_t{0};
    auto highest_column_id = new_schema.highest_field_id().value_or(
      nested_field::id_t{0});
    auto new_spec = spec.copy();
    new_spec.spec_id = partition_spec::id_t{0};
    // NOTE: by convention, the first partition field should be id 1000.
    auto highest_pid = partition_field::id_t{999};
    for (const auto& p : new_spec.fields) {
        highest_pid = std::max(highest_pid, p.field_id);
    }
    auto new_sort_order = sort_order{
      sort_order::id_t{0}, chunked_vector<sort_field>{}};

    // Construct a metadata with all initial values.
    chunked_vector<sort_order> sort_orders;
    sort_orders.emplace_back(std::move(new_sort_order));
    chunked_vector<schema> schemas;
    schemas.emplace_back(std::move(new_schema));
    chunked_vector<partition_spec> specs;
    specs.emplace_back(std::move(new_spec));
    table_metadata tmeta{
      .format_version = format_version::v2,
      .table_uuid = uuid_t::create(),
      .location = table_location_uri(table_ident),
      .last_sequence_number = sequence_number{0},
      .last_updated_ms = model::timestamp::now(),
      .last_column_id = highest_column_id,
      .schemas = std::move(schemas),
      .current_schema_id = schema::id_t{0},
      .partition_specs = std::move(specs),
      .default_spec_id = partition_spec::id_t{0},
      .last_partition_id = highest_pid,
      .snapshots = chunked_vector<snapshot>{},
      .sort_orders = std::move(sort_orders),
      .default_sort_order_id = sort_order::id_t{0},
    };
    // Ensure no version hint exists for this table. That would mean the
    // table effectively already exists.
    static constexpr std::optional<int32_t> expect_no_vhint = std::nullopt;
    auto ret = co_await write_table_meta(table_ident, tmeta, expect_no_vhint);
    if (ret.has_error()) {
        co_return ret.error();
    }
    co_return tmeta;
}

ss::future<checked<table_metadata, catalog::errc>>
filesystem_catalog::load_table(const table_identifier& table_ident) {
    auto table_res = co_await read_table_meta(table_ident);
    if (table_res.has_error()) {
        co_return table_res.error();
    }
    co_return std::move(table_res.value().tmeta);
}

ss::future<checked<void, catalog::errc>>
filesystem_catalog::drop_table(const table_identifier& table_id, bool purge) {
    // Check that the table exists.
    auto current_tmeta = co_await read_table_meta(table_id);
    if (current_tmeta.has_error()) {
        co_return current_tmeta.error();
    }

    // When purging, delete everything under the table location (metadata,
    // manifests, manifest lists, and data files). Otherwise only remove
    // the catalog metadata so data files are left intact.
    if (purge) {
        auto res = co_await table_io_.delete_all_table_files(
          table_location_path{fmt::format("{}/", table_location(table_id))});
        if (res.has_error()) {
            vlog(
              log.warn,
              "dropping table {} failed (purge={}, errc={})",
              table_id,
              purge,
              static_cast<int>(res.error()));
            co_return to_catalog_errc(res.error());
        }
    } else {
        auto res = co_await table_io_.delete_all_metadata(
          metadata_location_path{
            fmt::format("{}/metadata/", table_location(table_id))});
        if (res.has_error()) {
            vlog(
              log.warn,
              "dropping table {} failed (purge={}, errc={})",
              table_id,
              purge,
              static_cast<int>(res.error()));
            co_return to_catalog_errc(res.error());
        }
    }

    co_return outcome::success();
}

ss::future<checked<table_metadata, catalog::errc>>
filesystem_catalog::commit_txn(
  const table_identifier& table_ident, transaction txn) {
    if (txn.updates().updates.empty()) {
        vlog(
          log.debug,
          "Transaction has no updates to table {}, returning early",
          table_ident.table);
        co_return checked<table_metadata, catalog::errc>{
          std::move(txn).release_table()};
    }
    auto current_tmeta = co_await read_table_meta(table_ident);
    if (current_tmeta.has_error()) {
        co_return current_tmeta.error();
    }
    auto& new_tmeta = current_tmeta.value().tmeta;

    // Apply the updates to the latest version of the table, since it may have
    // been updated since the transaction was constructed.

    for (const auto& req : txn.updates().requirements) {
        auto check_res = table_requirement::check(req, &new_tmeta);
        if (check_res.has_error()) {
            vlog(
              log.warn,
              "Current version of table {} metadata doesn't satisfy tx "
              "requirement: {}",
              table_ident.table,
              check_res.error());
            co_return errc::unexpected_state;
        }
    }

    for (const auto& update : txn.updates().updates) {
        auto res = table_update::apply(update, new_tmeta);
        if (res != table_update::outcome::success) {
            vlog(
              log.warn,
              "Table {} update doesn't apply to current version of table "
              "metadata",
              table_ident.table);
            co_return errc::unexpected_state;
        }
    }
    auto current_version = current_tmeta.value().version;
    auto write_res = co_await write_table_meta(
      table_ident, new_tmeta, current_version);
    if (write_res.has_error()) {
        co_return write_res.error();
    }
    co_return std::move(new_tmeta);
}

ss::future<checked<void, catalog_describe_error>>
filesystem_catalog::describe_catalog() {
    co_return outcome::success();
}

ss::sstring
filesystem_catalog::table_location(const table_identifier& id) const {
    return fmt::format(
      "{}/{}/{}", base_location_, fmt::join(id.ns, "/"), id.table);
}

uri filesystem_catalog::table_location_uri(const table_identifier& id) const {
    return table_io_.to_uri(
      fmt::format("{}/{}/{}", base_location_, fmt::join(id.ns, "/"), id.table));
}

version_hint_path
filesystem_catalog::vhint_path(const table_identifier& id) const {
    return version_hint_path{
      fmt::format("{}/metadata/{}", table_location(id), vhint_filename)};
}
table_metadata_path filesystem_catalog::tmeta_path(
  const table_identifier& id, int32_t version) const {
    return table_metadata_path{fmt::format(
      "{}/metadata/{}", table_location(id), tmeta_filename(version))};
}

ss::future<checked<std::nullopt_t, catalog::errc>>
filesystem_catalog::check_expected_version_hint(
  const version_hint_path& path, std::optional<int32_t> expected_cur_version) {
    if (expected_cur_version.has_value()) {
        auto vhint_res = co_await table_io_.download_version_hint(path);
        if (vhint_res.has_error()) {
            co_return to_catalog_errc(vhint_res.error());
        }
        auto cur_version = vhint_res.value();
        if (cur_version != expected_cur_version.value()) {
            vlog(
              log.warn,
              "Version hint file {} refers to version {}, expected {}",
              path,
              cur_version,
              expected_cur_version.value());
            co_return errc::unexpected_state;
        }
        co_return std::nullopt;
    }
    // No expected version: we expect no version hint object to exist.
    auto vhint_exists_res = co_await table_io_.version_hint_exists(path);
    if (vhint_exists_res.has_error()) {
        co_return to_catalog_errc(vhint_exists_res.error());
    }
    if (vhint_exists_res.value()) {
        vlog(log.warn, "Version hint {} already exists", path);
        co_return errc::already_exists;
    }
    co_return std::nullopt;
}

ss::future<checked<std::nullopt_t, catalog::errc>>
filesystem_catalog::rewrite_table_meta_for_tests(
  const table_identifier& table_ident, const table_metadata& tmeta) {
    auto read_res = co_await read_table_meta(table_ident);
    if (read_res.has_error()) {
        co_return read_res.error();
    }
    auto cur_version = read_res.value().version;
    co_return co_await write_table_meta(table_ident, tmeta, cur_version);
}

ss::future<checked<std::nullopt_t, catalog::errc>>
filesystem_catalog::write_table_meta(
  const table_identifier& table_ident,
  const table_metadata& tmeta,
  std::optional<int32_t> expected_cur_version) {
    // TODO: this is an imperfect check for version hint existence. Maybe we
    // can use conditional writes to ensure an atomic update?

    // First check to make sure the version hint matches what we expect.
    auto table_vhint_path = vhint_path(table_ident);
    auto vhint_check_res = co_await check_expected_version_hint(
      table_vhint_path, expected_cur_version);
    if (vhint_check_res.has_error()) {
        vlog(
          log.debug,
          "Version-hint check on file {} failed before uploading table "
          "metadata, exiting early without writing metadata",
          table_vhint_path);
        co_return vhint_check_res.error();
    }

    // Then upload the table metadata.
    auto expected_next_version = expected_cur_version.value_or(-1) + 1;
    auto path = tmeta_path(table_ident, expected_next_version);
    auto res = co_await table_io_.upload_table_meta(path, tmeta);
    if (res.has_error()) {
        co_return to_catalog_errc(res.error());
    }

    // Then check again to make sure the version hint still matches what we
    // expect.
    vhint_check_res = co_await check_expected_version_hint(
      table_vhint_path, expected_cur_version);
    if (vhint_check_res.has_error()) {
        vlog(
          log.warn,
          "Version-hint check on file {} failed after uploading table metadata "
          "{}, table may be inconsistent",
          table_vhint_path,
          path);
        co_return vhint_check_res.error();
    }

    // The hint is unchanged, upload a new one, recording the existence of the
    // new table metadata.
    auto vhint_up_res = co_await table_io_.upload_version_hint(
      table_vhint_path, expected_next_version);
    if (vhint_up_res.has_error()) {
        vlog(
          log.warn,
          "Version-hint upload of file {} failed after uploading table "
          "metadata {}, table may be inconsistent",
          table_vhint_path,
          path);
        co_return to_catalog_errc(vhint_up_res.error());
    }
    co_return std::nullopt;
}

ss::future<checked<filesystem_catalog::table_and_version, catalog::errc>>
filesystem_catalog::read_table_meta(const table_identifier& table_ident) {
    auto hint = vhint_path(table_ident);
    auto vhint_exists_res = co_await table_io_.version_hint_exists(hint);
    if (vhint_exists_res.has_error()) {
        co_return to_catalog_errc(vhint_exists_res.error());
    }
    if (!vhint_exists_res.value()) {
        // TODO: should list objects and have the table metadata be the source
        // of truth instead of this hint.
        co_return catalog::errc::not_found;
    }
    auto vhint_res = co_await table_io_.download_version_hint(hint);
    if (vhint_res.has_error()) {
        co_return to_catalog_errc(vhint_res.error());
    }
    int version = vhint_res.value();
    auto path = tmeta_path(table_ident, version);
    auto res = co_await table_io_.download_table_meta(path);
    if (res.has_error()) {
        co_return to_catalog_errc(res.error());
    }
    co_return table_and_version{std::move(res.value()), version};
}

} // namespace iceberg
