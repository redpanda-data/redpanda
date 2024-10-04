// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/seastarx.h"
#include "cloud_io/remote.h"
#include "iceberg/catalog.h"
#include "iceberg/partition.h"
#include "iceberg/schema.h"
#include "iceberg/table_io.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "model/fundamental.h"
#include "utils/named_type.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

namespace iceberg {

// A simple catalog implementation that uses object storage to manage updates
// to the table metadata. The file layout is similar to the Apache Iceberg
// HadoopCatalog, which places metadata into per-table directories. This means
// that metadata written with this catalog can be read by applications that
// know how to read from the HadoopCatalog.
//
// Like the HadoopCatalog, this catalog advertises the latest metadata with a
// "version hint", a file containing an integer that refers to the latest table
// metadata. Note, this version is an artifact of the HadoopCatalog only, and
// is not a part of the table metadata spec.
//
// The files laid out by this catalog look roughly like:
//
// /warehouse_location/my_namespace/my_table/metadata/version-hint.text
//   - the version hint: contains the string "42"
// /warehouse_location/my_namespace/my_table/metadata/v42.metadata.json
//   - contains the latest table metadata as JSON
// /warehouse_location/my_namespace/my_table/metadata/v41.metadata.json
// /warehouse_location/my_namespace/my_table/metadata/v40.metadata.json
// /warehouse_location/my_namespace/my_table/metadata/v...
//   - contain older versions of the table metadata
//   - Iceberg has the write.metadata.delete-after-commit.enabled property to
//     delete these, though this is not yet implemented by filesystem_catalog
//
// Unlike the HadoopCatalog, which uses atomic renames to avoid overwriting
// existing table metadata, this catalog is by comparison less safe, performing
// best-effort non-atomic operations to discourage concurrent updates.
//
// WARNING: until the concurrency issues are addressed, this catalog should
// only be used for testing and non-production workloads.
//
// WARNING: the file system based scheme to commit a metadata file is
// documented as deprecated and will eventually be phased out by the Apache
// Iceberg ecosystem.
//
// TODO: as implemented, the source of truth is the version-hint.text file, but
// we should probably move over to use conditional writes of table metadata and
// having the metadata be the source of truth. At that point we should use
// list-objects to ensure atomicity.
class filesystem_catalog : public catalog {
public:
    filesystem_catalog(filesystem_catalog&) = delete;
    filesystem_catalog(filesystem_catalog&&) = delete;
    filesystem_catalog& operator=(filesystem_catalog&) = delete;
    filesystem_catalog& operator=(filesystem_catalog&&) = delete;

    explicit filesystem_catalog(
      cloud_io::remote& io,
      const cloud_storage_clients::bucket_name& bucket,
      ss::sstring base_location)
      : io_(io)
      , table_io_(io, bucket)
      , base_location_(std::move(base_location)) {}

    ~filesystem_catalog() override = default;

    ss::future<checked<table_metadata, errc>> create_table(
      const table_identifier& table_ident,
      const schema& schema,
      const partition_spec& spec) final;

    ss::future<checked<table_metadata, errc>>
    load_table(const table_identifier& table_ident) final;

    ss::future<checked<std::nullopt_t, errc>>
    commit_txn(const table_identifier& table_ident, transaction) final;

private:
    // Constructs a path suitable for the table's location: a prefix to be used
    // for files relating to this table.
    //
    // TODO: should plumb this from the coordinator to the workers so we can
    // use it to prefix data files.
    ss::sstring table_location(const table_identifier& id) const;

    // Returns the version hint path for this table.
    version_hint_path vhint_path(const table_identifier&) const;

    // Returns the table's metadata path for the given version.
    table_metadata_path
    tmeta_path(const table_identifier&, int32_t version) const;

    // Returns success iff the given path contains the given expected version.
    // If the input version is nullopt, the object must not exist.
    ss::future<checked<std::nullopt_t, errc>> check_expected_version_hint(
      const version_hint_path& path, std::optional<int32_t> expected_version);

    // Uploads the given table metadata and writes a new version hint.
    //
    // Returns an error if we detect a new version before updating the hint.
    // If desired, it is left to callers to validate the existing hints before
    // uploading the new table metadata.
    ss::future<checked<std::nullopt_t, errc>> write_table_meta(
      const table_identifier& table_ident,
      const table_metadata&,
      std::optional<int32_t> expected_cur_version);

    struct table_and_version {
        table_metadata tmeta;
        int version;
    };
    // Reads the latest version hint and corresponding table metadata.
    ss::future<checked<table_and_version, errc>>
    read_table_meta(const table_identifier& table_ident);

    cloud_io::remote& io_;
    table_io table_io_;

    // Base location for the cluster.
    const ss::sstring base_location_;
};

} // namespace iceberg
