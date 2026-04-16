/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "container/chunked_vector.h"
#include "model/timestamp.h"
#include "utils/detailed_error.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <expected>
#include <ostream>

namespace cloud_topics::l1 {

class domain_manager_probe;
class io;
class replicated_database;

// An object garbage collector that garbage collects based on object metadata
// in an database.
//
// Garbage collection is based only on object metadata that has been persisted
// to object storage, which ensures that subsequent restores of the database do
// not end up referring to missing objects.
class db_garbage_collector {
public:
    enum class errc {
        // The database needs reopening, e.g. term has changed, domain has been
        // restored.
        db_needs_reopen,

        // There was an error in interacting with cloud or in replicating data.
        // Likely transient, but still might be worth looking at.
        io_error,
    };
    using error = detailed_error<errc>;

    explicit db_garbage_collector(io* io, domain_manager_probe* probe);

    // Removes all unreferenced objects from cloud storage, and collects stale
    // preregistered objects that need expiry. Returns the list of object IDs
    // whose is_preregistration flag should be cleared; the caller is
    // responsible for writing those rows with appropriate locking.
    ss::future<std::expected<chunked_vector<object_id>, error>>
    remove_unreferenced_objects(
      replicated_database*,
      ss::abort_source*,
      size_t batch_size,
      model::timestamp prereg_expiry_cutoff,
      model::timestamp deletion_delay_cutoff);

private:
    // Removes the given batch size worth of objects, evaluating objects
    // starting from the given object. Returns the next object that needs to be
    // evaluated, or std::nullopt if this batch finished all objects in the
    // database. Appends stale preregistered objects to to_expire_out.
    ss::future<std::expected<std::optional<object_id>, error>>
    remove_unreferenced_batch(
      replicated_database*,
      ss::abort_source*,
      size_t batch_size,
      std::optional<object_id>,
      model::timestamp prereg_expiry_cutoff,
      model::timestamp deletion_delay_cutoff,
      chunked_vector<object_id>& to_expire_out);

    io* io_;
    domain_manager_probe* probe_;
};

inline std::ostream& operator<<(std::ostream& o, db_garbage_collector::errc e) {
    switch (e) {
    case db_garbage_collector::errc::db_needs_reopen:
        return o << "db_needs_reopen";
    case db_garbage_collector::errc::io_error:
        return o << "io_error";
    }
    return o << "unknown";
}

} // namespace cloud_topics::l1
