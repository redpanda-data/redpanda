//// Copyright 2021 Redpanda Data, Inc.
////
//// Use of this software is governed by the Business Source License
//// included in the file licenses/BSL.md
////
//// As of the Change Date specified in that file, in accordance with
//// the Business Source License, use of this software will be governed
//// by the Apache License, Version 2.

#include "pandaproxy/schema_registry/seq_writer.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "kafka/client/client_fetch_batch_reader.h"
#include "model/namespace.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/storage.h"
#include "pandaproxy/schema_registry/types.h"
#include "ssx/future-util.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/coroutine/as_future.hh>

#include <exception>

using namespace std::chrono_literals;

namespace pandaproxy::schema_registry {

namespace {

struct batch_builder : public storage::record_batch_builder {
    explicit batch_builder(
      model::offset base_offset, std::optional<subject> sub)
      : record_batch_builder{model::record_batch_type::raft_data, model::offset{base_offset}}
      , sub{std::move(sub)} {}

    using record_batch_builder::add_raw_kv;
    using record_batch_builder::build;

    void operator()(std::optional<iobuf>&& key, std::optional<iobuf>&& value) {
        add_raw_kw(std::move(key), std::move(value), {});
    }

    template<typename K, typename V>
    requires requires(K k, V v) {
        to_json_iobuf(k);
        to_json_iobuf(v);
    }
    void operator()(K&& key, V&& value) {
        add_raw_kv(
          to_json_iobuf(std::forward<K>(key)),
          to_json_iobuf(std::forward<V>(value)));
    }

    void operator()(const seq_marker& s) {
        vlog(
          plog.debug,
          "Delete {} tombstoning sub={} at {}",
          to_string_view(s.key_type),
          sub,
          s);

        // Assumption: magic is the same as it was when key was
        // originally read.
        switch (s.key_type) {
        case seq_marker_key_type::schema: {
            auto key = schema_key{
              .seq{s.seq}, .node{s.node}, .sub{*sub}, .version{s.version}};
            add_raw_kv(to_json_iobuf(std::move(key)), std::nullopt);
        } break;
        case seq_marker_key_type::delete_subject: {
            auto key = delete_subject_key{
              .seq{s.seq}, .node{s.node}, .sub{*sub}};
            add_raw_kv(to_json_iobuf(std::move(key)), std::nullopt);
        } break;
        case seq_marker_key_type::config: {
            auto key = config_key{.seq{s.seq}, .node{s.node}, .sub{sub}};
            add_raw_kv(to_json_iobuf(std::move(key)), std::nullopt);
        } break;
        case seq_marker_key_type::mode: {
            auto key = mode_key{.seq{s.seq}, .node{s.node}, .sub{sub}};
            add_raw_kv(to_json_iobuf(std::move(key)), std::nullopt);
        } break;
        case seq_marker_key_type::invalid:
            vassert(false, "Unknown key type");
            break;
        }
    }

    void operator()(const std::vector<seq_marker>& sequences) {
        for (const seq_marker& s : sequences) {
            (*this)(s);
        }
    }

    std::optional<subject> sub;
};

} // namespace

/// Call this before reading from the store, if servicing
/// a REST API endpoint that requires global knowledge of latest
/// data (i.e. any listings)
ss::future<> seq_writer::read_sync() {
    auto offsets = co_await _client.local().list_offsets(
      model::schema_registry_internal_tp);

    auto max_offset = offsets.data.topics[0].partitions[0].offset;
    co_await wait_for(max_offset - model::offset{1});
}

ss::future<> seq_writer::check_mutable(const std::optional<subject>& sub) {
    auto mode = sub ? co_await _store.get_mode(*sub, default_to_global::yes)
                    : co_await _store.get_mode();
    if (mode == mode::read_only) {
        throw as_exception(mode_is_readonly(sub));
    }
    co_return;
}

ss::future<> seq_writer::wait_for(model::offset offset) {
    return container().invoke_on(0, _smp_opts, [offset](seq_writer& seq) {
        if (auto waiters = seq._wait_for_sem.waiters(); waiters != 0) {
            vlog(plog.trace, "wait_for waiting for {} waiters", waiters);
        }
        return ss::with_semaphore(seq._wait_for_sem, 1, [&seq, offset]() {
            if (offset > seq._loaded_offset) {
                vlog(
                  plog.debug,
                  "wait_for dirty!  Reading {}..{}",
                  seq._loaded_offset,
                  offset);

                return kafka::client::make_client_fetch_batch_reader(
                         seq._client.local(),
                         model::schema_registry_internal_tp,
                         seq._loaded_offset + model::offset{1},
                         offset + model::offset{1})
                  .consume(
                    consume_to_store{seq._store, seq}, model::no_timeout);
            } else {
                vlog(plog.trace, "wait_for clean (offset  {})", offset);
                return ss::make_ready_future<>();
            }
        });
    });
}

/// Helper for write methods that need to check + retry if their
/// write landed where they expected it to.
///
/// \param write_at Offset at which caller expects their write to land. If
/// std::nullopt, the offset is not checked.
/// \param batch Message to write
/// \return true if the write landed at `write_at`, else false
ss::future<bool> seq_writer::produce_and_apply(
  std::optional<model::offset> write_at, model::record_batch batch) {
    vassert(
      write_at.value_or(batch.base_offset()) == batch.base_offset(),
      "Set the base_offset to the expected write_at");

    kafka::partition_produce_response res
      = co_await _client.local().produce_record_batch(
        model::schema_registry_internal_tp, batch.copy());

    if (res.error_code != kafka::error_code::none) {
        throw kafka::exception(res.error_code, res.error_message.value_or(""));
    }

    auto success = write_at.value_or(res.base_offset) == res.base_offset;
    if (success) {
        vlog(plog.debug, "seq_writer: Successful write at {}", res.base_offset);
        co_await consume_to_store(_store, *this)(std::move(batch));
    } else {
        vlog(
          plog.debug,
          "seq_writer: Failed write at {} (wrote at {})",
          write_at,
          res.base_offset);
    }
    co_return success;
};

ss::future<> seq_writer::advance_offset(model::offset offset) {
    auto remote = [offset](seq_writer& s) { s.advance_offset_inner(offset); };

    return container().invoke_on(0, _smp_opts, remote);
}

void seq_writer::advance_offset_inner(model::offset offset) {
    if (_loaded_offset < offset) {
        vlog(
          plog.debug,
          "seq_writer::advance_offset {}->{}",
          _loaded_offset,
          offset);
        _loaded_offset = offset;
    } else {
        vlog(
          plog.debug,
          "seq_writer::advance_offset ignoring {} (have {})",
          offset,
          _loaded_offset);
    }
}

ss::future<std::optional<schema_id>> seq_writer::do_write_subject_version(
  subject_schema schema, model::offset write_at) {
    co_await check_mutable(schema.schema.sub());

    // Check if store already contains this data: if
    // so, we do no I/O and return the schema ID.
    auto projected
      = co_await _store.project_ids(schema.share())
          .handle_exception([](std::exception_ptr e) {
              vlog(
                plog.debug, "write_subject_version: project_ids failed: {}", e);
              return ss::make_exception_future<sharded_store::insert_result>(e);
          });

    if (!projected.inserted) {
        vlog(plog.debug, "write_subject_version: no-op");
        co_return projected.id;
    } else {
        auto canonical = std::move(schema.schema);
        auto sub = canonical.sub();
        vlog(
          plog.debug,
          "seq_writer::write_subject_version project offset={} "
          "subject={} "
          "schema={} "
          "version={}",
          write_at,
          sub,
          projected.id,
          projected.version);

        auto key = schema_key{
          .seq{write_at},
          .node{_node_id},
          .sub{sub},
          .version{projected.version}};
        auto value = canonical_schema_value{
          .schema{std::move(canonical)},
          .version{projected.version},
          .id{projected.id},
          .deleted = is_deleted::no};

        batch_builder rb(write_at, sub);
        rb(std::move(key), std::move(value));

        if (co_await produce_and_apply(write_at, std::move(rb).build())) {
            co_return projected.id;
        } else {
            // Pass up a None, our caller's cue to retry
            co_return std::nullopt;
        }
    }
}

ss::future<schema_id> seq_writer::write_subject_version(subject_schema schema) {
    co_return co_await sequenced_write(
      [&schema](model::offset write_at, seq_writer& seq) {
          return seq.do_write_subject_version(schema.share(), write_at);
      });
}

ss::future<std::optional<bool>> seq_writer::do_write_config(
  std::optional<subject> sub,
  compatibility_level compat,
  model::offset write_at) {
    vlog(
      plog.debug,
      "write_config sub={} compat={} offset={}",
      sub,
      to_string_view(compat),
      write_at);

    co_await check_mutable(sub);

    try {
        // Check for no-op case
        compatibility_level existing;
        if (sub.has_value()) {
            existing = co_await _store.get_compatibility(
              sub.value(), default_to_global::no);
        } else {
            existing = co_await _store.get_compatibility();
        }
        if (existing == compat) {
            co_return false;
        }
    } catch (const exception&) {
        // ignore
    }

    batch_builder rb(write_at, sub);
    rb(
      config_key{.seq{write_at}, .node{_node_id}, .sub{sub}},
      config_value{.compat = compat});

    if (co_await produce_and_apply(write_at, std::move(rb).build())) {
        co_return true;
    } else {
        // Pass up a None, our caller's cue to retry
        co_return std::nullopt;
    }
}

ss::future<bool> seq_writer::write_config(
  std::optional<subject> sub, compatibility_level compat) {
    return sequenced_write(
      [sub{std::move(sub)}, compat](model::offset write_at, seq_writer& seq) {
          return seq.do_write_config(sub, compat, write_at);
      });
}

ss::future<std::optional<bool>> seq_writer::do_delete_config(subject sub) {
    vlog(plog.debug, "delete config sub={}", sub);

    co_await check_mutable(sub);

    try {
        co_await _store.get_compatibility(sub, default_to_global::no);
    } catch (const exception&) {
        // subject config already blank
        co_return false;
    }

    batch_builder rb{model::offset{0}, sub};
    rb(co_await _store.get_subject_config_written_at(sub));

    if (co_await produce_and_apply(std::nullopt, std::move(rb).build())) {
        co_return true;
    } else {
        // Pass up a None, our caller's cue to retry
        co_return std::nullopt;
    }
}

ss::future<bool> seq_writer::delete_config(subject sub) {
    return sequenced_write(
      [sub{std::move(sub)}](model::offset, seq_writer& seq) {
          return seq.do_delete_config(sub);
      });
}

ss::future<std::optional<bool>> seq_writer::do_write_mode(
  std::optional<subject> sub, mode m, force f, model::offset write_at) {
    vlog(
      plog.debug,
      "write_mode sub={} mode={} force={} offset={}",
      sub,
      to_string_view(m),
      f,
      write_at);

    _store.check_mode_mutability(force::no);

    try {
        // Check for no-op case
        mode existing = sub ? co_await _store.get_mode(
                                sub.value(), default_to_global::no)
                            : co_await _store.get_mode();
        if (existing == m) {
            co_return false;
        }
    } catch (const exception& e) {
        if (e.code() != error_code::mode_not_found) {
            throw;
        }
    }

    batch_builder rb(write_at, sub);
    rb(
      mode_key{.seq{write_at}, .node{_node_id}, .sub{sub}},
      mode_value{.mode = m});

    if (co_await produce_and_apply(write_at, std::move(rb).build())) {
        co_return true;
    } else {
        // Pass up a None, our caller's cue to retry
        co_return std::nullopt;
    }
}

ss::future<bool>
seq_writer::write_mode(std::optional<subject> sub, mode mode, force f) {
    return sequenced_write(
      [sub{std::move(sub)}, mode, f](model::offset write_at, seq_writer& seq) {
          return seq.do_write_mode(sub, mode, f, write_at);
      });
}

ss::future<std::optional<bool>>
seq_writer::do_delete_mode(subject sub, model::offset write_at) {
    vlog(plog.debug, "delete mode sub={} offset={}", sub, write_at);

    // Report an error if the mode isn't registered
    co_await _store.get_mode(sub, default_to_global::no);
    _store.check_mode_mutability(force::no);

    batch_builder rb{write_at, sub};
    rb(co_await _store.get_subject_mode_written_at(sub));
    if (co_await produce_and_apply(std::nullopt, std::move(rb).build())) {
        co_return true;
    } else {
        // Pass up a None, our caller's cue to retry
        co_return std::nullopt;
    }
}

ss::future<bool> seq_writer::delete_mode(subject sub) {
    return sequenced_write(
      [sub{std::move(sub)}](model::offset write_at, seq_writer& seq) {
          return seq.do_delete_mode(sub, write_at);
      });
}

/// Impermanent delete: update a version with is_deleted=true
ss::future<std::optional<bool>> seq_writer::do_delete_subject_version(
  subject sub, schema_version version, model::offset write_at) {
    co_await check_mutable(sub);

    if (co_await _store.is_referenced(sub, version)) {
        throw as_exception(has_references(sub, version));
    }

    auto s_res = co_await _store.get_subject_schema(
      sub, version, include_deleted::yes);
    subject_schema ss = std::move(s_res);

    auto key = schema_key{
      .seq{write_at}, .node{_node_id}, .sub{sub}, .version{version}};
    vlog(plog.debug, "seq_writer::delete_subject_version {}", key);
    auto value = canonical_schema_value{
      .schema{std::move(ss.schema)},
      .version{version},
      .id{ss.id},
      .deleted{is_deleted::yes}};

    batch_builder rb(write_at, sub);
    rb(std::move(key), std::move(value));

    {
        // Clear config if this is a delete of the last version
        auto vec = co_await _store.get_versions(sub, include_deleted::no);
        if (vec.size() == 1 && vec.front() == version) {
            rb(co_await _store.get_subject_config_written_at(sub));
        }
    }
    if (co_await produce_and_apply(write_at, std::move(rb).build())) {
        co_return true;
    } else {
        // Pass up a None, our caller's cue to retry
        co_return std::nullopt;
    }
}

ss::future<bool>
seq_writer::delete_subject_version(subject sub, schema_version version) {
    return sequenced_write(
      [sub{std::move(sub)}, version](model::offset write_at, seq_writer& seq) {
          return seq.do_delete_subject_version(sub, version, write_at);
      });
}

ss::future<std::optional<std::vector<schema_version>>>
seq_writer::do_delete_subject_impermanent(subject sub, model::offset write_at) {
    co_await check_mutable(sub);

    // Grab the versions before they're gone.
    auto versions = co_await _store.get_versions(sub, include_deleted::no);

    // Inspect the subject to see if its already deleted
    if (co_await _store.is_subject_deleted(sub)) {
        co_return std::make_optional(versions);
    }

    auto is_referenced = co_await ssx::parallel_transform(
      versions.begin(), versions.end(), [this, &sub](const auto& ver) {
          return _store.is_referenced(sub, ver);
      });
    if (std::any_of(is_referenced.begin(), is_referenced.end(), [](auto v) {
            return v;
        })) {
        throw as_exception(has_references(sub, versions.back()));
    }

    // Proceed to write
    batch_builder rb{write_at, sub};
    rb(
      delete_subject_key{.seq{write_at}, .node{_node_id}, .sub{sub}},
      delete_subject_value{.sub{sub}});

    try {
        rb(co_await _store.get_subject_mode_written_at(sub));
    } catch (const exception& e) {
        if (e.code() != error_code::subject_not_found) {
            throw;
        }
    }

    try {
        rb(co_await _store.get_subject_config_written_at(sub));
    } catch (const exception& e) {
        if (e.code() != error_code::subject_not_found) {
            throw;
        }
    }

    if (co_await produce_and_apply(write_at, std::move(rb).build())) {
        co_return versions;
    } else {
        // Pass up a None, our caller's cue to retry
        co_return std::nullopt;
    }
}

ss::future<std::vector<schema_version>>
seq_writer::delete_subject_impermanent(subject sub) {
    vlog(plog.debug, "delete_subject_impermanent sub={}", sub);
    return sequenced_write(
      [sub{std::move(sub)}](model::offset write_at, seq_writer& seq) {
          return seq.do_delete_subject_impermanent(sub, write_at);
      });
}

/// Permanent deletions (i.e. writing tombstones for previous sequenced
/// records) do not themselves need sequence numbers.
/// Include a version if we are only to hard delete that version, otherwise
/// will hard-delete the whole subject.
ss::future<std::vector<schema_version>> seq_writer::delete_subject_permanent(
  subject sub, std::optional<schema_version> version) {
    return sequenced_write(
      [sub{std::move(sub)}, version](model::offset, seq_writer& seq) {
          return seq.delete_subject_permanent_inner(sub, version);
      });
}

ss::future<std::optional<std::vector<schema_version>>>
seq_writer::delete_subject_permanent_inner(
  subject sub, std::optional<schema_version> version) {
    std::vector<seq_marker> sequences;
    batch_builder rb{model::offset{0}, sub};

    /// Check for whether our victim is already soft-deleted happens
    /// within these store functions (will throw a 404-equivalent if so)
    vlog(plog.debug, "delete_subject_permanent sub={}", sub);

    co_await check_mutable(sub);

    if (version.has_value()) {
        // Check version first to see if the version exists
        sequences = co_await _store.get_subject_version_written_at(
          sub, version.value());
    }

    // Stash the list of versions to return at end
    auto versions = co_await _store.get_versions(sub, include_deleted::yes);

    // Deleting the subject, or the last version, deletes the subject
    if (!version.has_value() || versions.size() == 1) {
        rb(co_await _store.get_subject_written_at(sub));
    }
    rb(sequences);

    if (co_await produce_and_apply(std::nullopt, std::move(rb).build())) {
        co_return versions;
    } else {
        // Pass up a None, our caller's cue to retry
        co_return std::nullopt;
    }
}

} // namespace pandaproxy::schema_registry
