//// Copyright 2021 Redpanda Data, Inc.
////
//// Use of this software is governed by the Business Source License
//// included in the file licenses/BSL.md
////
//// As of the Change Date specified in that file, in accordance with
//// the Business Source License, use of this software will be governed
//// by the Apache License, Version 2.

#include "pandaproxy/schema_registry/seq_writer.h"

#include "kafka/client/client_fetch_batch_reader.h"
#include "pandaproxy/error.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/storage.h"
#include "random/simple_time_jitter.h"
#include "ssx/future-util.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

using namespace std::chrono_literals;

namespace pandaproxy::schema_registry {

/// Call this before reading from the store, if servicing
/// a REST API endpoint that requires global knowledge of latest
/// data (i.e. any listings)
ss::future<> seq_writer::read_sync() {
    auto offsets = co_await _client.local().list_offsets(
      model::schema_registry_internal_tp);

    auto max_offset = offsets.data.topics[0].partitions[0].offset;
    co_await wait_for(max_offset - model::offset{1});
}

ss::future<> seq_writer::wait_for(model::offset offset) {
    return container().invoke_on(0, _smp_opts, [offset](seq_writer& seq) {
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
                vlog(plog.debug, "wait_for clean (offset  {})", offset);
                return ss::make_ready_future<>();
            }
        });
    });
}

/// Helper for write methods that need to check + retry if their
/// write landed where they expected it to.
///
/// \param write_at Offset at which caller expects their write to land
/// \param batch Message to write
/// \return true if the write landed at `write_at`, else false
ss::future<bool> seq_writer::produce_and_check(
  model::offset write_at, model::record_batch batch) {
    // Because we rely on checking exactly where our message (singular) landed,
    // only use this function with batches of a single message.
    vassert(batch.record_count() == 1, "Only single-message batches allowed");

    kafka::partition_produce_response res
      = co_await _client.local().produce_record_batch(
        model::schema_registry_internal_tp, std::move(batch));

    // TODO(Ben): Check the error reporting here
    if (res.error_code != kafka::error_code::none) {
        throw kafka::exception(res.error_code, *res.error_message);
    }

    auto wrote_at = res.base_offset;
    if (wrote_at == write_at) {
        vlog(plog.debug, "seq_writer: Successful write at {}", wrote_at);

        co_return true;
    } else {
        vlog(
          plog.debug,
          "seq_writer: Failed write at {} (wrote at {})",
          write_at,
          wrote_at);
        co_return false;
    }
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
  subject_schema schema, model::offset write_at, seq_writer& seq) {
    // Check if store already contains this data: if
    // so, we do no I/O and return the schema ID.
    auto projected = co_await seq._store.project_ids(schema);

    if (!projected.inserted) {
        vlog(plog.debug, "write_subject_version: no-op");
        co_return projected.id;
    } else {
        vlog(
          plog.debug,
          "seq_writer::write_subject_version project offset={} "
          "subject={} "
          "schema={} "
          "version={}",
          write_at,
          schema.schema.sub(),
          projected.id,
          projected.version);

        auto key = schema_key{
          .seq{write_at},
          .node{seq._node_id},
          .sub{schema.schema.sub()},
          .version{projected.version}};
        auto value = schema_value{
          .schema{schema.schema},
          .version{projected.version},
          .id{projected.id},
          .deleted = is_deleted::no};

        auto batch = as_record_batch(key, value);

        auto success = co_await seq.produce_and_check(
          write_at, std::move(batch));
        if (success) {
            auto applier = consume_to_store(seq._store, seq);
            co_await applier.apply(write_at, key, value);
            seq.advance_offset_inner(write_at);
            co_return projected.id;
        } else {
            co_return std::nullopt;
        }
    }
}

ss::future<schema_id> seq_writer::write_subject_version(subject_schema schema) {
    return sequenced_write([this, schema{std::move(schema)}](
                             model::offset write_at, seq_writer& seq) {
        return do_write_subject_version(schema, write_at, seq);
    });
}

ss::future<std::optional<bool>> seq_writer::do_write_config(
  std::optional<subject> sub,
  compatibility_level compat,
  model::offset write_at,
  seq_writer& seq) {
    vlog(
      plog.debug,
      "write_config sub={} compat={} offset={}",
      sub,
      to_string_view(compat),
      write_at);

    try {
        // Check for no-op case
        compatibility_level existing;
        if (sub.has_value()) {
            existing = co_await seq._store.get_compatibility(
              sub.value(), default_to_global::no);
        } else {
            existing = co_await seq._store.get_compatibility();
        }
        if (existing == compat) {
            co_return false;
        }
    } catch (const exception&) {
        // ignore
    }

    auto key = config_key{.seq{write_at}, .node{seq._node_id}, .sub{sub}};
    auto value = config_value{.compat = compat};
    auto batch = as_record_batch(key, value);

    auto success = co_await seq.produce_and_check(write_at, std::move(batch));
    if (success) {
        auto applier = consume_to_store(seq._store, seq);
        co_await applier.apply(write_at, key, value);
        seq.advance_offset_inner(write_at);
        co_return true;
    } else {
        // Pass up a None, our caller's cue to retry
        co_return std::nullopt;
    }
}

ss::future<bool> seq_writer::write_config(
  std::optional<subject> sub, compatibility_level compat) {
    return sequenced_write([this, sub{std::move(sub)}, compat](
                             model::offset write_at, seq_writer& seq) {
        return do_write_config(sub, compat, write_at, seq);
    });
}

/// Impermanent delete: update a version with is_deleted=true
ss::future<std::optional<bool>> seq_writer::do_delete_subject_version(
  subject sub,
  schema_version version,
  model::offset write_at,
  seq_writer& seq) {
    if (co_await seq._store.is_referenced(sub, version)) {
        throw as_exception(has_references(sub, version));
    }

    auto s_res = co_await seq._store.get_subject_schema(
      sub, version, include_deleted::yes);
    subject_schema ss = std::move(s_res);

    auto key = schema_key{
      .seq{write_at}, .node{seq._node_id}, .sub{sub}, .version{version}};
    vlog(plog.debug, "seq_writer::delete_subject_version {}", key);
    auto value = schema_value{
      .schema{std::move(ss.schema)},
      .version{version},
      .id{ss.id},
      .deleted{is_deleted::yes}};

    auto batch = as_record_batch(key, value);

    auto success = co_await seq.produce_and_check(write_at, std::move(batch));
    if (success) {
        auto applier = consume_to_store(seq._store, seq);
        co_await applier.apply(write_at, key, value);
        seq.advance_offset_inner(write_at);
        co_return true;
    } else {
        // Pass up a None, our caller's cue to retry
        co_return std::nullopt;
    }
}

ss::future<bool>
seq_writer::delete_subject_version(subject sub, schema_version version) {
    return sequenced_write([this, sub{std::move(sub)}, version](
                             model::offset write_at, seq_writer& seq) {
        return do_delete_subject_version(sub, version, write_at, seq);
    });
}

ss::future<std::optional<std::vector<schema_version>>>
seq_writer::do_delete_subject_impermanent(
  subject sub, model::offset write_at, seq_writer& seq) {
    // Grab the versions before they're gone.
    auto versions = co_await seq._store.get_versions(sub, include_deleted::no);

    // Inspect the subject to see if its already deleted
    if (co_await seq._store.is_subject_deleted(sub)) {
        co_return std::make_optional(versions);
    }

    auto is_referenced = co_await ssx::parallel_transform(
      versions.begin(), versions.end(), [&seq, &sub](auto const& ver) {
          return seq._store.is_referenced(sub, ver);
      });
    if (std::any_of(is_referenced.begin(), is_referenced.end(), [](auto v) {
            return v;
        })) {
        throw as_exception(has_references(sub, versions.back()));
    }

    // Proceed to write
    auto key = delete_subject_key{
      .seq{write_at}, .node{seq._node_id}, .sub{sub}};
    auto value = delete_subject_value{.sub{sub}};
    auto batch = as_record_batch(key, value);

    auto success = co_await seq.produce_and_check(write_at, std::move(batch));
    if (success) {
        auto applier = consume_to_store(seq._store, seq);
        co_await applier.apply(write_at, key, value);
        seq.advance_offset_inner(write_at);
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
      [this, sub{std::move(sub)}](model::offset write_at, seq_writer& seq) {
          return do_delete_subject_impermanent(sub, write_at, seq);
      });
}

/// Permanent deletions (i.e. writing tombstones for previous sequenced
/// records) do not themselves need sequence numbers.
/// Include a version if we are only to hard delete that version, otherwise
/// will hard-delete the whole subject.
ss::future<std::vector<schema_version>> seq_writer::delete_subject_permanent(
  subject sub, std::optional<schema_version> version) {
    return container().invoke_on(0, _smp_opts, [sub, version](seq_writer& seq) {
        return ss::with_semaphore(seq._write_sem, 1, [sub, version, &seq]() {
            return seq.delete_subject_permanent_inner(sub, version);
        });
    });
}

ss::future<std::vector<schema_version>>
seq_writer::delete_subject_permanent_inner(
  subject sub, std::optional<schema_version> version) {
    std::vector<seq_marker> sequences;
    /// Check for whether our victim is already soft-deleted happens
    /// within these store functions (will throw a 404-equivalent if so)
    vlog(plog.debug, "delete_subject_permanent sub={}", sub);
    if (version.has_value()) {
        // Check version first to see if the version exists
        sequences = co_await _store.get_subject_version_written_at(
          sub, version.value());
    }

    // Stash the list of versions to return at end
    auto versions = co_await _store.get_versions(sub, include_deleted::yes);

    // Deleting the subject, or the last version, deletes the subject
    if (!version.has_value() || versions.size() == 1) {
        sequences = co_await _store.get_subject_written_at(sub);
    }

    storage::record_batch_builder rb{
      model::record_batch_type::raft_data, model::offset{0}};

    std::vector<std::variant<schema_key, delete_subject_key, config_key>> keys;
    for (auto s : sequences) {
        vlog(
          plog.debug,
          "Delete subject_permanent: tombstoning sub={} at {}",
          sub,
          s);

        // Assumption: magic is the same as it was when key was
        // originally read.
        switch (s.key_type) {
        case seq_marker_key_type::schema: {
            auto key = schema_key{
              .seq{s.seq}, .node{s.node}, .sub{sub}, .version{s.version}};
            keys.push_back(key);
            rb.add_raw_kv(to_json_iobuf(std::move(key)), std::nullopt);
        } break;
        case seq_marker_key_type::delete_subject: {
            auto key = delete_subject_key{
              .seq{s.seq}, .node{s.node}, .sub{sub}};
            keys.push_back(key);
            rb.add_raw_kv(to_json_iobuf(std::move(key)), std::nullopt);
        } break;
        case seq_marker_key_type::config: {
            auto key = config_key{.seq{s.seq}, .node{s.node}, .sub{sub}};
            keys.push_back(key);
            rb.add_raw_kv(to_json_iobuf(std::move(key)), std::nullopt);
        } break;
        default:
            vassert(false, "Unknown key type");
        }
    }

    // Produce tombstones.  We do not need to check where they landed,
    // because these can arrive in any order and be safely repeated.
    auto batch = std::move(rb).build();

    kafka::partition_produce_response res
      = co_await _client.local().produce_record_batch(
        model::schema_registry_internal_tp, std::move(batch));
    if (res.error_code != kafka::error_code::none) {
        vlog(
          plog.error,
          "Error writing to schema topic: {} {}",
          res.error_code,
          res.error_message);
        throw kafka::exception(res.error_code, *res.error_message);
    }

    // Replay the persisted deletions into our store
    auto applier = consume_to_store(_store, *this);
    auto offset = res.base_offset;
    for (auto k : keys) {
        co_await ss::visit(
          k,
          [&applier, &offset](const schema_key& skey) {
              return applier.apply(offset, skey, std::nullopt);
          },
          [&applier, &offset](const delete_subject_key& dkey) {
              return applier.apply(offset, dkey, std::nullopt);
          },
          [&applier, &offset](const config_key& ckey) {
              return applier.apply(offset, ckey, std::nullopt);
          });
        advance_offset_inner(offset);
        offset++;
    }
    co_return versions;
}

} // namespace pandaproxy::schema_registry
