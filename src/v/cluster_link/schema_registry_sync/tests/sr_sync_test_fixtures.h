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

#include "cluster_link/schema_registry_sync/reconciler.h"
#include "cluster_link/schema_registry_sync/source_reader.h"
#include "cluster_link/schema_registry_sync/tail_reader.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/types.h"
#include "schema/registry.h"
#include "schema/tests/fake_registry.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/util/noncopyable_function.hh>

#include <deque>
#include <memory>
#include <ranges>
#include <vector>

// Test fakes and helpers shared by the reconciler unit tests
// (reconciler_test.cc) and the mirroring-task integration tests
// (mirroring_task_test.cc). Header-only: every free function is `inline` so the
// two translation units that include this share one definition.
namespace cluster_link::tests {

namespace ppsr = pandaproxy::schema_registry;
namespace srs = cluster_link::schema_registry_sync;

inline ppsr::stored_schema make_schema(
  const ppsr::context_subject& sub,
  int32_t version,
  std::string_view def,
  ppsr::is_deleted deleted = ppsr::is_deleted::no) {
    return ppsr::stored_schema{
      .schema = ppsr::
        subject_schema{sub, ppsr::schema_definition{ppsr::schema_definition::raw_string{def}, ppsr::schema_type::avro}},
      .version = ppsr::schema_version{version},
      .id = ppsr::schema_id{version},
      .deleted = deleted};
}

// A reference to one (subject, version) node. References to default-context
// subjects are emitted unqualified (the common case); others are qualified.
inline ppsr::schema_reference
ref_to(const ppsr::context_subject& sub, int32_t ver) {
    return ppsr::schema_reference{
      .name = ss::sstring{sub.sub()},
      .sub = ppsr::
        context_subject_reference{sub, sub.ctx == ppsr::default_context ? ppsr::is_qualified::no : ppsr::is_qualified::yes},
      .version = ppsr::schema_version{ver}};
}

// Collects schema_references into a definition's references container, so a
// caller can write `refs_to({ref_to(a, 1), ref_to(b, 1)})` inline.
inline ppsr::schema_definition::references
refs_to(std::initializer_list<ppsr::schema_reference> refs) {
    ppsr::schema_definition::references out;
    for (const auto& ref : refs) {
        out.push_back(ref);
    }
    return out;
}

inline ppsr::stored_schema make_schema_with_refs(
  const ppsr::context_subject& sub,
  int32_t version,
  std::string_view def,
  ppsr::schema_definition::references refs,
  ppsr::schema_id id) {
    return ppsr::stored_schema{
      .schema = ppsr::
        subject_schema{sub, ppsr::schema_definition{ppsr::schema_definition::raw_string{def}, ppsr::schema_type::avro, std::move(refs), std::nullopt}},
      .version = ppsr::schema_version{version},
      .id = id,
      .deleted = ppsr::is_deleted::no};
}

// Index of the first stored schema whose subject matches `subject`, or -1.
inline int index_of(
  const std::vector<ppsr::stored_schema>& all, std::string_view subject) {
    for (size_t i = 0; i < all.size(); ++i) {
        if (all[i].schema.sub().sub() == ppsr::subject{ss::sstring{subject}}) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

inline ppsr::subject_version
key(const ppsr::context_subject& sub, int32_t version) {
    return ppsr::subject_version{sub, ppsr::schema_version{version}};
}

/// Test-owned source-of-truth that the injected source reader serves from.
struct fake_source_state {
    chunked_vector<ppsr::context> contexts{ppsr::default_context};
    chunked_vector<ppsr::stored_schema> schemas;
    // Whether the source echoes the `deleted` flag in per-version bodies. True
    // (Redpanda, and Confluent queried with the extended-fields header) makes
    // read_subject_version report source_schema_read::deleted; false models a
    // source that omits it entirely, so deleted is nullopt and the caller must
    // fall back to the listing-derived soft-delete set.
    bool reports_deleted_flag = true;
    std::optional<srs::source_error> list_contexts_error;
    std::optional<srs::source_error> list_subjects_error;
    // Forces list_subject_versions to fail for specific subjects, letting a
    // test inject a per-subject enumeration failure (e.g. operation_failed)
    // without taking down the whole listing.
    chunked_hash_map<ppsr::context_subject, srs::source_error>
      list_versions_errors;
    // Forces list_schema_id_subject_versions to fail for specific ids: a real
    // probe failure, as opposed to the miss an unallocated id yields anyway.
    chunked_hash_map<ppsr::schema_id, srs::source_error> schema_id_errors;
    // list_schema_id_subject_versions call count, keyed by probed id.
    chunked_hash_map<ppsr::schema_id, uint32_t> schema_id_probe_counts;
    // read_subject_version call count, keyed by (subject, version).
    chunked_hash_map<ppsr::subject_version, uint32_t> read_counts;
    // Total list_subject_versions calls, so a test can assert a sync did no
    // source discovery at all.
    size_t list_versions_calls{0};
    // Total list_contexts calls, so a test can assert a tail tick did not
    // re-read the source's contexts when its batch reported none.
    size_t list_contexts_calls{0};
    // Forces read_subject_version to fail for specific (subject, version)
    // nodes, letting a test inject a mid-run source fault (e.g.
    // source_unavailable).
    chunked_hash_map<ppsr::subject_version, srs::source_error> read_errors;
    // Source own-override modes/configs, keyed by subject or context-only
    // target. Absence means "no explicit override" (read_mode/read_config
    // return nullopt). The default context, if present, models the global
    // mode/config.
    chunked_hash_map<ppsr::context_subject, ppsr::mode> modes;
    chunked_hash_map<ppsr::context_subject, ppsr::compatibility_level> configs;
    // Forces read_mode/read_config to fail for specific targets, letting a test
    // model an unmappable source value (operation_failed) or a fault.
    chunked_hash_map<ppsr::context_subject, srs::source_error> read_mode_errors;
    chunked_hash_map<ppsr::context_subject, srs::source_error>
      read_config_errors;

    // Unsupported config fields to attach to a target's read_config, letting a
    // test drive the config-path unsupported-feature policy.
    chunked_hash_map<
      ppsr::context_subject,
      chunked_vector<ppsr::unsupported_feature>>
      config_unsupported;

    void set_config_unsupported(
      const ppsr::context_subject& sub,
      chunked_vector<ppsr::unsupported_feature> features) {
        config_unsupported[sub] = std::move(features);
    }

    void fail_read(
      const ppsr::context_subject& sub,
      int32_t version,
      srs::source_error err) {
        read_errors.emplace(
          ppsr::subject_version{sub, ppsr::schema_version{version}},
          std::move(err));
    }

    // Unsupported features to attach to a specific (subject, version)'s read,
    // letting a test drive the reconciler's unsupported-feature policy.
    chunked_hash_map<
      ppsr::subject_version,
      chunked_vector<ppsr::unsupported_feature>>
      unsupported_features;

    void set_unsupported(
      const ppsr::context_subject& sub,
      int32_t version,
      chunked_vector<ppsr::unsupported_feature> features) {
        unsupported_features[ppsr::subject_version{
          sub, ppsr::schema_version{version}}] = std::move(features);
    }

    void add(
      const ppsr::context_subject& sub,
      int32_t version,
      ppsr::is_deleted deleted = ppsr::is_deleted::no) {
        auto s = make_schema(
          sub, version, fmt::format("{{\"v\":{}}}", version), deleted);
        s.id = next_id();
        schemas.push_back(std::move(s));
    }

    void add_with_refs(
      const ppsr::context_subject& sub,
      int32_t version,
      ppsr::schema_definition::references refs) {
        schemas.push_back(make_schema_with_refs(
          sub,
          version,
          fmt::format("{{\"v\":{}}}", version),
          std::move(refs),
          next_id()));
    }

    // Soft-deletes one version at the source, as DELETE
    // /subjects/{sub}/versions/{v} does: the version keeps existing but drops
    // out of the active listing.
    void soft_delete(const ppsr::context_subject& sub, int32_t version) {
        for (auto& stored : schemas) {
            if (
              stored.schema.sub() == sub
              && stored.version == ppsr::schema_version{version}) {
                stored.deleted = ppsr::is_deleted::yes;
            }
        }
    }

    // Drops every version of `sub`, as a source-side hard delete of the whole
    // subject does: its listings then report it absent.
    void remove_subject(const ppsr::context_subject& sub) {
        schemas = schemas
                  | std::views::filter(
                    [&sub](const ppsr::stored_schema& stored) {
                        return stored.schema.sub() != sub;
                    })
                  | std::views::as_rvalue
                  | std::ranges::to<chunked_vector<ppsr::stored_schema>>();
    }

    // A globally-unique schema id, matching SR's per-context id namespace where
    // distinct subjects never share an id.
    ppsr::schema_id next_id() {
        return ppsr::schema_id{static_cast<int32_t>(schemas.size() + 1)};
    }

    uint32_t reads(const ppsr::context_subject& sub, int32_t version) const {
        auto it = read_counts.find(
          ppsr::subject_version{sub, ppsr::schema_version{version}});
        return it == read_counts.end() ? 0 : it->second;
    }

    uint32_t probes(int32_t id) const {
        auto it = schema_id_probe_counts.find(ppsr::schema_id{id});
        return it == schema_id_probe_counts.end() ? 0 : it->second;
    }
};

class fake_source_reader final : public srs::source_reader {
public:
    explicit fake_source_reader(fake_source_state* state)
      : _state(state) {}

    ss::future<srs::source_result<chunked_vector<ppsr::context>>>
    list_contexts(ss::abort_source&) override {
        ++_state->list_contexts_calls;
        if (_state->list_contexts_error.has_value()) {
            co_return std::unexpected(*_state->list_contexts_error);
        }
        co_return _state->contexts.copy();
    }

    ss::future<srs::source_result<chunked_vector<ppsr::context_subject>>>
    list_subjects(ppsr::context ctx, ss::abort_source&) override {
        if (_state->list_subjects_error.has_value()) {
            co_return std::unexpected(*_state->list_subjects_error);
        }
        chunked_hash_set<ppsr::context_subject> seen;
        chunked_vector<ppsr::context_subject> subjects;
        for (const auto& s : _state->schemas) {
            if (s.schema.sub().ctx != ctx) {
                continue;
            }
            if (seen.insert(s.schema.sub()).second) {
                subjects.push_back(s.schema.sub());
            }
        }
        co_return subjects;
    }

    ss::future<srs::source_result<chunked_vector<ppsr::schema_version>>>
    list_subject_versions(
      ppsr::context_subject sub,
      ppsr::include_deleted include_deleted,
      ss::abort_source&) override {
        ++_state->list_versions_calls;
        if (
          auto it = _state->list_versions_errors.find(sub);
          it != _state->list_versions_errors.end()) {
            co_return std::unexpected(it->second);
        }
        chunked_vector<ppsr::schema_version> versions;
        bool subject_exists = false;
        for (const auto& s : _state->schemas) {
            if (s.schema.sub() != sub) {
                continue;
            }
            subject_exists = true;
            if (
              include_deleted == ppsr::include_deleted::no
              && s.deleted == ppsr::is_deleted::yes) {
                continue;
            }
            versions.push_back(s.version);
        }
        // Mirror the real SR: active-only listing of a fully soft-deleted
        // subject 404s rather than returning an empty list.
        if (
          include_deleted == ppsr::include_deleted::no && subject_exists
          && versions.empty()) {
            co_return std::unexpected(
              srs::source_error{
                .kind = srs::source_error_kind::subject_not_found,
                .message = "subject not found (fully soft-deleted)"});
        }
        co_return versions;
    }

    ss::future<srs::source_result<chunked_vector<ppsr::subject_version>>>
    list_schema_id_subject_versions(
      ppsr::schema_id id, ppsr::context ctx, ss::abort_source&) override {
        ++_state->schema_id_probe_counts[id];
        if (
          auto it = _state->schema_id_errors.find(id);
          it != _state->schema_id_errors.end()) {
            co_return std::unexpected(it->second);
        }
        // The real endpoint decides these independently: 404 only when the
        // schema does not resolve, then the pair listing filters soft-deleted
        // out. So a fully soft-deleted id is a hit with an empty list.
        bool id_resolves = false;
        chunked_vector<ppsr::subject_version> pairs;
        for (const auto& s : _state->schemas) {
            if (s.id != id || s.schema.sub().ctx != ctx) {
                continue;
            }
            id_resolves = true;
            if (s.deleted == ppsr::is_deleted::no) {
                pairs.push_back(
                  ppsr::subject_version{s.schema.sub(), s.version});
            }
        }
        if (!id_resolves) {
            co_return std::unexpected(
              srs::source_error{
                .kind = srs::source_error_kind::schema_id_not_found,
                .message = fmt::format("schema id not found: {}", id())});
        }
        co_return pairs;
    }

    ss::future<srs::source_result<ppsr::source_schema_read>>
    read_subject_version(
      ppsr::context_subject sub,
      ppsr::schema_version version,
      ss::abort_source&) override {
        ++_state->read_counts[ppsr::subject_version{sub, version}];
        if (
          auto it = _state->read_errors.find(
            ppsr::subject_version{sub, version});
          it != _state->read_errors.end()) {
            co_return std::unexpected(it->second);
        }
        for (const auto& s : _state->schemas) {
            if (s.schema.sub() == sub && s.version == version) {
                chunked_vector<ppsr::unsupported_feature> unsupported;
                if (
                  auto it = _state->unsupported_features.find(
                    ppsr::subject_version{sub, version});
                  it != _state->unsupported_features.end()) {
                    unsupported = it->second.copy();
                }
                // A source that reports the flag echoes its state; one that
                // omits it yields nullopt, and the reconciler's fallback (via
                // into_stored) supplies the deleted state instead.
                auto shared = s.share();
                co_return ppsr::source_schema_read{
                  .schema = std::move(shared.schema),
                  .version = shared.version,
                  .id = shared.id,
                  .deleted = _state->reports_deleted_flag
                               ? std::optional<ppsr::is_deleted>{s.deleted}
                               : std::nullopt,
                  .unsupported = std::move(unsupported)};
            }
        }
        co_return std::unexpected(
          srs::source_error{
            .kind = srs::source_error_kind::operation_failed,
            .message = "not found in source"});
    }

    ss::future<srs::source_result<std::optional<ppsr::mode>>>
    read_mode(ppsr::context_subject sub, ss::abort_source&) override {
        if (
          auto it = _state->read_mode_errors.find(sub);
          it != _state->read_mode_errors.end()) {
            co_return std::unexpected(it->second);
        }
        if (auto it = _state->modes.find(sub); it != _state->modes.end()) {
            co_return std::optional<ppsr::mode>{it->second};
        }
        co_return std::optional<ppsr::mode>{std::nullopt};
    }

    ss::future<srs::source_result<srs::source_config_read>>
    read_config(ppsr::context_subject sub, ss::abort_source&) override {
        if (
          auto it = _state->read_config_errors.find(sub);
          it != _state->read_config_errors.end()) {
            co_return std::unexpected(it->second);
        }
        srs::source_config_read out;
        if (auto it = _state->configs.find(sub); it != _state->configs.end()) {
            out.compatibility = it->second;
        }
        if (
          auto it = _state->config_unsupported.find(sub);
          it != _state->config_unsupported.end()) {
            out.unsupported = it->second.copy();
        }
        co_return out;
    }

private:
    fake_source_state* _state;
};

class fake_source_reader_factory final : public srs::source_reader_factory {
public:
    explicit fake_source_reader_factory(fake_source_state* state)
      : _state(state) {}

    std::unique_ptr<srs::source_reader> create(
      const cluster_link::model::schema_registry_sync_config::
        shadow_schema_registry_api*) override {
        return std::make_unique<fake_source_reader>(_state);
    }

private:
    fake_source_state* _state;
};

// Scripted tail reader: `batches` are handed to successive polls, so a test
// stages the changes a tick should see. An exhausted queue polls empty, which
// is what an idle source does.
struct fake_tail_state {
    // Makes arm() throw rather than report unavailable, which the tail_reader
    // contract forbids -- so a reader that breaks it must not take the full
    // sync down with it.
    bool arm_throws{false};
    std::optional<srs::source_error> poll_error;
    std::deque<srs::tail_batch> batches;
    size_t arms{0};
    size_t polls{0};
    /// Batches the task handed back for a later tick to replay.
    size_t rewinds{0};
    /// The batch the last poll reported, which a rewind puts back.
    std::optional<srs::tail_batch> replayable;
    /// Reported by every poll instead of draining `batches`, so a state the
    /// batch induces holds rather than lasting a single tick -- which a test
    /// would otherwise have one tail interval to observe.
    std::optional<srs::tail_batch> sticky;
    // While unresolved, parks poll() and with it the whole tail tick, so a test
    // can observe the in-progress sync's reported type. Resolve it to let the
    // tick proceed; polls after that are not delayed.
    std::unique_ptr<ss::shared_promise<>> poll_gate;

    void open_poll_gate() {
        poll_gate = std::make_unique<ss::shared_promise<>>();
    }

    /// Element-wise because a tail_batch is move-only.
    static srs::tail_batch copy_of(const srs::tail_batch& batch) {
        srs::tail_batch copy;
        for (const auto& subject : batch.subjects) {
            copy.subjects.insert(subject);
        }
        for (const auto& target : batch.mode_configs) {
            copy.mode_configs.insert(target);
        }
        for (const auto& ctx : batch.contexts) {
            copy.contexts.insert(ctx);
        }
        copy.truncated = batch.truncated;
        return copy;
    }

    /// Keeps what a poll reported, as a real reader's position does, so a
    /// rewind can hand it back.
    void keep_for_rewind(const srs::tail_batch& batch) {
        replayable = copy_of(batch);
    }
    void release_poll_gate() { poll_gate->set_value(); }
};

class fake_tail_reader final : public srs::tail_reader {
public:
    explicit fake_tail_reader(fake_tail_state* state)
      : _state(state) {}

    ss::future<srs::tail_availability> arm(ss::abort_source&) override {
        ++_state->arms;
        if (_state->arm_throws) {
            throw std::runtime_error("tail arm failed");
        }
        co_return srs::tail_availability::available;
    }

    ss::future<srs::source_result<srs::tail_batch>>
    poll(ss::abort_source&) override {
        ++_state->polls;
        if (_state->poll_gate) {
            co_await _state->poll_gate->get_shared_future();
        }
        if (_state->poll_error.has_value()) {
            co_return std::unexpected(*_state->poll_error);
        }
        if (_state->sticky.has_value()) {
            co_return fake_tail_state::copy_of(*_state->sticky);
        }
        if (_state->batches.empty()) {
            co_return srs::tail_batch{};
        }
        auto batch = std::move(_state->batches.front());
        _state->batches.pop_front();
        _state->keep_for_rewind(batch);
        co_return batch;
    }

    ss::future<> rewind() override {
        ++_state->rewinds;
        // Puts the batch back at the front, as a real reader's position does.
        if (_state->replayable.has_value()) {
            _state->batches.push_front(std::move(*_state->replayable));
            _state->replayable.reset();
        }
        co_return;
    }

private:
    fake_tail_state* _state;
};

class fake_tail_reader_factory final : public srs::tail_reader_factory {
public:
    explicit fake_tail_reader_factory(fake_tail_state* state)
      : _state(state) {}

    std::unique_ptr<srs::tail_reader> create(cluster_link::link*) override {
        return std::make_unique<fake_tail_reader>(_state);
    }

private:
    fake_tail_state* _state;
};

// Holds the pieces a standalone reconcile test needs: a source state + reader,
// a destination registry, and an all-contexts in_scope predicate.
struct reconcile_harness {
    fake_source_state source;
    fake_source_reader reader{&source};
    schema::fake_registry destination;

    // Generous memory and a single worker by default. parallelism = 1 is
    // intentional: with one worker the discover/import order is deterministic,
    // so per-node fetch-count assertions (e.g. "a is fetched exactly once") are
    // stable. Production defaults to 4 (driven by cluster config); under N > 1
    // the fetch order across independent nodes is nondeterministic, so tests
    // that exercise concurrency override `lim` and avoid exact fetch-count
    // assertions.
    srs::reconciler::limits lim{.memory_bytes = 1u << 20, .parallelism = 1};
    model::schema_registry_sync_config::unsupported_feature_policy
      feature_policy
      = model::schema_registry_sync_config::unsupported_feature_policy::fail;

    // Identity mapping by default: source and destination contexts coincide, so
    // reconcile tests need no remapping.
    srs::context_mapper mapper;

    srs::reconciler make(
      ss::noncopyable_function<bool(const ppsr::context_subject&)> in_scope =
        [](const ppsr::context_subject&) { return true; }) {
        return srs::reconciler{
          &reader,
          &destination,
          std::move(in_scope),
          mapper,
          lim,
          feature_policy};
    }

    ss::future<srs::source_result<srs::reconcile_stats>>
    run(srs::work_set work, chunked_hash_set<ppsr::subject_version> seed = {}) {
        ss::abort_source as;
        auto r = make();
        srs::reconcile_stats stats;
        auto res = co_await r.reconcile(
          std::move(work), std::move(seed), stats, as);
        if (!res.has_value()) {
            co_return std::unexpected(std::move(res.error()));
        }
        co_return stats;
    }
};

// Base for test registries that wrap a real inner registry and override only
// the calls a given test cares about; every other call delegates unchanged.
class delegating_registry : public schema::registry {
public:
    explicit delegating_registry(schema::registry* inner)
      : _inner(inner) {}

    ss::future<ppsr::context_schema_id>
    import_schema(ppsr::stored_schema schema) override {
        return _inner->import_schema(std::move(schema));
    }
    bool is_enabled() const override { return _inner->is_enabled(); }
    ss::future<> ensure_internal_topic() override {
        return _inner->ensure_internal_topic();
    }
    ss::future<ppsr::schema_getter*> getter() const override {
        return _inner->getter();
    }
    ss::future<ppsr::schema_getter*> synced_getter() const override {
        return _inner->synced_getter();
    }
    ss::future<ss::lowres_clock::time_point>
    sync(ss::lowres_clock::duration max_age) override {
        return _inner->sync(max_age);
    }
    ss::future<ppsr::schema_definition>
    get_schema_definition(ppsr::context_schema_id id) const override {
        return _inner->get_schema_definition(id);
    }
    ss::future<ppsr::stored_schema> get_subject_schema(
      ppsr::context_subject sub,
      std::optional<ppsr::schema_version> version) const override {
        return _inner->get_subject_schema(std::move(sub), version);
    }
    ss::future<chunked_vector<ppsr::subject_version_deleted>>
    list_subject_versions(
      ss::noncopyable_function<bool(const ppsr::context_subject&)> filter,
      ppsr::include_deleted inc) const override {
        return _inner->list_subject_versions(std::move(filter), inc);
    }
    ss::future<bool>
    has_subjects(ppsr::context ctx, ppsr::include_deleted inc) const override {
        return _inner->has_subjects(std::move(ctx), inc);
    }
    ss::future<chunked_vector<ppsr::context_subject>>
    get_subjects(ppsr::include_deleted inc) const override {
        return _inner->get_subjects(inc);
    }
    ss::future<chunked_vector<ppsr::context>> list_contexts() const override {
        return _inner->list_contexts();
    }
    ss::future<ppsr::context_schema_id>
    create_schema(ppsr::subject_schema s) override {
        return _inner->create_schema(std::move(s));
    }
    ss::future<bool> soft_delete_schema(
      ppsr::context_subject sub, ppsr::schema_version v) override {
        return _inner->soft_delete_schema(std::move(sub), v);
    }
    ss::future<chunked_vector<ppsr::schema_version>> permanent_delete_schema(
      ppsr::context_subject sub,
      std::optional<ppsr::schema_version> v) override {
        return _inner->permanent_delete_schema(std::move(sub), v);
    }
    ss::future<bool>
    write_mode(ppsr::context_subject sub, ppsr::mode m) override {
        return _inner->write_mode(std::move(sub), m);
    }
    ss::future<bool> delete_mode(ppsr::context_subject sub) override {
        return _inner->delete_mode(std::move(sub));
    }
    ss::future<bool> write_config(
      ppsr::context_subject sub, ppsr::compatibility_level c) override {
        return _inner->write_config(std::move(sub), c);
    }
    ss::future<bool> delete_config(ppsr::context_subject sub) override {
        return _inner->delete_config(std::move(sub));
    }
    ss::future<> delete_context(ppsr::context ctx) override {
        return _inner->delete_context(std::move(ctx));
    }

protected:
    schema::registry* _inner;
};

// Counts destination inventory scans, so a test can assert a sync did not scan
// at all rather than inferring it from the counters a scan would leave
// unchanged. list_subject_versions over the whole in-scope set is what a scan
// is (see scan_destination_inventory).
class scan_counting_registry final : public delegating_registry {
public:
    using delegating_registry::delegating_registry;

    ss::future<chunked_vector<ppsr::subject_version_deleted>>
    list_subject_versions(
      ss::noncopyable_function<bool(const ppsr::context_subject&)> filter,
      ppsr::include_deleted inc) const override {
        ++scans;
        return delegating_registry::list_subject_versions(
          std::move(filter), inc);
    }

    mutable size_t scans{0};
};

// Wraps a destination registry, suspending `import_schema` on an abortable wait
// so a test can abort the sync while a worker holds byte-budget units around an
// in-flight import. Every other call delegates to the inner registry.
class blocking_import_registry final : public delegating_registry {
public:
    /// `block_after` imports are forwarded to the inner registry; the next
    /// import parks (signalling `entered`) until the test aborts. Default 0
    /// blocks the very first import.
    explicit blocking_import_registry(
      schema::registry* inner, size_t block_after = 0)
      : delegating_registry(inner)
      , _block_after(block_after) {}

    ss::future<ppsr::context_schema_id>
    import_schema(ppsr::stored_schema schema) override {
        if (_imports_seen++ < _block_after) {
            co_return co_await _inner->import_schema(std::move(schema));
        }
        if (!_entered_set) {
            _entered_set = true;
            _entered.set_value();
        }
        // Park until the test releases (clean completion) or aborts (the wait
        // resolves with abort_requested).
        co_await _release.get_future();
        co_return co_await _inner->import_schema(std::move(schema));
    }

    ss::future<> entered() { return _entered.get_future(); }
    // Resolve the parked import as an abort, so the run unwinds through the
    // reconciler's abort path.
    void abort() {
        _release.set_exception(
          std::make_exception_ptr(ss::abort_requested_exception{}));
    }
    // Resolve the parked import cleanly so it forwards to the inner registry.
    void unblock() { _release.set_value(); }

private:
    size_t _block_after;
    size_t _imports_seen{0};
    bool _entered_set{false};
    ss::promise<> _entered;
    ss::promise<> _release;
};

// Wraps a destination registry, throwing a configured schema-registry error for
// specific (subject, version) imports and delegating everything else. Lets a
// test inject a per-item import failure the in-memory fake would never produce
// on its own -- a body the destination cannot parse, or a reference that does
// not resolve at import time.
class failing_import_registry final : public delegating_registry {
public:
    using delegating_registry::delegating_registry;

    void fail_import(
      const ppsr::subject_version& key,
      ppsr::error_code code,
      ss::sstring message) {
        _failures.emplace(key, ppsr::error_info{code, std::move(message)});
    }

    ss::future<ppsr::context_schema_id>
    import_schema(ppsr::stored_schema schema) override {
        ppsr::subject_version key{schema.schema.sub(), schema.version};
        if (auto it = _failures.find(key); it != _failures.end()) {
            return ss::make_exception_future<ppsr::context_schema_id>(
              ppsr::as_exception(it->second));
        }
        return _inner->import_schema(std::move(schema));
    }

private:
    chunked_hash_map<ppsr::subject_version, ppsr::error_info> _failures;
};

// Wraps a destination registry so a test can make write_config fail for a
// subject, exercising the REMOVE "no count on a failed config write" path the
// plain fake (which never rejects a config write) cannot.
class failing_config_registry final : public delegating_registry {
public:
    using delegating_registry::delegating_registry;

    void fail_config(
      const ppsr::context_subject& sub,
      ppsr::error_code code,
      ss::sstring message) {
        _failures.emplace(sub, ppsr::error_info{code, std::move(message)});
    }

    ss::future<bool> write_config(
      ppsr::context_subject sub, ppsr::compatibility_level c) override {
        if (auto it = _failures.find(sub); it != _failures.end()) {
            return ss::make_exception_future<bool>(
              ppsr::as_exception(it->second));
        }
        return _inner->write_config(std::move(sub), c);
    }

private:
    chunked_hash_map<ppsr::context_subject, ppsr::error_info> _failures;
};

// Wraps a destination registry, modeling the real Schema Registry's refusal to
// permanently delete a version still referenced by a live one (the in-memory
// fake never rejects a delete). Lets a test drive the hard-delete retry loop: a
// referent stays blocked until its referrer is purged, so the sync must delete
// the referrer first and retry the referent in a later round rather than count
// an error; a permanently-blocked delete ends up counted once.
class deferred_delete_registry final : public delegating_registry {
public:
    using delegating_registry::delegating_registry;

    // Reject permanent deletes of `referent` until `referrer` has been
    // permanently deleted -- models a live reference the purge must clear
    // first.
    void block_until_purged(
      const ppsr::context_subject& referent,
      const ppsr::context_subject& referrer) {
        _blocked_on.insert_or_assign(referent, referrer);
    }
    // Reject every permanent delete of `subject` -- models a permanently-stuck
    // delete (e.g. a reference cycle) that must end up counted as an error.
    void reject_forever(const ppsr::context_subject& subject) {
        _forever.insert(subject);
    }
    // Reject every permanent delete of `subject` with a non-reference error --
    // models a real destination fault the purge must count immediately rather
    // than retry as if it were reference ordering.
    void fail_with(const ppsr::context_subject& subject, ppsr::error_code ec) {
        _fail_with.insert_or_assign(subject, ec);
    }
    size_t
    permanent_delete_attempts(const ppsr::context_subject& subject) const {
        auto it = _attempts.find(subject);
        return it == _attempts.end() ? 0 : it->second;
    }

    ss::future<chunked_vector<ppsr::schema_version>> permanent_delete_schema(
      ppsr::context_subject sub,
      std::optional<ppsr::schema_version> v) override {
        ++_attempts[sub];
        if (auto f = _fail_with.find(sub); f != _fail_with.end()) {
            return ss::make_exception_future<
              chunked_vector<ppsr::schema_version>>(ppsr::as_exception(
              ppsr::error_info{f->second, "destination fault"}));
        }
        auto it = _blocked_on.find(sub);
        const bool blocked
          = _forever.contains(sub)
            || (it != _blocked_on.end() && !_purged.contains(it->second));
        if (blocked) {
            return ss::make_exception_future<
              chunked_vector<ppsr::schema_version>>(ppsr::as_exception(
              ppsr::error_info{
                ppsr::error_code::subject_version_has_references,
                "referenced by a live schema"}));
        }
        _purged.insert(sub);
        return _inner->permanent_delete_schema(std::move(sub), v);
    }

private:
    chunked_hash_map<ppsr::context_subject, ppsr::context_subject> _blocked_on;
    chunked_hash_set<ppsr::context_subject> _forever;
    chunked_hash_map<ppsr::context_subject, ppsr::error_code> _fail_with;
    chunked_hash_map<ppsr::context_subject, size_t> _attempts;
    chunked_hash_set<ppsr::context_subject> _purged;
};

} // namespace cluster_link::tests
