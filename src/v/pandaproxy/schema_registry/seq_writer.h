//// Copyright 2021 Redpanda Data, Inc.
////
//// Use of this software is governed by the Business Source License
//// included in the file licenses/BSL.md
////
//// As of the Change Date specified in that file, in accordance with
//// the Business Source License, use of this software will be governed
//// by the Apache License, Version 2.

#pragma once

#include "base/outcome.h"
#include "kafka/client/client.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/types.h"
#include "random/simple_time_jitter.h"
#include "ssx/semaphore.h"
#include "utils/retry.h"

namespace pandaproxy::schema_registry {

using namespace std::chrono_literals;

static const int max_retries = 4;

class seq_writer final : public ss::peering_sharded_service<seq_writer> {
public:
    seq_writer(
      model::node_id node_id,
      ss::smp_service_group smp_group,
      ss::sharded<kafka::client::client>& client,
      sharded_store& store)
      : _smp_opts(ss::smp_submit_to_options{smp_group})
      , _client(client)
      , _store(store)
      , _node_id(node_id) {}

    ss::future<> read_sync();

    // Throws 42205 if the subject cannot be modified
    ss::future<> check_mutable(const std::optional<subject>& sub);

    // API for readers: notify us when they have read and applied an offset
    ss::future<> advance_offset(model::offset offset);

    ss::future<schema_id> write_subject_version(subject_schema schema);

    ss::future<bool>
    write_config(std::optional<subject> sub, compatibility_level compat);

    ss::future<bool> delete_config(subject sub);

    ss::future<bool> write_mode(std::optional<subject> sub, mode m, force f);

    ss::future<bool> delete_mode(subject sub);

    ss::future<bool>
    delete_subject_version(subject sub, schema_version version);

    ss::future<std::vector<schema_version>>
    delete_subject_impermanent(subject sub);

    ss::future<std::vector<schema_version>> delete_subject_permanent(
      subject sub, std::optional<schema_version> version);

private:
    ss::smp_submit_to_options _smp_opts;

    ss::sharded<kafka::client::client>& _client;
    sharded_store& _store;

    model::node_id _node_id;

    void advance_offset_inner(model::offset offset);

    ss::future<std::optional<schema_id>>
    do_write_subject_version(subject_schema schema, model::offset write_at);

    ss::future<std::optional<bool>> do_write_config(
      std::optional<subject> sub,
      compatibility_level compat,
      model::offset write_at);

    ss::future<std::optional<bool>> do_delete_config(subject sub);

    ss::future<std::optional<bool>> do_write_mode(
      std::optional<subject> sub, mode m, force f, model::offset write_at);

    ss::future<std::optional<bool>>
    do_delete_mode(subject sub, model::offset write_at);

    ss::future<std::optional<bool>> do_delete_subject_version(
      subject sub, schema_version version, model::offset write_at);

    ss::future<std::optional<std::vector<schema_version>>>
    do_delete_subject_impermanent(subject sub, model::offset write_at);

    ss::future<std::optional<std::vector<schema_version>>>
    delete_subject_permanent_inner(
      subject sub, std::optional<schema_version> version);

    simple_time_jitter<ss::lowres_clock> _jitter{std::chrono::milliseconds{50}};

    /// Helper for write paths that use sequence+retry logic to synchronize
    /// multiple writing nodes.
    template<typename F>
    auto sequenced_write(F f) {
        auto base_backoff = _jitter.next_duration();
        auto remote = [base_backoff, f](seq_writer& seq) {
            if (auto waiters = seq._write_sem.waiters(); waiters != 0) {
                vlog(
                  plog.trace,
                  "sequenced_write waiting for {} waiters",
                  waiters);
            }
            return ss::with_semaphore(
              seq._write_sem, 1, [&seq, f, base_backoff]() {
                  if (auto waiters = seq._wait_for_sem.waiters();
                      waiters != 0) {
                      vlog(
                        plog.debug,
                        "sequenced_write acquired write_sem with {} "
                        "wait_for_sem waiters",
                        waiters);
                  }
                  return retry_with_backoff(
                    max_retries,
                    [f, &seq]() { return seq.sequenced_write_inner(f); },
                    base_backoff);
              });
        };

        return container().invoke_on(0, _smp_opts, remote).then([](auto res) {
            return std::move(res).value();
        });
    }

    /// The part of sequenced_write that runs on shard zero
    ///
    /// This is declared as a separate member function rather than
    /// inline in sequenced_write, to avoid compiler issues (and resulting
    /// crashes) seen when passing in a coroutine lambda.
    ///
    /// The return of f is wrapped in an outcome<>, to transport exceptions
    /// without causing a retry.
    template<
      typename F,
      typename invoke_result_t = typename std::
        invoke_result_t<F, model::offset, seq_writer&>::value_type::value_type>
    ss::future<
      outcome::outcome<invoke_result_t, std::error_code, std::exception_ptr>>
    sequenced_write_inner(F f) {
        // If we run concurrently with them, redundant replays to the store
        // will be safely dropped based on offset.
        co_await read_sync();

        auto next_offset = _loaded_offset + model::offset{1};
        std::optional<invoke_result_t> r;
        try {
            r = co_await f(next_offset, *this);
        } catch (const exception& e) {
            co_return std::current_exception();
        }
        if (r.has_value()) {
            co_return r.value();
        } else {
            throw exception(
              error_code::write_collision,
              fmt::format("Write collision at offset {}", next_offset));
        }
    }

    ss::future<bool> produce_and_apply(
      std::optional<model::offset> write_at, model::record_batch batch);

    /// Block until this offset is available, fetching if necessary
    ss::future<> wait_for(model::offset offset);

    // Global (Shard 0) State
    // ======================

    /// Serialize wait_for operations, to avoid issuing
    /// gratuitous number of reads to the topic on concurrent GETs.
    ssx::semaphore _wait_for_sem{1, "pproxy/schema-wait"};

    /// Shard 0 only: Reads have progressed as far as this offset
    model::offset _loaded_offset{-1};

    /// Shard 0 only: Serialize write operations.
    ssx::semaphore _write_sem{1, "pproxy/schema-write"};

    // ======================
    // End of Shard 0 state
};

} // namespace pandaproxy::schema_registry
