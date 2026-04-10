/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cloud_topics/level_one/metastore/lsm/replicated_persistence.h"

#include "cloud_topics/level_one/metastore/lsm/lsm_update.h"
#include "cloud_topics/level_one/metastore/lsm/stm.h"
#include "lsm/core/exceptions.h"
#include "lsm/io/cloud_cache_persistence.h"
#include "lsm/io/persistence.h"
#include "lsm/lsm.h"
#include "lsm/proto/manifest.proto.h"
#include "model/batch_builder.h"

namespace cloud_topics::l1 {

namespace {

class replicated_metadata_persistence : public lsm::io::metadata_persistence {
public:
    replicated_metadata_persistence(
      stm* stm,
      domain_uuid domain_uuid,
      std::unique_ptr<lsm::io::metadata_persistence> cloud_persistence)
      : _stm(stm)
      , _expected_domain_uuid(domain_uuid)
      , _cloud_persistence(std::move(cloud_persistence)) {}

    ss::future<std::optional<iobuf>>
    read_manifest(lsm::internal::database_epoch max_epoch) override {
        _as.check();
        auto _ = _gate.hold();
        auto term_result = co_await _stm->sync(std::chrono::seconds(30), _as);
        if (!term_result.has_value()) {
            using enum stm::errc;
            switch (term_result.error()) {
            case shutting_down:
                throw lsm::abort_requested_exception(
                  "shutting down while syncing STM when loading manifest");
            case not_leader:
            case raft_error:
                throw lsm::io_error_exception(
                  "failed to sync STM when loading manifest: {}",
                  term_result.error());
            }
        }
        if (_stm->state().domain_uuid != _expected_domain_uuid) {
            // Caller needs to rebuild the metadata persistence with the
            // appropriate prefix.
            throw lsm::io_error_exception(
              "Domain UUID has changed, likely due to domain recovery, "
              "expected {}, got {}",
              _expected_domain_uuid,
              _stm->state().domain_uuid);
        }
        auto term = term_result.value();
        auto stm_epoch = _stm->state().to_epoch(term);
        if (stm_epoch > max_epoch) {
            // Callers are expected to have opened the database with an epoch
            // at or higher than what is in the STM.
            throw lsm::io_error_exception(
              "Can't load manifest above current epoch {}, STM epoch: {}",
              max_epoch(),
              stm_epoch);
        }
        if (!_stm->state().persisted_manifest.has_value()) {
            // There is no persisted manifest.
            co_return std::nullopt;
        }
        co_return _stm->mutable_state().persisted_manifest->buf.share();
    }

    ss::future<>
    write_manifest(lsm::internal::database_epoch epoch, iobuf b) override {
        _as.check();
        auto h = _gate.hold();
        co_await _cloud_persistence->write_manifest(epoch, b.share());

        // Now that the manifest has be persisted successfully, replicate to
        // the log.
        auto m = co_await lsm::proto::manifest::from_proto(b.share());
        auto serialized_man = lsm_state::serialized_manifest{
          .buf = std::move(b),
          .last_seqno = lsm::sequence_number(m.get_last_seqno()),
          .database_epoch = lsm::internal::database_epoch(
            m.get_database_epoch()),
        };
        auto update_res = persist_manifest_update::build(
          _stm->state(), _expected_domain_uuid, std::move(serialized_man));
        if (!update_res.has_value()) {
            throw lsm::io_error_exception(
              "Invalid persist_manifest update: {}", update_res.error());
        }
        model::batch_builder builder;
        builder.set_batch_type(model::record_batch_type::l1_stm);
        builder.add_record(
          {.key = serde::to_iobuf(lsm_update_key::persist_manifest),
           .value = serde::to_iobuf(std::move(update_res.value()))});
        auto batch = co_await std::move(builder).build();

        auto replicate_result = co_await _stm->replicate_and_wait(
          _stm->state().to_term(epoch), std::move(batch), _as);
        if (!replicate_result.has_value()) {
            using enum stm::errc;
            switch (replicate_result.error()) {
            case shutting_down:
                throw lsm::abort_requested_exception(
                  "shutting down while replicating manifest after persisting");
            case not_leader:
            case raft_error:
                throw lsm::io_error_exception(
                  "replication error after persisting manifest: {}",
                  replicate_result.error());
            }
        }
    }

    ss::future<> close() override {
        _as.request_abort_ex(
          lsm::abort_requested_exception(
            "replicated_persistence shutting down"));
        auto fut = _gate.close();
        auto persistence_fut = _cloud_persistence->close();
        co_await std::move(persistence_fut);
        co_await std::move(fut);
    }

private:
    ss::gate _gate;
    ss::abort_source _as;
    stm* _stm;
    domain_uuid _expected_domain_uuid;
    std::unique_ptr<metadata_persistence> _cloud_persistence;
};

} // namespace

ss::future<std::unique_ptr<lsm::io::metadata_persistence>>
open_replicated_metadata_persistence(
  stm* stm,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  domain_uuid domain_uuid,
  const cloud_storage_clients::object_key& prefix) {
    auto cloud_persistence = co_await lsm::io::open_cloud_metadata_persistence(
      remote, bucket, prefix);
    co_return std::make_unique<replicated_metadata_persistence>(
      stm, domain_uuid, std::move(cloud_persistence));
}

} // namespace cloud_topics::l1
