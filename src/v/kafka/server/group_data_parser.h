/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * as of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "features/feature_table.h"
#include "kafka/server/group.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/logger.h"
#include "model/record.h"
#include "model/record_batch_types.h"

template<typename T>
T parse_tx_batch(const model::record_batch& batch, int8_t version) {
    vassert(batch.record_count() == 1, "tx batch must contain a single record");
    auto r = batch.copy_records();
    auto& record = *r.begin();
    auto key_buf = record.release_key();
    auto val_buf = record.release_value();

    iobuf_parser val_reader(std::move(val_buf));
    auto tx_version = reflection::adl<int8_t>{}.from(val_reader);
    vassert(
      tx_version == version,
      "unknown group inflight tx record version: {} expected: {}",
      tx_version,
      version);
    auto cmd = reflection::adl<T>{}.from(val_reader);

    iobuf_parser key_reader(std::move(key_buf));
    auto batch_type = reflection::adl<model::record_batch_type>{}.from(
      key_reader);
    const auto& hdr = batch.header();
    vassert(
      hdr.type == batch_type,
      "broken tx group message. expected batch type {} got: {}",
      hdr.type,
      batch_type);
    auto p_id = model::producer_id(reflection::adl<int64_t>{}.from(key_reader));
    auto bid = model::batch_identity::from(hdr);
    vassert(
      p_id == bid.pid.id,
      "broken tx group message. expected pid/id {} got: {}",
      bid.pid.id,
      p_id);
    return cmd;
}

namespace kafka {

template<class T>
concept GroupDataParserBase = requires(T base, model::record_batch b) {
    base.handle_raft_data(std::move(b));
    base.handle_tx_offsets(b.header(), kafka::group_tx::offsets_metadata{});
    base.handle_commit(b.header(), group_tx::commit_metadata{});
    base.handle_abort(b.header(), group_tx::abort_metadata{});
    base.handle_fence_v0(b.header(), group_tx::fence_metadata_v0{});
    base.handle_fence_v1(b.header(), group_tx::fence_metadata_v1{});
    base.handle_fence(b.header(), kafka::group_tx::fence_metadata{});
    base.handle_version_fence(features::feature_table::version_fence{});
};

template<class Base>
class group_data_parser {
public:
    group_data_parser() {
        static_assert(
          GroupDataParserBase<Base>,
          "Base does not implement all the required methods.");
    }

protected:
    ss::future<> parse(model::record_batch b) {
        if (b.header().type == model::record_batch_type::raft_data) {
            return handle_raft_data(std::move(b));
        }
        // silently ignore raft configuration.
        if (b.header().type == model::record_batch_type::raft_configuration) {
            return ss::now();
        }
        if (b.header().type == model::record_batch_type::group_prepare_tx) {
            auto data = parse_tx_batch<kafka::group_tx::offsets_metadata>(
              b, group::prepared_tx_record_version);
            return handle_tx_offsets(b.header(), std::move(data));
        }
        if (b.header().type == model::record_batch_type::group_commit_tx) {
            auto data = parse_tx_batch<group_tx::commit_metadata>(
              b, group::commit_tx_record_version);
            return handle_commit(b.header(), std::move(data));
        }
        if (b.header().type == model::record_batch_type::group_abort_tx) {
            auto data = parse_tx_batch<group_tx::abort_metadata>(
              b, group::aborted_tx_record_version);
            return handle_abort(b.header(), std::move(data));
        }
        if (b.header().type == model::record_batch_type::tx_fence) {
            /*
              A potential bug in 24.2.0 resulted in a situation where tx_fence
              batches were retained _after_ compaction while their corresponding
              data/commit/abort batches were compacted away. This applied to
              only group transactions that used tx_fence to begin the
              transaction.

              Historical context: tx_fence was considered historically as
              fence batch that begins a group transaction and regular data
              partition transaction. That changed starting 24.2.0 where a
              dedicated fence batch type (group_tx_fence) was used for group
              transaction fencing.

              After this buggy compaction, these uncleaned tx_fence batches are
              accounted as open transactions when computing
              max_collectible_offset thus blocking further compaction after
              upgrade to 24.2.x.

              We just ignore tx_fence batches going forward, the rationale is as
              follows.

              - Fistly they are not currently in use starting 24.2 (in favor of
              a dedicated group_tx_fence), anyone starting group transactions
              from 24.2 shouldn't see any conflicts

              - For sealed transactions, commit/abort/data batches were already
              removed if the compaction ran, so ignoring tx_fence should be the
              right thing to in such cases without any conflicts/correctness
              issues

              - Hypothetically if the compaction didn't run, it is still ok to
              ignore those batches because in group transactions commited
              transactions are atomically rewritten as a separate raft_data
              batch along with commit marker which will be applied in the stm
              (so no state will be lost)

              - Any group transaction using tx_fence likely belonged to 24.1.x
              which is atleast 6 months old at the time of writing, so
              reasonable to assume all such transactions are already sealed,
              especially since we started enforcing max transaction timeout of
              15mins.

              - The only case where it could theoretically be a problem is
              during an upgrade from 24.1.x with an open transaction upgrading
              to 24.2.x (with the fix) and the transaction remaining open
              throughout the upgrade which then be considered aborted (if
              leadership is assumed on a 24.2.x broker). This is a highly
              unlikey scenario but the suggestion is to stop all running group
              transactions (kstreams) applications when doing the upgrade
            */
        }
        if (b.header().type == model::record_batch_type::group_fence_tx) {
            return parse_fence(std::move(b));
        }
        if (b.header().type == model::record_batch_type::version_fence) {
            auto fence = features::feature_table::decode_version_fence(
              std::move(b));
            return handle_version_fence(fence);
        }
        vlog(klog.warn, "ignoring batch with type: {}", b.header().type);
        return ss::make_ready_future<>();
    }

private:
    ss::future<> parse_fence(model::record_batch b) {
        auto r = b.copy_records();
        auto& record = *r.begin();
        auto key_buf = record.release_key();
        auto val_buf = record.release_value();

        iobuf_parser key_reader(std::move(key_buf));
        auto batch_type = reflection::adl<model::record_batch_type>{}.from(
          key_reader);
        const auto& hdr = b.header();
        vassert(
          hdr.type == batch_type,
          "broken tx group message. expected batch type {} got: {}",
          hdr.type,
          batch_type);
        auto p_id = model::producer_id(
          reflection::adl<int64_t>{}.from(key_reader));
        auto bid = model::batch_identity::from(hdr);
        vassert(
          p_id == bid.pid.id,
          "broken tx group message. expected pid/id {} got: {}",
          bid.pid.id,
          p_id);

        iobuf_parser val_reader(std::move(val_buf));
        auto fence_version = reflection::adl<int8_t>{}.from(val_reader);

        if (fence_version == group::fence_control_record_v0_version) {
            auto data = reflection::adl<group_tx::fence_metadata_v0>{}.from(
              val_reader);
            return handle_fence_v0(hdr, std::move(data));
        } else if (fence_version == group::fence_control_record_v1_version) {
            auto data = reflection::adl<group_tx::fence_metadata_v1>{}.from(
              val_reader);
            return handle_fence_v1(hdr, std::move(data));
        } else if (fence_version == group::fence_control_record_version) {
            auto data = reflection::adl<group_tx::fence_metadata>{}.from(
              val_reader);
            return handle_fence(hdr, std::move(data));
        }
        vassert(
          false,
          "unknown group fence record version: {} expected at most: {}",
          fence_version,
          group::fence_control_record_version);
    }
    ss::future<> handle_raft_data(model::record_batch b) {
        return static_cast<Base*>(this)->handle_raft_data(std::move(b));
    }
    ss::future<> handle_tx_offsets(
      model::record_batch_header header,
      kafka::group_tx::offsets_metadata data) {
        return static_cast<Base*>(this)->handle_tx_offsets(
          header, std::move(data));
    }
    ss::future<> handle_fence_v0(
      model::record_batch_header header,
      kafka::group_tx::fence_metadata_v0 data) {
        return static_cast<Base*>(this)->handle_fence_v0(
          header, std::move(data));
    }
    ss::future<> handle_fence_v1(
      model::record_batch_header header,
      kafka::group_tx::fence_metadata_v1 data) {
        return static_cast<Base*>(this)->handle_fence_v1(
          header, std::move(data));
    }
    ss::future<> handle_fence(
      model::record_batch_header header, kafka::group_tx::fence_metadata data) {
        return static_cast<Base*>(this)->handle_fence(header, std::move(data));
    }
    ss::future<> handle_abort(
      model::record_batch_header header, kafka::group_tx::abort_metadata data) {
        return static_cast<Base*>(this)->handle_abort(header, std::move(data));
    }
    ss::future<> handle_commit(
      model::record_batch_header header,
      kafka::group_tx::commit_metadata data) {
        return static_cast<Base*>(this)->handle_commit(header, std::move(data));
    }
    ss::future<>
    handle_version_fence(features::feature_table::version_fence fence) {
        return static_cast<Base*>(this)->handle_version_fence(fence);
    }
};
} // namespace kafka
