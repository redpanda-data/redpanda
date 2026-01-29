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
    { base.handle_raft_data(std::move(b)) } -> std::same_as<ss::future<>>;
    {
        base.handle_tx_offsets(b.header(), kafka::group_tx::offsets_metadata{})
    } -> std::same_as<ss::future<>>;
    {
        base.handle_commit(b.header(), group_tx::commit_metadata{})
    } -> std::same_as<ss::future<>>;
    {
        base.handle_abort(b.header(), group_tx::abort_metadata{})
    } -> std::same_as<ss::future<>>;
    {
        base.handle_fence_v0(b.header(), group_tx::fence_metadata_v0{})
    } -> std::same_as<ss::future<>>;
    {
        base.handle_fence_v1(b.header(), group_tx::fence_metadata_v1{})
    } -> std::same_as<ss::future<>>;
    {
        base.handle_fence(b.header(), kafka::group_tx::fence_metadata{})
    } -> std::same_as<ss::future<>>;
    {
        base.handle_version_fence(features::feature_table::version_fence{})
    } -> std::same_as<ss::future<>>;
    {
        base.handle_group_block(
          kafka::group_block{std::move(b.copy_records()[0])})
    } -> std::same_as<void>;
    {
        std::as_const(base).is_group_blocked(kafka::group_id{})
    } -> std::same_as<bool>;
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
    // Callers to maintain the object lifetime via a gate or otherwise.
    ss::future<> parse(model::record_batch b) {
        switch (b.header().type) {
        case model::record_batch_type::raft_data:
            return handle_raft_data(std::move(b));
        case model::record_batch_type::raft_configuration:
            // silently ignore raft configuration.
            return ss::now();
        case model::record_batch_type::group_prepare_tx: {
            auto data = parse_tx_batch<kafka::group_tx::offsets_metadata>(
              b, group::prepared_tx_record_version);
            return handle_tx_offsets(b.header(), std::move(data));
        }
        case model::record_batch_type::group_commit_tx: {
            auto data = parse_tx_batch<group_tx::commit_metadata>(
              b, group::commit_tx_record_version);
            return handle_commit(b.header(), std::move(data));
        }
        case model::record_batch_type::group_abort_tx: {
            auto data = parse_tx_batch<group_tx::abort_metadata>(
              b, group::aborted_tx_record_version);
            return handle_abort(b.header(), std::move(data));
        }
        case model::record_batch_type::tx_fence:
        case model::record_batch_type::group_fence_tx:
            return parse_fence(std::move(b));
        case model::record_batch_type::version_fence: {
            auto fence = features::feature_table::decode_version_fence(
              std::move(b));
            return handle_version_fence(fence);
        }
        case model::record_batch_type::group_block: {
            return handle_group_block(std::move(b));
        }
        default:
            vlog(klog.debug, "ignoring batch with type: {}", b.header().type);
            return ss::make_ready_future<>();
        }
    }

    bool is_group_blocked_verbose(
      kafka::group_id group_id, std::string_view skipped_msg) const {
        if (unlikely(
              static_cast<const Base*>(this)->is_group_blocked(group_id))) {
            vlog(
              cg_klog.error,
              "[group: {}] skipping {}, group is blocked",
              group_id,
              skipped_msg);
            return true;
        }
        return false;
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
        if (is_group_blocked_verbose(data.group_id, "tx offsets")) {
            return ss::now();
        }
        return static_cast<Base*>(this)->handle_tx_offsets(
          header, std::move(data));
    }
    ss::future<> handle_fence_v0(
      model::record_batch_header header,
      kafka::group_tx::fence_metadata_v0 data) {
        if (is_group_blocked_verbose(data.group_id, "fence v0")) {
            return ss::now();
        }
        return static_cast<Base*>(this)->handle_fence_v0(
          header, std::move(data));
    }
    ss::future<> handle_fence_v1(
      model::record_batch_header header,
      kafka::group_tx::fence_metadata_v1 data) {
        if (is_group_blocked_verbose(data.group_id, "fence v1")) {
            return ss::now();
        }
        return static_cast<Base*>(this)->handle_fence_v1(
          header, std::move(data));
    }
    ss::future<> handle_fence(
      model::record_batch_header header, kafka::group_tx::fence_metadata data) {
        if (is_group_blocked_verbose(data.group_id, "fence")) {
            return ss::now();
        }
        return static_cast<Base*>(this)->handle_fence(header, std::move(data));
    }
    ss::future<> handle_abort(
      model::record_batch_header header, kafka::group_tx::abort_metadata data) {
        return static_cast<Base*>(this)->handle_abort(header, std::move(data));
    }
    ss::future<> handle_commit(
      model::record_batch_header header,
      kafka::group_tx::commit_metadata data) {
        if (is_group_blocked_verbose(data.group_id, "commit")) {
            return ss::now();
        }
        return static_cast<Base*>(this)->handle_commit(header, std::move(data));
    }
    ss::future<>
    handle_version_fence(features::feature_table::version_fence fence) {
        return static_cast<Base*>(this)->handle_version_fence(fence);
    }
    ss::future<> handle_group_block(model::record_batch b) {
        co_await model::for_each_record(b, [this](model::record& r) {
            static_cast<Base*>(this)->handle_group_block(
              kafka::group_block{std::move(r)});
        });
    }
};
} // namespace kafka
