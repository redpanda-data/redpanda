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
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"

namespace kafka {
namespace detail {
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

} // namespace detail

using group_block_info_map
  = chunked_hash_map<kafka::group_id, group_block_info>;

template<class T>
concept GroupDataParserBase = requires(T impl, model::record_batch b) {
    { impl.handle_raft_data(std::as_const(b)) } -> std::same_as<ss::future<>>;
    {
        impl.handle_tx_offsets(b.header(), kafka::group_tx::offsets_metadata{})
    } -> std::same_as<ss::future<>>;
    {
        impl.handle_commit(b.header(), group_tx::commit_metadata{})
    } -> std::same_as<ss::future<>>;
    {
        impl.handle_abort(b.header(), group_tx::abort_metadata{})
    } -> std::same_as<ss::future<>>;
    {
        impl.handle_fence_v0(b.header(), group_tx::fence_metadata_v0{})
    } -> std::same_as<ss::future<>>;
    {
        impl.handle_fence_v1(b.header(), group_tx::fence_metadata_v1{})
    } -> std::same_as<ss::future<>>;
    {
        impl.handle_fence(b.header(), kafka::group_tx::fence_metadata{})
    } -> std::same_as<ss::future<>>;
    {
        impl.handle_version_fence(features::feature_table::version_fence{})
    } -> std::same_as<ss::future<>>;
    {
        impl.handle_group_block(kafka::group_block{{}, {}})
    } -> std::same_as<void>;
    { impl.group_blocks() } -> std::same_as<group_block_info_map&>;
    {
        std::as_const(impl).group_blocks()
    } -> std::same_as<const group_block_info_map&>;
};

template<class Impl>
class group_data_parser {
public:
    group_data_parser() {
        static_assert(
          GroupDataParserBase<Impl>,
          "Base does not implement all the required methods.");
    }

protected:
    // Callers to maintain the object lifetime via a gate or otherwise. The
    // batch is only copied for the record types whose decoders consume it.
    ss::future<> parse(const model::record_batch& b) {
        switch (b.header().type) {
        case model::record_batch_type::raft_data:
            return handle_raft_data(b);
        case model::record_batch_type::raft_configuration:
            // silently ignore raft configuration.
            return ss::now();
        case model::record_batch_type::group_prepare_tx: {
            auto data
              = detail::parse_tx_batch<kafka::group_tx::offsets_metadata>(
                b, group::prepared_tx_record_version);
            return handle_tx_offsets(b.header(), std::move(data));
        }
        case model::record_batch_type::group_commit_tx: {
            auto data = detail::parse_tx_batch<group_tx::commit_metadata>(
              b, group::commit_tx_record_version);
            return handle_commit(b.header(), std::move(data));
        }
        case model::record_batch_type::group_abort_tx: {
            auto data = detail::parse_tx_batch<group_tx::abort_metadata>(
              b, group::aborted_tx_record_version);
            return handle_abort(b.header(), std::move(data));
        }
        case model::record_batch_type::tx_fence:
        case model::record_batch_type::group_fence_tx:
            return parse_fence(b.copy());
        case model::record_batch_type::version_fence: {
            auto fence = features::feature_table::decode_version_fence(
              b.copy());
            return handle_version_fence(fence);
        }
        case model::record_batch_type::group_block: {
            return handle_group_block(b.copy());
        }
        default:
            vlog(klog.debug, "ignoring batch with type: {}", b.header().type);
            return ss::make_ready_future<>();
        }
    }

    bool is_group_blocked_verbose(
      kafka::group_id group_id, std::string_view skipped_msg) const {
        const auto& bim = static_cast<const Impl*>(this)->group_blocks();
        auto it = bim.find(group_id);
        if (unlikely(it != bim.end() && it->second.is_blocked)) {
            vlog(
              cg_klog.warn,
              "[group: {}] skipping {}, group is blocked",
              group_id,
              skipped_msg);
            return true;
        }
        return false;
    }

    void do_handle_group_block(kafka::group_block gb) {
        auto& bim = static_cast<Impl*>(this)->group_blocks();
        auto it = bim.find(gb.group_id);
        if (it == bim.end()) {
            bim.emplace(gb.group_id, gb.info);
            return;
        }
        if (gb.info.revision_id == model::revision_id{}) [[unlikely]] {
            vlog(
              cg_klog.debug,
              "applying a legacy group block {} over {} unconditionally",
              gb,
              it->second);
            it->second = gb.info;
            return;
        }
        if (it->second.revision_id > gb.info.revision_id) [[unlikely]] {
            vlog(
              cg_klog.warn,
              "ignoring stale group block {}, as it already has newer block "
              "info {} ",
              gb,
              it->second);
            return;
        }
        if (
          it->second.revision_id == gb.info.revision_id
          && it->second.is_blocked != gb.info.is_blocked) [[unlikely]] {
            vlog(
              cg_klog.error,
              "ignoring invalid group block {} substituting existing {} ",
              gb,
              it->second);
            return;
        }
        it->second = gb.info;
    }

    using base_t = group_data_parser;

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

    ss::future<> handle_raft_data(const model::record_batch& b) {
        return static_cast<Impl*>(this)->handle_raft_data(b);
    }
    ss::future<> handle_tx_offsets(
      model::record_batch_header header,
      kafka::group_tx::offsets_metadata data) {
        if (is_group_blocked_verbose(data.group_id, "tx offsets")) {
            return ss::now();
        }
        return static_cast<Impl*>(this)->handle_tx_offsets(
          header, std::move(data));
    }
    ss::future<> handle_fence_v0(
      model::record_batch_header header,
      kafka::group_tx::fence_metadata_v0 data) {
        if (is_group_blocked_verbose(data.group_id, "fence v0")) {
            return ss::now();
        }
        return static_cast<Impl*>(this)->handle_fence_v0(
          header, std::move(data));
    }
    ss::future<> handle_fence_v1(
      model::record_batch_header header,
      kafka::group_tx::fence_metadata_v1 data) {
        if (is_group_blocked_verbose(data.group_id, "fence v1")) {
            return ss::now();
        }
        return static_cast<Impl*>(this)->handle_fence_v1(
          header, std::move(data));
    }
    ss::future<> handle_fence(
      model::record_batch_header header, kafka::group_tx::fence_metadata data) {
        if (is_group_blocked_verbose(data.group_id, "fence")) {
            return ss::now();
        }
        return static_cast<Impl*>(this)->handle_fence(header, std::move(data));
    }
    ss::future<> handle_abort(
      model::record_batch_header header, kafka::group_tx::abort_metadata data) {
        return static_cast<Impl*>(this)->handle_abort(header, std::move(data));
    }
    ss::future<> handle_commit(
      model::record_batch_header header,
      kafka::group_tx::commit_metadata data) {
        if (is_group_blocked_verbose(data.group_id, "commit")) {
            return ss::now();
        }
        return static_cast<Impl*>(this)->handle_commit(header, std::move(data));
    }
    ss::future<>
    handle_version_fence(features::feature_table::version_fence fence) {
        return static_cast<Impl*>(this)->handle_version_fence(fence);
    }
    ss::future<> handle_group_block(model::record_batch b) {
        co_await b.for_each_record_async(
          [that = static_cast<Impl*>(this)](model::record r) {
              return that->handle_group_block(group_block{std::move(r)});
          });
    }
};
} // namespace kafka
