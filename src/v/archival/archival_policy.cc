/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/archival_policy.h"

#include "storage/disk_log_impl.h"
#include "storage/segment.h"
#include "storage/segment_set.h"

namespace archival {

class arch_policy_non_compacted_impl {
public:
    arch_policy_non_compacted_impl(
      model::ntp ntp, const model::revision_id& rev)
      : _ntp(std::move(ntp))
      , _rev(rev) {}

    std::optional<manifest> make_local_manifest(storage::log_manager& lm) {
        manifest tmp(_ntp, _rev);
        std::optional<storage::log> log = lm.get(_ntp);
        if (!log) {
            return std::nullopt;
        }
        auto plog = dynamic_cast<storage::disk_log_impl*>(log->get_impl());
        if (plog == nullptr) {
            return std::nullopt;
        }
        for (const auto& segment : plog->segments()) {
            bool closed = !segment->has_appender();
            bool compacted = segment->is_compacted_segment();
            if (!closed || compacted) {
                continue;
            }
            manifest::segment_meta meta{
              .is_compacted = segment->is_compacted_segment(),
              .size_bytes = segment->size_bytes(),
              .base_offset = segment->offsets().base_offset,
              .committed_offset = segment->offsets().committed_offset,
              .is_deleted_locally = false,
            };
            auto path = std::filesystem::path(segment->reader().filename());
            auto seg_name = path.filename().string();
            // generate segment path
            tmp.add(segment_name(seg_name), meta);
        }
        return tmp;
    }

private:
    model::ntp _ntp;
    model::revision_id _rev;
};

template<class PolicyImpl>
class manifest_based_policy final
  : public PolicyImpl
  , public archival_policy_base {
public:
    manifest_based_policy(const model::ntp& ntp, const model::revision_id& rev)
      : PolicyImpl(ntp, rev) {}

    std::optional<manifest> generate_upload_set(
      const manifest& remote, storage::log_manager& lm) final {
        auto local = PolicyImpl::make_local_manifest(lm);
        if (!local) {
            return local;
        }
        return local->difference(remote);
    }

    std::optional<manifest> generate_delete_set(
      const manifest& remote, storage::log_manager& lm) final {
        auto local = PolicyImpl::make_local_manifest(lm);
        if (!local) {
            return local;
        }
        return remote.difference(*local);
    }
};

using manifest_based_policy_non_compacted
  = manifest_based_policy<arch_policy_non_compacted_impl>;

std::unique_ptr<archival_policy_base> make_archival_policy(
  [[maybe_unused]] upload_policy_selector e,
  [[maybe_unused]] delete_policy_selector d,
  model::ntp ntp,
  model::revision_id rev) {
    return std::make_unique<manifest_based_policy_non_compacted>(ntp, rev);
}
} // namespace archival
