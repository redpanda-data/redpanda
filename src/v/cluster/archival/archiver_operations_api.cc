/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/archival/archiver_operations_api.h"

namespace archival {

archiver_operations_api::~archiver_operations_api() {}

bool reconciled_upload_candidate::operator==(
  const reconciled_upload_candidate& o) const noexcept {
    return ntp == o.ntp && size_bytes == o.size_bytes && metadata == o.metadata;
}

std::ostream&
operator<<(std::ostream& o, const reconciled_upload_candidate& s) {
    fmt::print(
      o,
      "reconciled_upload_candidate({}, {}, {})",
      s.ntp,
      s.size_bytes,
      s.metadata);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const upload_candidate_search_parameters& s) {
    fmt::print(
      o,
      "upload_candidate_search_parameters(ntp={}, target_size={}, min_size={}, "
      "upload_size_quota={}, upload_requests_quota={}, compacted_reupload={}, "
      "inline_manifest={})",
      s.ntp,
      s.target_size,
      s.min_size,
      s.upload_size_quota,
      s.upload_requests_quota,
      s.compacted_reupload,
      s.inline_manifest);
    return o;
}

upload_candidate_search_parameters::upload_candidate_search_parameters(
  model::ntp ntp,
  model::term_id archiver_term,
  size_t target_size,
  size_t min_size,
  std::optional<size_t> upload_size_quota,
  std::optional<size_t> upload_requests_quota,
  bool compacted_reupload,
  bool inline_manifest)
  : ntp(std::move(ntp))
  , archiver_term(archiver_term)
  , target_size(target_size)
  , min_size(min_size)
  , upload_size_quota(upload_size_quota)
  , upload_requests_quota(upload_requests_quota)
  , compacted_reupload(compacted_reupload)
  , inline_manifest(inline_manifest) {}

bool reconciled_upload_candidates_list::operator==(
  const reconciled_upload_candidates_list& rhs) const {
    return read_write_fence == rhs.read_write_fence
           && std::equal(
             results.begin(),
             results.end(),
             rhs.results.begin(),
             [](
               const reconciled_upload_candidate_ptr& lhs,
               const reconciled_upload_candidate_ptr& rhs) {
                 return *lhs == *rhs;
             });
}

std::ostream&
operator<<(std::ostream& o, const reconciled_upload_candidates_list& r) {
    fmt::print(
      o,
      "reconciled_upload_candidates_list(num_results={}, "
      "read_write_fence={})",
      r.results.size(),
      r.read_write_fence);
    return o;
}

bool upload_results_list::operator==(
  const upload_results_list& o) const noexcept {
    return manifest_clean_offset == o.manifest_clean_offset && stats == o.stats
           && results == o.results && num_put_requests == o.num_put_requests
           && num_bytes_sent == o.num_bytes_sent;
}

std::ostream& operator<<(std::ostream& o, const upload_results_list& s) {
    std::stringstream str;
    for (auto r : s.results) {
        str << r << ' ';
    }
    fmt::print(
      o,
      "upload_results_list(results={}, clean_offset={}, "
      "read_write_fence={}, num_put_request={}, bytes_sent={})",
      str.str(),
      s.manifest_clean_offset,
      s.read_write_fence,
      s.num_put_requests,
      s.num_bytes_sent);
    return o;
}

std::ostream& operator<<(std::ostream& o, const admit_uploads_result& s) {
    fmt::print(
      "admit_uploads_result(num_succeeded={}, num_failed={}, "
      "dirty_offset={})",
      s.num_succeeded,
      s.num_failed,
      s.manifest_dirty_offset);
    return o;
}

std::ostream& operator<<(std::ostream& o, const manifest_upload_result& arg) {
    fmt::print(
      o,
      "manifest_upload_result(ntp={}, num_put_requests={}, "
      "size_bytes={})",
      arg.ntp,
      arg.num_put_requests,
      arg.size_bytes);
    return o;
}

} // namespace archival
