/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_storage/topic_mount_manifest.h"

#include "base/vlog.h"
#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

namespace {

struct topic_mount_manifest_state
  : public serde::envelope<
      topic_mount_manifest_state,
      serde::version<1>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(label, tp_ns, rev); }

    bool operator==(const topic_mount_manifest_state&) const = default;

    cloud_storage::remote_label label;
    model::topic_namespace tp_ns;
    model::initial_revision_id rev;
};

} // namespace

namespace cloud_storage {

topic_mount_manifest::topic_mount_manifest(
  const remote_label& label,
  const model::topic_namespace& tp_ns,
  model::initial_revision_id rev)
  : _source_label(label)
  , _tp_ns(tp_ns)
  , _rev(rev) {}

ss::future<> topic_mount_manifest::update(ss::input_stream<char> is) {
    iobuf result;
    auto os = make_iobuf_ref_output_stream(result);
    co_await ss::copy(is, os).finally([&is, &os]() mutable {
        return is.close().finally([&os]() mutable { return os.close(); });
    });
    from_iobuf(std::move(result));
}

void topic_mount_manifest::from_iobuf(iobuf in) {
    auto m_state = serde::from_iobuf<topic_mount_manifest_state>(std::move(in));
    _source_label = std::move(m_state.label);
    _tp_ns = std::move(m_state.tp_ns);
    _rev = m_state.rev;
}

/// Serialize manifest object
///
/// \return asynchronous input_stream with the serialized json
ss::future<iobuf> topic_mount_manifest::serialize_buf() const {
    return ss::make_ready_future<iobuf>(
      serde::to_iobuf(topic_mount_manifest_state{
        .label = _source_label, .tp_ns = _tp_ns, .rev = _rev}));
}

/// Manifest object name in S3
remote_manifest_path topic_mount_manifest::get_manifest_path(
  const remote_path_provider& path_provider) const {
    return remote_manifest_path{path_provider.topic_mount_manifest_path(*this)};
}

manifest_type topic_mount_manifest::get_manifest_type() const {
    return manifest_type::topic_mount;
};

} // namespace cloud_storage
