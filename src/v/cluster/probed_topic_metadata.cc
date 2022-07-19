#include "cluster/probed_topic_metadata.h"

#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"
#include "ssx/metrics.h"

#include <seastar/core/metrics.hh>

namespace cluster {

probed_topic_metadata::probed_topic_metadata(topic_metadata md) noexcept
  : _topic_metadata(std::move(md))
  , _public_metrics(ssx::metrics::public_metrics_handle) {
    setup_metrics();
}

probed_topic_metadata::probed_topic_metadata(
  probed_topic_metadata&& other) noexcept
  : _topic_metadata(std::move(other._topic_metadata))
  , _public_metrics(std::move(other._public_metrics)) {
    setup_metrics();
}

bool probed_topic_metadata::is_topic_replicable() const {
    return _topic_metadata.is_topic_replicable();
}

model::revision_id probed_topic_metadata::get_revision() const {
    return _topic_metadata.get_revision();
}

std::optional<model::initial_revision_id>
probed_topic_metadata::get_remote_revision() const {
    return _topic_metadata.get_remote_revision();
}

const model::topic& probed_topic_metadata::get_source_topic() const {
    return _topic_metadata.get_source_topic();
}

const topic_configuration& probed_topic_metadata::get_configuration() const {
    return _topic_metadata.get_configuration();
}

topic_configuration& probed_topic_metadata::get_configuration() {
    return _topic_metadata.get_configuration();
}

const assignments_set& probed_topic_metadata::get_assignments() const {
    return _topic_metadata.get_assignments();
}

assignments_set& probed_topic_metadata::get_assignments() {
    return _topic_metadata.get_assignments();
}

topic_metadata& probed_topic_metadata::get_topic_metadata() {
    return _topic_metadata;
}

const topic_metadata& probed_topic_metadata::get_topic_metadata() const {
    return _topic_metadata;
}

void probed_topic_metadata::setup_metrics() {
    namespace sm = ss::metrics;

    if (ss::this_shard_id() != 0) {
        return;
    }

    if (config::shard_local_cfg().disable_public_metrics()) {
        return;
    }

    if (!is_topic_replicable()) {
        return;
    }

    _public_metrics.clear();

    auto tp_ns = _topic_metadata.get_configuration().tp_ns;
    std::vector<sm::label_instance> labels = {
      sm::label("rp_namespace")(tp_ns.ns()), sm::label("rp_topic")(tp_ns.tp())};

    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("kafka"),
      {
        sm::make_gauge(
          "replicas",
          [this] {
              return _topic_metadata.get_configuration().replication_factor;
          },
          sm::description("Number of configured replicas per topic"),
          labels)
          .aggregate({sm::shard_label}),
      });
}

} // namespace cluster
