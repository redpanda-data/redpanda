#include "cluster/types.h"

#include <seastar/core/metrics_registration.hh>

namespace cluster {

class probed_topic_metadata {
public:
    probed_topic_metadata(topic_metadata md) noexcept;
    probed_topic_metadata(probed_topic_metadata&&) noexcept;

    bool is_topic_replicable() const;
    model::revision_id get_revision() const;

    std::optional<model::initial_revision_id> get_remote_revision() const;
    const model::topic& get_source_topic() const;

    const topic_configuration& get_configuration() const;
    topic_configuration& get_configuration();

    const assignments_set& get_assignments() const;
    assignments_set& get_assignments();

    topic_metadata& get_topic_metadata();
    const topic_metadata& get_topic_metadata() const;

private:
    void setup_metrics();

private:
    topic_metadata _topic_metadata;
    ss::metrics::metric_groups _public_metrics;
};

} // namespace cluster
