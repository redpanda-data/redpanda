#include "metrics_registry.h"

#include "base/vassert.h"

void metrics_registry::register_metric(
  const group_name_t& group_name,
  const metric_name_t& metric_name,
  const std::vector<ss::metrics::label>& non_aggregated_labels,
  const std::vector<ss::metrics::label>& aggregated_labels) {
    auto& group = _registry[group_name];
    auto [metric_info_iter, inserted] = group.try_emplace(metric_name);
    auto& metric_info = metric_info_iter->second;

    if (inserted) {
        metric_info.non_aggregated_labels = non_aggregated_labels;
        metric_info.aggregated_labels = aggregated_labels;
    }
#ifndef NDEBUG
    else {
        // seastar allows registering foo_bar{label="1"} and foo_bar{label="2"}
        // with different aggregation labels. However, the aggregation labels of
        // the later would be silently ignored.
        // Here we check that that isn't happening.
        auto label_comparer = [](const auto& lhs, const auto& rhs) {
            return lhs.name() == rhs.name();
        };

        vassert(
          std::equal(
            metric_info.non_aggregated_labels.begin(),
            metric_info.non_aggregated_labels.end(),
            non_aggregated_labels.begin(),
            non_aggregated_labels.end(),
            label_comparer),
          "Different non-aggregation labels specified for the same metric"
          "{} {}",
          group_name,
          metric_name);

        vassert(
          std::equal(
            metric_info.aggregated_labels.begin(),
            metric_info.aggregated_labels.end(),
            aggregated_labels.begin(),
            aggregated_labels.end(),
            label_comparer),
          "Different aggregation labels specified for the same metric group"
          "{} {}",
          group_name,
          metric_name);
    }
#endif
}

void metrics_registry::update_aggregation_labels(bool aggregate_metrics) {
    for (const auto& [group_name, group] : _registry) {
        for (const auto& [metric_name, metric_info] : group) {
            ss::metrics::update_aggregate_labels(
              group_name,
              metric_name,
              aggregate_metrics ? metric_info.aggregated_labels
                                : metric_info.non_aggregated_labels);
        }
    }
}

thread_local metrics_registry metrics_registry::
  _local_instance; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
