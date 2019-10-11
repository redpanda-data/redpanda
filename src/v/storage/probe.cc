#include "storage/probe.h"

#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace storage {
void probe::setup_metrics(const model::ntp& ntp) {
    namespace sm = metrics;
    auto ns_label = sm::label("namespace");
    auto topic_label = sm::label("topic");
    auto partition_label = sm::label("partition");
    std::vector<sm::label_instance> labels = {
      ns_label(ntp.ns()),
      topic_label(ntp.tp.topic()),
      partition_label(ntp.tp.partition()),
    };
    _metrics.add_group(
      prometheus_sanitize::metrics_name("storage:log"),
      {
        sm::make_total_bytes(
          "bytes_written",
          [this] { return _bytes_written; },
          sm::description("Total number of bytes written"),
          labels),
        sm::make_derive(
          "batches_written",
          [this] { return _batches_written; },
          sm::description("Total number of batches written"),
          labels),
        sm::make_total_bytes(
          "bytes_read",
          [this] { return _bytes_read; },
          sm::description("Total number of bytes read"),
          labels),
        sm::make_derive(
          "batches_read",
          [this] { return _batches_read; },
          sm::description("Total number of batches read"),
          labels),
        sm::make_derive(
          "log_segments_created",
          [this] { return _log_segments_created; },
          sm::description("Number of created log segments"),
          labels),
        sm::make_derive(
          "batch_parse_errors",
          [this] { return _batch_parse_errors; },
          sm::description("Number of batch parsing (reading) errors"),
          labels),
        sm::make_derive(
          "batch_write_errors",
          [this] { return _batch_write_errors; },
          sm::description("Number of batch write errors"),
          labels),
      });
}
} // namespace storage
