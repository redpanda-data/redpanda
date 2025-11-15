#include "host_metrics_watcher.h"

#include "base/vlog.h"
#include "config/configuration.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/posix.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/later.hh>

#include <exception>
#include <ranges>
#include <unordered_map>

namespace metrics {

namespace {
static const std::
  array<ss::sstring, host_metrics_watcher::diskstats_field_count>
    diskstats_fields{
      "reads",             // 0
      "reads_merged",      // 1
      "sectors_read",      // 2
      "reads_ms",          // 3
      "writes",            // 4
      "writes_merged",     // 5
      "sectors_written",   // 6
      "writes_ms",         // 7
      "io_in_progress",    // 8
      "io_ms",             // 9
      "io_weighted_ms",    // 10
      "discards",          // 11
      "discards_merged",   // 12
      "sectors_discarded", // 13
      "discards_ms",       // 14
      "flushes",           // 15
      "flushes_ms"         // 16
    };
}

template<typename StatsWrapper, typename RefreshF, typename MetricsF>
void setup_parser(
  ss::logger& logger,
  StatsWrapper& stats,
  ss::sstring filename,
  RefreshF refresh,
  MetricsF setup_metrics) {
    try {
        stats.fd = ss::file_desc::open(filename, O_RDONLY);

        refresh();

        if (stats.errored) {
            return;
        }

        setup_metrics();
    } catch (...) {
        stats.errored = true;
        vlog(
          logger.error,
          "Error opening {} file: {}",
          filename,
          std::current_exception());
    }
}

host_metrics_watcher::host_metrics_watcher(ss::logger& log)
  : _logger(log) {
    if (!config::shard_local_cfg().enable_host_metrics()) {
        return;
    }

    vlog(log.info, "Setting up host metrics exporter");

    setup_parser(
      _logger,
      _diskstats,
      "/proc/diskstats",
      [this] { maybe_refresh_diskstats(); },
      [this] {
          // if more disks get added after startup we will be missing them.
          // Unlikely to happen.
          for (const auto& [diskname, _] : _diskstats.stats) {
              setup_metrics_for_disk(diskname);
          }
      });

    setup_parser(
      _logger,
      _netstats,
      "/proc/net/netstat",
      [this]() { maybe_refresh_netstat(); },
      [this]() { setup_netstat_metrics(); });

    setup_parser(
      _logger,
      _snmp_stats,
      "/proc/net/snmp",
      [this]() { maybe_refresh_snmp(); },
      [this]() { setup_snmp_metrics(); });
}

void host_metrics_watcher::setup_metrics_for_disk(const std::string& diskname) {
    const auto& stats = _diskstats.stats[diskname];

    auto disk_label = seastar::metrics::label("disk");
    const std::vector<seastar::metrics::label_instance> labels = {
      disk_label(diskname),
    };

    for (size_t i = 0; i < stats.size() && i < diskstats_fields.size(); i++) {
        _metrics.add_group(
          "host_diskstats",
          {
            seastar::metrics::make_gauge(
              diskstats_fields[i],
              [this, &stats, i] {
                  maybe_refresh_diskstats();
                  return stats[i];
              },
              seastar::metrics::description(
                fmt::format("Host diskstat {}", diskstats_fields[i])),
              labels),
          });
    }
}

void host_metrics_watcher::setup_netstat_metrics() {
    _metrics.add_group(
      "host_netstat",
      {
        seastar::metrics::make_counter(
          "bytes_received",
          [this] {
              maybe_refresh_netstat();
              return _netstats.stats.bytes_received;
          },
          seastar::metrics::description("Host IP bytes received")),
        seastar::metrics::make_counter(
          "bytes_sent",
          [this] {
              maybe_refresh_netstat();
              return _netstats.stats.bytes_sent;
          },
          seastar::metrics::description("Host IP bytes sent")),
      });
}

void host_metrics_watcher::setup_snmp_metrics() {
    _metrics.add_group(
      "host_snmp",
      {
        seastar::metrics::make_counter(
          "packets_received",
          [this] {
              maybe_refresh_snmp();
              return _snmp_stats.stats.packets_received;
          },
          seastar::metrics::description("Host IP packets received")),
        seastar::metrics::make_counter(
          "packets_sent",
          [this] {
              maybe_refresh_snmp();
              return _snmp_stats.stats.packets_sent;
          },
          seastar::metrics::description("Host IP packets sent")),
        seastar::metrics::make_gauge(
          "tcp_established",
          [this] {
              maybe_refresh_snmp();
              return _snmp_stats.stats.tcp_established;
          },
          seastar::metrics::description("Host TCP established connections")),
      });
}

void host_metrics_watcher::parse_diskstats(
  std::string_view diskstats_lines, diskstats_map& diskstats) {
    for (auto diskline : std::views::split(diskstats_lines, '\n')) {
        auto fields_view = std::views::split(diskline, ' ')
                           | std::views::filter(
                             [](const auto& s) { return !s.empty(); });
        auto fields = std::ranges::to<std::vector<std::string>>(fields_view);

        if (fields.size() < 14) {
            // min amount of fields is 14 in linux < 4.18
            // abort if not enough fields, something is wrong
            continue;
        }

        auto diskname = fields[2];

        // skip major number, minor number and diskname
        auto stat_fields = fields | std::views::drop(3);

        for (auto [stats, new_stats] :
             std::views::zip(diskstats[diskname], stat_fields)) {
            stats = std::stoull(new_stats);
        }
    }
}

std::unordered_map<ss::sstring, std::unordered_map<ss::sstring, uint64_t>>
parse_net_like(std::string_view netstat_lines) {
    // There doesn't seem to be good documentation around the netstat files and
    // less clear whether they are stable so we are parse them in a bit more
    // generic fashion.

    // section -> fields -> values
    std::unordered_map<ss::sstring, std::unordered_map<ss::sstring, uint64_t>>
      netstat_map;

    ss::sstring last_section;
    std::vector<ss::sstring> last_fields;

    // Files are basically like this:
    // Foo: Header1 Header2 Header3
    // Foo: Value1 Value2 Value3
    // So we parse by going over each line and treat first line as headers and
    // then next as values
    for (auto line : std::views::split(netstat_lines, '\n')) {
        auto fields_view = std::views::split(line, ' ')
                           | std::views::filter(
                             [](const auto& s) { return !s.empty(); });
        auto fields = std::ranges::to<std::vector<std::string>>(fields_view);

        if (fields.size() < 2) {
            // Bogus: Need at least two columns (section and one header/value)
            break;
        }

        if (last_section.empty()) {
            // header line now
            last_section = fields[0];
            auto header_fields = fields | std::views::drop(1);
            last_fields = std::ranges::to<std::vector<ss::sstring>>(
              header_fields);
        } else if (fields[0] != last_section) {
            // bogus: section changed without values line
            break;
        } else {
            // Parse values
            auto stat_fields = fields | std::views::drop(1);
            for (auto [field_name, value] :
                 std::views::zip(last_fields, stat_fields)) {
                netstat_map[fields[0]][field_name] = std::stoull(value);
            }
            last_section = "";
            last_fields.clear();
        }
    }

    return netstat_map;
}

// snmp stats are defined here: https://www.rfc-editor.org/rfc/rfc1213
// For the Linux extensions I couldn't find a proper documentation.

void host_metrics_watcher::parse_netstat(
  std::string_view netstat_lines, netstat_stats& net_stats) {
    auto netstat_map = parse_net_like(netstat_lines);
    net_stats.bytes_received = netstat_map["IpExt:"]["InOctets"];
    net_stats.bytes_sent = netstat_map["IpExt:"]["OutOctets"];
}

void host_metrics_watcher::parse_snmp(
  std::string_view snmp_lines, snmp_stats& snmp_stats) {
    auto snmp_map = parse_net_like(snmp_lines);
    snmp_stats.packets_received = snmp_map["Ip:"]["InReceives"];
    snmp_stats.packets_sent = snmp_map["Ip:"]["OutRequests"];
    snmp_stats.tcp_established = snmp_map["Tcp:"]["CurrEstab"];
}

template<typename StatsWrapper, typename ParseF>
void refresh_stats(StatsWrapper& stats, ss::logger& logger, ParseF parsef) {
    if (stats.errored) {
        return;
    }

    if (stats.last_read + std::chrono::seconds(5) > ss::lowres_clock::now()) {
        return;
    }

    try {
        // These files aren't really big (2KiB max) so we just use a single
        // buffer and don't dynamically read the full thing.
        std::array<char, 16384> read_buffer;

        // We are doing a blocking read here this is because you can't do
        // dma_reads into /proc. Reading from /proc should never block so this
        // should be ~fine~. From tracing the calls take less than 100us
        // generally.
        auto bytes_read = stats.fd.value().pread(
          read_buffer.data(), read_buffer.size(), 0);
        auto lines = std::string_view(read_buffer.data(), bytes_read);

        parsef(lines);

        stats.last_read = ss::lowres_clock::now();
    } catch (...) {
        stats.errored = true;
        vlog(
          logger.error,
          "Error reading diskstats: {}",
          std::current_exception());
    }
}

void host_metrics_watcher::maybe_refresh_diskstats() {
    refresh_stats(_diskstats, _logger, [this](const auto& stat_lines) {
        parse_diskstats(stat_lines, _diskstats.stats);
    });
}

void host_metrics_watcher::maybe_refresh_netstat() {
    refresh_stats(_netstats, _logger, [this](const auto& stat_lines) {
        parse_netstat(stat_lines, _netstats.stats);
    });
}

void host_metrics_watcher::maybe_refresh_snmp() {
    refresh_stats(_snmp_stats, _logger, [this](const auto& stat_lines) {
        parse_snmp(stat_lines, _snmp_stats.stats);
    });
}

} // namespace metrics
