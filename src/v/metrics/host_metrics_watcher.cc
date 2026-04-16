#include "host_metrics_watcher.h"

#include "base/vlog.h"
#include "config/configuration.h"
#include "utils/device_utils.h"

#include <seastar/core/io_queue.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/posix.hh>
#include <seastar/core/reactor.hh>

#include <sys/stat.h>

#include <cerrno>
#include <exception>
#include <ranges>
#include <system_error>
#include <unordered_map>

// Device label conventions used for diskstats metrics:
//
//   disk       - The block device associated with the series. For the default
//                series these are generally partitions, while for the
//                _underlying series, these are a guess at the underlying disk.
//   underlying - For series which are associated the nearest block device
//                (e.g., the partition), this is the best guess at the
//                underlying disk. For example, if the disk is nvme0n1p1,
//                underlying is nvme0n1.
//   mountpoint - mountpoint identifying the specific Seastar io_queue,
//                with the semantics as Seastar's mountpoint label on its own
//                io_queue metrics.
//                This is the mountpoint path specified in io-properties; which
//                is not necessarily a "mount point" at all but may be any path
//                at all. Empty mountpoint is the default io_queue (id=0).
//   data_disk  - "1" if the series is associated with the device that contains
//                the data directory, "0" otherwise
//   cache_disk - "1" if the series is associated with the device that contains
//                the cache directory, "0" otherwise
//
namespace metrics {

// A /proc/diskstats entry we want to monitor, with role flags.
struct diskstats_entry {
    ss::sstring name;       // device name as it appears in /proc/diskstats
    ss::sstring underlying; // best-guess underlying disk, empty if unknown
    dev_t dev_id{0};        // device ID from stat(), 0 if unresolved
    bool is_data;
    bool is_cache;
};

// Result of resolve_monitored_devices: deduped partition and disk entries,
// plus the union of their names for filtering /proc/diskstats.
struct resolved_devices {
    std::vector<diskstats_entry> partitions;
    std::vector<diskstats_entry> disks;
    std::unordered_set<ss::sstring> filter;

    // Per-directory resolution results (empty string if resolution failed)
    ss::sstring data_device;
    ss::sstring cache_device;

    // Filesystem device IDs (st_dev) for data/cache directories, used to look
    // up Seastar IO queues. 0 if resolution failed.
    dev_t data_dev_id{0};
    dev_t cache_dev_id{0};
};

namespace {

// sectors in diskstats are always 512-byte units
// https://www.kernel.org/doc/Documentation/block/stat.txt
constexpr uint64_t bytes_per_sector = 512;

struct diskstats_field {
    ss::sstring name;
    // If present, means "name" is in sectors, expose an additional metric
    // converted to bytes for convenience
    std::optional<ss::sstring> bytes_name;
};

// NOLINTBEGIN(cppcoreguidelines-prefer-member-initializer,modernize-use-designated-initializers)
static const std::
  array<diskstats_field, host_metrics_watcher::diskstats_field_count>
    diskstats_fields{{
      {"reads"},                                // 0
      {"reads_merged"},                         // 1
      {"sectors_read", "bytes_read"},           // 2
      {"reads_ms"},                             // 3
      {"writes"},                               // 4
      {"writes_merged"},                        // 5
      {"sectors_written", "bytes_written"},     // 6
      {"writes_ms"},                            // 7
      {"io_in_progress"},                       // 8
      {"io_ms"},                                // 9
      {"io_weighted_ms"},                       // 10
      {"discards"},                             // 11
      {"discards_merged"},                      // 12
      {"sectors_discarded", "bytes_discarded"}, // 13
      {"discards_ms"},                          // 14
      {"flushes"},                              // 15
      {"flushes_ms"},                           // 16
    }};
// NOLINTEND(cppcoreguidelines-prefer-member-initializer,modernize-use-designated-initializers)
} // namespace

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

host_metrics_watcher::host_metrics_watcher(
  ss::logger& log,
  const ss::sstring& data_directory,
  const ss::sstring& cache_directory)
  : _logger(log) {
    if (
      config::shard_local_cfg().disable_metrics()
      || !config::shard_local_cfg().enable_host_metrics()) {
        return;
    }

    vlog(
      log.info,
      "Setting up host metrics exporter, data_directory: {}, cache_directory: "
      "{}",
      data_directory,
      cache_directory);

    auto devices = resolve_monitored_devices(data_directory, cache_directory);
    _monitored_devices = devices.filter;

    setup_parser(
      _logger,
      _diskstats,
      "/proc/diskstats",
      [this] { maybe_refresh_diskstats(); },
      [this, devs = devices] { setup_diskstats_metrics(devs); });

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

    setup_io_queue_config_metrics(
      devices.partitions, data_directory, cache_directory);
    setup_info_metric(devices);
}

void host_metrics_watcher::setup_diskstats_metrics(
  const resolved_devices& devices) {
    // Register metrics for the given "interesting" devices, or else fall
    // back to all.
    if (!devices.partitions.empty() || !devices.disks.empty()) {
        // The data and/or cache directories are resolved successfully.
        // Monitor both partition-level (normal names)
        // and whole-disk (underlying_ prefix). Otherwise fall back to all
        // devices.
        for (const auto& entry : devices.partitions) {
            setup_diskstats_for_entry(entry, "disk", "");
        }
        for (const auto& entry : devices.disks) {
            setup_diskstats_for_entry(entry, "disk", "underlying_");
        }
    } else {
        // Could not resolve any devices (e.g. overlay/tmpfs filesystem).
        // Fall back to unfiltered monitoring without role labels.
        vlog(
          _logger.info,
          "No devices resolved, monitoring all {} devices from diskstats",
          _diskstats.stats.size());
        for (const auto& [diskname, _] : _diskstats.stats) {
            diskstats_entry entry{
              .name = ss::sstring(diskname),
              .underlying = "",
              .is_data = false,
              .is_cache = false};
            setup_diskstats_for_entry(entry, "disk", "");
        }
    }
}

void host_metrics_watcher::setup_diskstats_for_entry(
  const diskstats_entry& entry,
  std::string_view label_name,
  std::string_view metric_prefix) {
    // Register gauges for one /proc/diskstats device. label_name controls
    // whether the series is keyed by "device" (partition) or "disk" (whole
    // disk), and metric_prefix prepends e.g. "underlying_" to metric names.
    const auto& stats = _diskstats.stats[entry.name];

    vlog(
      _logger.info,
      "Setting up diskstats metrics for '{}' ({}label, prefix='{}', "
      "data_disk={}, cache_disk={})",
      entry.name,
      label_name,
      metric_prefix,
      entry.is_data,
      entry.is_cache);

    auto device_or_disk_label = seastar::metrics::label(
      ss::sstring(label_name));
    auto data_disk_label = seastar::metrics::label("data_disk");
    auto cache_disk_label = seastar::metrics::label("cache_disk");
    auto underlying_label = seastar::metrics::label("underlying");
    std::vector<seastar::metrics::label_instance> labels = {
      device_or_disk_label(entry.name),
      data_disk_label(entry.is_data ? "1" : "0"),
      cache_disk_label(entry.is_cache ? "1" : "0"),
    };
    if (!entry.underlying.empty()) {
        labels.push_back(underlying_label(entry.underlying));
    }

    for (size_t i = 0; i < stats.size() && i < diskstats_fields.size(); i++) {
        const auto& field = diskstats_fields[i];
        auto name = fmt::format("{}{}", metric_prefix, field.name);
        _metrics.add_group(
          "host_diskstats",
          {
            seastar::metrics::make_gauge(
              name,
              [this, &stats, i] {
                  maybe_refresh_diskstats();
                  return stats[i];
              },
              seastar::metrics::description(
                fmt::format("Host diskstat {}", name)),
              labels),
          });

        if (field.bytes_name) {
            auto bytes_name = fmt::format(
              "{}{}", metric_prefix, *field.bytes_name);
            _metrics.add_group(
              "host_diskstats",
              {
                seastar::metrics::make_gauge(
                  bytes_name,
                  [this, &stats, i] {
                      maybe_refresh_diskstats();
                      return stats[i] * bytes_per_sector;
                  },
                  seastar::metrics::description(
                    fmt::format("Host diskstat {} (converted to bytes)", name)),
                  labels),
              });
        }
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
  std::string_view diskstats_lines,
  diskstats_map& diskstats,
  const std::unordered_set<ss::sstring>& monitored_devices) {
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

        // Filter by monitored devices if specified
        if (
          !monitored_devices.empty() && !monitored_devices.contains(diskname)) {
            continue;
        }

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

namespace {

// Blocking read of an entire /proc file into a string.
//
// We use blocking reads because you can't do dma_reads into /proc.
// Reading from /proc should never block so this should be ~fine~.
// From tracing the calls take less than 100us generally.
//
// /proc seq_files may return partial results from a single pread,
// so we loop until we get everything.
std::string read_proc_file(ss::file_desc& fd) {
    static constexpr size_t chunk_size = 4096;
    std::string result;

    for (;;) {
        auto prev_size = result.size();
        result.resize(prev_size + chunk_size);
        auto bytes_read = fd.pread(
          result.data() + prev_size, chunk_size, prev_size);
        result.resize(prev_size + bytes_read);
        if (bytes_read == 0) {
            break;
        }
    }

    return result;
}

} // namespace

template<typename StatsWrapper, typename ParseF>
void refresh_stats(StatsWrapper& stats, ss::logger& logger, ParseF parsef) {
    if (stats.errored) {
        return;
    }

    if (stats.last_read + std::chrono::seconds(5) > ss::lowres_clock::now()) {
        return;
    }

    try {
        auto lines = read_proc_file(*stats.fd);

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
        vlog(
          _logger.trace,
          "Refreshing diskstats, read {} bytes, monitored_devices: [{}]",
          stat_lines.size(),
          fmt::join(_monitored_devices, ", "));
        parse_diskstats(stat_lines, _diskstats.stats, _monitored_devices);
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

resolved_devices host_metrics_watcher::resolve_monitored_devices(
  const ss::sstring& data_directory, const ss::sstring& cache_directory) {
    // Map data and cache directory paths to their partition and whole-disk
    // device names. Returns deduped entry lists and a filter set for
    // /proc/diskstats parsing.
    resolved_devices result;

    struct resolved {
        ss::sstring partition;
        ss::sstring disk;
        dev_t dev_id;
    };

    auto resolve_one = [this](
                         std::string_view label,
                         const ss::sstring& dir) -> std::optional<resolved> {
        try {
            auto device = utils::device_resolver::device_for_path(dir);
            auto disk = utils::device_resolver::get_base_device(device.name);
            vlog(
              _logger.info,
              "Resolved {} '{}' to device '{}' (underlying disk '{}')",
              label,
              dir,
              device.name,
              disk);
            return resolved{
              .partition = std::move(device.name),
              .disk = std::move(disk),
              .dev_id = device.dev_id};
        } catch (const std::exception& e) {
            vlog(
              _logger.warn,
              "Failed to resolve {} '{}' to a block device: {}. "
              "Skipping diskstats monitoring for this path.",
              label,
              dir,
              e.what());
            return std::nullopt;
        }
    };

    auto data = resolve_one("data directory", data_directory);
    auto cache = resolve_one("cache directory", cache_directory);

    // Helper: insert or merge into a deduped entry list and filter set
    auto add_entry = [&result](
                       std::vector<diskstats_entry>& entries,
                       const ss::sstring& name,
                       const ss::sstring& underlying,
                       dev_t dev_id,
                       bool is_data,
                       bool is_cache) {
        for (auto& e : entries) {
            if (e.name == name) {
                e.is_data |= is_data;
                e.is_cache |= is_cache;
                return;
            }
        }
        entries.push_back(
          diskstats_entry{
            .name = name,
            .underlying = underlying,
            .dev_id = dev_id,
            .is_data = is_data,
            .is_cache = is_cache});
        result.filter.insert(name);
    };

    if (data) {
        result.data_device = data->partition;
        result.data_dev_id = data->dev_id;
        add_entry(
          result.partitions,
          data->partition,
          data->disk,
          data->dev_id,
          true,
          false);
        add_entry(result.disks, data->disk, {}, 0, true, false);
    }
    if (cache) {
        result.cache_device = cache->partition;
        result.cache_dev_id = cache->dev_id;
        add_entry(
          result.partitions,
          cache->partition,
          cache->disk,
          cache->dev_id,
          false,
          true);
        add_entry(result.disks, cache->disk, {}, 0, false, true);
    }

    if (!result.filter.empty()) {
        vlog(
          _logger.info,
          "Monitoring diskstats for devices: {}",
          fmt::join(result.filter, ", "));
    }

    return result;
}

// SPLIT_MARKER_IOQUEUE_START
void host_metrics_watcher::setup_io_queue_config_metrics(
  const std::vector<diskstats_entry>& partition_entries,
  const ss::sstring& data_directory,
  const ss::sstring& cache_directory) {
    // Export Seastar IO queue configuration (iotune bandwidth/IOPS) as metrics.
    // Deduplicates by queue identity: multiple partition entries may share one
    // Seastar IO queue (e.g. when no io-properties is configured), so we emit
    // one metric series per queue with merged data_disk/cache_disk flags.
    // Build a unified list of (dev_id, device label, disk label, role flags)
    // for IO queue lookup. When partition entries are available, dev_id was
    // resolved at startup. Otherwise fall back to stat'ing the directory paths.
    struct lookup_entry {
        dev_t dev_id;
        ss::sstring name;   // human-readable name for logging
        ss::sstring device; // device label (empty in fallback)
        ss::sstring disk;   // disk label (empty in fallback)
        bool is_data;
        bool is_cache;
    };

    std::vector<lookup_entry> lookups;

    if (!partition_entries.empty()) {
        for (const auto& entry : partition_entries) {
            lookups.push_back({
              .dev_id = entry.dev_id,
              .name = entry.name,
              .device = entry.name,
              .disk = entry.underlying,
              .is_data = entry.is_data,
              .is_cache = entry.is_cache,
            });
        }
    } else {
        // Device resolution failed — fall back to looking up IO queues
        // directly from the data/cache directory paths. We won't have
        // device or disk labels, but can still export the queue config.
        auto try_stat_dir =
          [&](const ss::sstring& dir, bool is_data, bool is_cache) {
              struct stat st{};
              if (stat(dir.c_str(), &st) != 0) {
                  vlog(
                    _logger.warn,
                    "Could not stat directory {} for IO queue metrics: {}",
                    dir,
                    std::system_category().message(errno));
                  return;
              }
              lookups.push_back({
                .dev_id = st.st_dev,
                .name = dir,
                .is_data = is_data,
                .is_cache = is_cache,
              });
          };
        try_stat_dir(data_directory, true, false);
        try_stat_dir(cache_directory, false, true);
    }

    struct queue_info {
        ss::io_queue* queue;
        ss::sstring device;
        ss::sstring disk;
        bool is_data{false};
        bool is_cache{false};
    };

    std::vector<queue_info> queues;

    for (const auto& entry : lookups) {
        try {
            auto& queue = ss::engine().get_io_queue(entry.dev_id);

            auto it = std::ranges::find_if(
              queues, [&](const auto& q) { return q.queue == &queue; });
            if (it != queues.end()) {
                it->is_data |= entry.is_data;
                it->is_cache |= entry.is_cache;
            } else {
                queues.emplace_back(
                  &queue,
                  entry.device,
                  entry.disk,
                  entry.is_data,
                  entry.is_cache);
            }
        } catch (...) {
            vlog(
              _logger.warn,
              "Failed to lookup IO queue for {}: {}",
              entry.name,
              std::current_exception());
        }
    }

    for (const auto& qi : queues) {
        auto cfg = qi.queue->get_config();

        vlog(
          _logger.info,
          "Setting up IO queue config metrics for device '{}' "
          "(disk='{}', mountpoint='{}', id={}, data_disk={}, "
          "cache_disk={})",
          qi.device,
          qi.disk,
          qi.queue->mountpoint(),
          cfg.id,
          qi.is_data ? 1 : 0,
          qi.is_cache ? 1 : 0);

        auto disk_label = seastar::metrics::label("disk");
        auto device_label = seastar::metrics::label("device");
        auto mountpoint_label = seastar::metrics::label("mountpoint");
        auto data_disk_label = seastar::metrics::label("data_disk");
        auto cache_disk_label = seastar::metrics::label("cache_disk");
        auto id_label = seastar::metrics::label("id");

        const std::vector<seastar::metrics::label_instance> labels = {
          disk_label(qi.disk),
          device_label(qi.device),
          mountpoint_label(qi.queue->mountpoint()),
          data_disk_label(qi.is_data ? "1" : "0"),
          cache_disk_label(qi.is_cache ? "1" : "0"),
          id_label(fmt::format("{}", cfg.id)),
        };

        _metrics.add_group(
          "io_queue_config",
          {
            seastar::metrics::make_gauge(
              "read_bytes_rate",
              [v = cfg.read_bytes_rate] { return v; },
              seastar::metrics::description(
                "Configured read bandwidth from io-properties (bytes/sec)"),
              labels),

            seastar::metrics::make_gauge(
              "write_bytes_rate",
              [v = cfg.write_bytes_rate] { return v; },
              seastar::metrics::description(
                "Configured write bandwidth from io-properties (bytes/sec)"),
              labels),

            seastar::metrics::make_gauge(
              "read_req_rate",
              [v = cfg.read_req_rate] { return v; },
              seastar::metrics::description(
                "Configured read IOPS from io-properties"),
              labels),

            seastar::metrics::make_gauge(
              "write_req_rate",
              [v = cfg.write_req_rate] { return v; },
              seastar::metrics::description(
                "Configured write IOPS from io-properties"),
              labels),

            seastar::metrics::make_gauge(
              "max_cost_function",
              [v = cfg.max_cost_function] { return v ? 1 : 0; },
              seastar::metrics::description(
                "IO cost function mode: 1 = max(iops, throughput), "
                "0 = sum(iops, throughput)"),
              labels),

            seastar::metrics::make_gauge(
              "duplex",
              [v = cfg.duplex] { return v ? 1 : 0; },
              seastar::metrics::description(
                "IO scheduler duplex mode: 0 = half-duplex, 1 = duplex"),
              labels),
          });
    }
}

void host_metrics_watcher::setup_info_metric(const resolved_devices& devices) {
    // Constant gauge (always 1) with labels describing the host metrics
    // configuration. Useful for joining with other metrics in dashboards.
    auto data_resolved = !devices.data_device.empty();
    auto cache_resolved = !devices.cache_device.empty();
    auto same_partition = data_resolved && cache_resolved
                          && devices.data_device == devices.cache_device;

    // Check if the data dir has a non-default IO queue (id > 0 means
    // explicit io-properties were provided for this device).
    bool data_has_io_queue = false;
    if (data_resolved) {
        try {
            auto& queue = ss::engine().get_io_queue(devices.data_dev_id);
            data_has_io_queue = queue.get_config().id > 0;
        } catch (...) {
            // No IO queue for this device — leave as false
        }
    }

    auto lbl = [](const char* name) { return seastar::metrics::label(name); };

    const std::vector<seastar::metrics::label_instance> labels = {
      lbl("data_device")(data_resolved ? devices.data_device : ""),
      lbl("cache_device")(cache_resolved ? devices.cache_device : ""),
      lbl("data_resolved")(data_resolved ? "1" : "0"),
      lbl("cache_resolved")(cache_resolved ? "1" : "0"),
      lbl("same_partition")(same_partition ? "1" : "0"),
      lbl("data_has_io_queue")(data_has_io_queue ? "1" : "0"),
    };

    _metrics.add_group(
      "host_metrics",
      {seastar::metrics::make_gauge(
        "info",
        [] { return 1; },
        seastar::metrics::description(
          "Host metrics configuration info (constant 1)"),
        labels)});
}

} // namespace metrics
