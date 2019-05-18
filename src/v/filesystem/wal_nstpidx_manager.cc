#include "wal_nstpidx_manager.h"

#include <algorithm>
#include <ctime>
#include <utility>

#include <fmt/format.h>
#include <seastar/core/metrics.hh>
#include <seastar/core/reactor.hh>
#include <smf/human_bytes.h>
#include <smf/log.h>
#include <smf/random.h>

#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"
#include "ioutil/priority_manager.h"
#include "prometheus/prometheus_sanitize.h"

// filesystem
#include "wal_name_extractor_utils.h"
#include "wal_nstpidx_repair.h"
#include "wal_pretty_print_utils.h"

namespace std {
ostream &
operator<<(ostream &o, const ::seastar::lowres_system_clock::time_point &t) {
  auto time = seastar::lowres_system_clock::to_time_t(t);
  o << std::put_time(std::gmtime(&time), "%F %T %Z");
  return o;
}
}  // namespace std

struct offset_comparator {
  /// NOTE: make sure that the end is exclusive - use -1 from end
  bool
  operator()(const int64_t &offset,
             const std::unique_ptr<wal_reader_node> &yptr) const {
    return offset < (yptr->ending_epoch() - 1);
  }
  bool
  operator()(const std::unique_ptr<wal_reader_node> &yptr,
             const int64_t &offset) const {
    return (yptr->ending_epoch()) - 1 < offset;
  }
};

wal_nstpidx_manager::~wal_nstpidx_manager() { log_cleanup_timeout_.cancel(); }
wal_nstpidx_manager::wal_nstpidx_manager(
  wal_opts o, const wal_topic_create_request *create_props, wal_nstpidx id,
  seastar::sstring work_directory)
  : opts(std::move(o)), tprops(THROW_IFNULL(create_props)), idx(id),
    work_dir(std::move(work_directory)) {
  // set metrics
  namespace sm = seastar::metrics;
  _metrics.add_group(
    prometheus_sanitize::metrics_name("wal_nstpidx_manager::" + work_dir),
    {sm::make_derive("read_bytes", prometheus_stats_.read_bytes,
                     sm::description("bytes read from disk")),
     sm::make_derive("read_reqs", prometheus_stats_.read_reqs,
                     sm::description("Number of read requests")),
     sm::make_derive("write_reqs", prometheus_stats_.write_reqs,
                     sm::description("Number of writes requests")),
     sm::make_derive("write_bytes", prometheus_stats_.write_bytes,
                     sm::description("Number of bytes writen to disk")),
     sm::make_derive("log_segment_rolls", prometheus_stats_.log_segment_rolls,
                     sm::description("Number of times, we rolled the log"))});

  // set the cleanup callback
  log_cleanup_timeout_.set_callback(
    [this] { cleanup_timer_cb_log_segments(); });

  // add jitter of up to 10seconds
  smf::random r;
  log_cleanup_timeout_.arm_periodic(
    std::chrono::hours(1) + std::chrono::milliseconds(r.next() % 10000));
}

void
wal_nstpidx_manager::cleanup_timer_cb_log_segments() {
  std::vector<std::unique_ptr<wal_reader_node>> to_remove;
  auto now = seastar::lowres_system_clock::now();

  // must be signed
  int32_t nodes_size = _nodes.size();

  // start from the front for time based
  while (nodes_size-- > 0) {
    auto &n = _nodes.front();

    if (n->filename == _writer->filename()) {
      // skip active writer
      // we are at the end of the queue
      break;
    }

    auto node_max_time = n->modified_time() + opts.max_retention_period;
    if (node_max_time < now) {
      n->mark_for_deletion();
      auto hrs = std::chrono::duration_cast<std::chrono::hours>(
                   opts.max_retention_period)
                   .count();
      LOG_INFO("Marking node {} for deletion. Last modified time "
               "{} expired for policy > {}hours. Current time: {}",
               n->filename, n->modified_time(), hrs, now);
      to_remove.push_back(std::move(_nodes.front()));
      _nodes.pop_front();
    }
  }

  // start from the back for size based
  nodes_size = _nodes.size();
  int64_t max_bytes = opts.max_retention_size;
  while (opts.max_retention_size > 0 && nodes_size-- > 0) {
    auto &n = _nodes[nodes_size];
    if (n->filename == _writer->filename()) { continue; }
    max_bytes -= n->file_size();
    if (max_bytes <= 0) {
      LOG_INFO(
        "Size retention exceeded: {}. Marking following nodes as deleted",
        smf::human_bytes(opts.max_retention_size));
      for (auto i = 0; i <= nodes_size; ++i) {
        to_remove.push_back(std::move(_nodes.front()));
        _nodes.pop_front();
        LOG_INFO("Marking node {} as deleted. Size retention policy",
                 to_remove.back()->filename);
      }
    }
  }
  if (!to_remove.empty()) {
    // do in background
    seastar::do_with(std::move(to_remove), [](auto &vec) {
      return seastar::do_for_each(
        std::move_iterator(vec.begin()), std::move_iterator(vec.end()),
        [](std::unique_ptr<wal_reader_node> rdr) {
          auto p = rdr.get();
          p->mark_for_deletion();
          return p->close().finally([rdr = std::move(rdr)] {});
        });
    });
  }
}

wal_nstpidx_manager::wal_nstpidx_manager(wal_nstpidx_manager &&o) noexcept
  : opts(std::move(o.opts)), tprops(o.tprops), idx(std::move(o.idx)),
    work_dir(std::move(o.work_dir)), _writer(std::move(o._writer)),
    _nodes(std::move(o._nodes)),
    prometheus_stats_(std::move(o.prometheus_stats_)),
    _metrics(std::move(o._metrics)) {}

seastar::future<std::unique_ptr<wal_write_reply>>
wal_nstpidx_manager::append(wal_write_request r) {
  prometheus_stats_.write_reqs++;
  prometheus_stats_.write_bytes +=
    std::accumulate(r.begin(), r.end(), int64_t(0),
                    [](int64_t acc, const wal_binary_record *x) {
                      return acc + x->data()->size();
                    });
  return _writer->append(std::move(r));
}

seastar::future<std::unique_ptr<wal_read_reply>>
wal_nstpidx_manager::get(wal_read_request r) {
  using retval_t = std::unique_ptr<wal_read_reply>;

  prometheus_stats_.read_reqs++;
  retval_t retval = std::make_unique<wal_read_reply>(
    r.req->ns(), r.req->topic(), r.req->partition(), r.req->offset(),
    r.req->server_validate_payload());
  if (r.req->max_bytes() <= 0) {
    DLOG_WARN("Received request with zero max_bytes() to read");
    return seastar::make_ready_future<retval_t>(std::move(retval));
  }
  if (_nodes.empty()) {
    return seastar::make_ready_future<retval_t>(std::move(retval));
  }
  auto it = std::lower_bound(_nodes.begin(), _nodes.end(), retval->next_epoch(),
                             offset_comparator{});
  return seastar::do_with(
    std::move(retval), std::move(it), [this, r](auto &read_result, auto &it) {
      auto retval = read_result.get();
      return seastar::do_until(
               [this, &it, retval, max_bytes = r.req->max_bytes()] {
                 return it == _nodes.end() ||
                        retval->on_disk_size() >= max_bytes ||
                        retval->reply().error !=
                          wal_read_errno::wal_read_errno_none;
               },
               [this, &it, retval, r]() mutable {
                 auto ptr = it->get();
                 auto offset = retval->next_epoch();
                 if (offset < ptr->starting_epoch ||
                     offset >= ptr->ending_epoch()) {
                   it = _nodes.end();
                   return seastar::make_ready_future<>();
                 }
                 return ptr->get(retval, r)
                   .finally([&it]() mutable { std::next(it); })
                   .handle_exception([this, &it, r](auto eptr) {
                     LOG_ERROR("Exception: {} (Offset:{}, req size:{})", eptr,
                               r.req->offset(), r.req->max_bytes());
                     it = _nodes.end();
                   });
               })
        .then([this, &read_result] {
          if (read_result->empty()) {
            read_result->set_next_offset(std::max(
              read_result->reply().next_offset, _nodes.back()->starting_epoch));
          }
          DLOG_TRACE("READER size of retval->reply().gets.size(): {}",
                     read_result->reply().gets.size());
          prometheus_stats_.read_bytes += read_result->on_disk_size();
          return seastar::make_ready_future<retval_t>(std::move(read_result));
        });
    });
}

seastar::future<>
wal_nstpidx_manager::create_log_handle_hook(seastar::sstring filename) {
  prometheus_stats_.log_segment_rolls++;
  auto [starting_epoch, term] =
    wal_name_extractor_utils::wal_segment_extract_epoch_term(filename);
  LOG_THROW_IF(
    !_nodes.empty() && starting_epoch <= _nodes.back()->starting_epoch,
    "Found unordered nodes. Critical order violation. eval epoch: {}. "
    "last known begin epoch: {}. filename: {}",
    starting_epoch, _nodes.back()->starting_epoch, filename);
  auto n = std::make_unique<wal_reader_node>(
    starting_epoch, term, 0 /*size*/, seastar::lowres_system_clock::now(),
    filename);
  DLOG_DEBUG("Create WAL file: {} ", n->filename);
  _nodes.push_back(std::move(n));
  return _nodes.back()->open();
}
seastar::future<>
wal_nstpidx_manager::segment_size_change_hook(seastar::sstring name,
                                              int64_t sz) {
  DLOG_THROW_IF(_nodes.empty(),
                "Failed to update size. Unknown file: {}, size: {}", name, sz);
  DLOG_THROW_IF(_nodes.back()->filename != name,
                "Tried to update an our of order file: {}. Last node is: {}",
                name, _nodes.back()->filename);
  auto &reader = _nodes.back();
  if (reader->file_size() == sz) { return seastar::make_ready_future<>(); }

  DLOG_TRACE("SEGMENT: {} size change update to: {}", name, sz);
  reader->update_file_size(sz);
  return seastar::make_ready_future<>();
}
wal_writer_node_opts
wal_nstpidx_manager::default_writer_opts() {
  return wal_writer_node_opts(
    opts, 0 /*epoch*/, 0 /*term*/, tprops, work_dir,
    priority_manager::get().streaming_write_priority(),
    wal_writer_node_opts::notify_create_log_handle_t(
      [this](auto s) { return create_log_handle_hook(s); }),
    wal_writer_node_opts::notify_size_change_handle_t(
      [this](auto f, auto sz) { return segment_size_change_hook(f, sz); }));
}

static void
print_repaired_files(const seastar::sstring &d,
                     const wal_nstpidx_repair::set_t &s) {
  if (s.empty()) return;
  std::vector<seastar::sstring> v;
  v.reserve(s.size());
  for (auto &i : s) {
    v.push_back(fmt::format("{}", i));
  }
  LOG_INFO("{} ({})", d, v);
}

seastar::future<>
wal_nstpidx_manager::open() {
  LOG_DEBUG("Open: opts={}, work_dir={}", opts, work_dir);
  return wal_nstpidx_repair::repair(work_dir)
    .then([this](auto repaired) {
      if (repaired.empty()) { return seastar::make_ready_future<>(); }
      print_repaired_files(work_dir, repaired);
      for (const auto &i : repaired) {
        auto full_path_file = work_dir + "/" + i.filename;
        auto n = std::make_unique<wal_reader_node>(i.epoch, i.term, i.size,
                                                   i.modified, full_path_file);
        _nodes.push_back(std::move(n));
      }
      LOG_INFO("{} total nodes: {}, {}-{} ({})", work_dir, _nodes.size(),
               _nodes.front()->starting_epoch, _nodes.back()->ending_epoch(),
               smf::human_bytes(_nodes.back()->ending_epoch() -
                                _nodes.front()->starting_epoch));
      return seastar::parallel_for_each(_nodes.begin(), _nodes.end(),
                                        [](auto &n) { return n->open(); });
    })
    .then([this] {
      auto wo = default_writer_opts();
      if (!_nodes.empty()) {
        wo.term = _nodes.back()->term;
        wo.epoch = _nodes.back()->ending_epoch();
      }
      _writer = std::make_unique<wal_writer_node>(std::move(wo));
      return _writer->open();
    });
}
seastar::future<>
wal_nstpidx_manager::close() {
  LOG_DEBUG("Close: opts={}, work_dir={}", opts, work_dir);
  auto elogger = [](auto eptr) {
    LOG_ERROR("Ignoring error closing partition manager: {}", eptr);
    return seastar::make_ready_future<>();
  };
  return _writer->close().handle_exception(elogger).then([this, elogger] {
    return seastar::do_for_each(_nodes.begin(), _nodes.end(),
                                [](auto &b) { return b->close(); })
      .handle_exception(elogger);
  });
}

std::unique_ptr<wal_partition_statsT>
wal_nstpidx_manager::stats() const {
  auto p = std::make_unique<wal_partition_statsT>();
  for (auto &n : _nodes) {
    p->segments.emplace_back(n->term, n->starting_epoch, n->ending_epoch());
  }
  p->ns = tprops->smeta()->persisted_ns();
  p->topic = tprops->smeta()->persisted_topic();
  p->partition = tprops->smeta()->persisted_partition();
  return p;
}

