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

namespace v {
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

wal_nstpidx_manager::wal_nstpidx_manager(
  wal_opts o, const wal_topic_create_request *create_props, wal_nstpidx id,
  seastar::sstring work_directory)
  : opts(std::move(o)), tprops(THROW_IFNULL(create_props)), idx(id),
    work_dir(std::move(work_directory)) {
  // set metrics
  namespace sm = seastar::metrics;
  metrics_.add_group(
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
  log_cleanup_timeout_.set_callback([this] {
    std::vector<std::unique_ptr<wal_reader_node>> to_remove;
    auto now = seastar::lowres_system_clock::now();

    // must be signed
    int32_t nodes_size = nodes_.size();

    // start from the front for time based
    while (nodes_size-- > 0) {
      auto &n = nodes_.front();

      if (n->filename == writer_->filename()) {
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
        to_remove.push_back(std::move(nodes_.front()));
        nodes_.pop_front();
      }
    }

    // start from the back for size based
    nodes_size = nodes_.size();
    int64_t max_bytes = opts.max_retention_size;
    while (opts.max_retention_size > 0 && nodes_size-- > 0) {
      auto &n = nodes_[nodes_size];
      if (n->filename == writer_->filename()) { continue; }
      max_bytes -= n->file_size();
      if (max_bytes <= 0) {
        LOG_INFO(
          "Size retention exceeded: {}. Marking following nodes as deleted",
          smf::human_bytes(opts.max_retention_size));
        for (auto i = 0; i <= nodes_size; ++i) {
          to_remove.push_back(std::move(nodes_.front()));
          nodes_.pop_front();
          LOG_INFO("Marking node {} as deleted. Size retention policy",
                   to_remove.back()->filename);
        }
      }
    }
    if (!to_remove.empty()) {
      // do in background
      seastar::do_with(std::move(to_remove), [](auto &vec) {
        return seastar::do_for_each(vec.begin(), vec.end(),
                                    [](auto &rdr) { return rdr->close(); });
      });
    }
  });

  // add jitter of up to 10seconds
  smf::random r;
  log_cleanup_timeout_.arm_periodic(
    std::chrono::hours(1) + std::chrono::milliseconds(r.next() % 10000));
}

wal_nstpidx_manager::~wal_nstpidx_manager() { log_cleanup_timeout_.cancel(); }

wal_nstpidx_manager::wal_nstpidx_manager(wal_nstpidx_manager &&o) noexcept
  : opts(std::move(o.opts)), tprops(o.tprops), idx(std::move(o.idx)),
    work_dir(std::move(o.work_dir)), writer_(std::move(o.writer_)),
    nodes_(std::move(o.nodes_)),
    prometheus_stats_(std::move(o.prometheus_stats_)),
    metrics_(std::move(o.metrics_)) {}

seastar::future<std::unique_ptr<wal_write_reply>>
wal_nstpidx_manager::append(wal_write_request r) {
  prometheus_stats_.write_reqs++;
  prometheus_stats_.write_bytes +=
    std::accumulate(r.begin(), r.end(), int64_t(0),
                    [](int64_t acc, const wal_binary_record *x) {
                      return acc + x->data()->size();
                    });
  return writer_->append(std::move(r));
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
  if (nodes_.empty()) {
    return seastar::make_ready_future<retval_t>(std::move(retval));
  }
  auto it = std::lower_bound(nodes_.begin(), nodes_.end(), retval->next_epoch(),
                             offset_comparator{});
  return seastar::do_with(
    std::move(retval), std::move(it), [this, r](auto &read_result, auto &it) {
      auto retval = read_result.get();
      return seastar::do_until(
               [this, &it, retval, max_bytes = r.req->max_bytes()] {
                 return it == nodes_.end() ||
                        retval->on_disk_size() >= max_bytes ||
                        retval->reply().error !=
                          wal_read_errno::wal_read_errno_none;
               },
               [this, &it, retval, r]() mutable {
                 auto ptr = it->get();
                 auto offset = retval->next_epoch();
                 if (offset < ptr->starting_epoch ||
                     offset >= ptr->ending_epoch()) {
                   it = nodes_.end();
                   return seastar::make_ready_future<>();
                 }
                 return ptr->get(retval, r)
                   .finally([&it]() mutable { std::next(it); })
                   .handle_exception([this, &it, r](auto eptr) {
                     LOG_ERROR("Exception: {} (Offset:{}, req size:{})", eptr,
                               r.req->offset(), r.req->max_bytes());
                     it = nodes_.end();
                   });
               })
        .then([this, &read_result] {
          if (read_result->empty()) {
            read_result->set_next_offset(std::max(
              read_result->reply().next_offset, nodes_.back()->starting_epoch));
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
  auto starting_epoch =
    wal_name_extractor_utils::wal_segment_extract_epoch(filename);
  LOG_THROW_IF(
    !nodes_.empty() && starting_epoch <= nodes_.back()->starting_epoch,
    "Found unordered nodes. Critical order violation. eval epoch: {}. "
    "last known begin epoch: {}. filename: {}",
    starting_epoch, nodes_.back()->starting_epoch, filename);
  auto n = std::make_unique<wal_reader_node>(
    starting_epoch, 0, seastar::lowres_system_clock::now(), filename);
  DLOG_DEBUG("Create WAL file: {} ", n->filename);
  nodes_.push_back(std::move(n));
  return nodes_.back()->open();
}
seastar::future<>
wal_nstpidx_manager::segment_size_change_hook(seastar::sstring name,
                                              int64_t sz) {
  DLOG_THROW_IF(nodes_.empty(),
                "Failed to update size. Unknown file: {}, size: {}", name, sz);
  DLOG_THROW_IF(nodes_.back()->filename != name,
                "Tried to update an our of order file: {}. Last node is: {}",
                name, nodes_.back()->filename);
  auto &reader = nodes_.back();
  if (reader->file_size() == sz) { return seastar::make_ready_future<>(); }

  DLOG_TRACE("SEGMENT: {} size change update to: {}", name, sz);
  reader->update_file_size(sz);
  return seastar::make_ready_future<>();
}
wal_writer_node_opts
wal_nstpidx_manager::default_writer_opts() {
  return wal_writer_node_opts(
    opts, tprops, work_dir, priority_manager::get().streaming_write_priority(),
    wal_writer_node_opts::notify_create_log_handle_t(
      [this](auto s) { return create_log_handle_hook(s); }),
    wal_writer_node_opts::notify_size_change_handle_t(
      [this](auto f, auto sz) { return segment_size_change_hook(f, sz); }));
}

static void
print_repaired_files(const seastar::sstring &d, const auto &s) {
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
        auto n = std::make_unique<wal_reader_node>(i.epoch, i.size, i.modified,
                                                   full_path_file);
        nodes_.push_back(std::move(n));
      }
      LOG_INFO("{} total nodes: {}, {}-{} ({})", work_dir, nodes_.size(),
               nodes_.front()->starting_epoch, nodes_.back()->ending_epoch(),
               smf::human_bytes(nodes_.back()->ending_epoch() -
                                nodes_.front()->starting_epoch));
      return seastar::parallel_for_each(nodes_.begin(), nodes_.end(),
                                        [](auto &n) { return n->open(); });
    })
    .then([this] {
      auto wo = default_writer_opts();
      if (!nodes_.empty()) { wo.epoch = nodes_.back()->ending_epoch(); }
      writer_ = std::make_unique<wal_writer_node>(std::move(wo));
      return writer_->open();
    });
}
seastar::future<>
wal_nstpidx_manager::close() {
  LOG_DEBUG("Close: opts={}, work_dir={}", opts, work_dir);
  auto elogger = [](auto eptr) {
    LOG_ERROR("Ignoring error closing partition manager: {}", eptr);
    return seastar::make_ready_future<>();
  };
  return writer_->close().handle_exception(elogger).then([this, elogger] {
    return seastar::do_for_each(nodes_.begin(), nodes_.end(),
                                [this](auto &b) { return b->close(); })
      .handle_exception(elogger);
  });
}

// TODO(agallego) - fix these stats, ask for argument
std::unique_ptr<wal_partition_stats>
wal_nstpidx_manager::stats() const {
  auto p = std::make_unique<wal_partition_stats>();
  if (!nodes_.empty()) {
    p->mutate_start_offset(nodes_[0]->starting_epoch);
    p->mutate_log_segment_count(nodes_.size() + 1);
  } else {
    p->mutate_start_offset(0);
    p->mutate_log_segment_count(1);
  }
  p->mutate_ending_offset(writer_->current_offset());
  return std::move(p);
}

}  // namespace v
