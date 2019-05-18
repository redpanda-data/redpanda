#include "write_ahead_log.h"

#include "ioutil/dir_utils.h"
#include "syschecks/syschecks.h"
#include "wal_cold_boot.h"
#include "wal_core_mapping.h"
#include "wal_name_extractor_utils.h"
#include "wal_requests.h"

#include <smf/log.h>

#include <sys/sdt.h>

#include <memory>
#include <utility>

write_ahead_log::write_ahead_log(wal_opts _opts)
  : opts(std::move(_opts))
  , _tm(opts) {
}

seastar::future<std::unique_ptr<wal_write_reply>>
write_ahead_log::append(wal_write_request r) {
    // OK to be in debug only - Services should repeat this call
    // in non debug mode if they want to validate it
    // see chain_replication_service.cc for example
    DLOG_THROW_IF(
      r.runner_core != seastar::engine().cpu_id(), "Incorrect core assignment");
    DLOG_THROW_IF(!wal_write_request::is_valid(r), "invalid write request");
    DTRACE_PROBE(rp, wal_write);
    return _tm.get_manager(r.idx).then([r = std::move(r)](auto m) mutable {
        if (SMF_LIKELY(m != nullptr)) {
            return m->append(std::move(r));
        }
        DLOG_ERROR(
          "append::Invalid namespace/topic/partition tuple. Out of range");
        auto ret = std::make_unique<wal_write_reply>(
          r.req->ns(), r.req->topic());
        ret->err = wal_put_errno::wal_put_errno_invalid_ns_topic_partition;
        return seastar::make_ready_future<decltype(ret)>(std::move(ret));
    });
}

seastar::future<std::unique_ptr<wal_read_reply>>
write_ahead_log::get(wal_read_request r) {
    // OK to be in debug only - Services should repeat this call
    // in non debug mode if they want to validate it
    // see chain_replication_service.cc for example
    DLOG_THROW_IF(!wal_read_request::is_valid(r), "invalid read request");
    DTRACE_PROBE(rp, wal_get);
    return _tm.get_manager(r.idx).then([r = std::move(r)](auto m) mutable {
        if (SMF_LIKELY(m != nullptr)) {
            return m->get(std::move(r));
        }
        DLOG_ERROR(
          "get::Invalid namespace/topic/partition tuple. Out of range");
        auto retval = std::make_unique<wal_read_reply>(
          r.req->ns(),
          r.req->topic(),
          r.req->partition(),
          r.req->offset(),
          r.req->server_validate_payload());
        retval->set_error(
          wal_read_errno::wal_read_errno_invalid_ns_topic_partition);
        return seastar::make_ready_future<decltype(retval)>(std::move(retval));
    });
}

std::unique_ptr<wal_stats_reply> write_ahead_log::stats() const {
    return _tm.stats();
};

seastar::future<std::unique_ptr<wal_create_reply>>
write_ahead_log::create(wal_create_request r) {
    // OK to be in debug only - Services should repeat this call
    // in non debug mode if they want to validate it
    // see redpanda_service.cc for example
    DLOG_THROW_IF(
      !wal_create_request::is_valid(r), "Invalid wal_create_request");
    switch (r.req->type()) {
    case wal_topic_type::wal_topic_type_compaction:
        LOG_ERROR(
          "Compaction topics not yet enabled, treating as normal topic");
        // Add to the compaction thread
        break;
    default:
        break;
    }
    return _tm.create(std::move(r));
}

seastar::future<> write_ahead_log::index() {
    LOG_INFO("Cold boot. Indexing directory: `{}`", opts.directory);
    return wal_cold_boot::filesystem_lcore_index(opts.directory)
      .then([this](auto boot) {
          return seastar::do_with(std::move(boot), [this](auto& cold_boot) {
              return seastar::do_for_each(
                cold_boot.fsidx.begin(),
                cold_boot.fsidx.end(),
                [this](auto& ns) {
                    return seastar::do_for_each(
                      ns.second.begin(),
                      ns.second.end(),
                      [this, nsstr = ns.first](auto& t) {
                          return seastar::do_for_each(
                            t.second.begin(),
                            t.second.end(),
                            [this, topic = t.first, ns = nsstr](
                              int32_t partition) {
                                return _tm.open(ns, topic, partition);
                            });
                      });
                });
          });
      });
}
seastar::future<> write_ahead_log::open() {
    LOG_INFO("starting: {}", opts);
    if (seastar::engine().cpu_id() == 0) {
        auto dir = opts.directory;
        return dir_utils::create_dir_tree(dir).then([dir] {
            LOG_INFO("Checking `{}` for supported filesystems", dir);
            return syschecks::disk(dir);
        });
    }
    return seastar::make_ready_future<>();
}

seastar::future<> write_ahead_log::close() {
    LOG_INFO("stopping: {}", opts);
    // close all topic managers
    return _tm.close();
}
