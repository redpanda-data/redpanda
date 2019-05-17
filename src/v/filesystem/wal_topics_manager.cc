#include "wal_topics_manager.h"

#include <memory>
#include <sys/sdt.h>
#include <utility>

#include <fmt/format.h>
#include <seastar/core/reactor.hh>
#include <smf/log.h>
#include <smf/native_type_utils.h>
#include <smf/time_utils.h>

#include "ioutil/dir_utils.h"
#include "ioutil/priority_manager.h"
#include "ioutil/readfile.h"

#include "wal_segment.h"
#include "wal_segment_record.h"

inline seastar::sstring
nstpidx_dirname(const wal_opts &o, seastar::sstring ns, seastar::sstring topic,
                int32_t partition) {
  return fmt::format("{}/{}/{}/{}", o.directory, ns, topic, partition);
}

inline seastar::sstring
metadata_path(const wal_opts &o, seastar::sstring ns, seastar::sstring topic,
              int32_t partition) {
  return fmt::format("{}/metadata", nstpidx_dirname(o, ns, topic, partition));
}

static seastar::future<>
write_partition_metadata(const wal_opts &opts, const wal_create_request &c,
                         int32_t partition) {

  std::unique_ptr<wal_topic_create_requestT> props(c.req->UnPack());
  if (props->smeta == nullptr) {
    props->smeta = std::make_unique<wal_topic_create_system_metadataT>();
  }
  // The values below are needed to merge topics/partitions across WALs
  props->smeta->persisted_partition = partition;
  props->smeta->persisted_ns = xxhash_64(props->ns.c_str(), props->ns.size());
  props->smeta->persisted_topic =
    xxhash_64(props->topic.c_str(), props->topic.size());
  auto buf =
    smf::native_table_as_buffer<wal_topic_create_request>(*props.get());

  // TODO(agallego): change this to something more useful
  seastar::sstring key =
    "created: " + seastar::to_sstring(smf::time_now_millis());

  auto data = wal_segment_record::coalesce(
    key.data(), key.size(), buf.get(), buf.size(),
    wal_compression_type::wal_compression_type_none);

  // sometimes xfs blocks if you create many directoes from all cores
  // under the same root dir
  auto directory = nstpidx_dirname(opts, props->ns, props->topic, partition);
  return seastar::smp::submit_to(
           0, [directory] { return dir_utils::create_dir_tree(directory); })
    .then([data = std::move(data), ns = props->ns, topic = props->topic, &opts,
           partition]() mutable {
      auto filename = metadata_path(opts, ns, topic, partition);
      auto sz = data->data.size();
      auto file = std::make_unique<wal_segment>(
        filename, priority_manager::get().streaming_write_priority(), sz, sz);
      auto f = file.get();
      return f->open()
        .then([f, sz, d = std::move(data)]() mutable {
          auto dptr = d.get();
          auto src = reinterpret_cast<const char *>(dptr->data.data());
          return f->append(src, sz).finally([r = std::move(d)] {});
        })
        .then([f] { return f->close(); })
        .finally([file = std::move(file)] {});
    });
}

wal_topics_manager::wal_topics_manager(wal_opts o) : opts(o) {}

seastar::future<std::unique_ptr<wal_create_reply>>
wal_topics_manager::create(wal_create_request r) {
  return seastar::do_with(
           std::move(r),
           [this](auto &cr) {
             seastar::sstring ns_str = cr.req->ns()->c_str();
             seastar::sstring topic_str = cr.req->topic()->c_str();

             for (int32_t p : cr.partition_assignments) {
               auto idx = wal_nstpidx::gen(ns_str, topic_str, p);
               if (props_.find(idx) != props_.end()) {
                 return seastar::make_ready_future<>();
               }
             }
             return seastar::with_semaphore(
               serialize_create_, 1, [this, ns_str, topic_str, &cr]() mutable {
                 // need to double check
                 for (int32_t p : cr.partition_assignments) {
                   auto idx = wal_nstpidx::gen(ns_str, topic_str, p);
                   if (props_.find(idx) != props_.end()) {
                     return seastar::make_ready_future<>();
                   }
                 }
                 LOG_INFO("creating `{}/{}: {}`", ns_str, topic_str,
                          cr.partition_assignments);
                 return seastar::do_for_each(
                   cr.partition_assignments.begin(),
                   cr.partition_assignments.end(),
                   [this, ns_str, topic_str, &cr](auto partition) mutable {
                     return write_partition_metadata(opts, cr, partition)
                       .then([this, partition, ns_str, topic_str] {
                         return open(ns_str, topic_str, partition);
                       });
                   });
               });
           })
    .then([]() {
      auto ret = std::make_unique<wal_create_reply>();
      return seastar::make_ready_future<decltype(ret)>(std::move(ret));
    });
}

seastar::future<wal_topic_create_request *>
wal_topics_manager::nstpidx_props(wal_nstpidx idx, seastar::sstring props_dir) {
  using ptr_t = wal_topic_create_request *;
  auto it = props_.find(idx);
  if (it != props_.end()) {
    return seastar::make_ready_future<ptr_t>(it->second.get());
  }
  seastar::sstring props_filename = props_dir + "/metadata";
  return readfile(props_filename).then([this, props_filename, idx](auto buf) {
    LOG_THROW_IF(buf.size() == 0, "could not read properties file: {}",
                 props_filename);
    auto [k, v] = wal_segment_record::extract_from_bin(buf.get(), buf.size());
    LOG_TRACE("Loaded properties: {} ({})", props_filename, idx);
    auto tb = smf::fbs_typed_buf<wal_topic_create_request>(std::move(v));
    auto ptr = tb.get();
    props_.emplace(idx, std::move(tb));
    return seastar::make_ready_future<ptr_t>(ptr);
  });
}

seastar::future<>
wal_topics_manager::open(seastar::sstring ns, seastar::sstring topic,
                         int32_t partition) {
  auto idx = wal_nstpidx::gen(ns, topic, partition);
  auto it = mngrs_.find(idx);
  if (it == mngrs_.end()) {
    auto workdir = nstpidx_dirname(opts, ns, topic, partition);
    // We can have background fibers doing the reopens
    // That's why we need this semaphore
    //
    return seastar::with_semaphore(
      serialize_open_, 1, [this, workdir, idx]() mutable {
        // need double-checking
        auto it2 = mngrs_.find(idx);
        if (it2 != mngrs_.end()) { return seastar::make_ready_future<>(); }

        // open and add to our map
        return nstpidx_props(idx, workdir)
          .then([this, workdir, idx](wal_topic_create_request *props) {
            auto x =
              std::make_unique<wal_nstpidx_manager>(opts, props, idx, workdir);
            auto y = x.get();
            return y->open().then([this, x = std::move(x), idx]() mutable {
              DTRACE_PROBE1(rp, wal_topics_manager_open, idx.id());
              mngrs_.emplace(idx, std::move(x));
              return seastar::make_ready_future<>();
            });
          });
      });
  }
  return seastar::make_ready_future<>();
}

seastar::future<wal_nstpidx_manager *>
wal_topics_manager::get_manager(wal_nstpidx idx) {
  wal_nstpidx_manager *ptr = nullptr;
  auto it = mngrs_.find(idx);
  if (SMF_UNLIKELY(it == mngrs_.end())) {
    return seastar::make_ready_future<wal_nstpidx_manager *>(nullptr);
  }
  ptr = it->second.get();
  return seastar::make_ready_future<wal_nstpidx_manager *>(ptr);
}

seastar::future<>
wal_topics_manager::close() {
  return seastar::do_for_each(mngrs_.begin(), mngrs_.end(),
                              [](auto &pm) { return pm.second->close(); });
}

std::unique_ptr<wal_stats_reply>
wal_topics_manager::stats() const {
  auto retval = std::make_unique<wal_stats_reply>();
  auto &ref = retval->stats;
  for (auto &mpair : mngrs_) {
    auto &m = mpair.second;
    ref.emplace(m->idx, m->stats());
  }
  return retval;
}

