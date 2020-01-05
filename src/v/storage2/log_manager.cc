#include "storage2/log_manager.h"

#include "config/configuration.h"
#include "model/fundamental.h"
#include "storage2/log.h"
#include "utils/directory_walker.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/util/log.hh>

#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/reversed.hpp>

namespace storage {

static logger slog("s/log_manager");

/**
 * This function is used by the discover_directory recursive traversal.
 * Whenever it reaches the third depth level from the working directory
 * root, it will store that path and stop the recursion, otherwise it
 * descends further into child nodes.
 */
static future<>
visit_directory(sstring path, std::vector<sstring>& accum, int depth = 0) {
    if (depth == 3) {
        accum.emplace_back(std::move(path));
        return make_ready_future<>();
    } else {
        return directory_walker::walk(
          path, [path, depth, &accum](directory_entry de) {
              if (de.type == directory_entry_type::directory) {
                  sstring childpath = path + "/" + de.name;
                  return visit_directory(
                    std::move(childpath), accum, depth + 1);
              }
              return make_ready_future<>();
          });
    }
}

/**
 * Used when initializing an instance of the log_manager. This function
 * will traverse the working directory and discover all ntps in the system
 *
 * The directory structure is: <some-path-to-working-dir>/ns/topic/log/...
 * The traversal begins at the root of the working directory and recursively
 * descends into child nodes until it reaches 3 levels of depth. That is where
 * the log directory is expected to be.
 */
static future<std::vector<model::ntp>> discover_directory(sstring path) {
    return do_with(
             std::vector<sstring>(),
             [&path](std::vector<sstring>& leafs) mutable {
                 return visit_directory(path, leafs).then([&leafs] {
                     return make_ready_future<std::vector<sstring>>(
                       std::move(leafs));
                 });
             })
      .then([](std::vector<sstring> ntp_dirs) {
          std::vector<model::ntp> ntps;
          for (auto& dir : ntp_dirs) {
              std::filesystem::path pathchunks(std::move(dir));

              // convert each sstring path to filesystem::path and iterate
              // over it in reverse order. The ntp will be discovered as:
              // p/t/n/...
              auto pathit = (pathchunks | boost::adaptors::reversed).begin();
              ntps.emplace_back(
                model::ntp{.ns = model::ns(std::next(pathit, 2)->string()),
                           .tp = model::topic_partition{
                             .topic = model::topic(std::next(pathit)->string()),
                             .partition = model::partition_id(
                               (std::stoi(pathit->string())))}});
          }
          return make_ready_future<std::vector<model::ntp>>(std::move(ntps));
      });
}

class log_manager::impl {
public:
    impl(
      sstring basedir,
      log_manager::config cfg,
      std::vector<model::ntp> discovered_ntps)
      : config_(std::move(cfg))
      , rootdir_(std::move(basedir))
      , ntps_(std::move(discovered_ntps)) {}

public:
    log_manager::ntp_range ntps(
      model::ns ns_filter,
      model::topic topic_filter,
      model::partition_id partition_filter) {
        std::function<bool(model::ns const&)> ns_predicate =
          [filter = std::move(ns_filter)](const model::ns& n) {
              return n == filter;
          };

        std::function<bool(const model::topic&)> topic_predicate =
          [](const model::topic&) { return true; };

        std::function<bool(const model::partition_id&)> partition_predicate =
          [](const model::partition_id&) { return true; };

        if (topic_filter != model::topic()) {
            topic_predicate = [filter = std::move(topic_filter)](
                                const model::topic& t) { return t == filter; };
        }

        if (partition_filter != model::partition_id(-1)) {
            partition_predicate = [filter = std::move(partition_filter)](
                                    const model::partition_id& p) {
                return p == filter;
            };
        }

        std::function<bool(const model::ntp&)> predicate =
          [tpred = std::move(topic_predicate),
           ppred = std::move(partition_predicate),
           npred = std::move(ns_predicate)](const model::ntp& ntp) {
              return tpred(ntp.tp.topic) && ppred(ntp.tp.partition) && npred(ntp.ns);
          };

        return ntps_ | boost::adaptors::filtered(std::move(predicate));
    }

    bool contains_ntp(const model::ntp& ntp) {
        auto range = ntps(ntp.ns, ntp.tp.topic, ntp.tp.partition);
        return std::distance(range.begin(), range.end()) != 0;
    }

    future<> add_ntp(model::ntp ntp) {
        auto ntppath = working_directory() / ntp.path().c_str();
        return seastar::recursive_touch_directory(ntppath.string())
          .then([this, ntp = std::move(ntp)] {
              ntps_.emplace_back(std::move(ntp));
          });
    }

    future<> close_all_logs() {
        std::vector<future<flush_result>> work;
        for (auto& kv : instances_) {
            work.emplace_back(kv.second.close());
        }
        return when_all_succeed(work.begin(), work.end()).discard_result();
    }

    const std::filesystem::path& working_directory() const { return rootdir_; }

    io_priority_class io_priority() const { return config_.io_priority; }

    const log_manager::config& configuration() const { return config_; }

private:
    const config config_;
    const std::filesystem::path rootdir_;

    log_manager::ntps_type ntps_;
    std::unordered_map<model::ntp, log> instances_;
};

/**
 * Defaults come either from the config file/test fixture
 * or set to optimal non-debug values.
 */
log_manager::config log_manager::config::testing_defaults() {
    return config{
      .max_segment_size = ::config::shard_local_cfg().log_segment_size(),
      .should_sanitize = sanitize_files::no,
      .enable_lazy_loading = lazy_loading::yes,
      .io_priority = default_priority_class()};
}

log_manager::log_manager(shared_ptr<impl> impl)
  : _impl(impl) {}

future<log_manager>
log_manager::open(sstring basedir, log_manager::config optional) {
    return seastar::recursive_touch_directory(basedir)
      .then([basedir] { return discover_directory(basedir); })
      .then([basedir, optional](std::vector<model::ntp> ntps) {
          // TODO: Here check in the config if we lazy loading is enabled
          // if its disabled, then preload all logs and their indices.
          return make_ready_future<std::vector<model::ntp>>(std::move(ntps));
      })
      .then([basedir, optional](std::vector<model::ntp> ntps) {
          return make_ready_future<log_manager>(log_manager(make_shared<impl>(
            impl(std::move(basedir), std::move(optional), std::move(ntps)))));
      });
}

future<log> log_manager::open_ntp(model::ntp ntp) {
    if (!_impl->contains_ntp(ntp)) {
        return make_exception_future<log>(
          std::invalid_argument(fmt::format("ntp {} not found", ntp)));
    }
    return log::open(*this, std::move(ntp), _impl->io_priority());
}

const std::filesystem::path& log_manager::working_directory() const {
    return _impl->working_directory();
}

log_manager::ntp_range log_manager::ntps(
  model::ns ns_filter,
  model::topic topic_filter,
  model::partition_id partition_filter) {
    return _impl->ntps(
      std::move(ns_filter),
      std::move(topic_filter),
      std::move(partition_filter));
}

future<log> log_manager::create_ntp(model::ntp ntp) {
    if (_impl->contains_ntp(ntp)) {
        return make_exception_future<log>(
          std::invalid_argument(fmt::format("ntp {} already exists.", ntp)));
    }

    slog.info("creating ntp: {}", ntp);
    return _impl->add_ntp(ntp).then([this, ntp = std::move(ntp)] {
        return log::open(*this, std::move(ntp), _impl->io_priority());
    });
}

future<std::vector<log>>
log_manager::create_topic(model::ns ns, model::topic name, size_t partitions) {
    std::vector<future<log>> workload;
    for (auto i = 0; i < partitions; ++i) {
        workload.emplace_back(create_ntp(model::ntp{
          .ns = ns,
          .tp = {.topic = name, .partition = model::partition_id(i)}}));
    }

    return when_all_succeed(workload.begin(), workload.end());
}

future<> log_manager::close() { return _impl->close_all_logs(); }

future<> log_manager::remove_ntp(model::ntp ntp) {
    // TOO dangerous, not implemented.
    return make_ready_future<>();
}

const log_manager::config& log_manager::configuration() const {
    return _impl->configuration();
}

} // namespace storage