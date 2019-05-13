#include "qps_load.h"

#include <boost/iterator/counting_iterator.hpp>
#include <boost/program_options.hpp>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <smf/log.h>

#include "hashing/jump_consistent_hash.h"
#include "redpanda/api/client_stats.h"

namespace v {
using namespace std::chrono;  // NOLINT

/// \brief used for qps_* methods. need helper struct for args
struct qpsargs {
  using time_t = seastar::lowres_system_clock::time_point;
  explicit qpsargs(uint32_t seconds) : secs(seconds), secs_sem(seconds) {
    test_start = seastar::lowres_system_clock::now();
  }
  const uint32_t secs;

  seastar::semaphore secs_sem;

  uint64_t
  duration_millis() const {
    return duration_cast<seconds>(test_end - test_start).count();
  }

  time_t test_start;
  time_t test_end;
  uint32_t secs_iteration{0};
};

qps_load::qps_load(const boost::program_options::variables_map *cfg)
  : opts(cfg) {
  auto ldrs = options()["concurrency"].as<int32_t>();
  for (auto i = 0; i < ldrs; ++i) {
    loaders_.push_back(std::make_unique<cli>(cfg));
  }
  rw_balance_ =
    std::min(double(1.0), std::abs(options()["rw-balance"].as<double>()));
  LOG_INFO("Balance writes vs reads: {}", rw_balance_);
  needle_threshold_ = std::numeric_limits<uint32_t>::max();
  if (rw_balance_ >= double(0.0) && rw_balance_ <= double(1.0)) {
    needle_threshold_ = static_cast<double>(needle_threshold_) * rw_balance_;
  } else {
    LOG_THROW("Invalid rw_balance:{}", rw_balance_);
  }
}

seastar::future<>
qps_load::coordinated_omision_writes() {
  auto qps = options()["qps"].as<int32_t>();
  LOG_INFO("Writing: {}", qps);
  return seastar::do_with(seastar::semaphore(qps), [this, qps](auto &limit) {
    return seastar::do_for_each(
             boost::counting_iterator<int>(0),
             boost::counting_iterator<int>(qps),
             [this, &limit](auto i) {
               return limit.wait(1).then([this, &limit, i]() {
                 auto lidx = jump_consistent_hash(i, loaders_.size());
                 auto ptr = loaders_[lidx].get();
                 // Don't return!, launch it in the background
                 ptr->one_write().finally([&limit] { limit.signal(1); });
                 return seastar::make_ready_future<>();
               });
             })
      .then([&limit, qps] { return limit.wait(qps); });
  });
}

seastar::future<>
qps_load::coordinated_omision_reads() {
  auto qps = options()["qps"].as<int32_t>();
  LOG_INFO("Reading: {}", qps);
  return seastar::do_with(seastar::semaphore(qps), [this, qps](auto &limit) {
    return seastar::do_for_each(
             boost::counting_iterator<int>(0),
             boost::counting_iterator<int>(qps),
             [this, &limit](auto i) {
               return limit.wait(1).then([this, &limit, i]() {
                 auto lidx = jump_consistent_hash(i, loaders_.size());
                 auto ptr = loaders_[lidx].get();
                 // Don't return!, launch it in the background
                 ptr->one_read().finally([&limit] { limit.signal(1); });
               });
             })
      .then([&limit, qps] { return limit.wait(qps); });
  });
}

seastar::future<>
qps_load::coordinated_omision_req() {
  auto x = rand_();
  LOG_INFO("is: {} <= {}", x, needle_threshold_);
  if (x <= needle_threshold_) { return coordinated_omision_writes(); }
  return coordinated_omision_reads();
}

static void
print_iteration_stats(uint32_t iterno, uint32_t max,
                      seastar::lowres_system_clock::time_point test_start,
                      seastar::lowres_system_clock::time_point method_start,
                      std::vector<std::unique_ptr<cli>> &loaders) {
  auto now = seastar::lowres_system_clock::now();
  auto method_duration_millis =
    duration_cast<milliseconds>(now - method_start).count();
  auto duration = duration_cast<seconds>(now - test_start).count();
  api::client_stats stats;
  for (const auto &l : loaders) {
    stats += l->api()->stats();
  }
  LOG_INFO("Iteration: {}: Test duration: {}s, method : {}ms, max: {}. {}",
           iterno, duration, method_duration_millis, max, stats);
}

seastar::future<>
qps_load::drive() {
  auto max = options()["seconds-duration"].as<int32_t>();
  auto args = seastar::make_lw_shared<qpsargs>(max);

  return method_sem_.wait(1).then([this, args]() mutable {
    LOG_INFO("Got method lock. Getting {} locks",
             args->secs_sem.available_units());
    return args->secs_sem.wait(args->secs)
      .then([this, args]() mutable {
        LOG_INFO("Got locks!");
        qps_timer_.set_callback([this, args]() mutable {
          ++args->secs_iteration;
          LOG_INFO("Starting iteration: {}", args->secs_iteration);
          if (args->secs_iteration >= args->secs) { qps_timer_.cancel(); }

          auto method_start_t = seastar::lowres_system_clock::now();
          // CANNOT return. It has to launch qps every time-interval
          //
          coordinated_omision_req().finally(
            [this, iterno = args->secs_iteration, method_start_t, args]() {
              if (args->secs_iteration >= args->secs) {
                args->test_end = seastar::lowres_system_clock::now();
              }
              // pretty print progress
              print_iteration_stats(iterno, args->secs, args->test_start,
                                    method_start_t, loaders_);
              // must be last thing
              args->secs_sem.signal(1);
            });
        });  // end of callback
        qps_timer_.arm_periodic(seconds(1));
      })
      .then([this, args]() mutable {
        return args->secs_sem.wait(args->secs).then([this, args] {
          LOG_INFO("Test took: {}", args->duration_millis());
          method_sem_.signal(1);
          drive_pr_.set_value();
        });
      });
  });
}

std::unique_ptr<smf::histogram>
qps_load::copy_histogram() const {
  auto h = smf::histogram::make_unique();
  for (auto &c : loaders_) {
    auto p = c->api()->get_histogram();
    *h += *p;
  }
  return h;
}
seastar::future<>
qps_load::open() {
  return seastar::with_semaphore(method_sem_, 1, [this] {
    LOG_INFO("Opening connections: {}", loaders_.size());
    return seastar::do_for_each(loaders_.begin(), loaders_.end(),
                                [](auto &i) { return i->open(); });
  });
}
seastar::future<>
qps_load::stop() {
  return seastar::with_semaphore(method_sem_, 1, [this] {
    return seastar::do_for_each(loaders_.begin(), loaders_.end(),
                                [](auto &i) { return i->stop(); });
  });
}
const boost::program_options::variables_map &
qps_load::options() const {
  return *opts;
}

}  // namespace v
