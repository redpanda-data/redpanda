#pragma once

#include <boost/program_options.hpp>
#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>

// rp
#include "random/fast_prng.h"
// bmtlctl
#include "cli.h"

namespace rp {
/// \brief the purpose of this class is to launch requests with
/// fixes for coordinated ommission
class qps_load {
 public:
  explicit qps_load(const boost::program_options::variables_map *cfg);
  ~qps_load() = default;

  seastar::future<> drive();

  std::unique_ptr<smf::histogram> copy_histogram() const;

  seastar::future<> open();

  seastar::future<> stop();

  const boost::program_options::variables_map &options() const;

  const boost::program_options::variables_map *opts;

 private:
  seastar::future<> coordinated_omision_req();
  seastar::future<> coordinated_omision_writes();
  seastar::future<> coordinated_omision_reads();

 private:
  std::vector<std::unique_ptr<cli>> loaders_;
  seastar::semaphore method_sem_{1};
  seastar::promise<> drive_pr_;
  seastar::timer<> qps_timer_;
  fast_prng rand_{};
  double rw_balance_ = 0.5;
  uint64_t needle_threshold_;
};
}  // namespace rp
