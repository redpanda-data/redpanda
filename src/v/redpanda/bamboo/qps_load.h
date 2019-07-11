#pragma once

#include "random/fast_prng.h"
#include "redpanda/bamboo/cli.h"
#include "seastarx.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>

#include <boost/program_options.hpp>

/// \brief the purpose of this class is to launch requests with
/// fixes for coordinated ommission
class qps_load {
public:
    explicit qps_load(const boost::program_options::variables_map* cfg);
    ~qps_load() = default;

    future<> drive();

    std::unique_ptr<smf::histogram> copy_histogram() const;

    future<> open();

    future<> stop();

    const boost::program_options::variables_map& options() const;

    const boost::program_options::variables_map* opts;

private:
    future<> coordinated_omision_req();
    future<> coordinated_omision_writes();
    future<> coordinated_omision_reads();

private:
    std::vector<std::unique_ptr<cli>> _loaders;
    semaphore method_sem_{1};
    promise<> drive_pr_;
    timer<> qps_timer_;
    fast_prng _rand{};
    double rw_balance_ = 0.5;
    uint64_t needle_threshold_;
};
