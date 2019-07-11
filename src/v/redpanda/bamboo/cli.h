#pragma once

#include "redpanda/api/client.h"
#include "seastarx.h"

#include <smf/random.h>

#include <boost/program_options.hpp>

#include <memory>

class cli {
public:
    explicit cli(const boost::program_options::variables_map* cfg);
    ~cli();

    future<> one_write();

    future<> one_read();

    future<> open();

    future<> stop();

    const boost::program_options::variables_map& options() const;

    const boost::program_options::variables_map* opts;

    api::client* api() const;

    uint64_t id() const {
        return _id;
    }

private:
    smf::random _rand;
    std::unique_ptr<api::client> _api;
    uint64_t _id;
    int32_t write_key_sz_;
    int32_t write_val_sz_;
    int32_t write_batch_sz_;
    int32_t partition_pref_;
};
