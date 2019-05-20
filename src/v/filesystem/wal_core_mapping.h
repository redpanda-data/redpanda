#pragma once
#include "wal_requests.h"

#include <seastar/core/sstring.hh>

#include <vector>

struct wal_core_mapping {
    static uint32_t nstpidx_to_lcore(wal_nstpidx idx);

    /// \brief map the request to the lcore that is going to handle the reads
    static wal_read_request core_assignment(const wal_get_request* r);

    /// \brief get a list of core assignments. Note that because the mapping is
    /// always consistent and based off the actual underlying hardware, a core
    /// might get multiple assignments.
    ///
    static std::vector<wal_write_request>
    core_assignment(const wal_put_request* p);

    /// \brief core assignment for create requests
    static std::vector<wal_create_request>
    core_assignment(const wal_topic_create_request* p);
};
