#pragma once

#include "raft/consensus.h"

#include <seastar/core/sstring.hh>

namespace raft::details {
future<temporary_buffer<char>> readfile(sstring name);

future<> writefile(sstring name, temporary_buffer<char> buf);

future<>
persist_voted_for(sstring filename, consensus::voted_for_configuration);

future<consensus::voted_for_configuration> read_voted_for(sstring filename);

} // namespace raft::details
