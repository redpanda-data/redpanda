#pragma once

#include "raft/configuration_bootstrap_state.h"
#include "raft/consensus.h"

#include <seastar/core/sstring.hh>

namespace raft::details {

/// reads all into the tmpbuf to file
future<temporary_buffer<char>> readfile(sstring name);

/// writes the tmpbuf to file
future<> writefile(sstring name, temporary_buffer<char> buf);

/// writes a yaml file with voted_for and term of the vote
future<>
persist_voted_for(sstring filename, consensus::voted_for_configuration);

/// reads the filename and returns voted_for and term of the vote
future<consensus::voted_for_configuration> read_voted_for(sstring filename);

/// copy all raft entries into N containers using the record_batch::share()
future<std::vector<std::vector<raft::entry>>>
copy_n(std::vector<raft::entry>&&, std::size_t);

/// parses the configuration out of the entry
future<raft::group_configuration> extract_configuration(raft::entry&&);

/// returns a fully parsed config state from a given storage log
future<raft::configuration_bootstrap_state> read_bootstrap_state(storage::log&);

} // namespace raft::details
