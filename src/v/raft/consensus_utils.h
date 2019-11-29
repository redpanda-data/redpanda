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
  share_n(std::vector<raft::entry>, std::size_t);

/// copy all raft entries into N containers using the record_batch::share()
/// it also wraps all the iobufs using the iobuf_share_foreign_n() method
/// which wraps the deallocator w/ a seastar::submit_to() call
future<std::vector<std::vector<raft::entry>>>
  foreign_share_n(std::vector<raft::entry>, std::size_t);

/// shares the contents of the entry in memory; should not be used w/ disk
/// backed record_batch_reader
future<std::vector<raft::entry>> share_one_entry(
  raft::entry, const size_t ncopies, const bool use_foreign_iobuf_share);

/// parses the configuration out of the entry
future<raft::group_configuration> extract_configuration(raft::entry);

/// serialize group configuration to the entry
raft::entry serialize_configuration(group_configuration cfg);

/// returns a fully parsed config state from a given storage log
future<raft::configuration_bootstrap_state> read_bootstrap_state(storage::log&);

/// looks up for the broker with request id in a vector of brokers
std::optional<model::broker>
find_machine(const std::vector<model::broker>&, model::node_id);

} // namespace raft::details
