#pragma once

#include "raft/configuration_bootstrap_state.h"
#include "raft/consensus.h"

#include <seastar/core/sstring.hh>

namespace raft::details {

/// reads all into the tmpbuf to file
ss::future<ss::temporary_buffer<char>> readfile(ss::sstring name);

/// writes the tmpbuf to file
ss::future<> writefile(ss::sstring name, ss::temporary_buffer<char> buf);

/// writes a yaml file with voted_for and term of the vote
ss::future<>
persist_voted_for(ss::sstring filename, consensus::voted_for_configuration);

/// reads the filename and returns voted_for and term of the vote
ss::future<consensus::voted_for_configuration>
read_voted_for(ss::sstring filename);

/// copy all raft entries into N containers using the record_batch::share()
ss::future<std::vector<std::vector<raft::entry>>>
  share_n(std::vector<raft::entry>, std::size_t);

/// copy all raft entries into N containers using the record_batch::share()
/// it also wraps all the iobufs using the iobuf_share_foreign_n() method
/// which wraps the deallocator w/ a ss::submit_to() call
ss::future<std::vector<std::vector<raft::entry>>>
  foreign_share_n(std::vector<raft::entry>, std::size_t);

/// shares the contents of the entry in memory; should not be used w/ disk
/// backed record_batch_reader
ss::future<std::vector<raft::entry>> share_one_entry(
  raft::entry, const size_t ncopies, const bool use_foreign_iobuf_share);

/// parses the configuration out of the entry
ss::future<raft::group_configuration> extract_configuration(raft::entry);

/// serialize group configuration to the entry
raft::entry serialize_configuration(group_configuration cfg);

/// returns a fully parsed config state from a given storage log
ss::future<raft::configuration_bootstrap_state>
  read_bootstrap_state(storage::log);

/// looks up for the broker with request id in a vector of brokers
template<typename Iterator>
Iterator find_machine(Iterator begin, Iterator end, model::node_id id) {
    return std::find_if(
      begin, end, [id](decltype(*begin) b) { return b.id() == id; });
}

class memory_batch_consumer {
public:
    ss::future<ss::stop_iteration> operator()(model::record_batch);
    std::vector<model::record_batch> end_of_stream();

private:
    std::vector<model::record_batch> _result;
};

/// in order traversal creates a raft::entry every time it encounters
/// a different record_batch.type() as all raft entries _must_ be for the
/// same record_batch type
std::vector<raft::entry> batches_as_entries(std::vector<model::record_batch>);

} // namespace raft::details
