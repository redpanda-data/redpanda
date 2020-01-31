#pragma once

#include "likely.h"
#include "model/record.h"
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

/// copy all record batch readers into N containers using the
/// record_batch::share()
ss::future<std::vector<model::record_batch_reader>>
share_n(model::record_batch_reader&&, std::size_t);

/// copy all readers into N containers using the record_batch::share()
/// it also wraps all the iobufs using the iobuf_share_foreign_n() method
/// which wraps the deallocator w/ a ss::submit_to() call
ss::future<std::vector<model::record_batch_reader>>
foreign_share_n(model::record_batch_reader&&, std::size_t);

/// parses the configuration out of the record_batch_reader
// if there are some configuration batch types
ss::future<std::optional<raft::group_configuration>>
extract_configuration(model::record_batch_reader&&);

/// serialize group configuration to the record_batch_reader
model::record_batch_reader serialize_configuration(group_configuration cfg);

/// returns a fully parsed config state from a given storage log
ss::future<raft::configuration_bootstrap_state>
  read_bootstrap_state(storage::log);

/// looks up for the broker with request id in a vector of brokers
template<typename Iterator>
Iterator find_machine(Iterator begin, Iterator end, model::node_id id) {
    return std::find_if(
      begin, end, [id](decltype(*begin) b) { return b.id() == id; });
}

inline constexpr model::offset next_offset(model::offset o) {
    if (o < model::offset{0}) {
        return model::offset{0};
    }
    return o + model::offset{1};
}

inline constexpr model::offset prev_offset(model::offset o) {
    if (o <= model::offset{0}) {
        return model::offset{};
    }
    return o - model::offset{1};
}

class term_assigning_reader : public model::record_batch_reader::impl {
public:
    term_assigning_reader(model::record_batch_reader r, model::term_id term)
      : _source(std::move(r).release())
      , _term(term) {}

    bool end_of_stream() const final { return _source->end_of_stream(); }

    ss::future<ss::circular_buffer<model::record_batch>>
    do_load_slice(model::timeout_clock::time_point tout) final {
        return _source->do_load_slice(tout).then(
          [t = _term](ss::circular_buffer<model::record_batch> ret) {
              for (auto& r : ret) {
                  r.set_term(t);
              }
              return std::move(ret);
          });
    }

private:
    std::unique_ptr<model::record_batch_reader::impl> _source;
    model::term_id _term;
};

// clang-format off
template<typename Func>
CONCEPT(
    requires requires(Func f, model::record_batch b){
        { f(std::move(b)) } 
            -> ss::futurize_t<std::result_of_t<Func(model::record_batch&&)>>;
    }
)
// clang-format on
// Consumer applying an async action to each element in the reader
class do_for_each_batch_consumer {
public:
    explicit do_for_each_batch_consumer(Func&& f)
      : _f(std::forward<Func>(f)) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        return _f(std::move(b)).then([] { return ss::stop_iteration::no; });
    }
    void end_of_stream() {}

    Func _f;
};

} // namespace raft::details
