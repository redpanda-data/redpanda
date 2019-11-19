#include "raft/consensus_utils.h"

#include "resource_mgmt/io_priority.h"

#include <seastar/core/thread.hh>

#include <yaml-cpp/yaml.h>

namespace YAML {
template<>
struct convert<::raft::consensus::voted_for_configuration> {
    static Node encode(const ::raft::consensus::voted_for_configuration& c) {
        Node node;
        node["voted_for"] = c.voted_for();
        node["term"] = c.term();
        return node;
    }
    static bool
    decode(const Node& node, ::raft::consensus::voted_for_configuration& c) {
        for (auto ref : {"term", "voted_for"}) {
            if (!node[ref]) {
                return false;
            }
        }
        c.voted_for = model::node_id(
          node["voted_for"].as<model::node_id::type>());
        c.term = model::term_id(node["term"].as<model::term_id::type>());
        return true;
    }
};
} // namespace YAML

namespace raft::details {
future<temporary_buffer<char>> readfile(sstring name) {
    return open_file_dma(std::move(name), open_flags::ro | open_flags::create)
      .then([](file f) {
          return f.size()
            .then([f](uint64_t size) mutable {
                return f.dma_read_bulk<char>(0, size);
            })
            .then([f](temporary_buffer<char> b) mutable {
                return f.close().then(
                  [f, b = std::move(b)]() mutable { return std::move(b); });
            });
      });
}

future<> writefile(sstring name, temporary_buffer<char> buf) {
    auto flags = open_flags::wo | open_flags::create | open_flags::truncate;
    return open_file_dma(std::move(name), flags)
      .then([b = std::move(buf)](file f) {
          auto out = make_lw_shared<output_stream<char>>(
            make_file_output_stream(std::move(f)));
          return out->write(b.get(), b.size())
            .then([out] { return out->flush(); })
            .then([out] { return out->close(); })
            .finally([out] {});
      });
}
future<>
persist_voted_for(sstring filename, consensus::voted_for_configuration r) {
    YAML::Emitter out;
    out << YAML::convert<consensus::voted_for_configuration>::encode(r);
    temporary_buffer<char> buf(out.size());
    std::copy_n(out.c_str(), out.size(), buf.get_write());
    return writefile(filename, std::move(buf));
}

future<consensus::voted_for_configuration> read_voted_for(sstring filename) {
    return readfile(filename).then([](temporary_buffer<char> buf) {
        if (buf.empty()) {
            return consensus::voted_for_configuration{};
        }
        // unfort it uses a std::string override
        // XXX update when YAML::Load supports string_view()'s
        // see https://github.com/jbeder/yaml-cpp/issues/765
        std::string s(buf.get(), buf.size());
        auto node = YAML::Load(s);
        return node.as<consensus::voted_for_configuration>();
    });
}

future<std::vector<std::vector<raft::entry>>>
share_n(std::vector<raft::entry>&& r, std::size_t ncopies) {
    using T = std::vector<raft::entry>;
    using ret_t = std::vector<T>;
    ret_t retval(ncopies);
    if (ncopies <= 1) {
        retval[0] = std::move(r);
        return make_ready_future<ret_t>(std::move(retval));
    }
    class consumer_type {
    public:
        explicit consumer_type(model::record_batch_type type, ret_t& ref)
          : _type(type)
          , _ref(ref) {
        }
        future<stop_iteration> operator()(model::record_batch batch) {
            _staging.push_back(std::move(batch));
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
        void end_of_stream() {
            // simply move the last one in
            for (auto i = 0; i < _ref.size() - 1; ++i) {
                std::vector<model::record_batch> batch_vec;
                batch_vec.reserve(_staging.size());
                std::transform(
                  _staging.begin(),
                  _staging.end(),
                  std::back_inserter(batch_vec),
                  [](model::record_batch& b) { return b.share(); });
                _ref[i].push_back(raft::entry(
                  _type,
                  model::make_memory_record_batch_reader(
                    std::move(batch_vec))));
            }
            _ref.back().push_back(raft::entry(
              _type,
              model::make_memory_record_batch_reader(std::move(_staging))));
        }

    private:
        model::record_batch_type _type;
        std::vector<model::record_batch> _staging;
        ret_t& _ref;
    };
    return do_with(std::move(r), std::move(retval), [](T& src, ret_t& dst) {
        // clang-format off
        return do_for_each(src, [&dst](raft::entry& e) {
            return e.reader().consume(
                     consumer_type(e.entry_type(), dst), model::no_timeout);
        }) .then([&dst] { return std::move(dst); });
        // clang-format on
    });
}

class memory_batch_consumer {
public:
    future<stop_iteration> operator()(model::record_batch b) {
        _result.push_back(std::move(b));
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }

    std::vector<model::record_batch> end_of_stream() {
        return std::move(_result);
    }

private:
    std::vector<model::record_batch> _result;
};

future<configuration_bootstrap_state> read_bootstrap_state(storage::log& log) {
    return async([&log]() mutable {
        auto retval = configuration_bootstrap_state{};
        // set term only if there are some segments already
        if (!log.segments().empty()) {
            retval.set_term((*log.segments().rbegin())->term());
        }
        auto it = log.segments().rbegin();
        auto end = log.segments().rend();
        for (; it != end && !retval.is_finished(); it++) {
            storage::log_reader_config rcfg{
              .start_offset = (*it)->base_offset(),
              .max_bytes = std::numeric_limits<size_t>::max(),
              .min_bytes = 0, // ok to be empty
              .prio = raft_priority()};
            auto reader = log.make_reader(rcfg);
            auto batches = reader
                             .consume(
                               memory_batch_consumer(), model::no_timeout)
                             .get0();
            for (auto& batch : batches) {
                retval.process_batch_in_thread(std::move(batch));
            }
        }
        if (it == end) {
            retval.set_end_of_log();
        }
        return retval;
    });
}

future<raft::group_configuration> extract_configuration(raft::entry&& e) {
    using cfg_t = raft::group_configuration;
    if (__builtin_expect(
          e.entry_type() != raft::configuration_batch_type, false)) {
        return make_exception_future<cfg_t>(std::runtime_error(fmt::format(
          "Configuration parser can only parse configs({}), asked "
          "to parse: {}",
          raft::configuration_batch_type,
          e.entry_type())));
    }
    struct consumer {
        raft::group_configuration* ptr;
        future<stop_iteration> operator()(model::record_batch b) {
            if (b.type() != raft::configuration_batch_type) {
                return make_exception_future<stop_iteration>(
                  std::runtime_error("Inconsistent batch format for config"));
            }
            if (b.compressed()) {
                return make_exception_future<stop_iteration>(std::runtime_error(
                  "Compressed configuration records are unsupported"));
            }

            return do_for_each(
                     b,
                     [this](model::record& r) {
                         cfg_count++;
                         return rpc::deserialize<cfg_t>(
                                  r.release_packed_value_and_headers())
                           .then([this](cfg_t c) { *ptr = std::move(c); });
                     })
              .then([] {
                  return make_ready_future<stop_iteration>(stop_iteration::no);
              });
        }
        void end_of_stream() {
            if (cfg_count == 0) {
                throw std::runtime_error("processed no configs");
            }
        }
        uint32_t cfg_count;
    };
    return do_with(cfg_t{}, std::move(e), [](cfg_t& cfg, raft::entry& e) {
        return e.reader()
          .consume(consumer{&cfg}, model::no_timeout)
          .then([&cfg] { return std::move(cfg); });
    });
}

std::optional<model::broker>
find_machine(const std::vector<model::broker>& v, model::node_id id) {
    auto it = std::find_if(
      std::cbegin(v), std::cend(v), [id](const model::broker& b) {
          return b.id() == id;
      });
    return it != std::cend(v) ? std::optional<model::broker>(*it) : std::nullopt;
}

} // namespace raft::details
