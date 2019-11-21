#include "raft/consensus_utils.h"

#include "resource_mgmt/io_priority.h"

#include <seastar/core/thread.hh>
// delete
#include <seastar/core/future-util.hh>

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

static inline void check_copy_out_of_range(size_t expected, size_t got) {
    if (__builtin_expect(expected != got, false)) {
        throw std::out_of_range("consensus_utils copy out of bounds");
    }
}

static inline std::vector<model::record_batch> share_n_record_batch(
  model::record_batch batch,
  const size_t copies,
  const bool use_foreign_iobuf_share) {
    std::vector<model::record_batch> ret;
    ret.reserve(copies);
    // the fast path
    if (!use_foreign_iobuf_share) {
        for (size_t i = 0; i < copies; ++i) {
            ret.push_back(batch.share());
        }
        return ret;
    }

    // foreign share

    if (batch.compressed()) {
        const auto hdr = batch.release_header();
        auto recs = std::move(batch).release();
        size_t recs_sz = recs.size();
        iobuf io = std::move(recs).release();
        auto vec = iobuf_share_foreign_n(std::move(io), copies);
        while (!vec.empty()) {
            ret.emplace_back(
              hdr,
              model::record_batch::compressed_records(
                recs_sz, std::move(vec.back())));
            vec.pop_back();
        }
        return ret;
    }

    std::vector<std::vector<model::record>> data(copies);
    for (model::record& r : batch) {
        auto const size_bytes = r.size_bytes();
        auto const attributes = r.attributes();
        auto const timestamp_delta = r.timestamp_delta();
        auto const offset_delta = r.offset_delta();
        auto kvec = iobuf_share_foreign_n(r.release_key(), copies);
        auto vvec = iobuf_share_foreign_n(
          std::move(r).release_packed_value_and_headers(), copies);
        for (auto& d : data) {
            iobuf key = std::move(kvec.back());
            iobuf val = std::move(vvec.back());
            kvec.pop_back();
            vvec.pop_back();

            d.push_back(model::record(
              size_bytes,
              attributes,
              timestamp_delta,
              offset_delta,
              std::move(key),
              std::move(val)));
        }
    }
    const auto hdr = batch.release_header();
    while (!data.empty()) {
        ret.emplace_back(hdr, std::move(data.back()));
        data.pop_back();
    }
    check_copy_out_of_range(ret.size(), copies);
    return ret;
}

static inline std::vector<std::vector<model::record_batch>> share_n_batches(
  std::vector<model::record_batch> batches,
  const size_t copies,
  const bool use_foreign_iobuf_share) {
    const size_t batches_size = batches.size();
    std::vector<std::vector<model::record_batch>> data(copies);
    for (auto& b : batches) {
        auto n_batches = share_n_record_batch(
          std::move(b), copies, use_foreign_iobuf_share);
        for (auto& d : data) {
            d.push_back(std::move(n_batches.back()));
            n_batches.pop_back();
        }
    }
    return data;
}

future<std::vector<raft::entry>> share_one_entry(
  raft::entry e, const size_t ncopies, const bool use_foreign_iobuf_share) {
    auto t = e.entry_type();
    return do_with(
             std::move(e),
             [ncopies, use_foreign_iobuf_share](raft::entry& e) {
                 return e.reader()
                   .consume(memory_batch_consumer(), model::no_timeout)
                   .then([=](std::vector<model::record_batch> batches) {
                       return share_n_batches(
                         std::move(batches), ncopies, use_foreign_iobuf_share);
                   });
             })
      .then([t, ncopies, use_foreign_iobuf_share](
              std::vector<std::vector<model::record_batch>> batches) {
          check_copy_out_of_range(ncopies, batches.size());
          std::vector<raft::entry> retval;
          retval.reserve(ncopies);

          while (!batches.empty()) {
              std::vector<model::record_batch> dbatches = std::move(
                batches.back());
              batches.pop_back();
              auto reader = model::make_memory_record_batch_reader(
                std::move(dbatches));
              retval.emplace_back(t, std::move(reader));
          }
          check_copy_out_of_range(ncopies, retval.size());
          return retval;
      });
}

// don't move out of impl file - internal detail
static inline future<std::vector<std::vector<raft::entry>>> share_entries(
  std::vector<raft::entry> r,
  const std::size_t ncopies,
  const bool use_foreign_iobuf_share) {
    using T = std::vector<raft::entry>;
    using ret_t = std::vector<T>;
    if (ncopies <= 1) {
        ret_t ret;
        ret.reserve(1);
        ret.push_back(std::move(r));
        return make_ready_future<ret_t>(std::move(ret));
    }
    return do_with(
      std::move(r),
      ret_t(ncopies),
      [ncopies, use_foreign_iobuf_share](T& src, ret_t& dst) {
          return do_until(
                   [&src] { return src.empty(); },
                   [ncopies, use_foreign_iobuf_share, &src, &dst]() mutable {
                       raft::entry e = std::move(src.back());
                       src.pop_back();
                       return share_one_entry(
                                std::move(e), ncopies, use_foreign_iobuf_share)
                         .then(
                           [ncopies, &dst](std::vector<raft::entry> entries) {
                               check_copy_out_of_range(ncopies, entries.size());
                               // move one entry into each container
                               for (std::vector<raft::entry>& i : dst) {
                                   i.push_back(std::move(entries.back()));
                                   entries.pop_back();
                               }
                           });
                   })
            .then([&dst] { return std::move(dst); });
      });
}

future<std::vector<std::vector<raft::entry>>>
foreign_share_n(std::vector<raft::entry> r, std::size_t ncopies) {
    return share_entries(std::move(r), ncopies, true);
}

future<std::vector<std::vector<raft::entry>>>
share_n(std::vector<raft::entry> r, std::size_t ncopies) {
    return share_entries(std::move(r), ncopies, false);
}

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

future<raft::group_configuration> extract_configuration(raft::entry e) {
    using cfg_t = raft::group_configuration;
    if (__builtin_expect(
          e.entry_type() != raft::configuration_batch_type, false)) {
        return make_exception_future<cfg_t>(std::runtime_error(fmt::format(
          "Configuration parser can only parse configs({}), asked "
          "to parse: {}",
          raft::configuration_batch_type,
          e.entry_type())));
    }
    return do_with(
             std::move(e),
             [](raft::entry& e) {
                 return e.reader().consume(
                   memory_batch_consumer(), model::no_timeout);
             })
      .then([](std::vector<model::record_batch> batches) {
          return do_with(
            std::move(batches),
            cfg_t{},
            [](std::vector<model::record_batch>& batches, cfg_t& cfg) {
                if (batches.empty()) {
                    return make_exception_future<cfg_t>(std::runtime_error(
                      "Invalid raft::entry configuration parsing"));
                }
                return do_for_each(
                         batches,
                         [&cfg](model::record_batch& b) {
                             if (b.type() != raft::configuration_batch_type) {
                                 return make_exception_future(
                                   std::runtime_error(
                                     "Inconsistent batch format for config"));
                             }
                             if (b.compressed()) {
                                 return make_exception_future(
                                   std::runtime_error(
                                     "Compressed configuration records are "
                                     "unsupported"));
                             }
                             auto it = std::prev(b.end());
                             auto val = it->release_packed_value_and_headers();
                             return rpc::deserialize<cfg_t>(std::move(val))
                               .then([&cfg](cfg_t c) { cfg = std::move(c); });
                         })
                  .then([&cfg] { return std::move(cfg); });
            });
      });
}

std::optional<model::broker>
find_machine(const std::vector<model::broker>& v, model::node_id id) {
    auto it = std::find_if(
      std::cbegin(v), std::cend(v), [id](const model::broker& b) {
          return b.id() == id;
      });
    return it != std::cend(v) ? std::optional<model::broker>(*it)
                              : std::nullopt;
}

} // namespace raft::details
