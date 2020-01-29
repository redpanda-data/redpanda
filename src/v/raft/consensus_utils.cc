#include "raft/consensus_utils.h"

#include "likely.h"
#include "resource_mgmt/io_priority.h"
#include "storage/record_batch_builder.h"

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
ss::future<ss::temporary_buffer<char>> readfile(ss::sstring name) {
    return ss::open_file_dma(
             std::move(name), ss::open_flags::ro | ss::open_flags::create)
      .then([](ss::file f) {
          return f.size()
            .then([f](uint64_t size) mutable {
                return f.dma_read_bulk<char>(0, size);
            })
            .then([f](ss::temporary_buffer<char> b) mutable {
                return f.close().then(
                  [f, b = std::move(b)]() mutable { return std::move(b); });
            });
      });
}

ss::future<> writefile(ss::sstring name, ss::temporary_buffer<char> buf) {
    auto flags = ss::open_flags::wo | ss::open_flags::create
                 | ss::open_flags::truncate;
    return ss::open_file_dma(std::move(name), flags)
      .then([b = std::move(buf)](ss::file f) {
          auto out = ss::make_lw_shared<ss::output_stream<char>>(
            make_file_output_stream(std::move(f)));
          return out->write(b.get(), b.size())
            .then([out] { return out->flush(); })
            .then([out] { return out->close(); })
            .finally([out] {});
      });
}
ss::future<>
persist_voted_for(ss::sstring filename, consensus::voted_for_configuration r) {
    YAML::Emitter out;
    out << YAML::convert<consensus::voted_for_configuration>::encode(r);
    ss::temporary_buffer<char> buf(out.size());
    std::copy_n(out.c_str(), out.size(), buf.get_write());
    return writefile(filename, std::move(buf));
}

ss::future<consensus::voted_for_configuration>
read_voted_for(ss::sstring filename) {
    return readfile(filename).then([](ss::temporary_buffer<char> buf) {
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
ss::future<ss::stop_iteration>
memory_batch_consumer::operator()(model::record_batch b) {
    _result.push_back(std::move(b));
    return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::no);
}

std::vector<model::record_batch> memory_batch_consumer::end_of_stream() {
    return std::move(_result);
}

static inline void check_copy_out_of_range(size_t expected, size_t got) {
    if (unlikely(expected != got)) {
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
        auto kvec = iobuf_share_foreign_n(r.share_key(), copies);
        auto vvec = iobuf_share_foreign_n(
          std::move(r).share_packed_value_and_headers(), copies);
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

ss::future<std::vector<raft::entry>> share_one_entry(
  raft::entry e, const size_t ncopies, const bool use_foreign_iobuf_share) {
    auto t = e.entry_type();
    return ss::do_with(
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
static inline ss::future<std::vector<std::vector<raft::entry>>> share_entries(
  std::vector<raft::entry> r,
  const std::size_t ncopies,
  const bool use_foreign_iobuf_share) {
    using T = std::vector<raft::entry>;
    using ret_t = std::vector<T>;
    if (ncopies <= 1 && !use_foreign_iobuf_share) {
        ret_t ret;
        ret.reserve(1);
        ret.push_back(std::move(r));
        return ss::make_ready_future<ret_t>(std::move(ret));
    }
    return ss::do_with(
      std::move(r),
      ret_t(ncopies),
      [ncopies, use_foreign_iobuf_share](T& src, ret_t& dst) {
          return ss::do_until(
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

ss::future<std::vector<std::vector<raft::entry>>>
foreign_share_n(std::vector<raft::entry> r, std::size_t ncopies) {
    return share_entries(std::move(r), ncopies, true);
}

ss::future<std::vector<std::vector<raft::entry>>>
share_n(std::vector<raft::entry> r, std::size_t ncopies) {
    return share_entries(std::move(r), ncopies, false);
}

ss::future<configuration_bootstrap_state>
read_bootstrap_state(storage::log log) {
    // TODO(agallego, michal) - iterate the log in reverse
    // as an optimization
    return ss::async([log]() mutable {
        auto retval = configuration_bootstrap_state{};
        model::offset it = log.start_offset();
        const model::offset end = log.max_offset();
        if (end() < 0) {
            // empty log
            return retval;
        }
        do {
            auto rcfg = storage::log_reader_config{
              .start_offset = it,
              .max_bytes = 1024 * 1024 /*1MB*/,
              .min_bytes = 1,
              .prio = raft_priority(),
              .max_offset = end};
            auto reader = log.make_reader(rcfg);
            auto batches = reader
                             .consume(
                               memory_batch_consumer(), model::no_timeout)
                             .get0();
            if (batches.empty()) {
                return retval;
            }
            it = batches.back().last_offset() + model::offset(1);
            for (auto& batch : batches) {
                retval.process_batch_in_thread(std::move(batch));
            }
        } while (it < end);

        return retval;
    });
}

ss::future<raft::group_configuration> extract_configuration(raft::entry e) {
    using cfg_t = raft::group_configuration;
    if (unlikely(e.entry_type() != raft::configuration_batch_type)) {
        return ss::make_exception_future<cfg_t>(std::runtime_error(fmt::format(
          "Configuration parser can only parse configs({}), asked "
          "to parse: {}",
          raft::configuration_batch_type,
          e.entry_type())));
    }
    return ss::do_with(
             std::move(e),
             [](raft::entry& e) {
                 return e.reader().consume(
                   memory_batch_consumer(), model::no_timeout);
             })
      .then([](std::vector<model::record_batch> batches) {
          return ss::do_with(
            std::move(batches),
            cfg_t{},
            [](std::vector<model::record_batch>& batches, cfg_t& cfg) {
                if (batches.empty()) {
                    return ss::make_exception_future<cfg_t>(std::runtime_error(
                      "Invalid raft::entry configuration parsing"));
                }
                return ss::do_for_each(
                         batches,
                         [&cfg](model::record_batch& b) {
                             if (b.type() != raft::configuration_batch_type) {
                                 return ss::make_exception_future(
                                   std::runtime_error(
                                     "Inconsistent batch format for config"));
                             }
                             if (b.compressed()) {
                                 return ss::make_exception_future(
                                   std::runtime_error(
                                     "Compressed configuration records are "
                                     "unsupported"));
                             }
                             auto it = std::prev(b.end());
                             cfg = reflection::adl<cfg_t>{}.from(
                               it->share_packed_value_and_headers());
                             return ss::make_ready_future<>();
                         })
                  .then([&cfg] { return std::move(cfg); });
            });
      });
}

raft::entry serialize_configuration(group_configuration cfg) {
    auto batch = std::move(
                   storage::record_batch_builder(
                     raft::configuration_batch_type, model::offset(0))
                     .add_raw_kv(iobuf(), reflection::to_iobuf(std::move(cfg))))
                   .build();
    std::vector<model::record_batch> batches;
    batches.push_back(std::move(batch));
    return raft::entry(
      raft::configuration_batch_type,
      model::make_memory_record_batch_reader(std::move(batches)));
}

static inline std::vector<std::pair<size_t, size_t>>
batch_type_positions(const std::vector<model::record_batch>& v) {
    std::vector<std::pair<size_t, size_t>> ret;
    for (size_t i = 0; i < v.size(); /*in body loop increment*/) {
        size_t j = i;
        const auto type = v[i].type();
        for (; j < v.size() && type == v[j].type(); ++j) {
            /*do nothing*/
        }
        ret.emplace_back(i, j);
        i = j;
    }
    return ret;
}
/// in order traversal creates a raft::entry every time it encounters
/// a different record_batch.type() as all raft entries _must_ be for the
/// same record_batch type
std::vector<raft::entry>
batches_as_entries(std::vector<model::record_batch> batches) {
    std::vector<raft::entry> ret;
    const auto indices = batch_type_positions(batches);
    for (auto& p : indices) {
        std::vector<model::record_batch> b;
        b.reserve(p.second - p.first);
        std::move(
          batches.begin() + p.first,
          batches.begin() + p.second,
          std::back_inserter(b));
        const auto type = b.front().type();
        auto e = raft::entry(
          type, model::make_memory_record_batch_reader(std::move(b)));
        ret.emplace_back(std::move(e));
    }
    return ret;
}

} // namespace raft::details
