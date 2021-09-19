/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/server/replicated_partition.h"

#include "kafka/protocol/errors.h"
#include "model/fundamental.h"
#include "raft/types.h"
#include "storage/types.h"

#include <seastar/core/coroutine.hh>

#include <optional>

namespace kafka {
replicated_partition::replicated_partition(
  ss::lw_shared_ptr<cluster::partition> p) noexcept
  : _partition(p)
  , _translator(_partition->get_offset_translator()) {
    vassert(
      _translator, "ntp {}: offset translator must be initialized", p->ntp());
}

// TODO: use previous translation speed up lookup
ss::future<model::record_batch_reader> replicated_partition::make_reader(
  storage::log_reader_config cfg,
  std::optional<model::timeout_clock::time_point> deadline) {
    cfg.start_offset = _translator->to_log_offset(cfg.start_offset);
    cfg.max_offset = _translator->to_log_offset(cfg.max_offset);
    cfg.type_filter = {model::record_batch_type::raft_data};

    class reader : public model::record_batch_reader::impl {
    public:
        reader(
          std::unique_ptr<model::record_batch_reader::impl> underlying,
          const ss::lw_shared_ptr<raft::offset_translator>& tr)
          : _underlying(std::move(underlying))
          , _translator(tr) {}

        bool is_end_of_stream() const final {
            return _underlying->is_end_of_stream();
        }

        void print(std::ostream& os) final {
            fmt::print(os, "kafka::partition reader for ");
            _underlying->print(os);
        }
        using storage_t = model::record_batch_reader::storage_t;
        using data_t = model::record_batch_reader::data_t;
        using foreign_data_t = model::record_batch_reader::foreign_data_t;

        model::record_batch_reader::data_t& get_batches(storage_t& st) {
            if (std::holds_alternative<data_t>(st)) {
                return std::get<data_t>(st);
            } else {
                return *std::get<foreign_data_t>(st).buffer;
            }
        }

        ss::future<storage_t>
        do_load_slice(model::timeout_clock::time_point t) final {
            return _underlying->do_load_slice(t).then([this](storage_t recs) {
                for (auto& batch : get_batches(recs)) {
                    batch.header().base_offset = _translator->from_log_offset(
                      batch.base_offset());
                }
                return recs;
            });
        }

        ss::future<> finally() noexcept final { return _underlying->finally(); }

    private:
        std::unique_ptr<model::record_batch_reader::impl> _underlying;
        ss::lw_shared_ptr<raft::offset_translator> _translator;
    };
    auto rdr = co_await _partition->make_reader(cfg, deadline);
    co_return model::make_record_batch_reader<reader>(
      std::move(rdr).release(), _translator);
}

ss::future<std::optional<storage::timequery_result>>
replicated_partition::timequery(
  model::timestamp ts, ss::io_priority_class io_pc) {
    return _partition->timequery(ts, io_pc).then(
      [this](std::optional<storage::timequery_result> r) {
          if (r) {
              r->offset = _translator->from_log_offset(r->offset);
          }
          return r;
      });
}

ss::future<result<model::offset>> replicated_partition::replicate(
  model::record_batch_reader rdr, raft::replicate_options opts) {
    using ret_t = result<model::offset>;
    return _partition->replicate(std::move(rdr), opts)
      .then([this](result<raft::replicate_result> r) {
          if (!r) {
              return ret_t(r.error());
          }
          return ret_t(_translator->from_log_offset(r.value().last_offset));
      });
}

raft::replicate_stages replicated_partition::replicate(
  model::batch_identity batch_id,
  model::record_batch_reader&& rdr,
  raft::replicate_options opts) {
    using ret_t = result<raft::replicate_result>;
    auto res = _partition->replicate_in_stages(batch_id, std::move(rdr), opts);
    res.replicate_finished = res.replicate_finished.then(
      [this](result<raft::replicate_result> r) {
          if (!r) {
              return ret_t(r.error());
          }
          return ret_t(raft::replicate_result{
            _translator->from_log_offset(r.value().last_offset)});
      });
    return res;
}
} // namespace kafka
