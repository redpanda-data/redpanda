#pragma once
#include "kafka/types.h"
#include "model/fundamental.h"
#include "raft/configuration_manager.h"

namespace kafka {
/**
 * Offset translator performs conversion between kafka offsets, which doesn't
 * include raft configuration batches and log offsets which includes
 */
class offset_translator {
public:
    explicit offset_translator(const raft::configuration_manager& config_mgr)
      : _cfg_mgr(config_mgr) {}

    model::offset to_kafka_offset(model::offset o) const {
        auto d = delta(o);
        return model::offset(o - d);
    }

    model::offset from_kafka_offset(
      model::offset kafka_offset, model::offset hint = model::offset{}) const {
        if (kafka_offset == model::offset::max()) {
            return kafka_offset;
        }

        auto next_cfg_it = _cfg_mgr.lower_bound(std::max(hint, kafka_offset));
        // fast exit, current offset is larger than last config batch
        // offset, just add delta
        if (next_cfg_it == _cfg_mgr.end()) {
            --next_cfg_it;
            return kafka_offset + next_cfg_it->second.idx();
        }

        auto delta = model::offset(next_cfg_it->second.idx() - 1);
        // largest kafka offset that is achievable before reaching next
        // control batch
        auto max_cfg_ko = next_cfg_it->first - delta;
        while (max_cfg_ko <= kafka_offset) {
            // skip to the next configuration batch
            ++next_cfg_it;
            // since we jumped over to next config batch we can just increment
            // delta
            ++delta;
            if (next_cfg_it == _cfg_mgr.end()) {
                break;
            }
            max_cfg_ko = next_cfg_it->first - delta;
        }
        return kafka_offset + delta;
    }

private:
    int64_t delta(model::offset o) const {
        auto it = _cfg_mgr.lower_bound(o);

        if (it == _cfg_mgr.begin()) {
            /**
             * iterator points to the first configuration with offset greater
             * than then requsted one. Knowing an index of that configuration we
             * know that there was exactly (index -1) configurations with offset
             * lower than the current one. We can simply subtract one from
             * index.
             */
            return std::max<int64_t>(0, it->second.idx() - 1);
        }

        return std::prev(it)->second.idx();
    }

    const raft::configuration_manager& _cfg_mgr;
};

} // namespace kafka
