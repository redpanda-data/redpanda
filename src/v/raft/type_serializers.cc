#include "raft/types.h"
#include "rpc/serialize.h"
#include "rpc/types.h"

namespace rpc {

template<>
void serialize(bytes_ostream& out, model::broker&& r) {
    rpc::serialize(out, r.id()());
    rpc::serialize(out, sstring(r.host()));
    rpc::serialize(out, r.port());
}

// from wire
template<>
inline future<model::offset> deserialize(source& in) {
    using type = model::offset::type;
    return deserialize<type>(in).then([] (type v) {
        return model::offset{std::move(v)};
    });
}
template<>
future<model::broker> deserialize(source& in) {
    struct broker_contents {
        model::node_id id;
        sstring host;
        int32_t port;
        std::optional<sstring> rack;
    };
    return deserialize<broker_contents>(in).then([](broker_contents res) {
        return model::broker(
          std::move(res.id),
          std::move(res.host),
          res.port,
          std::move(res.rack));
    });
}
    });
}

} // namespace rpc
