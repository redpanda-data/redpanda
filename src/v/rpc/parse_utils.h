/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iostream.h"
#include "compression/async_stream_zstd.h"
#include "hashing/xx.h"
#include "likely.h"
#include "reflection/async_adl.h"
#include "rpc/logger.h"
#include "rpc/types.h"
#include "seastarx.h"
#include "vlog.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include <fmt/format.h>

#include <memory>
#include <optional>

namespace rpc {
namespace detail {
inline void check_out_of_range(size_t got, size_t expected) {
    if (unlikely(got != expected)) {
        throw std::out_of_range(fmt::format(
          "parse_utils out of range. got:{} bytes and expected:{} bytes",
          got,
          expected));
    }
}
} // namespace detail

inline ss::future<std::optional<header>>
parse_header(ss::input_stream<char>& in) {
    return read_iobuf_exactly(in, size_of_rpc_header).then([](iobuf b) {
        if (b.size_bytes() != size_of_rpc_header) {
            return ss::make_ready_future<std::optional<header>>();
        }
        iobuf_parser parser(std::move(b));
        auto h = reflection::adl<header>{}.from(parser);
        if (auto got = checksum_header_only(h);
            unlikely(h.header_checksum != got)) {
            vlog(
              rpclog.info,
              "rpc header missmatching checksums. expected:{}, got:{} - {}",
              h.header_checksum,
              got,
              h);
            return ss::make_ready_future<std::optional<header>>();
        }
        return ss::make_ready_future<std::optional<header>>(h);
    });
}

inline void validate_payload_and_header(const iobuf& io, const header& h) {
    detail::check_out_of_range(io.size_bytes(), h.payload_size);
    auto in = iobuf::iterator_consumer(io.cbegin(), io.cend());
    incremental_xxhash64 hasher;
    size_t consumed = in.consume(
      io.size_bytes(), [&hasher](const char* src, size_t sz) {
          hasher.update(src, sz);
          return ss::stop_iteration::no;
      });
    detail::check_out_of_range(consumed, h.payload_size);
    const auto got_checksum = hasher.digest();
    if (h.payload_checksum != got_checksum) {
        throw std::runtime_error(fmt::format(
          "invalid rpc checksum. got:{}, expected:{}",
          got_checksum,
          h.payload_checksum));
    }
}

/*
 * the transition from adl to serde encoding in rpc requires a period of time
 * where both encodings are supported for all message types. however, we do not
 * want to extend this requirement to brand new messages / services, nor to rpc
 * types used in coproc which will remain in legacy adl format for now.
 *
 * we use the type system to enforce these rules and allow types to be opt-out
 * on a case-by-case basis for adl (new messages) or serde (legacy like coproc).
 *
 * the `rpc_adl_exempt` and `rpc_serde_exempt` type trait helpers can be used to
 * opt-out a type T from adl or serde support. a type is marked exempt by
 * defining the type `T::rpc_(adl|serde)_exempt`.  the typedef may be defined as
 * any type such as std::{void_t, true_type}.
 *
 * Example:
 *
 *     struct exempt_msg {
 *         using rpc_adl_exempt = std::true_type;
 *         ...
 *     };
 *
 * then use the `is_rpc_adl_exempt` or `is_rpc_serde_exempt` concept to test.
 */
template<typename T>
concept is_rpc_adl_exempt = requires {
    typename T::rpc_adl_exempt;
};

template<typename T>
concept is_rpc_serde_exempt = requires {
    typename T::rpc_serde_exempt;
};

/*
 * Encode a client request for the given transport version.
 *
 * Unless the message type T is explicitly exempt from adl<> support, type T
 * must be supported by both adl<> and serde encoding frameworks. When the type
 * is not exempt from adl<> support, serde is used when the version >= v2.
 *
 * It is also possible to exempt a type from serde support (i.e. adl-only). This
 * is an interim solution that allows things like coproc and some tests to avoid
 * having to add serde support in the short term.
 *
 * The returned version indicates what level of encoding is used. This is always
 * equal to the input version, except for serde-only messags which return v2.
 * Callers are expected to further validate the runtime implications of this.
 */
template<typename T>
ss::future<transport_version>
encode_for_version(iobuf& out, T msg, transport_version version) {
    static_assert(!is_rpc_adl_exempt<T> || !is_rpc_serde_exempt<T>);

    if constexpr (is_rpc_serde_exempt<T>) {
        return reflection::async_adl<T>{}.to(out, std::move(msg)).then([] {
            return transport_version::v0;
        });
    } else if constexpr (is_rpc_adl_exempt<T>) {
        vassert(version >= transport_version::v2, "Can't encode serde <= v2");
        return ss::do_with(std::move(msg), [&out, version](T& msg) {
            return serde::write_async(out, std::move(msg)).then([version] {
                return version;
            });
        });
    } else {
        if (version < transport_version::v2) {
            return reflection::async_adl<T>{}
              .to(out, std::move(msg))
              .then([version] { return version; });
        } else {
            return ss::do_with(std::move(msg), [&out, version](T& msg) {
                return serde::write_async(out, std::move(msg)).then([version] {
                    return version;
                });
            });
        }
    }
}

/*
 * Decode a client request at the given transport version.
 */
template<typename T>
ss::future<T>
decode_for_version(iobuf_parser& parser, transport_version version) {
    static_assert(!is_rpc_adl_exempt<T> || !is_rpc_serde_exempt<T>);

    if constexpr (is_rpc_serde_exempt<T>) {
        if (version != transport_version::v0) {
            return ss::make_exception_future<T>(std::runtime_error(fmt::format(
              "Unexpected adl-only message {} at {} != v0",
              typeid(T).name(),
              version)));
        }
        return reflection::async_adl<T>{}.from(parser);
    } else if constexpr (is_rpc_adl_exempt<T>) {
        if (version < transport_version::v2) {
            return ss::make_exception_future<T>(std::runtime_error(fmt::format(
              "Unexpected serde-only message {} at {} < v2",
              typeid(T).name(),
              version)));
        }
        return serde::read_async<T>(parser);
    } else {
        if (version < transport_version::v2) {
            return reflection::async_adl<T>{}.from(parser);
        } else {
            return serde::read_async<T>(parser);
        }
    }
}

/*
 * type used to factor out version-specific functionality from request handling
 * in services. this is used so that tests can specialize behavior.
 *
 * this is the default mixin that is used by the code generator.
 */
struct default_message_codec {
    /*
     * decodes a request (server) or response (client)
     */
    template<typename T>
    static ss::future<T>
    decode(iobuf_parser& parser, transport_version version) {
        return decode_for_version<T>(parser, version);
    }

    /*
     * Used by the server to determine which version use when sending a response
     * back to the client. The default behavior is maintain the same version as
     * the received request.
     */
    static transport_version response_version(const header& h) {
        return h.version;
    }

    /*
     * encodes a request (client) or response (server)
     */
    template<typename T>
    static ss::future<transport_version>
    encode(iobuf& out, T msg, transport_version version) {
        return encode_for_version(out, std::move(msg), version);
    }
};

/*
 * service specialization mixin to create a v0 compliant service. a v0 service
 * encodes and decodes using adl, ignores versions on requests, and sends
 * replies with v0 in the header.
 *
 * example:
 *   using echo_service_v0 = echo_service_base<v0_message_codec>;
 *
 * Note that for serde-supported messages a vassert(false) is generated. First,
 * the v0_message_encoder is only used in tests. Second, serde usage is not
 * possible in v0 servers, so this restriction is realistic. And from a
 * practical standpoint this allows us to avoid bifurcation of services (or more
 * sfinae magic) in tests so that serde-only types were never present within a
 * service configured with a v0_message_encoder.
 */
struct v0_message_codec {
    template<typename T>
    static ss::future<T> decode(iobuf_parser& parser, transport_version) {
        if constexpr (is_rpc_adl_exempt<T>) {
            vassert(false, "Cannot use serde-only types in v0 server");
        } else {
            return reflection::async_adl<T>{}.from(parser);
        }
    }

    static transport_version response_version(const header&) {
        return transport_version::v0;
    }

    template<typename T>
    static ss::future<transport_version>
    encode(iobuf& out, T msg, transport_version) {
        if constexpr (is_rpc_adl_exempt<T>) {
            vassert(false, "Cannot use serde-only types in v0 server");
        } else {
            return reflection::async_adl<T>{}.to(out, std::move(msg)).then([] {
                return transport_version::v0;
            });
        }
    }
};

template<typename T, typename Codec>
ss::future<T> parse_type(ss::input_stream<char>& in, const header& h) {
    return read_iobuf_exactly(in, h.payload_size).then([h](iobuf io) {
        validate_payload_and_header(io, h);

        ss::future<iobuf> iobuf_fut = ss::make_ready_future<iobuf>();

        switch (h.compression) {
        case compression_type::none:
            iobuf_fut = ss::make_ready_future<iobuf>(std::move(io));
            break;
        case compression_type::zstd: {
            auto& zstd_inst = compression::async_stream_zstd_instance();
            iobuf_fut = zstd_inst.uncompress(std::move(io));
            break;
        }
        default:
            iobuf_fut = ss::make_exception_future<iobuf>(std::runtime_error(
              fmt::format("no compression supported. header: {}", h)));
        }

        return iobuf_fut.then([h](iobuf io) {
            auto p = std::make_unique<iobuf_parser>(std::move(io));
            auto raw = p.get();
            return Codec::template decode<T>(*raw, h.version)
              .finally([p = std::move(p)] {});
        });
    });
}

} // namespace rpc
