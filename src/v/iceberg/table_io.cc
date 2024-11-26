// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/table_io.h"

#include "bytes/iobuf_parser.h"
#include "iceberg/table_metadata_json.h"
#include "json/chunked_buffer.h"

namespace iceberg {

ss::future<checked<table_metadata, metadata_io::errc>>
table_io::download_table_meta(const table_metadata_path& path) {
    return download_object<table_metadata>(
      path(), "iceberg::table_metadata", [](iobuf b) {
          iobuf_parser p(std::move(b));
          auto sz = p.bytes_left();
          json::Document parsed;
          parsed.Parse(p.read_string(sz));
          return parse_table_meta(parsed);
      });
}
ss::future<checked<size_t, metadata_io::errc>> table_io::upload_table_meta(
  const table_metadata_path& path, const table_metadata& m) {
    return upload_object<table_metadata>(
      path(), m, "iceberg::table_metadata", [](const table_metadata& m) {
          json::chunked_buffer b;
          iceberg::json_writer w(b);
          json::rjson_serialize(w, m);
          return std::move(b).as_iobuf();
      });
}

ss::future<checked<int, metadata_io::errc>>
table_io::download_version_hint(const version_hint_path& path) {
    return download_object<int>(path(), "iceberg::version_hint", [](iobuf b) {
        iobuf_parser p(std::move(b));
        auto sz = p.bytes_left();
        auto version_str = p.read_string(sz);
        return std::stoi(version_str);
    });
}

ss::future<checked<size_t, metadata_io::errc>>
table_io::upload_version_hint(const version_hint_path& path, int version) {
    return upload_object<int>(
      path(), version, "iceberg::version_hint", [](int v) {
          return iobuf::from(fmt::to_string(v));
      });
}

ss::future<checked<bool, metadata_io::errc>>
table_io::version_hint_exists(const version_hint_path& path) {
    return object_exists(path, "iceberg::version_hint");
}

ss::future<checked<std::nullopt_t, metadata_io::errc>>
table_io::delete_all_metadata(const metadata_location_path& path) {
    retry_chain_node root_rcn(
      io_.as(),
      ss::lowres_clock::duration{30s},
      100ms,
      retry_strategy::polling);
    retry_chain_logger ctxlog(log, root_rcn);

    auto log_exception = [&](
                           const std::exception_ptr& ex, std::string_view ctx) {
        auto level = ssx::is_shutdown_exception(ex) ? ss::log_level::debug
                                                    : ss::log_level::warn;
        vlogl(ctxlog, level, "exception while {} in {}: {}", ctx, path, ex);
    };

    // deleting may require several iterations if list_objects doesn't return
    // everything at once.
    while (true) {
        retry_chain_node list_rcn(10ms, retry_strategy::backoff, &root_rcn);
        auto list_fut = co_await ss::coroutine::as_future(io_.list_objects(
          bucket_, list_rcn, cloud_storage_clients::object_key{path}));
        if (list_fut.failed()) {
            log_exception(list_fut.get_exception(), "listing objects");
            co_return errc::failed;
        }

        auto list_res = std::move(list_fut.get());
        if (list_res.has_error()) {
            co_return errc::failed;
        }

        chunked_vector<cloud_storage_clients::object_key> to_delete;
        to_delete.reserve(list_res.value().contents.size());
        for (auto& obj : list_res.value().contents) {
            vlog(ctxlog.debug, "deleting metadata object {}", obj.key);
            to_delete.emplace_back(std::move(obj.key));
        }

        retry_chain_node delete_rcn(10ms, retry_strategy::backoff, &root_rcn);
        auto delete_fut = co_await ss::coroutine::as_future(io_.delete_objects(
          bucket_, std::move(to_delete), delete_rcn, [](size_t) {}));
        if (delete_fut.failed()) {
            log_exception(delete_fut.get_exception(), "deleting objects");
            co_return errc::failed;
        }

        switch (delete_fut.get()) {
        case cloud_io::upload_result::success:
            break;
        case cloud_io::upload_result::timedout:
            co_return errc::timedout;
        case cloud_io::upload_result::cancelled:
            co_return errc::shutting_down;
        case cloud_io::upload_result::failed:
            co_return errc::failed;
        }

        if (!list_res.value().is_truncated) {
            // deleted everything
            break;
        }

        auto retry = root_rcn.retry();
        if (!retry.is_allowed) {
            co_return errc::timedout;
        }
        co_await ss::sleep_abortable(retry.delay, *retry.abort_source);
    }

    co_return std::nullopt;
}

} // namespace iceberg
