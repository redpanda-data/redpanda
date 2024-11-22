// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/uri.h"

#include "cloud_io/provider.h"
#include "model/fundamental.h"
#include "ssx/sformat.h"
#include "thirdparty/ada/ada.h"

#include <seastar/util/variant_utils.hh>

namespace iceberg {

uri_converter::uri_converter(cloud_io::provider p)
  : _provider(std::move(p)) {}

uri uri_converter::to_uri(
  const cloud_storage_clients::bucket_name& bucket,
  const std::filesystem::path& path) const {
    return ss::visit(
      _provider,
      [&](const cloud_io::s3_compat_provider& s3_compat) {
          return uri(ssx::sformat(
            "{}://{}/{}", s3_compat.scheme, bucket(), path.native()));
      },
      [&](const cloud_io::abs_provider& abs) {
          // Always write to the dfs endpoint otherwise Trino w/ REST catalog
          // freak out due to endpoint mismatch. Also, they don't seem to work
          // when configured with the blob endpoint.
          //
          // It seems that Iceberg clients know how to handle this type of
          // endpoint for both types of storage accounts (blob/legacy/flat
          // namespace and dfs/data lake gen2).
          return uri(ssx::sformat(
            "abfss://{}@{}.dfs.core.windows.net/{}",
            bucket(),
            abs.account_name,
            path.native()));
      });
}

std::optional<std::filesystem::path> uri_converter::from_uri(
  const cloud_storage_clients::bucket_name& bucket, const uri& input) const {
    ada::result<> input_parse_result = ada::parser::parse_url(input());
    if (!input_parse_result) {
        return std::nullopt;
    }
    auto uri = input_parse_result.value();

    return ss::visit(
      _provider,
      [&](const cloud_io::s3_compat_provider& s3_compat)
        -> std::optional<std::filesystem::path> {
          const auto& protocol = uri.get_protocol();
          // Make sure that the protocol matches the provider.
          if (
            protocol.size() != s3_compat.scheme.size() + 1
            || !std::equal(
              s3_compat.scheme.begin(),
              s3_compat.scheme.end(),
              protocol.begin())
            || protocol.back() != ':') {
              return std::nullopt;
          }
          auto dotpos = uri.get_host().find('.');
          if (dotpos == std::string::npos) {
              // Host is the bucket name.
              if (uri.get_host() != bucket()) {
                  return std::nullopt;
              }
          } else {
              // We are not generating yet virtual host style URIs so don't
              // parse them yet.
              return std::nullopt;
          }
          // Ensure that minimum path length is at least enough to cover the
          // case of `/a`.
          if (uri.get_pathname().size() < 2) {
              return std::nullopt;
          }
          std::string_view path = uri.get_pathname();
          path.remove_prefix(1);
          // Rest is the object path/key.
          return std::make_optional<std::filesystem::path>(path);
      },
      [&](const cloud_io::abs_provider& abs)
        -> std::optional<std::filesystem::path> {
          // Single protocol support for now.
          if (uri.get_protocol() != "abfss:") {
              return std::nullopt;
          }
          // Bucket matches.
          if (uri.get_username() != bucket()) {
              return std::nullopt;
          }
          // Account name matches in the host of format
          // a) <account_name>.blob.core.microsoft.net.
          // b) <account_name>.dfs.core.windows.net
          auto dotpos = uri.get_host().find('.');
          if (dotpos == std::string::npos) {
              return std::nullopt;
          }
          if (uri.get_host().substr(0, dotpos) != abs.account_name) {
              return std::nullopt;
          }
          // Ensure that minimum path length is at least enough to cover the
          // case of `/a`.
          if (uri.get_pathname().size() < 2) {
              return std::nullopt;
          }
          std::string_view path = uri.get_pathname();
          path.remove_prefix(1);
          return std::make_optional<std::filesystem::path>(path);
      });
}

}; // namespace iceberg
