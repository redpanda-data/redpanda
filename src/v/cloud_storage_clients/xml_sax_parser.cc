
/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/xml_sax_parser.h"

#include "cloud_storage_clients/logger.h"
#include "cloud_storage_clients/util.h"

namespace {
struct aws_tags {
    static constexpr std::string_view list_bucket_results{"ListBucketResult"};
    static constexpr std::string_view contents{"Contents"};
    static constexpr std::string_view key{"Key"};
    static constexpr std::string_view size{"Size"};
    static constexpr std::string_view last_modified{"LastModified"};
    static constexpr std::string_view etag{"ETag"};
    static constexpr std::string_view is_truncated{"IsTruncated"};
    static constexpr std::string_view prefix{"Prefix"};
    static constexpr std::string_view next_continuation_token{
      "NextContinuationToken"};
    static constexpr std::string_view common_prefixes{"CommonPrefixes"};
};

struct abs_tags {
    static constexpr std::string_view prefix{"Prefix"};
    static constexpr std::string_view next_marker{"NextMarker"};
    static constexpr std::string_view blob{"Blob"};
    static constexpr std::string_view name{"Name"};
    static constexpr std::string_view size{"Content-Length"};
    static constexpr std::string_view etag{"Etag"};
    static constexpr std::string_view last_modified{"Last-Modified"};
    static constexpr std::string_view blob_prefix{"BlobPrefix"};
    static constexpr std::string_view enumeration_results{"EnumerationResults"};
    static constexpr std::string_view properties{"Properties"};
};

} // namespace

namespace cloud_storage_clients {

static parser_state* load_state(void* user_data) {
    return reinterpret_cast<parser_state*>(user_data);
}

bool aws_parse_impl::is_top_level() const {
    return _tags.size() == 1 && _tags[0] == aws_tags::list_bucket_results;
}

bool aws_parse_impl::is_in_contents() const {
    return _tags.size() == 2 && _tags[0] == aws_tags::list_bucket_results
           && _tags[1] == aws_tags::contents;
}

bool aws_parse_impl::is_in_common_prefixes() const {
    return _tags.size() == 2 && _tags[0] == aws_tags::list_bucket_results
           && _tags[1] == aws_tags::common_prefixes;
}

parser_state::parser_state(std::unique_ptr<impl> impl)
  : _impl(std::move(impl)) {}

parser_state::impl::impl(std::optional<client::item_filter> gather_item_if)
  : _item_filter(std::move(gather_item_if))
  , _current_tag(xml_tag::unset) {}

aws_parse_impl::aws_parse_impl(
  std::optional<client::item_filter> gather_item_if)
  : parser_state::impl{std::move(gather_item_if)} {}

void aws_parse_impl::handle_start_element(std::string_view element_name) {
    if (element_name == aws_tags::contents && is_top_level()) {
        // Reinitialize the item in preparation for next values
        _current_item.emplace();
    } else if (element_name == aws_tags::key && is_in_contents()) {
        _current_tag = xml_tag::key;
    } else if (element_name == aws_tags::size && is_in_contents()) {
        _current_tag = xml_tag::size;
    } else if (element_name == aws_tags::last_modified && is_in_contents()) {
        _current_tag = xml_tag::last_modified;
    } else if (element_name == aws_tags::etag && is_in_contents()) {
        _current_tag = xml_tag::etag;
    } else if (element_name == aws_tags::is_truncated && is_top_level()) {
        _current_tag = xml_tag::is_truncated;
    } else if (
      element_name == aws_tags::prefix
      && (is_top_level() || is_in_common_prefixes())) {
        _current_tag = xml_tag::prefix;
    } else if (
      element_name == aws_tags::next_continuation_token && is_top_level()) {
        _current_tag = xml_tag::next_continuation_token;
    }
    _tags.emplace_back(element_name.data(), element_name.size());
}

void aws_parse_impl::handle_end_element(std::string_view element_name) {
    _tags.pop_back();
    if (element_name == aws_tags::contents && _current_item) {
        if (!_item_filter || _item_filter.value()(_current_item.value())) {
            _items.contents.push_back(std::move(_current_item.value()));
        }
        _current_item.reset();
    }

    _current_tag = xml_tag::unset;
}

void aws_parse_impl::handle_characters(std::string_view characters) {
    switch (_current_tag) {
    case xml_tag::key:
        if (_current_item) {
            _current_item->key = {characters.data(), characters.size()};
        } else {
            throw xml_parse_exception{
              "Invalid state: parsing Key when not in Contents tag"};
        }
        break;
    case xml_tag::size:
        if (_current_item) {
            _current_item->size_bytes = std::stoll(
              {characters.data(), characters.size()});
        } else {
            throw xml_parse_exception{
              "Invalid state: parsing Size when not in Contents tag"};
        }
        break;
    case xml_tag::last_modified:
        if (_current_item) {
            _current_item->last_modified = util::parse_timestamp(characters);
        } else {
            throw xml_parse_exception{
              "Invalid state: parsing LastModified when not in Contents tag"};
        }
        break;
    case xml_tag::etag:
        if (_current_item) {
            _current_item->etag = {characters.data(), characters.size()};
        } else {
            throw xml_parse_exception{
              "Invalid state: parsing ETag when not in Contents tag"};
        }
        break;
    case xml_tag::is_truncated:
        _items.is_truncated = characters == "true";
        break;
    case xml_tag::prefix:
        // Parsing prefix at the top level: ListBucketResult -> Prefix
        if (_tags.size() == 2) {
            _items.prefix = {characters.data(), characters.size()};
            // Parsing common prefixes: ListBucketResult -> CommonPrefixes ->
            // Prefix
        } else if (_tags.size() == 3 && _tags[1] == aws_tags::common_prefixes) {
            _items.common_prefixes.emplace_back(
              characters.data(), characters.size());
        }
        break;
    case xml_tag::next_continuation_token:
        _items.next_continuation_token = {characters.data(), characters.size()};
        break;
    case xml_tag::unset:
        return;
    }
}

abs_parse_impl::abs_parse_impl(
  std::optional<client::item_filter> gather_item_if)
  : parser_state::impl{std::move(gather_item_if)} {}

bool abs_parse_impl::is_top_level() const {
    return _tags.size() == 1 && _tags[0] == abs_tags::enumeration_results;
}

bool abs_parse_impl::is_in_blob_properties() const {
    return _tags.size() == 4 && _tags.back() == abs_tags::properties;
}

bool abs_parse_impl::is_in_blob() const {
    return _tags.size() == 3 && _tags.back() == abs_tags::blob;
}

bool abs_parse_impl::is_in_blob_prefixes() const {
    return _tags.size() == 3 && _tags.back() == abs_tags::blob_prefix;
}

void abs_parse_impl::handle_start_element(std::string_view element_name) {
    if (
      (element_name == abs_tags::blob || element_name == abs_tags::blob_prefix)
      && _tags.size() == 2) {
        // Reinitialize the item in preparation for next values
        _current_item.emplace();
    } else if (element_name == abs_tags::name) {
        // The current element is either EnumerationResults->Blobs->Blob->Name
        if (is_in_blob()) {
            _current_tag = xml_tag::key;
            // Or EnumerationResults->Blobs->BlobPrefix->Name
        } else if (is_in_blob_prefixes()) {
            _current_tag = xml_tag::prefix;
        }
    } else if (element_name == abs_tags::size && is_in_blob_properties()) {
        _current_tag = xml_tag::size;
    } else if (
      element_name == abs_tags::last_modified && is_in_blob_properties()) {
        _current_tag = xml_tag::last_modified;
    } else if (element_name == abs_tags::etag && is_in_blob_properties()) {
        _current_tag = xml_tag::etag;
    } else if (element_name == abs_tags::next_marker && is_top_level()) {
        _current_tag = xml_tag::next_continuation_token;
    } else if (element_name == abs_tags::prefix && is_top_level()) {
        _current_tag = xml_tag::prefix;
    }
    _tags.emplace_back(element_name.data(), element_name.size());
}

void abs_parse_impl::handle_end_element(std::string_view element_name) {
    _tags.pop_back();

    // ABS returns a non empty NextMarker when the result is truncated. There
    // is no explicit field for is_truncated, so we infer this value from the
    // size of NextMarker.
    if (element_name == abs_tags::next_marker) {
        _items.is_truncated = !_items.next_continuation_token.empty();
    }

    if (element_name == abs_tags::blob && _current_item) {
        if (!_item_filter || _item_filter.value()(_current_item.value())) {
            _items.contents.push_back(std::move(_current_item.value()));
        }
        _current_item.reset();
    }

    _current_tag = xml_tag::unset;
}

void abs_parse_impl::handle_characters(std::string_view characters) {
    switch (_current_tag) {
    case xml_tag::key:
        if (_current_item) {
            _current_item->key = {characters.data(), characters.size()};
        } else {
            throw xml_parse_exception{
              "Invalid state: parsing Name when not in Blob tag"};
        }
        break;
    case xml_tag::size:
        if (_current_item) {
            _current_item->size_bytes = std::stoll(
              {characters.data(), characters.size()});
        } else {
            throw xml_parse_exception{
              "Invalid state: parsing Size when not in Blob tag"};
        }
        break;
    case xml_tag::last_modified:
        if (_current_item) {
            _current_item->last_modified = util::parse_timestamp(characters);
        } else {
            throw xml_parse_exception{
              "Invalid state: parsing Last-Modified when not in Blob tag"};
        }
        break;
    case xml_tag::etag:
        if (_current_item) {
            _current_item->etag = {characters.data(), characters.size()};
        } else {
            throw xml_parse_exception{
              "Invalid state: parsing ETag when not in Blob tag"};
        }
        break;
    case xml_tag::next_continuation_token:
        _items.next_continuation_token = {characters.data(), characters.size()};
        _items.is_truncated = true;
        break;
    case xml_tag::prefix:
        // Parsing prefix at the top level: ListBucketResult -> Prefix
        if (_tags.size() == 2) {
            _items.prefix = {characters.data(), characters.size()};
            // Parsing common prefixes: EnumerationResults -> Blobs ->
            // BlobPrefix -> Name
        } else if (_tags.size() == 4) {
            _items.common_prefixes.emplace_back(
              characters.data(), characters.size());
        }
        break;
    case xml_tag::is_truncated:
        [[fallthrough]];
    case xml_tag::unset:
        return;
    }
}

client::list_bucket_result parser_state::impl::parsed_items() const {
    return _items;
}

xml_sax_parser::xml_sax_parser(xml_sax_parser&& other) noexcept {
    vassert(!other._ctx, "parser moved after starting parse operation");
}

xml_sax_parser::~xml_sax_parser() {
    if (_ctx) {
        xmlFreeParserCtxt(_ctx);
    }
}

void xml_sax_parser::parse_chunk(ss::temporary_buffer<char> buffer) {
    vassert(_ctx, "parser is not initialized");
    auto res = xmlParseChunk(_ctx, buffer.get(), buffer.size(), 0);
    if (res != 0) {
        auto last_error = xmlGetLastError();
        vlog(
          s3_log.error,
          "Failed to parse response from S3: {} [{}]",
          last_error->message,
          res);
        throw xml_parse_exception{last_error->message};
    }
}

void xml_sax_parser::start_parse(std::unique_ptr<parser_state::impl> impl) {
    _handler = std::make_unique<xmlSAXHandler>();
    _handler->startElement = start_element;
    _handler->endElement = end_element;
    _handler->characters = characters;

    _state = std::make_unique<parser_state>(std::move(impl));
    _ctx = xmlCreatePushParserCtxt(
      _handler.get(), _state.get(), nullptr, 0, nullptr);
}

void xml_sax_parser::end_parse() {
    vassert(_ctx, "parser is not initialized");
    auto res = xmlParseChunk(_ctx, nullptr, 0, 1);
    if (res != 0) {
        auto last_error = xmlGetLastError();
        vlog(
          s3_log.error,
          "Failed to end parse: {} [{}]",
          last_error->message,
          res);
        throw xml_parse_exception{last_error->message};
    }
}

client::list_bucket_result xml_sax_parser::result() const {
    return _state->parsed_items();
}

void xml_sax_parser::start_element(
  void* user_data, const xmlChar* name, const xmlChar**) {
    auto* state = load_state(user_data);
    std::string_view element_name{reinterpret_cast<const char*>(name)};
    state->handle_start_element(element_name);
}

void xml_sax_parser::end_element(void* user_data, const xmlChar* name) {
    auto* state = load_state(user_data);
    std::string_view element_name{reinterpret_cast<const char*>(name)};
    state->handle_end_element(element_name);
}

void xml_sax_parser::characters(
  void* user_data, const xmlChar* data, int size) {
    auto* state = load_state(user_data);
    state->handle_characters(
      {reinterpret_cast<const char*>(data), static_cast<size_t>(size)});
}

} // namespace cloud_storage_clients
