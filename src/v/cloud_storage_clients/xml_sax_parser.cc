
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
constexpr std::string_view list_bucket_results{"ListBucketResult"};
constexpr std::string_view contents{"Contents"};
constexpr std::string_view key{"Key"};
constexpr std::string_view size{"Size"};
constexpr std::string_view last_modified{"LastModified"};
constexpr std::string_view etag{"ETag"};
constexpr std::string_view is_truncated{"IsTruncated"};
constexpr std::string_view prefix{"Prefix"};
constexpr std::string_view next_continuation_token{"NextContinuationToken"};
constexpr std::string_view common_prefixes{"CommonPrefixes"};
} // namespace

namespace cloud_storage_clients {

static parser_state* load_state(void* user_data) {
    return reinterpret_cast<parser_state*>(user_data);
}

static bool is_top_level(const std::vector<ss::sstring>& tags) {
    return tags.size() == 1 && tags[0] == list_bucket_results;
}

static bool is_in_contents(const std::vector<ss::sstring>& tags) {
    return tags.size() == 2 && tags[0] == list_bucket_results
           && tags[1] == contents;
}

static bool is_in_common_prefixes(const std::vector<ss::sstring>& tags) {
    return tags.size() == 2 && tags[0] == list_bucket_results
           && tags[1] == common_prefixes;
}

parser_state::parser_state(std::optional<client::item_filter> gather_item_if)
  : _current_tag(xml_tag::unset)
  , _item_filter(std::move(gather_item_if)) {}

void parser_state::handle_start_element(std::string_view element_name) {
    if (element_name == contents && is_top_level(_tags)) {
        // Reinitialize the item in preparation for next values
        _current_item.emplace();
    } else if (element_name == key && is_in_contents(_tags)) {
        _current_tag = xml_tag::key;
    } else if (element_name == size && is_in_contents(_tags)) {
        _current_tag = xml_tag::size;
    } else if (element_name == last_modified && is_in_contents(_tags)) {
        _current_tag = xml_tag::last_modified;
    } else if (element_name == etag && is_in_contents(_tags)) {
        _current_tag = xml_tag::etag;
    } else if (element_name == is_truncated && is_top_level(_tags)) {
        _current_tag = xml_tag::is_truncated;
    } else if (
      element_name == prefix
      && (is_top_level(_tags) || is_in_common_prefixes(_tags))) {
        _current_tag = xml_tag::prefix;
    } else if (element_name == next_continuation_token && is_top_level(_tags)) {
        _current_tag = xml_tag::next_continuation_token;
    }
    _tags.emplace_back(element_name.data(), element_name.size());
}

void parser_state::handle_end_element(std::string_view element_name) {
    _tags.pop_back();
    if (element_name == contents && _current_item) {
        if (!_item_filter || _item_filter.value()(_current_item.value())) {
            _items.contents.push_back(std::move(_current_item.value()));
        }
        _current_item.reset();
    }

    _current_tag = xml_tag::unset;
}

void parser_state::handle_characters(std::string_view characters) {
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
        } else if (_tags.size() == 3 && _tags[1] == common_prefixes) {
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

client::list_bucket_result parser_state::parsed_items() const { return _items; }

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

void xml_sax_parser::start_parse(
  std::optional<client::item_filter> gather_item_if) {
    _handler = std::make_unique<xmlSAXHandler>();
    _handler->startElement = start_element;
    _handler->endElement = end_element;
    _handler->characters = characters;

    _state = std::make_unique<parser_state>(std::move(gather_item_if));
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
