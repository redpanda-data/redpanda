
/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage_clients/client.h"
#include "thirdparty/libxml2/parser.h"

#include <stack>

namespace cloud_storage_clients {

class xml_parse_exception : public std::exception {
public:
    explicit xml_parse_exception(std::string_view error_message)
      : _error_message{error_message.data(), error_message.size()} {}

    const char* what() const noexcept override { return _error_message.data(); }

private:
    ss::sstring _error_message;
};

/// \brief The current tag the parser is processing, used to collect tag
/// contents
enum class xml_tag {
    key,
    size,
    last_modified,
    etag,
    is_truncated,
    prefix,
    next_continuation_token,
    unset,
};

struct parser_state {
    struct impl {
        explicit impl(std::optional<client::item_filter> = std::nullopt);
        impl(const impl&) = delete;
        impl& operator=(const impl&) = delete;
        impl& operator=(impl&&) = delete;

        impl(impl&&) = default;

        virtual void handle_start_element(std::string_view element_name) = 0;
        virtual void handle_end_element(std::string_view element_name) = 0;
        virtual void handle_characters(std::string_view characters) = 0;
        client::list_bucket_result parsed_items() const;

        virtual ~impl() = default;

    protected:
        std::optional<client::item_filter> _item_filter;
        client::list_bucket_result _items;
        std::optional<client::list_bucket_item> _current_item;

        xml_tag _current_tag;
        std::vector<ss::sstring> _tags;
    };

    explicit parser_state(std::unique_ptr<impl>);

    void handle_start_element(std::string_view element_name) {
        _impl->handle_start_element(element_name);
    }

    void handle_end_element(std::string_view element_name) {
        _impl->handle_end_element(element_name);
    }

    void handle_characters(std::string_view characters) {
        _impl->handle_characters(characters);
    }

    client::list_bucket_result parsed_items() const {
        return _impl->parsed_items();
    }

private:
    std::unique_ptr<impl> _impl;
};

struct aws_parse_impl final : public parser_state::impl {
    explicit aws_parse_impl(std::optional<client::item_filter> = std::nullopt);
    void handle_start_element(std::string_view element_name) override;
    void handle_end_element(std::string_view element_name) override;
    void handle_characters(std::string_view characters) override;

private:
    bool is_top_level() const;
    bool is_in_contents() const;
    bool is_in_common_prefixes() const;
};

struct abs_parse_impl final : public parser_state::impl {
    explicit abs_parse_impl(std::optional<client::item_filter> = std::nullopt);
    void handle_start_element(std::string_view element_name) override;
    void handle_end_element(std::string_view element_name) override;
    void handle_characters(std::string_view characters) override;

private:
    bool is_top_level() const;
    bool is_in_blob_properties() const;
    bool is_in_blob() const;
    bool is_in_blob_prefixes() const;
};

class xml_sax_parser {
public:
    xml_sax_parser(const xml_sax_parser&) = delete;
    xml_sax_parser& operator=(const xml_sax_parser&) = delete;
    xml_sax_parser& operator=(xml_sax_parser&&) = delete;

    xml_sax_parser() = default;
    /// \brief Placeholder to assert that parser is not moved mid-parse.
    xml_sax_parser(xml_sax_parser&& other) noexcept;

    void parse_chunk(ss::temporary_buffer<char> buffer);

    /// \brief Initializes the pointers in parser. This is decoupled from the
    /// constructor to allow for the parser to be initialized after move, for
    /// example when used in a `ss::do_with` construct, so that moving of
    /// internal state is avoided.
    void start_parse(std::unique_ptr<parser_state::impl> impl);

    /// \brief This function is expected to be called at the end of a parse
    /// after all the XML content has been processed through parse_chunk, to
    /// make sure that libxml2 parsing is finished.
    void end_parse();

    client::list_bucket_result result() const;

    /// \brief frees up the parser context pointer
    ~xml_sax_parser();

private:
    static void
    start_element(void* user_data, const xmlChar* name, const xmlChar**);
    static void end_element(void* user_data, const xmlChar* name);
    static void characters(void* user_data, const xmlChar* data, int size);

private:
    std::unique_ptr<parser_state> _state;
    std::unique_ptr<xmlSAXHandler> _handler;
    xmlParserCtxtPtr _ctx{nullptr};
};

} // namespace cloud_storage_clients
