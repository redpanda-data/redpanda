/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "encryption/field_transformer_ref.h"

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "crypto/crypto.h"
#include "crypto/ssl_utils.h"
#include "serde/avro/encoder.h"
#include "serde/avro/parser.h"
#include "serde/json/parser.h"
#include "serde/json/writer.h"
#include "serde/protobuf/encoder.h"
#include "serde/protobuf/parser.h"
#include "utils/base64.h"

#include <seastar/core/future.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/variant_utils.hh>

#include <avro/NodeImpl.hh>
#include <avro/Schema.hh>
#include <google/protobuf/descriptor.h>
#include <openssl/evp.h>

#include <coroutine>
#include <limits>
#include <stdexcept>
#include <variant>

namespace encryption {

namespace {

constexpr size_t gcm_iv_size = 12;
constexpr size_t gcm_tag_size = 16;

using evp_cipher_ctx_ptr
  = crypto::internal::handle<EVP_CIPHER_CTX, EVP_CIPHER_CTX_free>;

const EVP_CIPHER* cipher_for_key(bytes_view dek) {
    switch (dek.size()) {
    case 16:
        return EVP_aes_128_gcm();
    case 32:
        return EVP_aes_256_gcm();
    default:
        throw crypto::exception(
          fmt::format(
            "unsupported AES-GCM key size: {} bytes (need 16 or 32)",
            dek.size()));
    }
}

/// Walk an Avro parsed::message tree following a field path.
/// Returns a pointer to the leaf message node, or nullptr if the path
/// does not resolve (e.g. union branch is null).
///
/// NOTE: this traversal only handles singular record fields and unions.
/// Encryption of fields inside repeated/array/map containers is not
/// supported in this implementation.
serde::avro::parsed::message* walk_avro_path(
  serde::avro::parsed::message& root,
  const ::avro::ValidSchema& schema,
  const std::vector<ss::sstring>& path) {
    auto* current = &root;
    auto current_schema = schema.root();

    for (size_t i = 0; i < path.size(); ++i) {
        const auto& segment = path[i];

        // Unwrap unions: if the current node is a union, descend into it.
        while (
          std::holds_alternative<serde::avro::parsed::avro_union>(*current)) {
            auto& u = std::get<serde::avro::parsed::avro_union>(*current);
            // If the union branch is null, the field is absent.
            if (
              std::holds_alternative<serde::avro::parsed::primitive>(*u.message)
              && std::holds_alternative<serde::avro::parsed::avro_null>(
                std::get<serde::avro::parsed::primitive>(*u.message))) {
                return nullptr;
            }
            current_schema = current_schema->leafAt(static_cast<int>(u.branch));
            current = u.message.get();
        }

        // At this point we expect a record.
        auto* rec = std::get_if<serde::avro::parsed::record>(current);
        if (rec == nullptr) {
            return nullptr;
        }

        // Find the field index by name in the schema.
        size_t field_index = 0;
        if (!current_schema->nameIndex(std::string(segment), field_index)) {
            return nullptr;
        }
        if (field_index >= rec->fields.size()) {
            return nullptr;
        }

        current_schema = current_schema->leafAt(static_cast<int>(field_index));
        current = rec->fields[field_index].get();
    }

    // Unwrap union at the leaf too.
    while (std::holds_alternative<serde::avro::parsed::avro_union>(*current)) {
        auto& u = std::get<serde::avro::parsed::avro_union>(*current);
        if (
          std::holds_alternative<serde::avro::parsed::primitive>(*u.message)
          && std::holds_alternative<serde::avro::parsed::avro_null>(
            std::get<serde::avro::parsed::primitive>(*u.message))) {
            return nullptr;
        }
        current = u.message.get();
    }

    return current;
}

/// Encrypt the iobuf content of an Avro primitive field in place.
void encrypt_avro_field(serde::avro::parsed::message& node, bytes_view dek) {
    auto* prim = std::get_if<serde::avro::parsed::primitive>(&node);
    if (prim == nullptr) {
        return;
    }
    auto* buf = std::get_if<iobuf>(prim);
    if (buf == nullptr) {
        return;
    }
    *buf = encrypt_field_value(dek, std::move(*buf));
}

/// Encrypt a protobuf field in place by walking the path to find
/// the parent message and then modifying the leaf field.
///
/// NOTE: this traversal only handles singular message fields.
/// Encryption of fields inside repeated/map containers is not
/// supported in this implementation.
void encrypt_pb_field(
  serde::pb::parsed::message& msg,
  const google::protobuf::Descriptor& desc,
  const std::vector<ss::sstring>& path,
  bytes_view dek) {
    if (path.empty()) {
        return;
    }

    // Walk the path segments except the last one to reach the parent
    // message that contains the target field.
    auto* current_msg = &msg;
    const auto* current_desc = &desc;

    for (size_t i = 0; i + 1 < path.size(); ++i) {
        const auto* fd = current_desc->FindFieldByName(std::string(path[i]));
        if (fd == nullptr || fd->message_type() == nullptr) {
            return;
        }
        auto it = current_msg->fields.find(fd->number());
        if (it == current_msg->fields.end()) {
            return;
        }
        auto* sub = std::get_if<std::unique_ptr<serde::pb::parsed::message>>(
          &it->second);
        if (sub == nullptr || !*sub) {
            return;
        }
        current_desc = fd->message_type();
        current_msg = sub->get();
    }

    // Now current_msg is the parent, encrypt the leaf field.
    const auto& leaf_name = path.back();
    const auto* leaf_fd = current_desc->FindFieldByName(std::string(leaf_name));
    if (leaf_fd == nullptr) {
        return;
    }
    auto it = current_msg->fields.find(leaf_fd->number());
    if (it == current_msg->fields.end()) {
        return;
    }
    auto* buf = std::get_if<iobuf>(&it->second);
    if (buf == nullptr) {
        return;
    }
    *buf = encrypt_field_value(dek, std::move(*buf));
}

/// Check if a JSON key path (as stack of ancestor keys) matches
/// one of the tagged field paths.
struct json_path_match_result {
    bool matches{false};
    bytes_view dek;
};

json_path_match_result match_json_path(
  const std::vector<ss::sstring>& current_path,
  const std::vector<tagged_field>& tagged_fields,
  const dek_set& deks) {
    for (const auto& tf : tagged_fields) {
        if (tf.path == current_path) {
            auto dek_it = deks.find(tf.kek_name);
            if (dek_it != deks.end()) {
                return {
                  .matches = true,
                  .dek = dek_it->second.plaintext_dek,
                };
            }
        }
    }
    return {};
}

/// JSON transform: streaming parse/re-emit with encryption.
ss::future<iobuf> transform_json(
  iobuf value,
  const std::vector<tagged_field>& tagged_fields,
  const dek_set& deks) {
    auto p = serde::json::parser(std::move(value));
    serde::json::writer w;

    // Track the current object key path for field matching.
    // Each element is a key name. We push on key tokens and pop on
    // end_object/end_array when the context ends.
    std::vector<ss::sstring> path_stack;

    // Track whether the most recently seen key matches a tagged field.
    // If so, the next value token should be encrypted.
    bool encrypt_next_value = false;
    bytes_view active_dek;

    // Track context (object vs array) with the path_stack depth at entry
    // so we can restore it on scope exit. This is essential for arrays of
    // objects: each object in the array must start with the same path
    // prefix (the array key's ancestors), so end_object restores the
    // path_stack to the depth it had when start_object was seen.
    enum class context_type { object, array };
    struct context_entry {
        context_type type;
        size_t path_depth_at_entry;
    };
    std::vector<context_entry> context_stack;

    while (co_await p.next()) {
        switch (p.token()) {
        case serde::json::token::start_object:
            if (encrypt_next_value) {
                // An object value at an encrypted field path is not a
                // string/bytes -- skip encryption, just emit normally.
                encrypt_next_value = false;
            }
            w.begin_object();
            context_stack.push_back(
              {.type = context_type::object,
               .path_depth_at_entry = path_stack.size()});
            break;

        case serde::json::token::end_object: {
            w.end_object();
            size_t restore_depth = 0;
            if (!context_stack.empty()) {
                restore_depth = context_stack.back().path_depth_at_entry;
                context_stack.pop_back();
            }
            // Restore path_stack to depth at object entry so that sibling
            // objects in an array (or sibling keys in a parent object)
            // see the correct prefix.
            while (path_stack.size() > restore_depth) {
                path_stack.pop_back();
            }
            // If the parent context is an object, pop the key that led
            // to this object value.
            if (
              !path_stack.empty()
              && (context_stack.empty() || context_stack.back().type == context_type::object)) {
                path_stack.pop_back();
            }
            break;
        }

        case serde::json::token::start_array:
            if (encrypt_next_value) {
                encrypt_next_value = false;
            }
            w.begin_array();
            context_stack.push_back(
              {.type = context_type::array,
               .path_depth_at_entry = path_stack.size()});
            break;

        case serde::json::token::end_array: {
            w.end_array();
            size_t restore_depth = 0;
            if (!context_stack.empty()) {
                restore_depth = context_stack.back().path_depth_at_entry;
                context_stack.pop_back();
            }
            while (path_stack.size() > restore_depth) {
                path_stack.pop_back();
            }
            if (
              !path_stack.empty()
              && (context_stack.empty() || context_stack.back().type == context_type::object)) {
                path_stack.pop_back();
            }
            break;
        }

        case serde::json::token::key: {
            auto key_buf = p.value_string();
            auto key_str = key_buf.linearize_to_string();

            // Pop previous key at same level if we're in an object.
            // The current object's context_entry records the path depth
            // at entry; any path entries beyond that belong to a prior
            // sibling key and must be removed before pushing the new key.
            if (
              !context_stack.empty()
              && context_stack.back().type == context_type::object) {
                auto depth = context_stack.back().path_depth_at_entry;
                while (path_stack.size() > depth) {
                    path_stack.pop_back();
                }
            }

            path_stack.push_back(ss::sstring(key_str));
            w.key(key_buf);

            // Check if current path matches a tagged field.
            auto match = match_json_path(path_stack, tagged_fields, deks);
            encrypt_next_value = match.matches;
            if (match.matches) {
                active_dek = match.dek;
            }
            break;
        }

        case serde::json::token::value_string: {
            auto val = p.value_string();
            if (encrypt_next_value) {
                auto encrypted = encrypt_field_value(
                  active_dek, std::move(val));
                w.base64_string(encrypted);
                encrypt_next_value = false;
            } else {
                w.string(val);
            }
            break;
        }

        case serde::json::token::value_int: {
            auto val = p.value_int();
            if (encrypt_next_value) {
                // Encrypt integer: serialize to string, encrypt, base64.
                auto str = fmt::to_string(val);
                iobuf buf;
                buf.append_str(str);
                auto encrypted = encrypt_field_value(
                  active_dek, std::move(buf));
                w.base64_string(encrypted);
                encrypt_next_value = false;
            } else {
                if (
                  val >= std::numeric_limits<int32_t>::min()
                  && val <= std::numeric_limits<int32_t>::max()) {
                    w.integer(static_cast<int32_t>(val));
                } else {
                    w.append_raw_json(iobuf::from(fmt::to_string(val)));
                }
            }
            break;
        }

        case serde::json::token::value_double: {
            auto val = p.value_double();
            if (encrypt_next_value) {
                auto str = fmt::to_string(val);
                iobuf buf;
                buf.append_str(str);
                auto encrypted = encrypt_field_value(
                  active_dek, std::move(buf));
                w.base64_string(encrypted);
                encrypt_next_value = false;
            } else {
                w.number(val);
            }
            break;
        }

        case serde::json::token::value_null:
            // Null values are not encrypted even if tagged.
            w.null();
            encrypt_next_value = false;
            break;

        case serde::json::token::value_true:
            if (encrypt_next_value) {
                iobuf buf;
                buf.append_str("true");
                auto encrypted = encrypt_field_value(
                  active_dek, std::move(buf));
                w.base64_string(encrypted);
                encrypt_next_value = false;
            } else {
                w.boolean(true);
            }
            break;

        case serde::json::token::value_false:
            if (encrypt_next_value) {
                iobuf buf;
                buf.append_str("false");
                auto encrypted = encrypt_field_value(
                  active_dek, std::move(buf));
                w.base64_string(encrypted);
                encrypt_next_value = false;
            } else {
                w.boolean(false);
            }
            break;

        case serde::json::token::eof:
            break;

        case serde::json::token::error:
            throw std::runtime_error("JSON parse error during field transform");
        }
    }

    if (p.token() == serde::json::token::error) {
        throw std::runtime_error("JSON parse error at end of field transform");
    }

    co_return std::move(w).finish();
}

} // namespace

iobuf encrypt_field_value(bytes_view dek, iobuf plaintext) {
    const auto* cipher = cipher_for_key(dek);

    evp_cipher_ctx_ptr ctx(EVP_CIPHER_CTX_new());
    if (!ctx) {
        throw crypto::internal::ossl_error("failed to create EVP_CIPHER_CTX");
    }

    // Generate random IV.
    auto iv = crypto::generate_random(
      gcm_iv_size, crypto::use_private_rng::yes);

    if (
      1
      != EVP_EncryptInit_ex(
        ctx.get(), cipher, nullptr, dek.data(), iv.data())) {
        throw crypto::internal::ossl_error("EVP_EncryptInit_ex failed");
    }

    // Linearize plaintext for OpenSSL.
    auto pt_bytes = iobuf_to_bytes(plaintext);
    if (
      pt_bytes.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
        throw crypto::exception("plaintext too large for AES-GCM");
    }

    // Allocate output: same size as plaintext for ciphertext.
    bytes ct(bytes::initialized_later(), pt_bytes.size());
    int len = 0;
    if (
      1
      != EVP_EncryptUpdate(
        ctx.get(),
        ct.data(),
        &len,
        pt_bytes.data(),
        static_cast<int>(pt_bytes.size()))) {
        throw crypto::internal::ossl_error("EVP_EncryptUpdate failed");
    }
    int ct_len = len;

    int final_len = 0;
    if (1 != EVP_EncryptFinal_ex(ctx.get(), ct.data() + ct_len, &final_len)) {
        throw crypto::internal::ossl_error("EVP_EncryptFinal_ex failed");
    }
    ct_len += final_len;

    // Retrieve auth tag.
    bytes tag(bytes::initialized_later(), gcm_tag_size);
    if (
      1
      != EVP_CIPHER_CTX_ctrl(
        ctx.get(),
        EVP_CTRL_GCM_GET_TAG,
        static_cast<int>(gcm_tag_size),
        tag.data())) {
        throw crypto::internal::ossl_error("EVP_CTRL_GCM_GET_TAG failed");
    }

    // Build result: [IV | ciphertext | tag]
    iobuf result;
    result.append(iv.data(), gcm_iv_size);
    result.append(ct.data(), ct_len);
    result.append(tag.data(), gcm_tag_size);
    return result;
}

iobuf decrypt_field_value(bytes_view dek, iobuf ciphertext) {
    auto ct_bytes = iobuf_to_bytes(ciphertext);
    if (ct_bytes.size() < gcm_iv_size + gcm_tag_size) {
        throw crypto::exception("ciphertext too short for AES-GCM");
    }

    const auto* cipher = cipher_for_key(dek);

    bytes_view iv_view(ct_bytes.data(), gcm_iv_size);
    auto ct_data_size = ct_bytes.size() - gcm_iv_size - gcm_tag_size;
    if (ct_data_size > static_cast<size_t>(std::numeric_limits<int>::max())) {
        throw crypto::exception("ciphertext too large for AES-GCM");
    }
    bytes_view ct_view(ct_bytes.data() + gcm_iv_size, ct_data_size);
    bytes_view tag_view(
      ct_bytes.data() + gcm_iv_size + ct_data_size, gcm_tag_size);

    evp_cipher_ctx_ptr ctx(EVP_CIPHER_CTX_new());
    if (!ctx) {
        throw crypto::internal::ossl_error("failed to create EVP_CIPHER_CTX");
    }

    if (
      1
      != EVP_DecryptInit_ex(
        ctx.get(), cipher, nullptr, dek.data(), iv_view.data())) {
        throw crypto::internal::ossl_error("EVP_DecryptInit_ex failed");
    }

    bytes pt(bytes::initialized_later(), ct_data_size);
    int len = 0;
    if (
      1
      != EVP_DecryptUpdate(
        ctx.get(),
        pt.data(),
        &len,
        ct_view.data(),
        static_cast<int>(ct_data_size))) {
        throw crypto::internal::ossl_error("EVP_DecryptUpdate failed");
    }
    int pt_len = len;

    // Set the expected tag before finalizing.
    if (
      1
      != EVP_CIPHER_CTX_ctrl(
        ctx.get(),
        EVP_CTRL_GCM_SET_TAG,
        static_cast<int>(gcm_tag_size),
        // NOLINTNEXTLINE: const_cast needed for OpenSSL API
        const_cast<uint8_t*>(tag_view.data()))) {
        throw crypto::internal::ossl_error("EVP_CTRL_GCM_SET_TAG failed");
    }

    int final_len = 0;
    if (1 != EVP_DecryptFinal_ex(ctx.get(), pt.data() + pt_len, &final_len)) {
        throw crypto::exception("AES-GCM authentication failed");
    }
    pt_len += final_len;

    iobuf result;
    result.append(pt.data(), pt_len);
    return result;
}

ss::future<iobuf> ref_field_transformer::transform(
  iobuf value, const encryption_schema& schema, const dek_set& deks) {
    switch (schema.format) {
    case schema_format::avro: {
        auto* avro_schema = std::get_if<std::shared_ptr<::avro::ValidSchema>>(
          &schema.handle);
        if (avro_schema == nullptr || !*avro_schema) {
            throw std::runtime_error(
              "avro schema handle missing for avro format");
        }

        if (schema.tagged_fields.empty()) {
            co_return std::move(value);
        }

        auto parsed = co_await serde::avro::parse(
          std::move(value), **avro_schema);

        for (const auto& tf : schema.tagged_fields) {
            auto dek_it = deks.find(tf.kek_name);
            if (dek_it == deks.end()) {
                continue;
            }

            auto* node = walk_avro_path(*parsed, **avro_schema, tf.path);
            if (node != nullptr) {
                encrypt_avro_field(*node, dek_it->second.plaintext_dek);
            }
        }

        co_return co_await serde::avro::encode(*parsed, **avro_schema);
    }

    case schema_format::protobuf: {
        auto* const* pb_desc = std::get_if<const google::protobuf::Descriptor*>(
          &schema.handle);
        if (pb_desc == nullptr || *pb_desc == nullptr) {
            throw std::runtime_error(
              "protobuf descriptor missing for protobuf format");
        }

        if (schema.tagged_fields.empty()) {
            co_return std::move(value);
        }

        auto parsed = co_await serde::pb::parse(std::move(value), **pb_desc);

        for (const auto& tf : schema.tagged_fields) {
            auto dek_it = deks.find(tf.kek_name);
            if (dek_it == deks.end()) {
                continue;
            }
            encrypt_pb_field(
              *parsed, **pb_desc, tf.path, dek_it->second.plaintext_dek);
        }

        co_return co_await serde::pb::encode(*parsed, **pb_desc);
    }

    case schema_format::json: {
        co_return co_await transform_json(
          std::move(value), schema.tagged_fields, deks);
    }
    }

    __builtin_unreachable();
}

} // namespace encryption
