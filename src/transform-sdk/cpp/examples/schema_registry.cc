// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <redpanda/transform_sdk.h>

#include <iostream>
#include <string>
#include <string_view>
#include <system_error>

namespace {

namespace sr = redpanda::sr;

std::error_code
do_transform(redpanda::write_event event, redpanda::record_writer* writer);

std::unique_ptr<sr::schema_registry_client> sr_client;

constexpr std::string_view schema_def = R"({
	"type": "record",
	"name": "Example",
	"fields": [
		{"name": "a", "type": "long", "default": 0},
		{"name": "b", "type": "string", "default": ""}
	]
})";

} // namespace

int main() {
    sr_client = sr::new_client();
    if (auto res = sr_client->create_schema(
          "avro-value",
          sr::schema::new_avro(
            std::string{schema_def.data(), schema_def.size()}));
        !res.has_value()) {
        return 0;
    }
    redpanda::on_record_written(do_transform);
}

namespace {

std::error_code
do_transform(redpanda::write_event event, redpanda::record_writer* writer) {
    auto rec_bytes = event.record.value;
    if (!rec_bytes.has_value()) {
        return std::make_error_code(std::errc::illegal_byte_sequence);
    }
    std::optional<sr::schema_id> id;
    if (auto id_e = sr::decode_schema_id(rec_bytes.value()); id_e.has_value()) {
        id.emplace(id_e.value().first);
        rec_bytes = id_e.value().second;
    } else {
        return id_e.error();
    }
    std::optional<sr::schema> raw_schema;
    if (auto rs_e = sr_client->lookup_schema_by_id(id.value());
        rs_e.has_value()) {
        raw_schema.emplace(std::move(rs_e).value());
    } else {
        return rs_e.error();
    }
    // ?????
    // auto schema = avro::schema::parse_str(raw_schema.value().schema());

    std::optional<sr::subject_schema> latest_schema;
    if (auto latest_e = sr_client->lookup_latest_schema("avro-value");
        latest_e.has_value()) {
        latest_schema.emplace(std::move(latest_e).value());
    } else {
        return latest_e.error();
    }

    std::optional<sr::subject_schema> latest_direct;
    if (auto latest_e = sr_client->lookup_schema_by_version(
          "avro-value", latest_schema->version);
        latest_e.has_value()) {
        latest_direct.emplace(std::move(latest_e).value());
    } else {
        return latest_e.error();
    }

    if (latest_schema != latest_direct) {
        return std::make_error_code(std::errc::io_error);
    }
    std::cerr << "ALL IS GOOD" << std::endl;

    return writer->write(event.record);
}

} // namespace
