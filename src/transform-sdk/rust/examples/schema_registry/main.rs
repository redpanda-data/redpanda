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

mod avro;

use avro::example::Example;

use anyhow::Result;
use apache_avro::{from_avro_datum, from_value, AvroSchema};
use redpanda_transform_sdk::*;
use redpanda_transform_sdk_sr::*;
use serde_json::to_vec;

// This example shows usage of the schema registry integration:
//   - Extract a Schema ID from each record, lookup in SR
//   - Decode the record based on the resulting schema (or panic)
//   - Outputs the record as JSON

fn main() {
    let mut client = SchemaRegistryClient::new();
    let s: apache_avro::Schema = Example::get_schema();
    match client.create_schema(
        "avro-value",
        Schema::new_avro(s.canonical_form(), Vec::new()),
    ) {
        Ok(_) => {}
        Err(e) => println!("unable to register schema: {}", e),
    }
    on_record_written(|ev, w| avro_to_json_transform(ev, w, &client));
}

fn avro_to_json_transform(
    event: WriteEvent,
    writer: &mut RecordWriter,
    client: &SchemaRegistryClient,
) -> Result<()> {
    let rec_bytes = event.record.value().unwrap();
    let (id, mut rest) = decode_schema_id(rec_bytes)?;
    let raw_schema = client.lookup_schema_by_id(id)?;
    let s = apache_avro::Schema::parse_str(raw_schema.schema())?;
    let value = from_avro_datum(&s, &mut rest, None)?;
    let e: Example = from_value(&value)?;
    let j = to_vec(&e)?;
    writer.write(BorrowedRecord::new(event.record.key(), Some(&j)))?;
    Ok(())
}
