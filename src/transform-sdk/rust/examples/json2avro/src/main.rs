// Copyright 2023 Redpanda Data, Inc.
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

use anyhow::Result;
use redpanda_transform_sdk::*;

fn main() -> Result<()> {
    let schema = apache_avro::Schema::parse_str(
        r#"{
          "type": "record",
          "name":"Example",
          "namespace": "org.apache.avro",
          "fields": [
              {"name": "a", "type": "int"},
              {"name": "b", "type": "string"},
              {
                "name": "nested", 
                "type": {
                  "type": "record",
                  "name": "Node",
                  "fields": [
                      {"name": "label", "type": "string"},
                      {"name": "children", "type": {"type": "array", "items": "Node"}}
                  ]
                }
              }
          ]
        }"#,
    )?;
    on_record_written(|evt, writer| my_transform(&schema, evt, writer));
}

fn my_transform(
    schema: &apache_avro::Schema,
    event: WriteEvent,
    writer: &mut RecordWriter,
) -> Result<()> {
    let transformed_value = match event.record.value() {
        Some(bytes) => Some(json_to_avro(schema, bytes)?),
        None => None,
    };

    writer.write(BorrowedRecord::new_with_headers(
        event.record.key(),
        transformed_value.as_ref().map(|v| &v[..]),
        event.record.headers().to_owned(),
    ))?;
    Ok(())
}

type JsonValue = serde_json::Value;
type AvroValue = apache_avro::types::Value;

fn json_to_avro(schema: &apache_avro::Schema, json_bytes: &[u8]) -> Result<Vec<u8>> {
    let v: serde_json::Value = serde_json::from_slice(json_bytes)?;
    let v = json2avro(v)?;
    let mut w = apache_avro::Writer::new(schema, Vec::new());
    let _ = w.append_value_ref(&v)?;
    let _ = w.flush()?;
    Ok(w.into_inner()?)
}

fn json2avro(v: JsonValue) -> Result<AvroValue> {
    Ok(match v {
        JsonValue::Null => AvroValue::Null,
        JsonValue::Bool(b) => AvroValue::Boolean(b),
        JsonValue::Number(n) => match n.as_i64().map(AvroValue::Long) {
            Some(v) => v,
            None => n
                .as_f64()
                .map(AvroValue::Double)
                .ok_or(anyhow::anyhow!("unrepresentable avro value: {}", n))?,
        },
        JsonValue::String(s) => AvroValue::String(s),
        JsonValue::Array(a) => {
            AvroValue::Array(a.into_iter().map(json2avro).collect::<Result<Vec<_>>>()?)
        }
        JsonValue::Object(o) => AvroValue::Record(
            o.into_iter()
                .map(|(k, v)| json2avro(v).map(|v| (k, v)))
                .collect::<Result<Vec<_>>>()?,
        ),
    })
}
