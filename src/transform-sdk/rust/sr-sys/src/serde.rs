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

use redpanda_transform_sdk_sr_types::*;
use redpanda_transform_sdk_varint as varint;

pub(crate) fn decode_schema(subject: &str, buf: &[u8]) -> Result<SubjectSchema> {
    let decoded = varint::read(buf)?;
    let id = SchemaId(decoded.value as i32);
    let buf = &buf[decoded.read..];
    let decoded = varint::read(buf)?;
    let version = SchemaVersion(decoded.value as i32);
    let buf = &buf[decoded.read..];
    let schema = decode_schema_def(buf)?;
    Ok(SubjectSchema::new(schema, subject, version, id))
}

pub(crate) fn decode_schema_def(buf: &[u8]) -> Result<Schema> {
    let decoded = varint::read(buf)?;
    let format = match decoded.value {
        0 => SchemaFormat::Avro,
        1 => SchemaFormat::Protobuf,
        2 => SchemaFormat::Json,
        _ => return Err(SchemaRegistryError::Unknown(-1)),
    };
    let buf = &buf[decoded.read..];
    let decoded = varint::read_sized_buffer(buf)?;
    let buf = &buf[decoded.read..];
    let schema = String::from_utf8(decoded.value.unwrap_or_default().into())?;
    let decoded = varint::read(buf)?;
    let mut references: Vec<Reference> = Vec::with_capacity(decoded.value as usize);
    let mut buf = &buf[decoded.read..];
    for _ in 0..decoded.value {
        let decoded = varint::read_sized_buffer(buf)?;
        buf = &buf[decoded.read..];
        let name = String::from_utf8(decoded.value.unwrap_or_default().into())?;
        let decoded = varint::read_sized_buffer(buf)?;
        buf = &buf[decoded.read..];
        let subject = String::from_utf8(decoded.value.unwrap_or_default().into())?;
        let decoded = varint::read(buf)?;
        buf = &buf[decoded.read..];
        let version = SchemaVersion(decoded.value as i32);
        references.push(Reference::new(name, subject, version));
    }
    Ok(Schema::new(schema, format, references))
}

pub(crate) fn encode_schema_def(buf: &mut Vec<u8>, schema: &Schema) {
    varint::write(
        buf,
        match schema.format() {
            SchemaFormat::Avro => 0,
            SchemaFormat::Protobuf => 1,
            SchemaFormat::Json => 2,
        },
    );
    varint::write_sized_buffer(buf, Some(schema.schema().as_bytes()));
    varint::write(buf, schema.references().len() as i64);
    for reference in schema.references() {
        varint::write_sized_buffer(buf, Some(reference.name().as_bytes()));
        varint::write_sized_buffer(buf, Some(reference.subject().as_bytes()));
        varint::write(buf, reference.version().0 as i64);
    }
}

#[cfg(test)]
mod tests {
    use redpanda_transform_sdk_sr_types::SchemaFormat;

    use super::{decode_schema_def, encode_schema_def, Reference, Schema, SchemaVersion};

    #[test]
    fn roundtrip() {
        let schema = Schema::new(
            r#"{
                  "type": "record",
                  "namespace": "com.example.school",
                  "name": "Student",
                  "fields": [
                    {
                      "name": "Name",
                      "type": "string"
                    },
                    {
                      "name": "Age",
                      "type": "int"
                    },
                    {
                      "name": "Address",
                      "type": "com.example.school.Address"
                    }
                  ]
                }"#,
            SchemaFormat::Avro,
            vec![Reference::new(
                "com.example.school.Address",
                "Address",
                SchemaVersion(1),
            )],
        );
        let mut buf = vec![];
        encode_schema_def(&mut buf, &schema);
        let roundtripped = decode_schema_def(&buf).unwrap();
        assert_eq!(schema, roundtripped);
    }
}
