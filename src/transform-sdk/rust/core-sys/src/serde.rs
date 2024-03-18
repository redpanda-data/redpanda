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

use redpanda_transform_sdk_types::{BorrowedHeader, BorrowedRecord};
use redpanda_transform_sdk_varint::{DecodeError, Decoded};
extern crate redpanda_transform_sdk_varint as varint;

type KeyValuePair<'a> = (Option<&'a [u8]>, Option<&'a [u8]>);

fn read_kv(payload: &[u8]) -> Result<Decoded<KeyValuePair>, DecodeError> {
    let key = varint::read_sized_buffer(payload)?;
    let payload = &payload[key.read..];
    let value = varint::read_sized_buffer(payload)?;
    Ok(Decoded {
        value: (key.value, value.value),
        read: key.read + value.read,
    })
}

fn read_header_from_payload(payload: &[u8]) -> Result<Decoded<BorrowedHeader<'_>>, DecodeError> {
    read_kv(payload).map(|result| {
        result.map(|(key, value)| BorrowedHeader::new(key.unwrap_or("".as_bytes()), value))
    })
}

pub(crate) fn read_record_from_payload(payload: &[u8]) -> Result<BorrowedRecord<'_>, DecodeError> {
    let kv = read_kv(payload)?;
    let mut payload = &payload[kv.read..];
    let header_count_result = varint::read(payload)?;
    payload = &payload[header_count_result.read..];
    let mut headers: Vec<BorrowedHeader<'_>> =
        Vec::with_capacity(header_count_result.value as usize);
    for _ in 0..header_count_result.value {
        let header_result = read_header_from_payload(payload)?;
        payload = &payload[header_result.read..];
        headers.push(header_result.value);
    }
    let (key, value) = kv.value;
    Ok(BorrowedRecord::new_with_headers(key, value, headers))
}

pub(crate) fn write_record_payload(r: BorrowedRecord, payload: &mut Vec<u8>) {
    varint::write_sized_buffer(payload, r.key());
    varint::write_sized_buffer(payload, r.value());
    varint::write(payload, r.headers().len() as i64);
    for h in r.headers() {
        varint::write_sized_buffer(payload, Some(h.key()));
        varint::write_sized_buffer(payload, h.value());
    }
}

#[cfg(test)]
mod tests {
    use super::{read_record_from_payload, write_record_payload};
    use anyhow::{ensure, Result};
    use rand::prelude::*;
    use redpanda_transform_sdk_types::{Record, RecordHeader};

    #[test]
    fn payload_roundtrips() {
        let owned = Record::new_with_headers(
            Some(random_bytes(128)),
            Some(random_bytes(128)),
            vec![
                RecordHeader::new(random_bytes(12), Some(random_bytes(16))),
                RecordHeader::new(random_bytes(16), None),
                RecordHeader::new(random_bytes(16), Some(random_bytes(0))),
            ],
        );
        validate_roundtrip(&owned).unwrap();
        let owned = Record::new_with_headers(None, Some(random_bytes(128)), vec![]);
        validate_roundtrip(&owned).unwrap();
        let owned = Record::new_with_headers(
            Some(random_bytes(0)),
            Some(random_bytes(0)),
            vec![RecordHeader::new(random_bytes(12), Some(random_bytes(16)))],
        );
        validate_roundtrip(&owned).unwrap();
        let owned = Record::new_with_headers(
            Some(random_bytes(128)),
            None,
            vec![
                RecordHeader::new(random_bytes(12), Some(random_bytes(16))),
                RecordHeader::new(random_bytes(16), None),
                RecordHeader::new(random_bytes(16), Some(random_bytes(0))),
            ],
        );
        validate_roundtrip(&owned).unwrap();
    }

    quickcheck! {
        fn roundtrip(key: Option<Vec<u8>>, value: Option<Vec<u8>>, headers: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> Result<()> {
            let record = Record::new_with_headers(key, value, headers.into_iter().map(|(k, v)| RecordHeader::new(k, v)).collect());
            validate_roundtrip(&record)
        }
    }

    fn validate_roundtrip(owned: &Record) -> Result<()> {
        let mut buf: Vec<u8> = Vec::new();
        write_record_payload(owned.into(), &mut buf);
        let borrowed = read_record_from_payload(&buf)?;
        ensure!(borrowed.key() == owned.key(), "keys not equal");
        ensure!(borrowed.value() == owned.value(), "values not equal");
        ensure!(
            borrowed.headers().len() == owned.headers().len(),
            "header count not equal"
        );
        for (b, o) in borrowed.headers().iter().zip(owned.headers()) {
            ensure!(b.key() == o.key(), "header keys not equal");
            ensure!(b.value() == o.value(), "header values not equal");
        }
        Ok(())
    }

    fn random_bytes(n: usize) -> Vec<u8> {
        let mut v = vec![0; n];
        rand::thread_rng().fill(&mut v[..]);
        v
    }
}
