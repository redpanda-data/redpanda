use crate::varint::{varint_encode_to, varint_size};
//use redpanda_adl::to_bytes;
use crate::batch_crc::{batch_body_crc, batch_header_crc};
use redpanda_records::{RecordBatchHeader, RecordOwned, UnpackedRecordBatchHeader};
use std::io::Write;

/// The encoding style that a Kafka record uses for both the key+value of
/// a record, and the key+value of each record header.
fn encode_kv<T: Write>(out: &mut T, k: Option<&[u8]>, v: Option<&[u8]>) -> std::io::Result<usize> {
    let mut written: usize = 0;
    match k {
        Some(key) => {
            written += varint_encode_to(out, key.len() as i64)?;
            out.write(key)?;
            written += key.len();
        }
        None => {
            written += varint_encode_to(out, -1)?;
        }
    }

    match v {
        Some(value) => {
            written += varint_encode_to(out, value.len() as i64)?;
            out.write(value)?;
            written += value.len();
        }
        None => {
            written += varint_encode_to(out, -1)?;
        }
    }

    Ok(written)
}

fn kv_size(k: Option<&[u8]>, v: Option<&[u8]>) -> usize {
    let mut size: usize = 0;
    match k {
        Some(key) => {
            size += varint_size(key.len() as i64) + key.len();
        }
        None => {
            size += varint_size(-1);
        }
    }

    match v {
        Some(value) => {
            size += varint_size(value.len() as i64) + value.len();
        }
        None => {
            size += varint_size(-1);
        }
    }

    size
}

fn serialize_record<T: Write>(out: &mut T, record: &RecordOwned) -> std::io::Result<()> {
    // Calculate the length of the record.
    let mut record_len: usize = 1; // 1 byte for attrs
    record_len += varint_size(record.ts_delta as i64);
    record_len += varint_size(record.offset_delta as i64);
    record_len += kv_size(
        record.key.as_ref().map(|v| v.as_slice()),
        record.value.as_ref().map(|v| v.as_slice()),
    );
    record_len += varint_size(record.headers.len() as i64);
    for (h_key, h_val) in &record.headers {
        record_len += kv_size(Some(&h_key), Some(&h_val));
    }

    varint_encode_to(out, record_len as i64)?;
    let mut actual_write: usize = 0;
    out.write_all(&[record.attrs as u8])?;
    actual_write += 1;
    actual_write += varint_encode_to(out, record.ts_delta as i64)?;
    actual_write += varint_encode_to(out, record.offset_delta as i64)?;

    actual_write += encode_kv(
        out,
        record.key.as_ref().map(|v| v.as_slice()),
        record.value.as_ref().map(|v| v.as_slice()),
    )
    .unwrap();

    actual_write += varint_encode_to(out, record.headers.len() as i64)?;
    for (h_key, h_val) in &record.headers {
        actual_write += encode_kv(out, Some(&h_key), Some(&h_val))?;
    }

    assert_eq!(record_len, actual_write);

    Ok(())
}

fn serialize_header(header: &RecordBatchHeader, buffer: &mut [u8]) {
    // unwrap() because our serialization rules for headers do not include
    // possibility of failure.
    // let bytes = to_bytes(header).unwrap();
    // buffer.copy_from_slice(&bytes);
    let header_unpacked = UnpackedRecordBatchHeader::from(header);
    bincode::encode_into_slice(
        header_unpacked,
        buffer,
        bincode::config::standard()
            .with_little_endian()
            .with_fixed_int_encoding(),
    )
    .unwrap();
}

/// When you are modifying the contents of a batch: an existing RecordBatchHeader
/// provies the fields we will re-use (batch type etc).
pub fn reserialize_batch(old_header: &RecordBatchHeader, records: &Vec<RecordOwned>) -> Vec<u8> {
    // Intentionally do not modify the min/max header fields for offset and timestamp, even
    // if the records at those limits are no longer present.  Redpanda does this too when
    // compacting, and mirroring that behavior simplifies testing of round-trip encoding
    // of example data.
    // TODO: see if anything in Redpanda actually requires this property: it would be nice to
    // eventually switch to writing out pristine batches where the min/max fields really
    // do match the min/max values of the records within the batch.

    // Serialize each record into the body buffer
    let mut body_bytes: Vec<u8> = vec![];
    for r in records {
        // unwrap() because this is write()ing to a Vec that cannot fail
        serialize_record(&mut body_bytes, &r).unwrap();
    }

    let mut header = old_header.clone();
    header.record_count = records.len() as i32;
    header.size_bytes = (std::mem::size_of::<RecordBatchHeader>() + body_bytes.len()) as i32;
    // TODO: what about header.base_sequence?  Any adjustment required, or should we always
    // carry it through verbatim?

    // Calculate body CRC first: this value is required to calculate header CRC
    let (body_crc, crc_fields) = batch_body_crc(&header, &body_bytes);
    header.crc = body_crc;

    // Calculate header CRC
    let header_crc = batch_header_crc(&header, crc_fields);
    header.header_crc = header_crc;

    let mut result: Vec<u8> =
        Vec::with_capacity(std::mem::size_of::<RecordBatchHeader>() + body_bytes.len());
    for _ in 0..header.size_bytes as usize {
        result.push(0);
    }

    serialize_header(
        &header,
        &mut result[0..std::mem::size_of::<RecordBatchHeader>()],
    );
    result[std::mem::size_of::<RecordBatchHeader>()..].copy_from_slice(body_bytes.as_slice());

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch_reader::{BatchBuffer, BatchStream};
    use log::{error, info};
    use redpanda_records::BATCH_HEADER_BYTES;
    use std::cmp::min;
    use std::env;
    use tokio::fs::File;
    use tokio::io::BufReader;

    async fn load_batch(path: &str) -> BatchBuffer {
        let cargo_path = env::var("CARGO_MANIFEST_DIR").unwrap();
        let filename = cargo_path + path;
        let file = File::open(filename).await.unwrap();
        let reader = BufReader::new(file);
        let mut stream = BatchStream::new(reader);

        // Unwrap because we know that tests will only load from files containing
        // at least one valid batch
        stream.read_batch_buffer().await.unwrap()
    }

    #[test_log::test(tokio::test)]
    pub async fn encode_header() {
        // Use an example written by Redpanda 22.3 to get our batch header: decode and
        // then re-encode this: should get the same bytes.
        let batch_buffer = load_batch("/resources/test/3676-7429-77-1-v1.log.1").await;
        let header = &batch_buffer.header;

        let mut serialize_buf: [u8; std::mem::size_of::<RecordBatchHeader>()] =
            [0u8; std::mem::size_of::<RecordBatchHeader>()];

        serialize_header(header, &mut serialize_buf);
        for i in 0..std::mem::size_of::<RecordBatchHeader>() {
            if serialize_buf[i] != batch_buffer.bytes[i] {
                error!("Serialized error at byte {}", i);
            }
            assert_eq!(serialize_buf[i], batch_buffer.bytes[i]);
        }
    }

    /// Compare two byte slices and give a helpful error if they are not identical
    /// * `len` - the max length to compare.  Slices are allowed to be longer, but both
    ///           must be at least this long.
    fn assert_bytes_equal(left: &[u8], right: &[u8], len: usize) {
        let compare_len = min(len, min(left.len(), right.len()));
        for i in 0..compare_len {
            if left[i] != right[i] {
                error!("Byte {} differs: {:02x} != {:02x}", i, left[i], right[i]);
            }
            assert_eq!(left[i], right[i]);
        }
        assert_eq!(left.len(), right.len());
        assert!(left.len() >= len);
        assert!(right.len() >= len);
    }

    async fn assert_read_back(original_batch: &BatchBuffer, re_encoded: &[u8]) {
        let mut readback_cursor = std::io::Cursor::new(re_encoded.clone());
        let mut reader = BatchStream::new(&mut readback_cursor);
        let readback_batch = reader.read_batch_buffer().await.unwrap();
        assert_bytes_equal(&readback_batch.bytes, &re_encoded, re_encoded.len());
        assert_eq!(readback_batch.iter().len(), original_batch.iter().len());
    }

    #[test_log::test(tokio::test)]
    pub async fn rewrite_batch_one() {
        let batch_buffer = load_batch("/resources/test/3676-7429-77-1-v1.log.1").await;
        let header = &batch_buffer.header;
        info!(
            "Loaded header with header CRC {:08x}, body CRC {:08x}, body bytes {}",
            header.header_crc as u32,
            header.crc as u32,
            batch_buffer.bytes.len() - BATCH_HEADER_BYTES
        );

        let records: Vec<RecordOwned> = batch_buffer.iter().map(|r| r.to_owned()).collect();

        let re_encoded = reserialize_batch(header, &records);
        assert_bytes_equal(&re_encoded, &batch_buffer.bytes, batch_buffer.bytes.len());

        assert_read_back(&batch_buffer, &re_encoded).await;
    }

    #[test_log::test(tokio::test)]
    pub async fn rewrite_batch_two() {
        let batch_buffer = load_batch("/resources/test/test_segment.bin").await;
        let header = &batch_buffer.header;

        let records: Vec<RecordOwned> = batch_buffer.iter().map(|r| r.to_owned()).collect();
        // This example has records with zero timestamp deltas, so it should re-encode
        // as byte-identical to the incoming data.
        assert_eq!(records[0].ts_delta, 0);

        let re_encoded = reserialize_batch(header, &records);

        assert_bytes_equal(&re_encoded, &batch_buffer.bytes, batch_buffer.bytes.len());
        assert_read_back(&batch_buffer, &re_encoded).await;
    }
}
