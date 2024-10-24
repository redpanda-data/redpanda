use redpanda_records::{RecordBatchHeader, RecordBatchHeaderCrcFirst, RecordBatchHeaderCrcSecond};

/// We return the second part of the header struct because it is re-used in calculating
/// the body CRC.
pub fn batch_header_crc(
    batch_header: &RecordBatchHeader,
    header_crc_fields_two: RecordBatchHeaderCrcSecond,
) -> u32 {
    const HEADER_CRC_BYTES_LEN: usize = 17 + 40;
    let mut header_crc_bytes: [u8; HEADER_CRC_BYTES_LEN] = [0; HEADER_CRC_BYTES_LEN];
    let header_crc_fields = RecordBatchHeaderCrcFirst {
        size_bytes: batch_header.size_bytes,
        base_offset: batch_header.base_offset,
        record_batch_type: batch_header.record_batch_type,
        crc: batch_header.crc,
    };

    bincode::encode_into_slice(
        &header_crc_fields,
        &mut header_crc_bytes,
        bincode::config::standard()
            .with_little_endian()
            .with_fixed_int_encoding(),
    )
    .unwrap();

    bincode::encode_into_slice(
        header_crc_fields_two,
        &mut header_crc_bytes[17..],
        bincode::config::standard()
            .with_little_endian()
            .with_fixed_int_encoding(),
    )
    .unwrap();

    crc32c::crc32c(&header_crc_bytes)
}

pub fn batch_body_crc(
    batch_header: &RecordBatchHeader,
    body_bytes: &[u8],
) -> (u32, RecordBatchHeaderCrcSecond) {
    let header_fields = RecordBatchHeaderCrcSecond {
        record_batch_attributes: batch_header.record_batch_attributes,
        last_offset_delta: batch_header.last_offset_delta,
        first_timestamp: batch_header.first_timestamp,
        max_timestamp: batch_header.max_timestamp,
        producer_id: batch_header.producer_id,
        producer_epoch: batch_header.producer_epoch,
        base_sequence: batch_header.base_sequence,
        record_count: batch_header.record_count,
    };

    const HEADER2_CRC_BYTES_LEN: usize = 40;
    let mut header2_crc_bytes: [u8; HEADER2_CRC_BYTES_LEN] = [0; HEADER2_CRC_BYTES_LEN];
    bincode::encode_into_slice(
        header_fields,
        &mut header2_crc_bytes,
        bincode::config::standard()
            .with_big_endian()
            .with_fixed_int_encoding(),
    )
    .unwrap();

    let body_crc32c = crc32c::crc32c(&header2_crc_bytes);
    (
        crc32c::crc32c_append(body_crc32c, body_bytes),
        header_fields,
    )
}
