use crate::batch_crc::batch_body_crc;
use crate::error::DecodeError;
use crate::util::from_adl_bytes;
use log::{debug, info, trace};
use redpanda_records::{Record, RecordBatchHeader, RecordBatchType};
use std::io;
use tokio::io::AsyncReadExt;

use crate::varint::VarIntDecoder;

#[derive(Debug)]
pub enum DumpError {
    _Message(String),
    DecodeError(DecodeError),
    IOError(io::Error),
    EOF,
}

impl From<DecodeError> for DumpError {
    fn from(e: DecodeError) -> Self {
        Self::DecodeError(e)
    }
}

impl From<std::io::Error> for DumpError {
    fn from(e: std::io::Error) -> Self {
        Self::IOError(e)
    }
}

pub struct BatchStream<T: AsyncReadExt> {
    inner: T,
    bytes_read: u64,
    cur_batch_raw: Vec<u8>,
}

pub struct BatchBuffer {
    pub header: RecordBatchHeader,
    pub bytes: Vec<u8>,
}

pub struct RecordIter<'a> {
    /// Which record out of record_count to read next
    i: usize,
    /// Which bytes out of buf.bytes to read next
    n: usize,
    buf: &'a BatchBuffer,
}

impl<'a> RecordIter<'a> {
    fn read_u8(&mut self) -> u8 {
        let r = self.buf.bytes[self.n];
        self.n += 1;
        r
    }

    fn read_vari64(&mut self) -> i64 {
        let mut decoder = VarIntDecoder::new();
        loop {
            let b = self.read_u8();
            if decoder.feed(b) {
                break;
            }
        }

        decoder.result()
    }
}

impl<'a> ExactSizeIterator for RecordIter<'a> {
    fn len(&self) -> usize {
        self.buf.header.record_count as usize - self.i
    }
}

impl<'a> Iterator for RecordIter<'a> {
    type Item = Record<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.buf.header.record_count as usize {
            // TODO: an explicit validation step when in scan mode,
            // so that even if a batch has a valid CRC, we do not assume
            // that its contents will be valid.
            assert_eq!(
                self.n,
                self.buf.bytes.len(),
                "Read {}/{} records, {}/{} bytes, header {:?}",
                self.i,
                self.buf.header.record_count as usize,
                self.n,
                self.buf.bytes.len(),
                self.buf.header
            );
            None
        } else if self.n >= self.buf.bytes.len() {
            assert!(
                false,
                "Read {}/{} records, {}/{} bytes, header {:?}",
                self.i,
                self.buf.header.record_count as usize,
                self.n,
                self.buf.bytes.len(),
                self.buf.header
            );
            unreachable!();
        } else {
            let len = self.read_vari64();
            let initial_n = self.n;
            trace!("Len {:?}", len);
            let attrs: i8 = self.read_u8() as i8;
            trace!("Attrs {:x}", attrs);
            let ts_delta = self.read_vari64() as u32;
            let offset_delta = self.read_vari64() as u32;
            trace!("Deltas: {} {}", ts_delta, offset_delta);

            let key_len = self.read_vari64();
            trace!("key_len: {}", key_len);
            // key_len may be -1, means skip
            let key = if key_len >= 0 {
                self.n += key_len as usize;
                Some(&self.buf.bytes[self.n - key_len as usize..self.n])
            } else {
                None
            };

            // val_len may be -1, means skip.  We distinguish between a zero
            // length key and a None key, to simplify testing (so that deserializing
            // and reserializing a batch gives identical result)
            let val_len = self.read_vari64();
            let val = if val_len >= 0 {
                self.n += val_len as usize;
                Some(&self.buf.bytes[self.n - val_len as usize..self.n])
            } else {
                None
            };
            trace!("Key, val: {} {}", key_len, val_len);

            let mut headers: Vec<(&[u8], &[u8])> = vec![];
            let n_headers = self.read_vari64() as usize;
            for _header_i in 0..n_headers {
                let key_len = self.read_vari64() as usize;
                self.n += key_len;
                let h_key = &self.buf.bytes[self.n - key_len as usize..self.n];

                let val_len = self.read_vari64() as usize;
                self.n += val_len;
                let h_val = &self.buf.bytes[self.n - val_len as usize..self.n];
                headers.push((h_key, h_val));
                trace!("Header Key, val: {} {}", key_len, val_len);
            }

            let record_bytes_read = self.n - initial_n;
            debug!("Record bytes read {}, len {}", record_bytes_read, len);

            self.i += 1;

            Some(Record {
                len: len as u32,
                attrs,
                ts_delta,
                offset_delta,
                key,
                value: val,
                headers,
            })
        }
    }
}

impl BatchBuffer {
    pub fn iter(&self) -> RecordIter {
        return RecordIter {
            n: std::mem::size_of::<RecordBatchHeader>(),
            i: 0,
            buf: self,
        };
    }
}

fn hexdump(bytes: &[u8]) -> String {
    let mut output = String::new();
    for b in bytes {
        output.push_str(&format!("{:02x}", b));
    }
    output
}

impl<T: AsyncReadExt + Unpin> BatchStream<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            bytes_read: 0,
            cur_batch_raw: vec![],
        }
    }

    pub fn _get_bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Call read_exact on the inner stream, copying the read bytes into
    /// the `cur_batch_raw` accumulator buffer.
    async fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let r = self.inner.read_exact(buf).await?;
        self.cur_batch_raw.extend_from_slice(&buf[0..r]);
        Ok(r)
    }

    async fn take_exact(&mut self, n: usize) -> io::Result<usize> {
        let start = self.cur_batch_raw.len();
        for _ in 0..n {
            self.cur_batch_raw.push(0);
        }
        let r = self
            .inner
            .read_exact(&mut self.cur_batch_raw[start..start + n])
            .await?;
        self.bytes_read += r as u64;
        Ok(r)
    }

    async fn _read_vari64(&mut self) -> io::Result<(i64, u8)> {
        let mut decoder = VarIntDecoder::new();
        let mut read_bytes = 0u8;
        loop {
            let b = self._read_u8().await?;
            read_bytes += 1;
            if decoder.feed(b) {
                break;
            }
        }

        self.bytes_read += read_bytes as u64;
        Ok((decoder.result(), read_bytes))
    }

    async fn _read_u8(&mut self) -> io::Result<u8> {
        let mut b: [u8; 1] = [0u8; 1];
        let read_sz = self.read_exact(&mut b).await?;
        self.bytes_read += read_sz as u64;
        Ok(b[0])
    }

    async fn _read_vec(&mut self, size: usize) -> io::Result<Vec<u8>> {
        let mut b: [u8; 4096] = [0u8; 4096];
        let mut result = Vec::<u8>::new();

        if size == 0 {
            return Ok(result);
        }

        loop {
            let bytes_remaining = size - result.len();
            let read_sz = self.read_exact(&mut b[0..bytes_remaining]).await?;
            self.bytes_read += read_sz as u64;
            result.extend_from_slice(&b[0..bytes_remaining]);
            if result.len() >= size {
                return Ok(result);
            }
        }
    }

    async fn read_batch_header(&mut self) -> Result<(RecordBatchHeader, u32), DumpError> {
        let mut header_buf: [u8; std::mem::size_of::<RecordBatchHeader>()] =
            [0u8; std::mem::size_of::<RecordBatchHeader>()];
        let read_sz = self.read_exact(&mut header_buf).await?;
        self.bytes_read += read_sz as u64;
        if read_sz < header_buf.len() {
            info!("EOF");
            Err(DumpError::EOF)
        } else {
            let result: RecordBatchHeader =
                from_adl_bytes(&header_buf, bincode::config::standard())?;

            // Buffer for CRC32C calculation on the header_crc field of a batcn header: little
            // endian fixed-integer-width encoding of the header, omitting its leading 4 bytes
            // of CRC.
            let header_crc_bytes = &header_buf[4..];
            let header_crcc = crc32c::crc32c(header_crc_bytes);
            trace!(
                "read_batch_header header_crc_bytes({}) {} {}",
                header_crcc,
                header_crc_bytes.len(),
                hexdump(&header_crc_bytes)
            );
            Ok((result, header_crcc))
        }
    }

    pub async fn read_batch_buffer(&mut self) -> Result<BatchBuffer, DumpError> {
        // Read one batch header (fixed size)
        let (batch_header, header_hash) = self.read_batch_header().await?;
        debug!(
            "Batch {:?} {} {}",
            batch_header,
            header_hash,
            std::mem::size_of::<RecordBatchHeader>()
        );

        // Fast-reject path avoids calculating CRC if the header is clearly invalid
        if batch_header.record_batch_type == 0
            || batch_header.record_batch_type >= RecordBatchType::Max as i8
        {
            debug!("Blank batch hit (falloc region)");
            return Err(DumpError::EOF);
        }

        if batch_header.header_crc != header_hash {
            info!("Bad CRC on batch header, torn write at end of local-storage segment?");
            return Err(DumpError::EOF);
        }

        // CRC calculation is based on header bytes as they would be encoded by Kafka, not as
        // they are encoded by redpanda.  We must rebuild the header.

        // TODO: CRC check is not mandatory when reading from S3 objects, as these are
        // written atomically and do not have junk at end.
        // TODO: we must also check for bytes remaining, in case of an incompletely
        // written batch in a disk log.

        // Skip over the records: they will be consumed via RecordIter if
        // the caller really wants them.
        let body_size = batch_header.size_bytes as usize - std::mem::size_of::<RecordBatchHeader>();
        let r = self.take_exact(body_size).await?;
        if r != body_size {
            debug!("Short body read (header {:?})", batch_header);
            return Err(DumpError::EOF);
        }

        let (body_crc32c, _) = batch_body_crc(
            &batch_header,
            &self.cur_batch_raw[std::mem::size_of::<RecordBatchHeader>()..],
        );
        if body_crc32c != batch_header.crc {
            info!(
                "Batch CRC mismatch ({:08x} != {:08x})",
                body_crc32c, batch_header.crc as u32
            );
            // TODO: A stronger check: this _can_ happen on a torn write at the end
            // of o local disk segment, but if we aren't anywhere near the end of the seg,
            // or if we're reading an object storage segment, this is a real corruption.
            return Err(DumpError::EOF);
        }

        let batch_bytes = std::mem::take(&mut self.cur_batch_raw);
        Ok(BatchBuffer {
            header: batch_header,
            bytes: batch_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::info;
    use std::env;
    use tokio::fs::File;
    use tokio::io::BufReader;

    #[test_log::test(tokio::test)]
    pub async fn decode_simple() {
        let cargo_path = env::var("CARGO_MANIFEST_DIR").unwrap();

        // A simple segment file with 5 batches, 1 record in each one,
        // written by Redpanda 22.3
        let expect_batches = 5;
        let expect_records = 5;
        let filename = cargo_path + "/resources/test/test_segment.bin";

        let file = File::open(filename).await.unwrap();
        let reader = BufReader::new(file);
        let mut stream = BatchStream::new(reader);

        let types: Vec<RecordBatchType> = vec![
            RecordBatchType::RaftConfig,
            RecordBatchType::RaftData,
            RecordBatchType::RaftData,
            RecordBatchType::RaftData,
            RecordBatchType::RaftData,
        ];

        let mut count: usize = 0;
        let mut record_count: usize = 0;
        loop {
            match stream.read_batch_buffer().await {
                Ok(bb) => {
                    info!(
                        "Read batch {} bytes, header {:?}",
                        bb.bytes.len(),
                        bb.header
                    );
                    assert_eq!(types[count] as i8, bb.header.record_batch_type);
                    count += 1;
                    record_count += bb.iter().count();
                    assert!(count <= expect_batches);
                }
                Err(e) => {
                    info!("Stream complete: {:?}", e);
                    break;
                }
            }
        }
        assert_eq!(count, expect_batches);
        assert_eq!(record_count, expect_records);
        assert_eq!(stream.bytes_read, 929);
    }

    #[test_log::test(tokio::test)]
    pub async fn decode_simple_two() {
        let cargo_path = env::var("CARGO_MANIFEST_DIR").unwrap();

        // Written by Redpanda 22.3
        let expect_batches = 1;
        let expect_records = 1;
        let filename = cargo_path + "/resources/test/3676-7429-77-1-v1.log.1";

        let file = File::open(filename).await.unwrap();
        let file_size = file.metadata().await.unwrap().len();
        let reader = BufReader::new(file);
        let mut stream = BatchStream::new(reader);

        let mut count: usize = 0;
        let mut record_count: usize = 0;
        loop {
            match stream.read_batch_buffer().await {
                Ok(bb) => {
                    info!(
                        "Read batch {} bytes, header {:?}",
                        bb.bytes.len(),
                        bb.header
                    );
                    count += 1;
                    assert!(count <= expect_batches);
                    record_count += bb.iter().count();
                    assert_eq!(bb.header.record_batch_type, RecordBatchType::RaftData as i8);
                }
                Err(e) => {
                    info!("Stream complete: {:?}", e);
                    break;
                }
            }
        }

        assert_eq!(count, expect_batches);
        assert_eq!(record_count, expect_records);
        assert_eq!(stream.bytes_read, file_size);
    }
}
