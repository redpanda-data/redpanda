use serde::Serialize;
use std::io;
use std::io::{Error, ErrorKind};

pub mod bitpack;
pub mod envelope;

// The trait is implemented only for i64 since in redpanda we're only
// using DeltaFOR with this type.
pub trait DeltaAlg {
    fn new(step: Option<i64>) -> Self;
    fn encode(&mut self, last: i64, row: &[i64; 16], buf: &mut [i64; 16]) -> u8;
    fn decode(&mut self, init: i64, row: &mut [i64; 16]) -> i64;
}

pub struct DeltaXor {
    #[allow(dead_code)]
    step_size: i64,
}

impl DeltaAlg for DeltaXor {
    fn new(step: Option<i64>) -> Self {
        DeltaXor {
            step_size: step.unwrap_or(0),
        }
    }
    fn encode(&mut self, last: i64, row: &[i64; 16], buf: &mut [i64; 16]) -> u8 {
        let mut p = last;
        let mut agg: i64 = 0;
        for i in 0..16 {
            buf[i] = row[i] ^ p;
            agg = agg | buf[i];
            p = row[i];
        }
        return 64 - (agg as u64).leading_zeros() as u8;
    }
    fn decode(&mut self, init: i64, row: &mut [i64; 16]) -> i64 {
        let mut p = init;
        for i in 0..16 {
            row[i] = row[i] ^ p;
            p = row[i];
        }
        return p;
    }
}

pub struct DeltaDelta {
    step_size: i64,
}

impl DeltaAlg for DeltaDelta {
    fn new(step: Option<i64>) -> Self {
        DeltaDelta {
            step_size: step.unwrap_or(0),
        }
    }
    fn encode(&mut self, last: i64, row: &[i64; 16], buf: &mut [i64; 16]) -> u8 {
        let mut p = last;
        let mut agg: i64 = 0;
        for i in 0..16 {
            assert!(
                row[i] >= p,
                "Value {} can't be larger than the previous one {}",
                row[i],
                p
            );
            let delta = row[i] - p;
            assert!(
                delta >= self.step_size,
                "Delta {} can't be larger than step size {}",
                delta,
                self.step_size
            );
            buf[i] = (row[i] - p) - self.step_size;
            agg |= buf[i];
            p = row[i];
        }
        return 64 - (agg as u64).leading_zeros() as u8;
    }
    fn decode(&mut self, init: i64, row: &mut [i64; 16]) -> i64 {
        let mut p = init;
        for i in 0..16 {
            row[i] = row[i] + p + self.step_size;
            p = row[i];
        }
        return p;
    }
}

#[allow(dead_code)]
pub struct DeltaFOREncoder<Alg: DeltaAlg> {
    delta_alg: Alg,
    count: u64,
    last: i64,
}

#[allow(dead_code)]
impl<Alg: DeltaAlg> DeltaFOREncoder<Alg> {
    pub fn new(init: i64, step: Option<i64>) -> Self {
        DeltaFOREncoder {
            delta_alg: Alg::new(step),
            count: 0,
            last: init,
        }
    }

    pub fn add_row(&mut self, row: &[i64; 16], data: &mut dyn io::Write) -> Result<(), Error> {
        let mut buf: [i64; 16] = [0; 16];
        let nbits = self.delta_alg.encode(self.last, row, &mut buf);
        self.last = row[15];
        data.write_all(&nbits.to_le_bytes())?;
        let mut tmp = buf.map(|x| x as u64);
        bitpack::pack(&mut tmp, nbits as u32, data)?;
        self.count += 1;
        Ok(())
    }
}

pub struct DeltaFORDecoder<Alg: DeltaAlg> {
    delta_alg: Alg,
    count: u64,
    initial: i64,
    pos: u64,
}

impl<Alg: DeltaAlg> DeltaFORDecoder<Alg> {
    pub fn new(count: u64, initial_value: i64, step: Option<i64>) -> Self {
        DeltaFORDecoder {
            delta_alg: Alg::new(step),
            count: count,
            initial: initial_value,
            pos: 0,
        }
    }

    pub fn read_row(&mut self, row: &mut [i64; 16], data: &mut dyn io::Read) -> Result<(), Error> {
        if self.pos == self.count {
            return Err(Error::new(ErrorKind::UnexpectedEof, "EOF"));
        }
        let mut nbuf: [u8; 1] = [0; 1];
        data.read_exact(&mut nbuf).unwrap();
        let nbits = nbuf[0];
        let mut buf: [u64; 16] = [0; 16];
        bitpack::unpack(&mut buf, nbits as u32, data).unwrap();
        *row = buf.map(|x| x as i64);
        self.initial = self.delta_alg.decode(self.initial, row);
        self.pos = self.pos + 1;
        return Ok(());
    }
}

pub fn read_index_header(
    reader: &mut dyn io::Read,
) -> Result<envelope::OffsetIndexHeader, io::Error> {
    let mut hdr = envelope::OffsetIndexHeader::new();
    hdr.read(reader)?;
    Ok(hdr)
}

#[derive(Serialize)]
pub struct OffsetIndex {
    pub min_file_pos_step: i64,
    pub num_elements: u64,
    #[serde(rename = "base_offset")]
    pub base_rp: i64,
    #[serde(rename = "last_offset")]
    pub last_rp: i64,
    #[serde(rename = "base_kafka_offset")]
    pub base_kaf: i64,
    #[serde(rename = "last_kafka_offset")]
    pub last_kaf: i64,
    #[serde(rename = "base_file_offset")]
    pub base_file: i64,
    #[serde(rename = "last_file_offset")]
    pub last_file: i64,
    #[serde(rename = "redpanda_offsets")]
    pub rp_offsets: Vec<i64>,
    #[serde(rename = "kafka_offsets")]
    pub kaf_offsets: Vec<i64>,
    #[serde(rename = "file_offsets")]
    pub file_offsets: Vec<i64>,
}

pub fn read_index(reader: &mut dyn io::Read) -> Result<OffsetIndex, io::Error> {
    let header = read_index_header(reader)?;
    let mut index = OffsetIndex {
        min_file_pos_step: header.min_file_pos_step,
        num_elements: header.num_elements,
        base_rp: header.base_rp,
        last_rp: header.last_rp,
        base_kaf: header.base_kaf,
        last_kaf: header.last_kaf,
        base_file: header.base_file,
        last_file: header.last_file,
        rp_offsets: Vec::<i64>::new(),
        kaf_offsets: Vec::<i64>::new(),
        file_offsets: Vec::<i64>::new(),
    };
    // Decode rp-offsets
    {
        let mut dec =
            DeltaFORDecoder::<DeltaXor>::new(header.num_elements / 16, header.base_rp, Some(0));
        let mut rp_cursor = io::Cursor::new(header.rp_index);
        for _ in 0..header.num_elements / 16 {
            let mut buf: [i64; 16] = [0; 16];
            dec.read_row(&mut buf, &mut rp_cursor)?;
            for o in buf {
                index.rp_offsets.push(o);
            }
        }
        for i in 0..header.num_elements % 16 {
            index.rp_offsets.push(header.rp_write_buf[i as usize]);
        }
    }
    // Decode kaf-offsets
    {
        let mut dec =
            DeltaFORDecoder::<DeltaXor>::new(header.num_elements / 16, header.base_kaf, Some(0));
        let mut kaf_cursor = io::Cursor::new(header.kaf_index);
        for _ in 0..header.num_elements / 16 {
            let mut buf: [i64; 16] = [0; 16];
            dec.read_row(&mut buf, &mut kaf_cursor)?;
            for o in buf {
                index.kaf_offsets.push(o);
            }
        }
        for i in 0..header.num_elements % 16 {
            index.kaf_offsets.push(header.kaf_write_buf[i as usize]);
        }
    }
    // Decode file-offsets
    {
        let mut dec = DeltaFORDecoder::<DeltaDelta>::new(
            header.num_elements / 16,
            header.base_file,
            Some(header.min_file_pos_step),
        );
        let mut cursor = io::Cursor::new(header.file_index);
        for _ in 0..header.num_elements / 16 {
            let mut buf: [i64; 16] = [0; 16];
            dec.read_row(&mut buf, &mut cursor)?;
            for o in buf {
                index.file_offsets.push(o);
            }
        }
        for i in 0..header.num_elements % 16 {
            index.file_offsets.push(header.file_write_buf[i as usize]);
        }
    }
    Ok(index)
}
