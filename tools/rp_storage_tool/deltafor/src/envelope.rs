use std::io;

pub struct SerdeEnvelope {
    pub version: u8,
    pub compat_version: u8,
    pub size: u32,
}

impl SerdeEnvelope {
    pub fn new() -> Self {
        Self {
            version: 0,
            compat_version: 0,
            size: 0,
        }
    }

    fn read_impl(reader: &mut dyn io::Read) -> Result<(u8, u8, u32), io::Error> {
        let mut hdr: [u8; 6] = [0; 6];
        reader.read_exact(&mut hdr)?;
        Ok((
            hdr[0],
            hdr[1],
            u32::from_le_bytes(hdr[2..6].try_into().unwrap()),
        ))
    }

    pub fn from(reader: &mut dyn io::Read) -> Result<Self, io::Error> {
        let (version, compat_version, size) = Self::read_impl(reader)?;
        Ok(Self {
            version,
            compat_version,
            size,
        })
    }

    pub fn read(&mut self, reader: &mut dyn io::Read) -> Result<(), io::Error> {
        let (version, compat_version, size) = Self::read_impl(reader)?;
        self.version = version;
        self.compat_version = compat_version;
        self.size = size;
        Ok(())
    }
}

pub struct SerdeEnvelopeContext {
    pub envelope: SerdeEnvelope,
    start_position: u64,
}

/// When decoding, it is useful to remember the cursor position where we started
/// reading an envelope body, and later validate that we consumed the expected
/// number of bytes.
impl SerdeEnvelopeContext {
    pub fn from(
        my_version: u8,
        mut cursor: &mut std::io::Cursor<&[u8]>,
    ) -> Result<Self, io::Error> {
        let envelope = SerdeEnvelope::from(&mut cursor)?;
        assert!(envelope.compat_version <= my_version);
        Ok(SerdeEnvelopeContext {
            envelope,
            start_position: cursor.position(),
        })
    }

    pub fn end(&self, cursor: &std::io::Cursor<&[u8]>) {
        let expect_end = self.start_position + self.envelope.size as u64;
        if cursor.position() > expect_end {
            assert!(false, "Read too many bytes");
        } else {
            let trailing_bytes = expect_end - cursor.position();
            // TODO: something cleaner, currently we just assume that this
            // binary is always fully up to date on formats and therefore
            // should never have trailing bytes;
            assert_eq!(trailing_bytes, 0);
        }
    }
}

pub struct OffsetIndexHeader {
    pub header: SerdeEnvelope,
    pub min_file_pos_step: i64,
    pub num_elements: u64,
    pub base_rp: i64,
    pub last_rp: i64,
    pub base_kaf: i64,
    pub last_kaf: i64,
    pub base_file: i64,
    pub last_file: i64,
    pub rp_write_buf: Vec<i64>,
    pub kaf_write_buf: Vec<i64>,
    pub file_write_buf: Vec<i64>,
    pub rp_index: Vec<u8>,
    pub kaf_index: Vec<u8>,
    pub file_index: Vec<u8>,
}

impl OffsetIndexHeader {
    pub fn new() -> Self {
        OffsetIndexHeader {
            header: SerdeEnvelope::new(),
            min_file_pos_step: 0,
            num_elements: 0,
            base_rp: 0,
            last_rp: 0,
            base_kaf: 0,
            last_kaf: 0,
            base_file: 0,
            last_file: 0,
            rp_write_buf: Vec::<i64>::new(),
            kaf_write_buf: Vec::<i64>::new(),
            file_write_buf: Vec::<i64>::new(),
            rp_index: Vec::<u8>::new(),
            kaf_index: Vec::<u8>::new(),
            file_index: Vec::<u8>::new(),
        }
    }
    pub fn read(&mut self, reader: &mut dyn io::Read) -> Result<(), io::Error> {
        self.header.read(reader)?;
        let mut buf: [u8; 64] = [0; 64];
        reader.read_exact(&mut buf)?;
        self.min_file_pos_step = i64::from_le_bytes(buf[0..8].try_into().unwrap());
        self.num_elements = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        self.base_rp = i64::from_le_bytes(buf[16..24].try_into().unwrap());
        self.last_rp = i64::from_le_bytes(buf[24..32].try_into().unwrap());
        self.base_kaf = i64::from_le_bytes(buf[32..40].try_into().unwrap());
        self.last_kaf = i64::from_le_bytes(buf[40..48].try_into().unwrap());
        self.base_file = i64::from_le_bytes(buf[48..56].try_into().unwrap());
        self.last_file = i64::from_le_bytes(buf[56..64].try_into().unwrap());
        // Read 8 element vectors: 4-bytes size + 8-bytes * 16 elements;
        const BUF_SIZE: usize = 4 + 16 * 8;
        let mut buf_vec: [u8; BUF_SIZE] = [0; BUF_SIZE];
        let mut sz: u32;
        // Read rp_write_buf
        reader.read_exact(&mut buf_vec)?;
        sz = u32::from_le_bytes(buf_vec[0..4].try_into().unwrap());
        if sz != 16 {
            panic!("Unexpected vector size {}", sz);
        }
        for ix in 0..16 {
            let begin = 4 + ix * 8;
            let end = 4 + (ix + 1) * 8;
            let elem = i64::from_le_bytes(buf_vec[begin..end].try_into().unwrap());
            self.rp_write_buf.push(elem);
        }
        // Read kaf_write_buf
        reader.read_exact(&mut buf_vec)?;
        sz = u32::from_le_bytes(buf_vec[0..4].try_into().unwrap());
        if sz != 16 {
            panic!("Unexpected vector size {}", sz);
        }
        for ix in 0..16 {
            let begin = 4 + ix * 8;
            let end = 4 + (ix + 1) * 8;
            let elem = i64::from_le_bytes(buf_vec[begin..end].try_into().unwrap());
            self.kaf_write_buf.push(elem);
        }
        // Read file_write_buf
        reader.read_exact(&mut buf_vec)?;
        sz = u32::from_le_bytes(buf_vec[0..4].try_into().unwrap());
        if sz != 16 {
            panic!("Unexpected vector size {}", sz);
        }
        for ix in 0..16 {
            let begin = 4 + ix * 8;
            let end = 4 + (ix + 1) * 8;
            let elem = i64::from_le_bytes(buf_vec[begin..end].try_into().unwrap());
            self.file_write_buf.push(elem);
        }
        // Read iobufs
        // rp_index
        let mut sz_buf: [u8; 4] = [0; 4];
        reader.read_exact(&mut sz_buf)?;
        sz = u32::from_le_bytes(sz_buf);
        self.rp_index.resize(sz as usize, 0);
        reader.read_exact(&mut self.rp_index)?;
        // kaf_index
        reader.read_exact(&mut sz_buf)?;
        sz = u32::from_le_bytes(sz_buf);
        self.kaf_index.resize(sz as usize, 0);
        reader.read_exact(&mut self.kaf_index)?;
        // file_index
        reader.read_exact(&mut sz_buf)?;
        sz = u32::from_le_bytes(sz_buf);
        self.file_index.resize(sz as usize, 0);
        reader.read_exact(&mut self.file_index)?;
        Ok(())
    }
}
