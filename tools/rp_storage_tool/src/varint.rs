use std::io::Write;

pub struct VarIntDecoder {
    shift: u8,
    result: i64,
}

impl VarIntDecoder {
    pub fn new() -> Self {
        Self {
            shift: 0,
            result: 0,
        }
    }

    /// Return true when caller should stop feeding us bytes
    pub fn feed(&mut self, b: u8) -> bool {
        let stop = if b & 128 > 0 {
            self.result |= ((b & 0x7fu8) as i64) << self.shift as i64;
            false
        } else {
            self.result |= (b as i64) << (self.shift as i64);
            true
        };
        self.shift += 7;

        if (self.shift as usize) >= 8 * 7 + 1 {
            // We've taken the max bytes that can make up
            // the type we wanted: stop feeding
            true
        } else {
            stop
        }
    }

    pub fn result(&self) -> i64 {
        let r = (self.result >> 1) ^ (!(self.result & 1) + 1);
        r
    }
}

pub fn varint_encode_to<T: Write>(out: &mut T, v_raw: i64) -> std::io::Result<usize> {
    let mut v = (v_raw << 1) ^ (v_raw >> 63);
    let mut wrote = 0;

    loop {
        let mut bits = (v & 0x7fi64) as u8;
        let stop = if v > 0x7f {
            v = v >> 7;
            bits |= 0x80;
            false
        } else {
            true
        };

        let out_arr: [u8; 1] = [bits; 1];
        out.write_all(&out_arr)?;
        wrote += 1;
        if stop {
            break;
        }
    }

    Ok(wrote)
}

pub fn varint_size(v_raw: i64) -> usize {
    let mut v = (v_raw << 1) ^ (v_raw >> 63);
    let mut size = 0;
    loop {
        size += 1;
        if v <= 0x7f {
            break;
        } else {
            v = v >> 7;
        }
    }
    size
}

#[cfg(test)]
mod tests {
    use crate::varint::{varint_encode_to, varint_size, VarIntDecoder};
    use log::error;

    fn varint_encode(v_raw: i64) -> Vec<u8> {
        let mut out: Vec<u8> = vec![];
        varint_encode_to(&mut out, v_raw).unwrap();
        out
    }

    fn assert_decode<const N: usize>(bytes: [u8; N], expect: i64) {
        let mut d = VarIntDecoder::new();

        let mut stopped = false;
        for b in bytes {
            let stop = d.feed(b);
            if stop && !stopped {
                stopped = true;
            } else if stop && stopped {
                // We fed it more bytes than it wanted!
                assert!(false);
            }
        }

        // We fed it all our bytes, but it didn't see the end.
        assert!(stopped);

        assert_eq!(d.result(), expect);
    }

    fn assert_encode<const N: usize>(expect_bytes: [u8; N], n: i64) {
        let encoded = varint_encode(n);
        assert_eq!(encoded.len(), N);
        for i in 0..N {
            if encoded[i] != expect_bytes[i] {
                error!(
                    "Bad byte encoding {} -- {} {:x} != {:x}",
                    n, i, encoded[i], expect_bytes[i]
                );
            }
            assert_eq!(encoded[i], expect_bytes[i]);
        }

        assert_eq!(varint_size(n), encoded.len());
    }

    fn assert_encode_decode(n: i64) {
        let bytes = varint_encode(n);
        assert_eq!(bytes.len(), varint_size(n));

        let mut d = VarIntDecoder::new();
        for i in 0..bytes.len() {
            let stop = d.feed(bytes[i]);
            assert_eq!(stop, i == bytes.len() - 1);
        }

        assert_eq!(d.result(), n);
    }

    #[test_log::test]
    pub fn decode_simple() {
        assert_decode([0x01u8], -1);
        assert_decode([0x32u8], 25);
        assert_decode([0xcc, 0x14], 1318);
        assert_decode([0xee, 0xd], 887);
    }

    #[test_log::test]
    pub fn encode_simple() {
        assert_encode([0x32u8], 25);
        assert_encode([0x01u8], -1);
        assert_encode([0xcc, 0x14], 1318);
        assert_encode([0xee, 0xd], 887);
    }

    #[test_log::test]
    pub fn encode_roundtrip() {
        assert_encode_decode(-1);
        assert_encode_decode(-1000);
        assert_encode_decode(-10000000);
        assert_encode_decode(-10000000000);
        assert_encode_decode(0);
        assert_encode_decode(10);
        assert_encode_decode(1000);
        assert_encode_decode(100000000);
        assert_encode_decode(10000000000000);
    }
}
