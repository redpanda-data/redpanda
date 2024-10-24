use std::io;
use std::io::{Error, ErrorKind};
use std::mem::size_of;
use std::mem::MaybeUninit;

// Convert value to bit-sequence
// and back.
pub trait BitseqConvertable {
    fn to_bits(self) -> u64;
    fn from_bits(&mut self, d: u64);
}

#[repr(C)]
union f64xu64 {
    x: f64,
    d: u64,
}

impl BitseqConvertable for f64 {
    fn to_bits(self) -> u64 {
        let mut u = MaybeUninit::<f64xu64>::uninit();
        let ptr = u.as_mut_ptr() as *mut f64xu64;
        unsafe {
            (*ptr).x = self;
            return (*ptr).d;
        }
    }

    fn from_bits(&mut self, d: u64) {
        let mut u = MaybeUninit::<f64xu64>::uninit();
        let ptr = u.as_mut_ptr() as *mut f64xu64;
        unsafe {
            (*ptr).d = d;
            *self = (*ptr).x;
        }
    }
}

impl BitseqConvertable for u64 {
    fn to_bits(self) -> u64 {
        self as u64
    }
    fn from_bits(&mut self, d: u64) {
        *self = d as u64
    }
}

impl BitseqConvertable for i64 {
    fn to_bits(self) -> u64 {
        return self as u64;
    }
    fn from_bits(&mut self, d: u64) {
        *self = d as i64
    }
}

fn _pack_n<T>(xs: &[u64; 16], buf: &mut dyn io::Write) -> Result<(), Error> {
    let sz = size_of::<T>();
    for x in xs {
        buf.write_all(&x.to_le_bytes()[0..sz])?;
    }
    Ok(())
}

fn _pack1(xs: &[u64; 16], buf: &mut dyn io::Write) -> Result<(), Error> {
    let mut value: u16 = 0;
    for ix in 0..16 {
        let bit = xs[ix] & 1;
        value |= (bit << ix) as u16;
    }
    buf.write_all(&value.to_le_bytes())
}

fn _pack2(xs: &[u64; 16], buf: &mut dyn io::Write) -> Result<(), Error> {
    let mut value: u32 = 0;
    for ix in 0..16 {
        let bit = xs[ix] & 1;
        value |= (bit << 2 * ix) as u32;
    }
    buf.write_all(&value.to_le_bytes())
}

fn _pack3(xs: &[u64; 16], buf: &mut dyn io::Write) -> Result<(), Error> {
    let mut bits0: u32 = 0;
    let mut bits1: u16 = 0;
    bits0 |= (xs[0] & 7) as u32;
    bits0 |= ((xs[1] & 7) << 3) as u32;
    bits0 |= ((xs[2] & 7) << 6) as u32;
    bits0 |= ((xs[3] & 7) << 9) as u32;
    bits0 |= ((xs[4] & 7) << 12) as u32;
    bits0 |= ((xs[5] & 7) << 15) as u32;
    bits0 |= ((xs[6] & 7) << 18) as u32;
    bits0 |= ((xs[7] & 7) << 21) as u32;
    bits0 |= ((xs[8] & 7) << 24) as u32;
    bits0 |= ((xs[9] & 7) << 27) as u32;
    bits0 |= ((xs[10] & 3) << 30) as u32;
    bits1 |= ((xs[10] & 4) >> 2) as u16;
    bits1 |= ((xs[11] & 7) << 1) as u16;
    bits1 |= ((xs[12] & 7) << 4) as u16;
    bits1 |= ((xs[13] & 7) << 7) as u16;
    bits1 |= ((xs[14] & 7) << 10) as u16;
    bits1 |= ((xs[15] & 7) << 13) as u16;
    buf.write_all(&bits0.to_le_bytes())?;
    buf.write_all(&bits1.to_le_bytes())
}

fn _pack4(xs: &[u64; 16], buf: &mut dyn io::Write) -> Result<(), Error> {
    let mut bits0: u64 = 0;
    bits0 |= xs[0] & 0xF;
    bits0 |= (xs[1] & 0xF) << 4;
    bits0 |= (xs[2] & 0xF) << 8;
    bits0 |= (xs[3] & 0xF) << 12;
    bits0 |= (xs[4] & 0xF) << 16;
    bits0 |= (xs[5] & 0xF) << 20;
    bits0 |= (xs[6] & 0xF) << 24;
    bits0 |= (xs[7] & 0xF) << 28;
    bits0 |= (xs[8] & 0xF) << 32;
    bits0 |= (xs[9] & 0xF) << 36;
    bits0 |= (xs[10] & 0xF) << 40;
    bits0 |= (xs[11] & 0xF) << 44;
    bits0 |= (xs[12] & 0xF) << 48;
    bits0 |= (xs[13] & 0xF) << 52;
    bits0 |= (xs[14] & 0xF) << 56;
    bits0 |= (xs[15] & 0xF) << 60;
    buf.write_all(&bits0.to_le_bytes())
}

fn _pack5(xs: &[u64; 16], buf: &mut dyn io::Write) -> Result<(), Error> {
    let mut bits0: u64 = 0;
    let mut bits1: u16 = 0;
    bits0 |= xs[0] & 0x1F;
    bits0 |= (xs[1] & 0x1F) << 5;
    bits0 |= (xs[2] & 0x1F) << 10;
    bits0 |= (xs[3] & 0x1F) << 15;
    bits0 |= (xs[4] & 0x1F) << 20;
    bits0 |= (xs[5] & 0x1F) << 25;
    bits0 |= (xs[6] & 0x1F) << 30;
    bits0 |= (xs[7] & 0x1F) << 35;
    bits0 |= (xs[8] & 0x1F) << 40;
    bits0 |= (xs[9] & 0x1F) << 45;
    bits0 |= (xs[10] & 0x1F) << 50;
    bits0 |= (xs[11] & 0x1F) << 55;
    bits0 |= (xs[12] & 0x0F) << 60;
    bits1 |= ((xs[12] & 0x10) >> 4) as u16;
    bits1 |= ((xs[13] & 0x1F) << 1) as u16;
    bits1 |= ((xs[14] & 0x1F) << 6) as u16;
    bits1 |= ((xs[15] & 0x1F) << 11) as u16;
    buf.write_all(&bits0.to_le_bytes())?;
    buf.write_all(&bits1.to_le_bytes())
}

fn _pack6(xs: &[u64; 16], buf: &mut dyn io::Write) -> Result<(), Error> {
    let mut bits0: u64 = 0;
    let mut bits1: u32 = 0;
    bits0 |= xs[0] & 0x3F;
    bits0 |= (xs[1] & 0x3F) << 6;
    bits0 |= (xs[2] & 0x3F) << 12;
    bits0 |= (xs[3] & 0x3F) << 18;
    bits0 |= (xs[4] & 0x3F) << 24;
    bits0 |= (xs[5] & 0x3F) << 30;
    bits0 |= (xs[6] & 0x3F) << 36;
    bits0 |= (xs[7] & 0x3F) << 42;
    bits0 |= (xs[8] & 0x3F) << 48;
    bits0 |= (xs[9] & 0x3F) << 54;
    bits0 |= (xs[10] & 0x0F) << 60;
    bits1 |= ((xs[10] & 0x30) >> 4) as u32;
    bits1 |= ((xs[11] & 0x3F) << 2) as u32;
    bits1 |= ((xs[12] & 0x3F) << 8) as u32;
    bits1 |= ((xs[13] & 0x3F) << 14) as u32;
    bits1 |= ((xs[14] & 0x3F) << 20) as u32;
    bits1 |= ((xs[15] & 0x3F) << 26) as u32;
    buf.write_all(&bits0.to_le_bytes())?;
    buf.write_all(&bits1.to_le_bytes())
}

fn _pack7(xs: &[u64; 16], buf: &mut dyn io::Write) -> Result<(), Error> {
    let mut bits0: u64 = 0;
    let mut bits1: u32 = 0;
    let mut bits2: u16 = 0;
    bits0 |= xs[0] & 0x7F;
    bits0 |= (xs[1] & 0x7F) << 7;
    bits0 |= (xs[2] & 0x7F) << 14;
    bits0 |= (xs[3] & 0x7F) << 21;
    bits0 |= (xs[4] & 0x7F) << 28;
    bits0 |= (xs[5] & 0x7F) << 35;
    bits0 |= (xs[6] & 0x7F) << 42;
    bits0 |= (xs[7] & 0x7F) << 49;
    bits0 |= (xs[8] & 0x7F) << 56;
    bits0 |= (xs[9] & 0x01) << 63;
    bits1 |= ((xs[9] & 0x7E) >> 1) as u32;
    bits1 |= ((xs[10] & 0x7F) << 6) as u32;
    bits1 |= ((xs[11] & 0x7F) << 13) as u32;
    bits1 |= ((xs[12] & 0x7F) << 20) as u32;
    bits1 |= ((xs[13] & 0x1F) << 27) as u32;
    bits2 |= ((xs[13] & 0x60) >> 5) as u16;
    bits2 |= ((xs[14] & 0x7F) << 2) as u16;
    bits2 |= ((xs[15] & 0x7F) << 9) as u16;
    buf.write_all(&bits0.to_le_bytes())?;
    buf.write_all(&bits1.to_le_bytes())?;
    buf.write_all(&bits2.to_le_bytes())
}

fn _shift_n<T>(xs: &mut [u64; 16]) {
    for ix in 0..16 {
        xs[ix] >>= 8 * size_of::<T>();
    }
}

fn _pack24(xs: &mut [u64; 16], buf: &mut dyn io::Write) -> Result<(), Error> {
    _pack_n::<u16>(xs, buf)?;
    _shift_n::<u16>(xs);
    _pack_n::<u8>(xs, buf)?;
    _shift_n::<u8>(xs);
    Ok(())
}

fn _pack40(xs: &mut [u64; 16], buf: &mut dyn io::Write) -> Result<(), Error> {
    _pack_n::<u32>(xs, buf)?;
    _shift_n::<u32>(xs);
    _pack_n::<u8>(xs, buf)?;
    _shift_n::<u8>(xs);
    Ok(())
}

fn _pack48(xs: &mut [u64; 16], buf: &mut dyn io::Write) -> Result<(), Error> {
    _pack_n::<u32>(xs, buf)?;
    _shift_n::<u32>(xs);
    _pack_n::<u16>(xs, buf)?;
    _shift_n::<u16>(xs);
    Ok(())
}

fn _pack56(xs: &mut [u64; 16], buf: &mut dyn io::Write) -> Result<(), Error> {
    _pack_n::<u32>(xs, buf)?;
    _shift_n::<u32>(xs);
    _pack_n::<u16>(xs, buf)?;
    _shift_n::<u16>(xs);
    _pack_n::<u8>(xs, buf)?;
    _shift_n::<u8>(xs);
    Ok(())
}

#[allow(dead_code)]
pub fn pack(xs: &mut [u64; 16], num_bits: u32, buf: &mut dyn io::Write) -> Result<(), Error> {
    match num_bits {
        0 => Ok(()),
        1 => _pack1(xs, buf),
        2 => _pack2(xs, buf),
        3 => _pack3(xs, buf),
        4 => _pack4(xs, buf),
        5 => _pack5(xs, buf),
        6 => _pack6(xs, buf),
        7 => _pack7(xs, buf),
        8 => _pack_n::<u8>(xs, buf),
        9 => {
            _pack_n::<u8>(xs, buf)?;
            _shift_n::<u8>(xs);
            _pack1(xs, buf)
        }
        10 => {
            _pack_n::<u8>(xs, buf)?;
            _shift_n::<u8>(xs);
            _pack2(xs, buf)
        }
        11 => {
            _pack_n::<u8>(xs, buf)?;
            _shift_n::<u8>(xs);
            _pack3(xs, buf)
        }
        12 => {
            _pack_n::<u8>(xs, buf)?;
            _shift_n::<u8>(xs);
            _pack4(xs, buf)
        }
        13 => {
            _pack_n::<u8>(xs, buf)?;
            _shift_n::<u8>(xs);
            _pack5(xs, buf)
        }
        14 => {
            _pack_n::<u8>(xs, buf)?;
            _shift_n::<u8>(xs);
            _pack6(xs, buf)
        }
        15 => {
            _pack_n::<u8>(xs, buf)?;
            _shift_n::<u8>(xs);
            _pack7(xs, buf)
        }
        16 => _pack_n::<u16>(xs, buf),
        17 => {
            _pack_n::<u16>(xs, buf)?;
            _shift_n::<u16>(xs);
            _pack1(xs, buf)
        }
        18 => {
            _pack_n::<u16>(xs, buf)?;
            _shift_n::<u16>(xs);
            _pack2(xs, buf)
        }
        19 => {
            _pack_n::<u16>(xs, buf)?;
            _shift_n::<u16>(xs);
            _pack3(xs, buf)
        }
        20 => {
            _pack_n::<u16>(xs, buf)?;
            _shift_n::<u16>(xs);
            _pack4(xs, buf)
        }
        21 => {
            _pack_n::<u16>(xs, buf)?;
            _shift_n::<u16>(xs);
            _pack5(xs, buf)
        }
        22 => {
            _pack_n::<u16>(xs, buf)?;
            _shift_n::<u16>(xs);
            _pack6(xs, buf)
        }
        23 => {
            _pack_n::<u16>(xs, buf)?;
            _shift_n::<u16>(xs);
            _pack7(xs, buf)
        }
        24 => {
            _pack_n::<u16>(xs, buf)?;
            _shift_n::<u16>(xs);
            _pack_n::<u8>(xs, buf)
        }
        25 => {
            _pack24(xs, buf)?;
            _pack1(xs, buf)
        }
        26 => {
            _pack24(xs, buf)?;
            _pack2(xs, buf)
        }
        27 => {
            _pack24(xs, buf)?;
            _pack3(xs, buf)
        }
        28 => {
            _pack24(xs, buf)?;
            _pack4(xs, buf)
        }
        29 => {
            _pack24(xs, buf)?;
            _pack5(xs, buf)
        }
        30 => {
            _pack24(xs, buf)?;
            _pack6(xs, buf)
        }
        31 => {
            _pack24(xs, buf)?;
            _pack7(xs, buf)
        }
        32 => _pack_n::<u32>(xs, buf),
        33 => {
            _pack_n::<u32>(xs, buf)?;
            _shift_n::<u32>(xs);
            _pack1(xs, buf)
        }
        34 => {
            _pack_n::<u32>(xs, buf)?;
            _shift_n::<u32>(xs);
            _pack2(xs, buf)
        }
        35 => {
            _pack_n::<u32>(xs, buf)?;
            _shift_n::<u32>(xs);
            _pack3(xs, buf)
        }
        36 => {
            _pack_n::<u32>(xs, buf)?;
            _shift_n::<u32>(xs);
            _pack4(xs, buf)
        }
        37 => {
            _pack_n::<u32>(xs, buf)?;
            _shift_n::<u32>(xs);
            _pack5(xs, buf)
        }
        38 => {
            _pack_n::<u32>(xs, buf)?;
            _shift_n::<u32>(xs);
            _pack6(xs, buf)
        }
        39 => {
            _pack_n::<u32>(xs, buf)?;
            _shift_n::<u32>(xs);
            _pack7(xs, buf)
        }
        40 => {
            _pack_n::<u32>(xs, buf)?;
            _shift_n::<u32>(xs);
            _pack_n::<u8>(xs, buf)
        }
        41 => {
            _pack40(xs, buf)?;
            _pack1(xs, buf)
        }
        42 => {
            _pack40(xs, buf)?;
            _pack2(xs, buf)
        }
        43 => {
            _pack40(xs, buf)?;
            _pack3(xs, buf)
        }
        44 => {
            _pack40(xs, buf)?;
            _pack4(xs, buf)
        }
        45 => {
            _pack40(xs, buf)?;
            _pack5(xs, buf)
        }
        46 => {
            _pack40(xs, buf)?;
            _pack6(xs, buf)
        }
        47 => {
            _pack40(xs, buf)?;
            _pack7(xs, buf)
        }
        48 => {
            _pack_n::<u32>(xs, buf)?;
            _shift_n::<u32>(xs);
            _pack_n::<u16>(xs, buf)
        }
        49 => {
            _pack48(xs, buf)?;
            _pack1(xs, buf)
        }
        50 => {
            _pack48(xs, buf)?;
            _pack2(xs, buf)
        }
        51 => {
            _pack48(xs, buf)?;
            _pack3(xs, buf)
        }
        52 => {
            _pack48(xs, buf)?;
            _pack4(xs, buf)
        }
        53 => {
            _pack48(xs, buf)?;
            _pack5(xs, buf)
        }
        54 => {
            _pack48(xs, buf)?;
            _pack6(xs, buf)
        }
        55 => {
            _pack48(xs, buf)?;
            _pack7(xs, buf)
        }
        56 => {
            _pack48(xs, buf)?;
            _pack_n::<u8>(xs, buf)
        }
        57 => {
            _pack56(xs, buf)?;
            _pack1(xs, buf)
        }
        58 => {
            _pack56(xs, buf)?;
            _pack2(xs, buf)
        }
        59 => {
            _pack56(xs, buf)?;
            _pack3(xs, buf)
        }
        60 => {
            _pack56(xs, buf)?;
            _pack4(xs, buf)
        }
        61 => {
            _pack56(xs, buf)?;
            _pack5(xs, buf)
        }
        62 => {
            _pack56(xs, buf)?;
            _pack6(xs, buf)
        }
        63 => {
            _pack56(xs, buf)?;
            _pack7(xs, buf)
        }
        64 => _pack_n::<u64>(xs, buf),
        _ => Err(Error::new(ErrorKind::Other, "invalid number of bits")),
    }
}

fn _unpack64(xs: &mut [u64; 16], shift: u32, buf: &mut dyn io::Read) -> Result<(), Error> {
    for i in 0..16 {
        let mut bytes = Vec::<u8>::new();
        bytes.resize(8, 0);
        buf.read_exact(bytes.as_mut_slice())?;
        xs[i] |= u64::from_le_bytes(bytes.as_slice().try_into().unwrap()) << shift;
    }
    Ok(())
}

fn _unpack32(xs: &mut [u64; 16], shift: u32, buf: &mut dyn io::Read) -> Result<(), Error> {
    for i in 0..16 {
        let mut bytes = Vec::<u8>::new();
        bytes.resize(4, 0);
        buf.read_exact(bytes.as_mut_slice())?;
        xs[i] |= (u32::from_le_bytes(bytes.as_slice().try_into().unwrap()) as u64) << shift;
    }
    Ok(())
}

fn _unpack16(xs: &mut [u64; 16], shift: u32, buf: &mut dyn io::Read) -> Result<(), Error> {
    for i in 0..16 {
        let mut bytes = Vec::<u8>::new();
        bytes.resize(2, 0);
        buf.read_exact(bytes.as_mut_slice())?;
        xs[i] |= (u16::from_le_bytes(bytes.as_slice().try_into().unwrap()) as u64) << shift;
    }
    Ok(())
}

fn _unpack8(xs: &mut [u64; 16], shift: u32, buf: &mut dyn io::Read) -> Result<(), Error> {
    for i in 0..16 {
        let mut bytes = Vec::<u8>::new();
        bytes.resize(1, 0);
        buf.read_exact(bytes.as_mut_slice())?;
        xs[i] |= (u8::from_le_bytes(bytes.as_slice().try_into().unwrap()) as u64) << shift;
    }
    Ok(())
}

fn _unpack1(xs: &mut [u64; 16], shift: u32, buf: &mut dyn io::Read) -> Result<(), Error> {
    let mut bytes = Vec::<u8>::new();
    bytes.resize(2, 0);
    buf.read_exact(bytes.as_mut_slice())?;
    let bits: u16 = u16::from_le_bytes(bytes.as_slice().try_into().unwrap());
    for i in 0..16 {
        xs[i] |= (((bits as u64) & (1 << i)) >> i) << shift;
    }
    Ok(())
}

fn _unpack2(xs: &mut [u64; 16], shift: u32, buf: &mut dyn io::Read) -> Result<(), Error> {
    let mut bytes = Vec::<u8>::new();
    bytes.resize(4, 0);
    buf.read_exact(bytes.as_mut_slice())?;
    let bits: u32 = u32::from_le_bytes(bytes.as_slice().try_into().unwrap());
    for i in 0..16 {
        xs[i] |= (((bits as u64) & (3 << 2 * i)) >> 2 * i) << shift;
    }
    Ok(())
}

fn _unpack3(xs: &mut [u64; 16], shift: u32, buf: &mut dyn io::Read) -> Result<(), Error> {
    let mut bytes = Vec::<u8>::new();
    bytes.resize(4, 0);
    buf.read_exact(bytes.as_mut_slice())?;
    let bits0 = u32::from_le_bytes(bytes.as_slice().try_into().unwrap()) as u64;
    bytes.resize(2, 0);
    buf.read_exact(bytes.as_mut_slice())?;
    let bits1 = u16::from_le_bytes(bytes.as_slice().try_into().unwrap()) as u64;
    xs[0] |= (bits0 & 7) << shift;
    xs[1] |= ((bits0 & (7 << 3)) >> 3) << shift;
    xs[2] |= ((bits0 & (7 << 6)) >> 6) << shift;
    xs[3] |= ((bits0 & (7 << 9)) >> 9) << shift;
    xs[4] |= ((bits0 & (7 << 12)) >> 12) << shift;
    xs[5] |= ((bits0 & (7 << 15)) >> 15) << shift;
    xs[6] |= ((bits0 & (7 << 18)) >> 18) << shift;
    xs[7] |= ((bits0 & (7 << 21)) >> 21) << shift;
    xs[8] |= ((bits0 & (7 << 24)) >> 24) << shift;
    xs[9] |= ((bits0 & (7 << 27)) >> 27) << shift;
    xs[10] |= (((bits0 & (3 << 30)) >> 30) | ((bits1 & 1) << 2)) << shift;
    xs[11] |= ((bits1 & (7 << 1)) >> 1) << shift;
    xs[12] |= ((bits1 & (7 << 4)) >> 4) << shift;
    xs[13] |= ((bits1 & (7 << 7)) >> 7) << shift;
    xs[14] |= ((bits1 & (7 << 10)) >> 10) << shift;
    xs[15] |= ((bits1 & (7 << 13)) >> 13) << shift;
    Ok(())
}

fn _unpack4(xs: &mut [u64; 16], shift: u32, buf: &mut dyn io::Read) -> Result<(), Error> {
    let mut bytes = Vec::<u8>::new();
    bytes.resize(8, 0);
    buf.read_exact(bytes.as_mut_slice())?;
    let bits = u64::from_le_bytes(bytes.as_slice().try_into().unwrap());
    xs[0] |= (bits & 15) << shift;
    xs[1] |= ((bits & (15 << 4)) >> 4) << shift;
    xs[2] |= ((bits & (15 << 8)) >> 8) << shift;
    xs[3] |= ((bits & (15 << 12)) >> 12) << shift;
    xs[4] |= ((bits & (15 << 16)) >> 16) << shift;
    xs[5] |= ((bits & (15 << 20)) >> 20) << shift;
    xs[6] |= ((bits & (15 << 24)) >> 24) << shift;
    xs[7] |= ((bits & (15 << 28)) >> 28) << shift;
    xs[8] |= ((bits & (15 << 32)) >> 32) << shift;
    xs[9] |= ((bits & (15 << 36)) >> 36) << shift;
    xs[10] |= ((bits & (15 << 40)) >> 40) << shift;
    xs[11] |= ((bits & (15 << 44)) >> 44) << shift;
    xs[12] |= ((bits & (15 << 48)) >> 48) << shift;
    xs[13] |= ((bits & (15 << 52)) >> 52) << shift;
    xs[14] |= ((bits & (15 << 56)) >> 56) << shift;
    xs[15] |= ((bits & (15 << 60)) >> 60) << shift;
    Ok(())
}

fn _unpack5(xs: &mut [u64; 16], shift: u32, buf: &mut dyn io::Read) -> Result<(), Error> {
    let mut bytes = Vec::<u8>::new();
    bytes.resize(8, 0);
    buf.read_exact(bytes.as_mut_slice())?;
    let bits0: u64 = u64::from_le_bytes(bytes.as_slice().try_into().unwrap());
    bytes.resize(2, 0);
    buf.read_exact(bytes.as_mut_slice())?;
    let bits1 = u16::from_le_bytes(bytes.as_slice().try_into().unwrap()) as u64;
    xs[0] |= (bits0 & 0x1F) << shift;
    xs[1] |= ((bits0 & (0x1F << 5)) >> 5) << shift;
    xs[2] |= ((bits0 & (0x1F << 10)) >> 10) << shift;
    xs[3] |= ((bits0 & (0x1F << 15)) >> 15) << shift;
    xs[4] |= ((bits0 & (0x1F << 20)) >> 20) << shift;
    xs[5] |= ((bits0 & (0x1F << 25)) >> 25) << shift;
    xs[6] |= ((bits0 & (0x1F << 30)) >> 30) << shift;
    xs[7] |= ((bits0 & (0x1F << 35)) >> 35) << shift;
    xs[8] |= ((bits0 & (0x1F << 40)) >> 40) << shift;
    xs[9] |= ((bits0 & (0x1F << 45)) >> 45) << shift;
    xs[10] |= ((bits0 & (0x1F << 50)) >> 50) << shift;
    xs[11] |= ((bits0 & (0x1F << 55)) >> 55) << shift;
    xs[12] |= (((bits0 & (0x0F << 60)) >> 60) | ((bits1 & 1) << 4)) << shift;
    xs[13] |= ((bits1 & (0x1F << 1)) >> 1) << shift;
    xs[14] |= ((bits1 & (0x1F << 6)) >> 6) << shift;
    xs[15] |= ((bits1 & (0x1F << 11)) >> 11) << shift;
    Ok(())
}

fn _unpack6(xs: &mut [u64; 16], shift: u32, buf: &mut dyn io::Read) -> Result<(), Error> {
    let mut bytes = Vec::<u8>::new();
    bytes.resize(8, 0);
    buf.read_exact(bytes.as_mut_slice())?;
    let bits0: u64 = u64::from_le_bytes(bytes.as_slice().try_into().unwrap());
    bytes.resize(4, 0);
    buf.read_exact(bytes.as_mut_slice())?;
    let bits1 = u32::from_le_bytes(bytes.as_slice().try_into().unwrap()) as u64;
    xs[0] |= (bits0 & 0x3F) << shift;
    xs[1] |= ((bits0 & (0x3F << 6)) >> 6) << shift;
    xs[2] |= ((bits0 & (0x3F << 12)) >> 12) << shift;
    xs[3] |= ((bits0 & (0x3F << 18)) >> 18) << shift;
    xs[4] |= ((bits0 & (0x3F << 24)) >> 24) << shift;
    xs[5] |= ((bits0 & (0x3F << 30)) >> 30) << shift;
    xs[6] |= ((bits0 & (0x3F << 36)) >> 36) << shift;
    xs[7] |= ((bits0 & (0x3F << 42)) >> 42) << shift;
    xs[8] |= ((bits0 & (0x3F << 48)) >> 48) << shift;
    xs[9] |= ((bits0 & (0x3F << 54)) >> 54) << shift;
    xs[10] |= (((bits0 & (0xF << 60)) >> 60) | (bits1 & 0x3) << 4) << shift;
    xs[11] |= ((bits1 & (0x3F << 2)) >> 2) << shift;
    xs[12] |= ((bits1 & (0x3F << 8)) >> 8) << shift;
    xs[13] |= ((bits1 & (0x3F << 14)) >> 14) << shift;
    xs[14] |= ((bits1 & (0x3F << 20)) >> 20) << shift;
    xs[15] |= ((bits1 & (0x3F << 26)) >> 26) << shift;
    Ok(())
}

fn _unpack7(xs: &mut [u64; 16], shift: u32, buf: &mut dyn io::Read) -> Result<(), Error> {
    let mut bytes = Vec::<u8>::new();
    bytes.resize(8, 0);
    buf.read_exact(bytes.as_mut_slice())?;
    let bits0: u64 = u64::from_le_bytes(bytes.as_slice().try_into().unwrap());
    bytes.resize(4, 0);
    buf.read_exact(bytes.as_mut_slice())?;
    let bits1 = u32::from_le_bytes(bytes.as_slice().try_into().unwrap()) as u64;
    bytes.resize(2, 0);
    buf.read_exact(bytes.as_mut_slice())?;
    let bits2 = u16::from_le_bytes(bytes.as_slice().try_into().unwrap()) as u64;
    xs[0] |= (bits0 & 0x7F) << shift;
    xs[1] |= ((bits0 & (0x7F << 7)) >> 7) << shift;
    xs[2] |= ((bits0 & (0x7F << 14)) >> 14) << shift;
    xs[3] |= ((bits0 & (0x7F << 21)) >> 21) << shift;
    xs[4] |= ((bits0 & (0x7F << 28)) >> 28) << shift;
    xs[5] |= ((bits0 & (0x7F << 35)) >> 35) << shift;
    xs[6] |= ((bits0 & (0x7F << 42)) >> 42) << shift;
    xs[7] |= ((bits0 & (0x7F << 49)) >> 49) << shift;
    xs[8] |= ((bits0 & (0x7F << 56)) >> 56) << shift;
    xs[9] |= (((bits0 & (0x01 << 63)) >> 63) | ((bits1 & 0x3F) << 1)) << shift;
    xs[10] |= ((bits1 & (0x7F << 6)) >> 6) << shift;
    xs[11] |= ((bits1 & (0x7F << 13)) >> 13) << shift;
    xs[12] |= ((bits1 & (0x7F << 20)) >> 20) << shift;
    xs[13] |= (((bits1 & (0x1F << 27)) >> 27) | ((bits2 & 0x03) << 5)) << shift;
    xs[14] |= ((bits2 & (0x7F << 2)) >> 2) << shift;
    xs[15] |= ((bits2 & (0x7F << 9)) >> 9) << shift;
    Ok(())
}

pub fn unpack(output: &mut [u64; 16], num_bits: u32, buf: &mut dyn io::Read) -> Result<(), Error> {
    match num_bits {
        0 => Ok(()),
        1 => _unpack1(output, 0, buf),
        2 => _unpack2(output, 0, buf),
        3 => _unpack3(output, 0, buf),
        4 => _unpack4(output, 0, buf),
        5 => _unpack5(output, 0, buf),
        6 => _unpack6(output, 0, buf),
        7 => _unpack7(output, 0, buf),
        8 => _unpack8(output, 0, buf),
        9 => _unpack8(output, 0, buf).and_then(|_| _unpack1(output, 8, buf)),
        10 => _unpack8(output, 0, buf).and_then(|_| _unpack2(output, 8, buf)),
        11 => _unpack8(output, 0, buf).and_then(|_| _unpack3(output, 8, buf)),
        12 => _unpack8(output, 0, buf).and_then(|_| _unpack4(output, 8, buf)),
        13 => _unpack8(output, 0, buf).and_then(|_| _unpack5(output, 8, buf)),
        14 => _unpack8(output, 0, buf).and_then(|_| _unpack6(output, 8, buf)),
        15 => _unpack8(output, 0, buf).and_then(|_| _unpack7(output, 8, buf)),
        16 => _unpack16(output, 0, buf),
        17 => _unpack16(output, 0, buf).and_then(|_| _unpack1(output, 16, buf)),
        18 => _unpack16(output, 0, buf).and_then(|_| _unpack2(output, 16, buf)),
        19 => _unpack16(output, 0, buf).and_then(|_| _unpack3(output, 16, buf)),
        20 => _unpack16(output, 0, buf).and_then(|_| _unpack4(output, 16, buf)),
        21 => _unpack16(output, 0, buf).and_then(|_| _unpack5(output, 16, buf)),
        22 => _unpack16(output, 0, buf).and_then(|_| _unpack6(output, 16, buf)),
        23 => _unpack16(output, 0, buf).and_then(|_| _unpack7(output, 16, buf)),
        24 => _unpack16(output, 0, buf).and_then(|_| _unpack8(output, 16, buf)),
        25 => _unpack16(output, 0, buf)
            .and_then(|_| _unpack8(output, 16, buf))
            .and_then(|_| _unpack1(output, 24, buf)),
        26 => _unpack16(output, 0, buf)
            .and_then(|_| _unpack8(output, 16, buf))
            .and_then(|_| _unpack2(output, 24, buf)),
        27 => _unpack16(output, 0, buf)
            .and_then(|_| _unpack8(output, 16, buf))
            .and_then(|_| _unpack3(output, 24, buf)),
        28 => _unpack16(output, 0, buf)
            .and_then(|_| _unpack8(output, 16, buf))
            .and_then(|_| _unpack4(output, 24, buf)),
        29 => _unpack16(output, 0, buf)
            .and_then(|_| _unpack8(output, 16, buf))
            .and_then(|_| _unpack5(output, 24, buf)),
        30 => _unpack16(output, 0, buf)
            .and_then(|_| _unpack8(output, 16, buf))
            .and_then(|_| _unpack6(output, 24, buf)),
        31 => _unpack16(output, 0, buf)
            .and_then(|_| _unpack8(output, 16, buf))
            .and_then(|_| _unpack7(output, 24, buf)),
        32 => _unpack32(output, 0, buf),
        33 => _unpack32(output, 0, buf).and_then(|_| _unpack1(output, 32, buf)),
        34 => _unpack32(output, 0, buf).and_then(|_| _unpack2(output, 32, buf)),
        35 => _unpack32(output, 0, buf).and_then(|_| _unpack3(output, 32, buf)),
        36 => _unpack32(output, 0, buf).and_then(|_| _unpack4(output, 32, buf)),
        37 => _unpack32(output, 0, buf).and_then(|_| _unpack5(output, 32, buf)),
        38 => _unpack32(output, 0, buf).and_then(|_| _unpack6(output, 32, buf)),
        39 => _unpack32(output, 0, buf).and_then(|_| _unpack7(output, 32, buf)),
        40 => _unpack32(output, 0, buf).and_then(|_| _unpack8(output, 32, buf)),
        41 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack8(output, 32, buf))
            .and_then(|_| _unpack1(output, 40, buf)),
        42 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack8(output, 32, buf))
            .and_then(|_| _unpack2(output, 40, buf)),
        43 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack8(output, 32, buf))
            .and_then(|_| _unpack3(output, 40, buf)),
        44 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack8(output, 32, buf))
            .and_then(|_| _unpack4(output, 40, buf)),
        45 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack8(output, 32, buf))
            .and_then(|_| _unpack5(output, 40, buf)),
        46 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack8(output, 32, buf))
            .and_then(|_| _unpack6(output, 40, buf)),
        47 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack8(output, 32, buf))
            .and_then(|_| _unpack7(output, 40, buf)),
        48 => _unpack32(output, 0, buf).and_then(|_| _unpack16(output, 32, buf)),
        49 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack16(output, 32, buf))
            .and_then(|_| _unpack1(output, 48, buf)),
        50 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack16(output, 32, buf))
            .and_then(|_| _unpack2(output, 48, buf)),
        51 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack16(output, 32, buf))
            .and_then(|_| _unpack3(output, 48, buf)),
        52 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack16(output, 32, buf))
            .and_then(|_| _unpack4(output, 48, buf)),
        53 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack16(output, 32, buf))
            .and_then(|_| _unpack5(output, 48, buf)),
        54 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack16(output, 32, buf))
            .and_then(|_| _unpack6(output, 48, buf)),
        55 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack16(output, 32, buf))
            .and_then(|_| _unpack7(output, 48, buf)),
        56 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack16(output, 32, buf))
            .and_then(|_| _unpack8(output, 48, buf)),
        57 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack16(output, 32, buf))
            .and_then(|_| _unpack8(output, 48, buf))
            .and_then(|_| _unpack1(output, 56, buf)),
        58 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack16(output, 32, buf))
            .and_then(|_| _unpack8(output, 48, buf))
            .and_then(|_| _unpack2(output, 56, buf)),
        59 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack16(output, 32, buf))
            .and_then(|_| _unpack8(output, 48, buf))
            .and_then(|_| _unpack3(output, 56, buf)),
        60 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack16(output, 32, buf))
            .and_then(|_| _unpack8(output, 48, buf))
            .and_then(|_| _unpack4(output, 56, buf)),
        61 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack16(output, 32, buf))
            .and_then(|_| _unpack8(output, 48, buf))
            .and_then(|_| _unpack5(output, 56, buf)),
        62 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack16(output, 32, buf))
            .and_then(|_| _unpack8(output, 48, buf))
            .and_then(|_| _unpack6(output, 56, buf)),
        63 => _unpack32(output, 0, buf)
            .and_then(|_| _unpack16(output, 32, buf))
            .and_then(|_| _unpack8(output, 48, buf))
            .and_then(|_| _unpack7(output, 56, buf)),
        64 => _unpack64(output, 0, buf),
        _ => panic!("Unexpected number of bits {}", num_bits),
    }
}
