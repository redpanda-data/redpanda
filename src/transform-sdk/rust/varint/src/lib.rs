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

use core::fmt;
use std::error::Error;

#[derive(Debug, PartialEq, Eq)]
pub enum DecodeError {
    Overflow,
    ShortRead,
    ShortReadBuffer {
        buf_size: usize,
        payload_remaining: usize,
    },
}

impl Error for DecodeError {}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DecodeError::Overflow => write!(f, "decoded varint would overflow i64::MAX"),
            DecodeError::ShortRead => write!(f, "short read when decoding varint"),
            DecodeError::ShortReadBuffer {
                buf_size,
                payload_remaining,
            } => write!(
                f,
                "decoded sized buffer required size: {}, but only {} was remaining in buffer",
                buf_size, payload_remaining
            ),
        }
    }
}

pub type Result<T> = std::result::Result<T, DecodeError>;

#[derive(PartialEq, Eq)]
pub struct Decoded<T> {
    pub value: T,
    pub read: usize,
}

impl<T> Decoded<T> {
    pub fn map<U, F: FnOnce(T) -> U>(self, op: F) -> Decoded<U> {
        Decoded {
            value: op(self.value),
            read: self.read,
        }
    }
}

// The maximum encoded size of an i64
pub const MAX_LENGTH: usize = 10;

fn zigzag_encode(x: i64) -> u64 {
    ((x << 1) ^ (x >> 63)) as u64
}

fn zigzag_decode(x: u64) -> i64 {
    ((x >> 1) as i64) ^ (-((x & 1) as i64))
}

fn read_unsigned(payload: &[u8]) -> Result<Decoded<u64>> {
    let mut decoded: u64 = 0;
    let mut shift = 0;
    for (i, b) in payload.iter().enumerate() {
        if i >= MAX_LENGTH {
            return Err(DecodeError::Overflow);
        }
        decoded |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok(Decoded {
                value: decoded,
                read: i + 1,
            });
        }
        shift += 7;
    }
    Err(DecodeError::ShortRead)
}

pub fn read(payload: &[u8]) -> Result<Decoded<i64>> {
    read_unsigned(payload).map(|r| r.map(zigzag_decode))
}

pub fn read_sized_buffer(payload: &[u8]) -> Result<Decoded<Option<&[u8]>>> {
    let result = read(payload)?;
    if result.value < 0 {
        return Ok(result.map(|_| None));
    }
    let payload = &payload[result.read..];
    let buf_size = result.value as usize;
    if buf_size > payload.len() {
        return Err(DecodeError::ShortReadBuffer {
            buf_size,
            payload_remaining: payload.len(),
        });
    }
    Ok(Decoded {
        value: Some(&payload[..buf_size]),
        read: result.read + buf_size,
    })
}

fn write_unsigned(payload: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        let b = (v as u8) | 0x80;
        v >>= 7;
        payload.push(b);
    }
    payload.push(v as u8);
}

pub fn write(payload: &mut Vec<u8>, v: i64) {
    write_unsigned(payload, zigzag_encode(v))
}

pub fn write_sized_buffer(payload: &mut Vec<u8>, buf: Option<&[u8]>) {
    match buf {
        Some(b) => {
            write(payload, b.len() as i64);
            payload.extend_from_slice(b);
        }
        None => write(payload, -1),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        read, read_sized_buffer, read_unsigned, write, write_sized_buffer, write_unsigned,
        zigzag_decode, zigzag_encode,
    };

    use quickcheck::quickcheck;

    quickcheck! {
        fn zigzag(n: i64) -> bool {
            n == zigzag_decode(zigzag_encode(n))
        }
    }
    quickcheck! {
        fn roundtrip_unsigned(n: u64) -> bool {
            let mut buf = Vec::new();
            write_unsigned(&mut buf, n);
            let r = read_unsigned(&buf[..]).expect("valid buffer");
            if r.read != buf.len() {
                panic!("expected to consume the whole buffer: {read} != {remaining}", read = r.read, remaining = buf.len());
            }
            r.value == n
        }
    }
    quickcheck! {
        fn roundtrip_signed(n: i64) -> bool {
            let mut buf = Vec::new();
            write(&mut buf, n);
            let r = read(&buf[..]).expect("valid buffer");
            if r.read != buf.len() {
                panic!("expected to consume the whole buffer: {read} != {remaining}", read = r.read, remaining = buf.len());
            }
            r.value == n
        }
    }
    quickcheck! {
        fn roundtrip_buffer(input: Option<Vec<u8>>) -> bool {
            let mut buf = Vec::new();
            write_sized_buffer(&mut buf, input.as_ref().map(|b| &b[..]));
            let r = read_sized_buffer(&buf[..]).expect("valid buffer");
            if r.read != buf.len() {
                panic!("expected to consume the whole buffer: {read} != {remaining}", read = r.read, remaining = buf.len());
            }
            r.value == input.as_ref().map(|b| &b[..])
        }
    }
}
