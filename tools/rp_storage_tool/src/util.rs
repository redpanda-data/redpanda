use bincode::config::Config;
use serde;
use serde::de::{DeserializeOwned, DeserializeSeed, SeqAccess, Visitor};

use crate::error::DecodeError;
pub type DecodeResult<T> = Result<T, DecodeError>;

use bincode::de::read::Reader;
use bincode::de::read::SliceReader;
use bincode::de::Decoder;

/// The Decode class from `bincode` is exactly what we would write ourselves for the general
/// "bytes that can be consumed" concept, so let's use theirs.
pub struct Deserializer<'de, DE: Decoder> {
    input: &'de mut DE,
}

impl<'de, DE: Decoder> Deserializer<'de, DE> {
    fn take_bytes<const N: usize>(&mut self) -> DecodeResult<[u8; N]> {
        self.input.claim_bytes_read(N)?;
        let mut bytes = [0u8; N];
        self.input.reader().read(&mut bytes)?;
        Ok(bytes)
    }
}

/// This isn't a full impl of ADL encoding, but for situations we care about (like
/// RecordBatchHeader), it is enough.
pub fn from_adl_bytes<T, C: Config>(slice: &[u8], config: C) -> DecodeResult<T>
where
    T: DeserializeOwned,
{
    let reader = SliceReader::new(slice);
    let mut decoder = bincode::de::DecoderImpl::new(reader, config);
    let deserializer = Deserializer {
        input: &mut decoder,
    };
    let t = T::deserialize(deserializer)?;
    Ok(t)
}

impl<'a, 'de, DE: Decoder> serde::Deserializer<'de> for Deserializer<'a, DE> {
    type Error = DecodeError;

    fn deserialize_any<V>(self, _visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_bool<V>(self, _visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i8<V>(mut self, visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i8(i8::from_le_bytes(self.take_bytes::<1>()?))
    }

    fn deserialize_i16<V>(mut self, visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i16(i16::from_le_bytes(self.take_bytes::<2>()?))
    }

    fn deserialize_i32<V>(mut self, visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i32(i32::from_le_bytes(self.take_bytes::<4>()?))
    }

    fn deserialize_i64<V>(mut self, visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i64(i64::from_le_bytes(self.take_bytes::<8>()?))
    }

    fn deserialize_u8<V>(mut self, visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u8(u8::from_le_bytes(self.take_bytes::<1>()?))
    }

    fn deserialize_u16<V>(mut self, visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u16(u16::from_le_bytes(self.take_bytes::<2>()?))
    }

    fn deserialize_u32<V>(mut self, visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u32(u32::from_le_bytes(self.take_bytes::<4>()?))
    }

    fn deserialize_u64<V>(mut self, visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u64(u64::from_le_bytes(self.take_bytes::<8>()?))
    }

    fn deserialize_f32<V>(mut self, visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_f32(f32::from_le_bytes(self.take_bytes::<4>()?))
    }

    fn deserialize_f64<V>(mut self, visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_f64(f64::from_le_bytes(self.take_bytes::<8>()?))
    }

    fn deserialize_char<V>(self, _visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_str<V>(self, _visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_string<V>(self, _visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_bytes<V>(self, _visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_option<V>(self, _visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit<V>(self, _visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, _visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_seq<V>(self, _visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple<V>(self, _len: usize, _visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, _visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_struct<V>(
        mut self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        struct Access<'a, 'b, DE: Decoder> {
            deserializer: &'a mut Deserializer<'b, DE>,
            len: usize,
        }

        impl<'de, 'a, 'b: 'a, DE: Decoder + 'b> SeqAccess<'de> for Access<'a, 'b, DE> {
            type Error = DecodeError;

            fn next_element_seed<T>(&mut self, seed: T) -> DecodeResult<Option<T::Value>>
            where
                T: DeserializeSeed<'de>,
            {
                if self.len > 0 {
                    self.len -= 1;
                    let value = DeserializeSeed::deserialize(
                        seed,
                        Deserializer {
                            input: self.deserializer.input,
                        },
                    )?;

                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }

            fn size_hint(&self) -> Option<usize> {
                Some(self.len)
            }
        }

        visitor.visit_seq(Access {
            deserializer: &mut self,
            len: fields.len(),
        })
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_identifier<V>(self, _visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> DecodeResult<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }
}
