use std;
use std::array::TryFromSliceError;
use std::fmt::{self, Display};

use serde::de::StdError;
use serde::{de, ser};

/// General purpose error class for code that does I/O to remote storage.
/// TODO: clean up naming, this may be used in circumstances other than reading from a bucket
#[derive(Debug)]
pub enum BucketReaderError {
    ReadError(object_store::Error),
    StreamReadError(std::io::Error),
    ParseError(serde_json::Error),
    SyntaxError(String),
}

impl From<object_store::Error> for BucketReaderError {
    fn from(e: object_store::Error) -> Self {
        BucketReaderError::ReadError(e)
    }
}

impl From<serde_json::Error> for BucketReaderError {
    fn from(e: serde_json::Error) -> Self {
        BucketReaderError::ParseError(e)
    }
}

impl From<std::io::Error> for BucketReaderError {
    fn from(e: std::io::Error) -> Self {
        BucketReaderError::StreamReadError(e)
    }
}

#[derive(Debug)]
pub enum DecodeError {
    Message(String),

    // Any kind of conversion failure
    SyntaxError,
    // We expected more bytes than the stream provided
    EndOfStream,
    // We did not consume the whole stream
    TrailingBytes,
}

#[derive(Debug)]
pub enum EncodeError {
    Message(String),
}

impl StdError for EncodeError {}

impl ser::Error for EncodeError {
    fn custom<T: Display>(msg: T) -> Self {
        EncodeError::Message(msg.to_string())
    }
}

impl Display for EncodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EncodeError::Message(msg) => formatter.write_str(msg),
        }
    }
}

impl StdError for DecodeError {}

impl From<TryFromSliceError> for DecodeError {
    fn from(_: TryFromSliceError) -> Self {
        Self::SyntaxError
    }
}

impl From<bincode::error::DecodeError> for DecodeError {
    fn from(_: bincode::error::DecodeError) -> Self {
        Self::SyntaxError
    }
}

impl ser::Error for DecodeError {
    fn custom<T: Display>(msg: T) -> Self {
        DecodeError::Message(msg.to_string())
    }
}

impl de::Error for DecodeError {
    fn custom<T: Display>(msg: T) -> Self {
        DecodeError::Message(msg.to_string())
    }
}

impl Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DecodeError::Message(msg) => formatter.write_str(msg),
            DecodeError::EndOfStream => formatter.write_str("Unexpected end of stream"),
            DecodeError::TrailingBytes => formatter.write_str("Trailing bytes"),
            DecodeError::SyntaxError => formatter.write_str("Bad bytes"),
        }
    }
}
