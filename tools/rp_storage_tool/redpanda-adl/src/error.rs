use std;
use std::array::TryFromSliceError;
use std::fmt::{self, Display};

use serde::de::StdError;
use serde::{de, ser};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
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

impl StdError for Error {}

impl From<TryFromSliceError> for Error {
    fn from(_: TryFromSliceError) -> Self {
        Self::SyntaxError
    }
}

impl From<bincode::error::DecodeError> for Error {
    fn from(_: bincode::error::DecodeError) -> Self {
        Self::SyntaxError
    }
}

impl ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Message(msg) => formatter.write_str(msg),
            Error::EndOfStream => formatter.write_str("Unexpected end of stream"),
            Error::TrailingBytes => formatter.write_str("Trailing bytes"),
            Error::SyntaxError => formatter.write_str("Bad bytes"),
        }
    }
}
