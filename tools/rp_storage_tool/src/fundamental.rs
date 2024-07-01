use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

// Equivalent to model::offset
pub type RawOffset = i64;
// Equivalent to kafka::offset
pub type KafkaOffset = i64;
// Equivalent to model::term_id
pub type RaftTerm = i64;
// Equivalent to model::timestamp
pub type Timestamp = i64;

// A difference between a RawOffset and a KafkaOffset.  RawOffset is always greater
// so deltas are always positive.
pub type DeltaOffset = u64;

pub fn raw_to_kafka(r: RawOffset, d: DeltaOffset) -> KafkaOffset {
    (r - d as RawOffset) as KafkaOffset
}

#[derive(Eq, PartialEq, Hash, Debug, Clone, Serialize, Deserialize, Ord, PartialOrd)]
pub struct NTP {
    pub namespace: String,
    pub topic: String,
    pub partition_id: u32,
}

impl fmt::Display for NTP {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "{}/{}/{}",
            self.namespace, self.topic, self.partition_id
        ))
    }
}

/// A Topic, uniquely identified by its revision ID
#[derive(Clone, Eq, PartialEq, Hash, Debug, Ord, PartialOrd)]
pub struct NTR {
    pub namespace: String,
    pub topic: String,
    pub revision_id: i64,
}

impl NTR {
    pub fn from_str(s: &str) -> Self {
        lazy_static! {
            static ref NTP_MASK_EXPR: Regex = Regex::new("([^]]+)/([^]]+)_([^_]+)").unwrap();
        }

        if let Some(grps) = NTP_MASK_EXPR.captures(&s) {
            let namespace = grps.get(1).unwrap().as_str().to_string();
            let topic = grps.get(2).unwrap().as_str().to_string();
            let revision = grps.get(3).unwrap().as_str().to_string();

            Self {
                namespace,
                topic,
                revision_id: revision.parse::<i64>().unwrap(),
            }
        } else {
            panic!("Malformed NTP query string");
        }
    }
}

impl fmt::Display for NTR {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "{}/{}_{}",
            self.namespace, self.topic, self.revision_id
        ))
    }
}

impl Serialize for NTR {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let stringized = format!("{}", self);
        stringized.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NTR {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let stringized = String::deserialize(deserializer)?;
        Ok(Self::from_str(stringized.as_str()))
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug, Ord, PartialOrd)]
pub struct LabeledNTR {
    pub ntr: NTR,
    pub label: Option<String>,
}

impl LabeledNTR {
    pub fn from_str(s: &str) -> Self {
        lazy_static! {
            static ref NTP_MASK_EXPR: Regex =
                Regex::new("([^]]+/)?([^]]+)/([^]]+)_([^_]+)").unwrap();
        }

        if let Some(grps) = NTP_MASK_EXPR.captures(&s) {
            // Remove the trailing /.
            let label = grps
                .get(1)
                .map(|s| s.as_str().chars().take(s.len() - 1).collect());

            let namespace = grps.get(2).unwrap().as_str().to_string();
            let topic = grps.get(3).unwrap().as_str().to_string();
            let revision = grps.get(4).unwrap().as_str().to_string();

            let ntr = NTR {
                namespace,
                topic,
                revision_id: revision.parse::<i64>().unwrap(),
            };
            Self { ntr, label }
        } else {
            panic!("Malformed NTP query string");
        }
    }
}

impl fmt::Display for LabeledNTR {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let maybe_label = self
            .label
            .as_ref()
            .map_or("".to_owned(), |l| format!("{}/", l));
        write!(
            f,
            "{}{}/{}_{}",
            maybe_label, self.ntr.namespace, self.ntr.topic, self.ntr.revision_id
        )
    }
}

impl Serialize for LabeledNTR {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let stringized = format!("{}", self);
        stringized.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for LabeledNTR {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let stringized = String::deserialize(deserializer)?;
        Ok(Self::from_str(stringized.as_str()))
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug, Ord, PartialOrd)]
pub struct LabeledNTPR {
    pub ntpr: NTPR,
    pub label: Option<String>,
}

impl LabeledNTPR {
    pub fn to_ntr(&self) -> LabeledNTR {
        return LabeledNTR {
            ntr: NTR {
                namespace: self.ntpr.ntp.namespace.clone(),
                topic: self.ntpr.ntp.topic.clone(),
                revision_id: self.ntpr.revision_id,
            },
            label: self.label.clone(),
        };
    }

    // Converts the given NTPR and prefix of a manifest or segment path and constructs a labeled
    // NTPR.
    pub fn from_ntpr_and_prefix(ntpr: NTPR, prefix: String) -> Self {
        Self {
            ntpr,
            // Prefixes of exactly 8 characters are hashes rather than labels.
            label: if prefix.len() != 8 {
                Some(prefix)
            } else {
                None
            },
        }
    }

    pub fn from_str(s: &str) -> Self {
        lazy_static! {
            static ref NTP_MASK_EXPR: Regex =
                Regex::new("([^]]+/)?([^]]+)/([^]]+)/([^_]+)_([^_]+)").unwrap();
        }

        if let Some(grps) = NTP_MASK_EXPR.captures(&s) {
            // Remove the trailing /.
            let label = grps
                .get(1)
                .map(|s| s.as_str().chars().take(s.len() - 1).collect());

            let namespace = grps.get(2).unwrap().as_str().to_string();
            let topic = grps.get(3).unwrap().as_str().to_string();
            let partition = grps.get(4).unwrap().as_str().to_string();
            let revision = grps.get(5).unwrap().as_str().to_string();

            Self {
                ntpr: NTPR {
                    ntp: NTP {
                        namespace,
                        topic,
                        partition_id: partition.parse::<u32>().unwrap(),
                    },
                    revision_id: revision.parse::<i64>().unwrap(),
                },
                label,
            }
        } else {
            panic!("Malformed NTP query string");
        }
    }
}

impl Serialize for LabeledNTPR {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let stringized = format!("{}", self);
        stringized.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for LabeledNTPR {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let stringized = String::deserialize(deserializer)?;
        Ok(Self::from_str(stringized.as_str()))
    }
}

impl fmt::Display for LabeledNTPR {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let maybe_label = if let Some(l) = &self.label {
            format!("{}/", l)
        } else {
            "".to_string()
        };
        f.write_fmt(format_args!(
            "{}{}_{}",
            maybe_label, self.ntpr.ntp, self.ntpr.revision_id
        ))
    }
}

/// A Partition, uniquely identified by its revision ID
#[derive(Eq, PartialEq, Hash, Debug, Clone, Ord, PartialOrd)]
pub struct NTPR {
    pub ntp: NTP,
    pub revision_id: i64,
}

impl NTPR {
    pub fn to_ntr(&self) -> NTR {
        return NTR {
            namespace: self.ntp.namespace.clone(),
            topic: self.ntp.topic.clone(),
            revision_id: self.revision_id,
        };
    }

    pub fn from_str(s: &str) -> Self {
        lazy_static! {
            static ref NTP_MASK_EXPR: Regex =
                Regex::new("([^]]+)/([^]]+)/([^_]+)_([^_]+)").unwrap();
        }

        if let Some(grps) = NTP_MASK_EXPR.captures(&s) {
            let namespace = grps.get(1).unwrap().as_str().to_string();
            let topic = grps.get(2).unwrap().as_str().to_string();
            let partition = grps.get(3).unwrap().as_str().to_string();
            let revision = grps.get(4).unwrap().as_str().to_string();

            Self {
                ntp: NTP {
                    namespace,
                    topic,
                    partition_id: partition.parse::<u32>().unwrap(),
                },
                revision_id: revision.parse::<i64>().unwrap(),
            }
        } else {
            panic!("Malformed NTP query string");
        }
    }
}

impl Serialize for NTPR {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let stringized = format!("{}", self);
        stringized.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NTPR {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let stringized = String::deserialize(deserializer)?;
        Ok(Self::from_str(stringized.as_str()))
    }
}

impl fmt::Display for NTPR {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}_{}", self.ntp, self.revision_id))
    }
}
