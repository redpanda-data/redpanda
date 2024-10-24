use crate::fundamental::{LabeledNTPR, LabeledNTR, NTP, NTR};
use crate::NTPR;
use regex::Regex;
use std::fmt::{Display, Formatter};
use std::num::ParseIntError;

#[derive(Clone)]
pub struct NTPFilter {
    ns: Option<String>,
    topic: Option<Regex>,
    partition: Option<u32>,
    revision: Option<i64>,
}

#[derive(Debug)]
pub enum NTPFilterParseError {
    BadInt(ParseIntError),
    BadRegex(regex::Error),
}

impl From<regex::Error> for NTPFilterParseError {
    fn from(value: regex::Error) -> Self {
        Self::BadRegex(value)
    }
}

impl From<ParseIntError> for NTPFilterParseError {
    fn from(value: ParseIntError) -> Self {
        Self::BadInt(value)
    }
}

impl Display for NTPFilterParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let reason = match self {
            Self::BadInt(e) => format!("Bad integer component: {}", e),
            Self::BadRegex(e) => format!("Bad regex component: {}", e),
        };
        f.write_str(&reason)
    }
}

impl NTPFilter {
    pub fn match_all() -> NTPFilter {
        NTPFilter {
            ns: None,
            topic: None,
            partition: None,
            revision: None,
        }
    }

    fn init_opt_regex(input: &str) -> Result<Option<Regex>, regex::Error> {
        if input == "*" || input == ".*" {
            Ok(None)
        } else {
            // Ergonomic flourish:
            // If a caller passes "kafka/foo/0", they probably didn't want
            // to match topics like "blafooo" or "foo_v2": so if they didn't
            // put ^$ bounds on their regex, add them.
            let mut regex = if !input.starts_with("^") {
                format!("^{}", input)
            } else {
                input.to_string()
            };

            if !regex.ends_with("$") {
                regex = format!("{}$", regex);
            }

            Ok(Some(Regex::new(&regex)?))
        }
    }

    pub fn from_str(s: &str) -> Result<NTPFilter, NTPFilterParseError> {
        // lazy_static! {
        //     static ref NTP_MASK_EXPR: Regex =
        //         Regex::new("([^]]+)/([^]]+)/([^_]+)_([^_]+)").unwrap();
        // }
        //
        // if let Some(grps) = NTP_MASK_EXPR.captures(input) {
        //     Ok(NTPMask {
        //         namespace: Self::init_opt_regex(grps.get(1).unwrap().as_str())?,
        //         topic: Self::init_opt_regex(grps.get(2).unwrap().as_str())?,
        //         partition: Self::init_opt_regex(grps.get(3).unwrap().as_str())?,
        //         revision_id: Self::init_opt_regex(grps.get(4).unwrap().as_str())?,
        //     })
        // } else {
        //     Err(regex::Error::Syntax(
        //         "Malformed NTP query string".to_string(),
        //     ))
        // }

        let elements: Vec<&str> = s.split("/").collect();
        let mut filter = Self::match_all();

        if let Some(e) = elements.get(0) {
            if *e != "*" {
                filter.ns = Some(e.to_string());
            }
        }

        if let Some(e) = elements.get(1) {
            filter.topic = Self::init_opt_regex(*e)?;
        }

        if let Some(e) = elements.get(2) {
            let underscore_split: Vec<&str> = e.split("_").collect();
            let partition_str = underscore_split.get(0).unwrap();
            if *partition_str != "*" {
                filter.partition = Some(partition_str.parse::<u32>()?);
            }

            if let Some(r_str) = underscore_split.get(1) {
                if *r_str != "*" {
                    filter.revision = Some(r_str.parse::<i64>()?);
                }
            }
        }

        Ok(filter)
    }

    pub fn match_parts(
        &self,
        match_ns: &str,
        match_topic: &str,
        match_partition: Option<u32>,
        match_revision: Option<i64>,
    ) -> bool {
        if let Some(ns) = &self.ns {
            if match_ns != *ns {
                return false;
            }
        }

        if let Some(topic) = &self.topic {
            if !topic.is_match(match_topic) {
                return false;
            }
        }

        if let Some(match_partition) = match_partition {
            if let Some(partition) = &self.partition {
                if match_partition != *partition {
                    return false;
                }
            }
        }

        if let Some(match_revision) = match_revision {
            if let Some(revision) = self.revision {
                if revision != match_revision {
                    return false;
                }
            }
        }

        true
    }

    pub fn match_nt(&self, match_ns: &str, match_topic: &str) -> bool {
        self.match_parts(match_ns, match_topic, None, None)
    }

    pub fn match_ntr(&self, ntr: &NTR) -> bool {
        self.match_parts(&ntr.namespace, &ntr.topic, None, Some(ntr.revision_id))
    }

    pub fn match_lntr(&self, lntr: &LabeledNTR) -> bool {
        self.match_parts(
            &lntr.ntr.namespace,
            &lntr.ntr.topic,
            None,
            Some(lntr.ntr.revision_id),
        )
    }

    pub fn match_ntp(&self, ntp: &NTP) -> bool {
        self.match_parts(&ntp.namespace, &ntp.topic, Some(ntp.partition_id), None)
    }

    pub fn match_ntpr(&self, ntpr: &NTPR) -> bool {
        self.match_parts(
            &ntpr.ntp.namespace,
            &ntpr.ntp.topic,
            Some(ntpr.ntp.partition_id),
            Some(ntpr.revision_id),
        )
    }
    pub fn match_lntpr(&self, lntpr: &LabeledNTPR) -> bool {
        self.match_parts(
            &lntpr.ntpr.ntp.namespace,
            &lntpr.ntpr.ntp.topic,
            Some(lntpr.ntpr.ntp.partition_id),
            Some(lntpr.ntpr.revision_id),
        )
    }
}

impl std::fmt::Display for NTPFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{}/{}/{}_*",
            self.ns.as_ref().map(|r| r.as_str()).unwrap_or("*"),
            self.topic.as_ref().map(|r| r.as_str()).unwrap_or("*"),
            self.partition
                .as_ref()
                .map(|r| format!("{}", r))
                .unwrap_or("*".to_string())
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fundamental::NTP;

    #[test_log::test]
    fn test_filter() {
        let example1 = NTPR {
            ntp: NTP {
                namespace: "foo".to_string(),
                topic: "bar".to_string(),
                partition_id: 66,
            },
            revision_id: 123,
        };
        assert_eq!(true, NTPFilter::match_all().match_ntpr(&example1));
        assert_eq!(
            true,
            NTPFilter::from_str("*/*/*_*")
                .unwrap()
                .match_ntpr(&example1)
        );
        assert_eq!(
            true,
            NTPFilter::from_str("foo/bar/66_123")
                .unwrap()
                .match_ntpr(&example1)
        );

        assert_eq!(
            false,
            NTPFilter::from_str("boof/bar/66_123")
                .unwrap()
                .match_ntpr(&example1)
        );
    }
}
