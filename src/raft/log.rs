//! Raft log domain types shared by the protocol core and storage adapters.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{self, Display, Formatter},
};

use super::Term;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EntryPayload {
    Noop,
    Command(Vec<u8>),
    Configuration(MembershipEntry),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterConfiguration {
    members: BTreeMap<u64, String>,
}

impl ClusterConfiguration {
    pub fn new(
        members: impl IntoIterator<Item = (u64, String)>,
    ) -> Result<Self, ConfigurationError> {
        let mut unique = BTreeMap::new();
        for (node_id, endpoint) in members {
            if node_id == 0 {
                return Err(ConfigurationError::ZeroNodeId);
            }
            if endpoint.is_empty() || endpoint.len() > 2_048 {
                return Err(ConfigurationError::InvalidEndpoint);
            }
            if unique.insert(node_id, endpoint).is_some() {
                return Err(ConfigurationError::DuplicateNode(node_id));
            }
        }
        if unique.is_empty() {
            return Err(ConfigurationError::Empty);
        }
        Ok(Self { members: unique })
    }

    pub fn members(&self) -> impl Iterator<Item = (u64, &str)> {
        self.members
            .iter()
            .map(|(node_id, endpoint)| (*node_id, endpoint.as_str()))
    }

    pub fn node_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.members.keys().copied()
    }

    pub fn endpoint(&self, node_id: u64) -> Option<&str> {
        self.members.get(&node_id).map(String::as_str)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigurationPhase {
    Joint,
    Stable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipEntry {
    pub operation_id: [u8; 16],
    pub phase: ConfigurationPhase,
    pub old: ClusterConfiguration,
    pub new: ClusterConfiguration,
}

impl MembershipEntry {
    pub fn initial(configuration: ClusterConfiguration) -> Self {
        Self {
            operation_id: [0xff; 16],
            phase: ConfigurationPhase::Stable,
            old: configuration.clone(),
            new: configuration,
        }
    }

    pub fn validate(&self) -> Result<(), ConfigurationError> {
        if self.operation_id == [0; 16] {
            return Err(ConfigurationError::ZeroOperationId);
        }
        if self.phase == ConfigurationPhase::Stable && self.old != self.new {
            return Err(ConfigurationError::InvalidStableTransition);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigurationError {
    Empty,
    ZeroNodeId,
    DuplicateNode(u64),
    InvalidEndpoint,
    ZeroOperationId,
    InvalidStableTransition,
    InvalidEncoding,
}

impl Display for ConfigurationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid cluster configuration: {self:?}")
    }
}

impl Error for ConfigurationError {}

pub fn encode_membership(entry: &MembershipEntry) -> Result<Vec<u8>, ConfigurationError> {
    entry.validate()?;
    let mut output = Vec::new();
    output.extend_from_slice(b"RKVCFG01");
    output.push(match entry.phase {
        ConfigurationPhase::Joint => 1,
        ConfigurationPhase::Stable => 2,
    });
    output.extend_from_slice(&entry.operation_id);
    encode_configuration(&mut output, &entry.old)?;
    encode_configuration(&mut output, &entry.new)?;
    Ok(output)
}

pub fn decode_membership(bytes: &[u8]) -> Result<MembershipEntry, ConfigurationError> {
    let mut input = ConfigurationReader { bytes, offset: 0 };
    if input.take(8)? != b"RKVCFG01" {
        return Err(ConfigurationError::InvalidEncoding);
    }
    let phase = match input.take(1)?[0] {
        1 => ConfigurationPhase::Joint,
        2 => ConfigurationPhase::Stable,
        _ => return Err(ConfigurationError::InvalidEncoding),
    };
    let operation_id = input
        .take(16)?
        .try_into()
        .map_err(|_| ConfigurationError::InvalidEncoding)?;
    let old = decode_configuration(&mut input)?;
    let new = decode_configuration(&mut input)?;
    if input.offset != bytes.len() {
        return Err(ConfigurationError::InvalidEncoding);
    }
    let entry = MembershipEntry {
        operation_id,
        phase,
        old,
        new,
    };
    entry.validate()?;
    Ok(entry)
}

fn encode_configuration(
    output: &mut Vec<u8>,
    configuration: &ClusterConfiguration,
) -> Result<(), ConfigurationError> {
    let count = u32::try_from(configuration.members.len())
        .map_err(|_| ConfigurationError::InvalidEncoding)?;
    output.extend_from_slice(&count.to_be_bytes());
    for (node_id, endpoint) in configuration.members() {
        output.extend_from_slice(&node_id.to_be_bytes());
        let length =
            u32::try_from(endpoint.len()).map_err(|_| ConfigurationError::InvalidEncoding)?;
        output.extend_from_slice(&length.to_be_bytes());
        output.extend_from_slice(endpoint.as_bytes());
    }
    Ok(())
}

fn decode_configuration(
    input: &mut ConfigurationReader<'_>,
) -> Result<ClusterConfiguration, ConfigurationError> {
    let count = input.u32()?;
    if usize::try_from(count).map_err(|_| ConfigurationError::InvalidEncoding)? > input.bytes.len()
    {
        return Err(ConfigurationError::InvalidEncoding);
    }
    let mut members = Vec::new();
    for _ in 0..count {
        let node_id = input.u64()?;
        let length =
            usize::try_from(input.u32()?).map_err(|_| ConfigurationError::InvalidEncoding)?;
        let endpoint = String::from_utf8(input.take(length)?.to_vec())
            .map_err(|_| ConfigurationError::InvalidEncoding)?;
        members.push((node_id, endpoint));
    }
    ClusterConfiguration::new(members)
}

struct ConfigurationReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ConfigurationReader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], ConfigurationError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(ConfigurationError::InvalidEncoding)?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32, ConfigurationError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| ConfigurationError::InvalidEncoding)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, ConfigurationError> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| ConfigurationError::InvalidEncoding)?,
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogEntry {
    pub index: u64,
    pub term: Term,
    pub payload: EntryPayload,
}
