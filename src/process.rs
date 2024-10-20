use anyhow::{bail, Result};
use bytes::{Buf, BufMut};
use crate::kafka_protocol::{read_compact_array, read_compact_string, read_uvarint, ApiKey, Deserializable, ErrorCode, Request, Response, Serializable};

fn write_uvarint(buf: &mut Vec<u8>, value: u64) {
    let mut value = value;
    if value == 0 {
        buf.put_u8(0);
    } else {
        while value > 0 {
            let mut encoded = (value & 0x7f) as u8;
            value = value >> 7;

            if value > 0 {
                encoded |= 0x80;
            }
            buf.put_u8(encoded);
        }
    }
}

fn write_uuid(buf: &mut Vec<u8>, uuid: uuid::Uuid) {
    buf.put_slice(uuid.as_bytes());
}

fn write_compact_array<T>(buf: &mut Vec<u8>, collection: Vec<T>)
where T: Serializable
{
    write_uvarint(buf, (collection.len() + 1) as u64);
    for value in collection {
        let as_vec = value.into();
        buf.put_slice(&as_vec);
    }
}

struct ApiVersion {
    api_key: ApiKey,
    min_version: i16,
    max_version: i16,
}

impl From<&[u8]> for ApiVersion {
    fn from(value: &[u8]) -> Self {
        let api_key = ApiKey::try_from(i16::from_be_bytes(value[0..2].try_into().unwrap())).unwrap();
        let min_version = i16::from_be_bytes(value[2..4].try_into().unwrap());
        let max_version = i16::from_be_bytes(value[4..6].try_into().unwrap());

        ApiVersion {
            api_key,
            min_version,
            max_version,
        }
    }
}

const API_VERSIONS: &[ApiVersion] = &[
    ApiVersion {
        api_key: ApiKey::ApiVersions,
        min_version: 4,
        max_version: 4,
    },
    ApiVersion {
        api_key: ApiKey::Fetch,
        min_version: 16,
        max_version: 16,
    },
    ApiVersion {
        api_key: ApiKey::DescribeTopicPartitions,
        min_version: 0,
        max_version: 0,
    },
];

fn process_api_versions(req: Request) -> Result<Response> {
    let mut body: Vec<u8> = vec![];

    if req.api_version != 4 {
        body.put_i16(ErrorCode::UnsupportedVersion as i16);
    } else {
        body.put_i16(ErrorCode::None as i16);
        write_uvarint(&mut body, (API_VERSIONS.len() + 1) as u64);
        for api_version in API_VERSIONS {
            body.put_i16(api_version.api_key as i16);
            body.put_i16(api_version.min_version);
            body.put_i16(api_version.max_version);
            write_uvarint(&mut body, 0); // Empty tag buffer
        }
        body.put_u32(0); // throttle_time
        write_uvarint(&mut body, 0); // Empty tag buffer
    }

    Ok(Response::new(crate::kafka_protocol::ResponseVer::V0, req.correlation_id, body))
}

#[derive(Debug)]
struct Partition {
    partition: u32,
    current_leader_epoch: u32,
    fetch_offset: u64,
    last_fetched_epoch: u32,
    log_start_offset: u64,
    partition_max_bytes: u32,
}

impl Deserializable for Partition {
    fn try_from(data: &mut &[u8]) -> Result<Self> {
        let partition = data.get_u32();
        let current_leader_epoch = data.get_u32();
        let fetch_offset = data.get_u64();
        let last_fetched_epoch = data.get_u32();
        let log_start_offset = data.get_u64();
        let partition_max_bytes = data.get_u32();
        assert_eq!(read_uvarint(data)?, 0); // We don't expect a tag buffer

        Ok(Partition {
            partition,
            current_leader_epoch,
            fetch_offset,
            last_fetched_epoch,
            log_start_offset,
            partition_max_bytes,
        })
    }
}

#[derive(Debug)]
struct Topic {
    topic_id: uuid::Uuid,
    partitions: Vec<Partition>,
}

impl Deserializable for Topic {
    fn try_from(value: &mut &[u8]) -> Result<Self> {
        let topic_id = uuid::Builder::from_bytes(value.copy_to_bytes(16)
            .as_ref()
            .try_into()?).into_uuid();
        let partitions = match read_compact_array::<Partition>(value)? {
            None => vec![],
            Some(partitions) => partitions,
        };
        //let n_partitions = read_uvarint(value)?;
        //let mut partitions = vec![];
        //for _ in 0..n_partitions {
        //    partitions.push(Partition::try_from(value)?);
        //}
        assert_eq!(read_uvarint(value)?, 0); // We don't expect a tag buffer

        Ok(Topic {
            topic_id,
            partitions,
        })
    }
}

struct ResponsePartition {
    error_code: ErrorCode,
}

impl Serializable for ResponsePartition {
    fn into(self: Self) -> Vec<u8> {
        let mut buf = vec![];

        buf.put_i32(0); // Partition index
        buf.put_i16(self.error_code as i16);
        buf.put_i64(0); // High watermark
        buf.put_i64(0); // Last stable offset
        buf.put_i64(0); // Log start offset
        write_uvarint(&mut buf, 1); // Aborted transactions
        buf.put_i32(0); // Preferred read replica
        write_uvarint(&mut buf, 1); // Records
        write_uvarint(&mut buf, 0); // Tag buffer

        buf
    }
}

struct FetchResponse {
    topic_id: uuid::Uuid,
    partitions: Vec<ResponsePartition>,
}

impl Serializable for FetchResponse {
    fn into(self: Self) -> Vec<u8> {
        let mut buf = vec![];

        write_uuid(&mut buf, self.topic_id);
        write_compact_array(&mut buf, self.partitions);
        write_uvarint(&mut buf, 0); // Tag buffer

        buf
    }
}

#[derive(Debug)]
struct FetchRequest {
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: u8,
    session_id: i32,
    session_epoch: i32,
    topics: Option<Vec<Topic>>,
    forgotten_topics: Option<Vec<Topic>>,
    rack_id: String,
}

impl Deserializable for FetchRequest {
    fn try_from(value: &mut &[u8]) -> anyhow::Result<Self> where Self: Sized {
        let max_wait_ms = value.get_i32();
        let min_bytes = value.get_i32();
        let max_bytes = value.get_i32();
        let isolation_level = value.get_u8();
        let session_id = value.get_i32();
        let session_epoch = value.get_i32();
        let topics = read_compact_array::<Topic>(value)?;
        let forgotten_topics = read_compact_array::<Topic>(value)?;
        let rack_id = read_compact_string(value)?;
        assert_eq!(read_uvarint(value)?, 0); // We don't expect a tag buffer
        
        Ok(FetchRequest {
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics,
            rack_id,
        })
    }
}

fn process_fetch(req: Request) -> Result<Response> {
    let fetch_req = <FetchRequest as Deserializable>::try_from(&mut req.body.as_slice())?;
    eprintln!("Fetch Request: {fetch_req:#?}");

    let mut resp_body: Vec<u8> = vec![];

    if req.api_version != 16 {
        resp_body.put_i16(ErrorCode::UnsupportedVersion as i16);
    } else {
        resp_body.put_i32(0); // throttle_time_ms
        resp_body.put_i16(ErrorCode::None as i16);
        resp_body.put_i32(fetch_req.session_id);
        if let Some(topics) = fetch_req.topics {
            let responses = topics
                .into_iter()
                .map(|t| {
                    // There are no instructions, so this is the only way I've found
                    // so far to consistently distinguish between the two cases
                    let error_code = match t.topic_id.as_fields() {
                        (_, 0, 0, _) => ErrorCode::UnknownTopicId,
                        _ => ErrorCode::None,
                    };
                    FetchResponse {
                        topic_id: t.topic_id,
                        partitions: vec![
                            ResponsePartition {
                                error_code,
                            },
                        ],
                    }
                })
                .collect();
            write_compact_array(&mut resp_body, responses);
        }
        else {
            write_uvarint(&mut resp_body, 0); // No responses
        }
        write_uvarint(&mut resp_body, 0); // Empty tag buffer
    }

    Ok(Response::new(crate::kafka_protocol::ResponseVer::V1, req.correlation_id, resp_body))
}

fn process_describe_topic_partitions(req: Request) -> Result<Response> {
    todo!()
}

pub fn build_response(req: Request) -> Result<Response> {
    #[allow(unreachable_patterns)]
    match &req.api_key {
        ApiKey::ApiVersions => Ok(process_api_versions(req)?),
        ApiKey::Fetch => Ok(process_fetch(req)?),
        ApiKey::DescribeTopicPartitions => Ok(process_describe_topic_partitions(req)?),
        other => bail!("API Key {other:?} not yet implemented")
    }
}
