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
}

impl Serializable for ResponsePartition {
    fn into(self: Self) -> Vec<u8> {
        let mut buf = vec![];

        buf.put_i32(0); // Partition index
        buf.put_i16(ErrorCode::UnknownTopicId as i16);
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

fn process_fetch(req: Request) -> Result<Response> {
    let mut req_body = req.body.as_slice();

    let _max_wait_ms = req_body.get_i32();
    let _min_bytes = req_body.get_i32();
    let _max_bytes = req_body.get_i32();
    let _isolation_level = req_body.get_u8();
    let session_id = req_body.get_i32();
    let _session_epoch = req_body.get_i32();
    let maybe_topics = read_compact_array::<Topic>(&mut req_body)?;
    let _forgotten_topics = read_compact_array::<Topic>(&mut req_body)?;
    let _rack_id = read_compact_string(&mut req_body)?;
    assert_eq!(read_uvarint(&mut req_body)?, 0); // We don't expect a tag buffer

    let mut resp_body: Vec<u8> = vec![];

    if req.api_version != 16 {
        resp_body.put_i16(ErrorCode::UnsupportedVersion as i16);
    } else {
        resp_body.put_i32(0); // throttle_time_ms
        resp_body.put_i16(ErrorCode::None as i16);
        resp_body.put_i32(session_id);
        if let Some(topics) = maybe_topics {
            let responses = topics
                .into_iter()
                .map(|t| FetchResponse {
                    topic_id: t.topic_id,
                    partitions: vec![
                        ResponsePartition {
                        },
                    ],
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

pub fn build_response(req: Request) -> Result<Response> {
    #[allow(unreachable_patterns)]
    match &req.api_key {
        ApiKey::ApiVersions => Ok(process_api_versions(req)?),
        ApiKey::Fetch => Ok(process_fetch(req)?),
        other => bail!("API Key {other:?} not yet implemented")
    }
}
