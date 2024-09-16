use anyhow::{bail, Result};
use bytes::BufMut;
use crate::kafka_protocol::{ApiKey, ErrorCode, Request, Response};

struct ApiVersion {
    api_key: ApiKey,
    min_version: u16,
    max_version: u16,
}

impl From<&[u8]> for ApiVersion {
    fn from(value: &[u8]) -> Self {
        let api_key = ApiKey::try_from(u16::from_be_bytes(value[0..2].try_into().unwrap())).unwrap();
        let min_version = u16::from_be_bytes(value[2..4].try_into().unwrap());
        let max_version = u16::from_be_bytes(value[4..6].try_into().unwrap());

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
        min_version: 0,
        max_version: 2,
    },
];

fn process_api_versions(req: Request) -> Result<Response> {
    let mut body: Vec<u8> = vec![];

    body.put_u16(ErrorCode::None as u16);
    body.put_u32(API_VERSIONS.len() as u32);
    for api_version in API_VERSIONS {
        body.put_u16(api_version.min_version);
        body.put_u16(api_version.max_version);
        body.put_u16(api_version.api_key as u16);
    }

    Ok(Response::new(req.correlation_id, body))
}

pub fn build_response(req: Request) -> Result<Response> {
    match &req.api_key {
        ApiKey::ApiVersions => Ok(process_api_versions(req)?),
        other => bail!("API Key {other:?} not yet implemented")
    }
}
