use anyhow::{bail, Result};
use bytes::BufMut;
use crate::kafka_protocol::{ApiKey, ErrorCode, Request, Response};

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
        min_version: 0,
        max_version: 4,
    },
];

fn process_api_versions(req: Request) -> Result<Response> {
    let mut body: Vec<u8> = vec![];

    if req.api_version < 0 ||req.api_version > 4 {
        body.put_i16(ErrorCode::UnsupportedVersion as i16);
    } else {
        body.put_i16(ErrorCode::None as i16);
        body.put_u32(API_VERSIONS.len() as u32);
        for api_version in API_VERSIONS {
            body.put_i16(api_version.min_version);
            body.put_i16(api_version.max_version);
            body.put_i16(api_version.api_key as i16);
        }
    }

    Ok(Response::new(req.correlation_id, body))
}

pub fn build_response(req: Request) -> Result<Response> {
    match &req.api_key {
        ApiKey::ApiVersions => Ok(process_api_versions(req)?),
        other => bail!("API Key {other:?} not yet implemented")
    }
}
