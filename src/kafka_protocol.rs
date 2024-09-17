use tokio::io;
use bytes::{Buf, BufMut};

fn read_varint(buf: &mut &[u8]) -> Result<u64, io::Error> {
    let mut result = 0;
    let mut shift = 0;
    while buf.remaining() > 0 {
        let byte = buf.get_u8();
        result |= ((byte & 0x7F) as u64) << shift;
        if (byte & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Invalid varint"))
}

fn read_nullable_string(buf: &mut &[u8]) -> Result<Option<String>, io::Error> {
    let len = buf.get_u16() as usize;
    if len == 0 {
        return Ok(None);
    }
    let str_bytes = &buf[..len];
    buf.advance(len);
    Ok(Some(String::from_utf8_lossy(str_bytes).to_string()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKey {
    // More API keys will be included as support is implemented
    ApiVersions = 18,
}

#[repr(i16)]
pub enum ErrorCode {
    // More error codes will be included as needed
    UnknownServerError = -1,
    None = 0,
    UnsupportedVersion = 35,
}

impl TryFrom<i16> for ApiKey {
    type Error = io::Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            18 => Ok(ApiKey::ApiVersions),
            _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "Unsupported API key")),
        }
    }
}

#[derive(Debug)]
pub struct Request {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub client_version: Option<String>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn size(&self) -> u32 {
        8 + self.body.len() as u32
    }

    pub fn try_from(value: Vec<u8>) -> Result<Self, io::Error> {
        let mut buf = &value[..];
        let api_key = ApiKey::try_from(buf.get_i16())?;
        let api_version = buf.get_i16();
        let correlation_id = buf.get_i32();
        let client_id = if api_version > 0 {
            read_nullable_string(&mut buf)?
        } else {
            None
        };
        let body = buf.to_vec();

        Ok(Request {
            api_key,
            api_version,
            correlation_id,
            client_id,
            client_version: None,
            body,
        })
    }

    pub fn try_from_message(value: Vec<u8>) -> Result<Self, io::Error> {
        if value.len() < 4 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Invalid kafka message"));
        }
        let mut buf = &value[..];
        let size = buf.get_u32();

        if size > buf.remaining().try_into().unwrap() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Invalid request: size doesn't match"));
        }

        Self::try_from(value[4..(size as usize)+4].to_vec())
    }

}   

#[derive(Debug)]
pub struct Response {
    pub correlation_id: i32,
    pub body: Vec<u8>,
}

impl Response {
    pub fn new(correlation_id: i32, body: Vec<u8>) -> Self {
        Response {
            correlation_id,
            body,
        }
    }

    pub fn size(&self) -> u32 {
        8 + self.body.len() as u32
    }

    pub fn to_message(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity((self.size() + 4) as usize);
        buf.put_u32(self.size());
        buf.put_i32(self.correlation_id);
        buf.extend(self.body);
        buf
    }
}

#[derive(Debug)]
pub enum RequestOrResponse {
    Request(Request),
    Response(Response),
}

impl RequestOrResponse {
    pub fn size(&self) -> u32 {
        match self {
            RequestOrResponse::Request(request) => request.size(),
            RequestOrResponse::Response(response) => response.size(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test;

    #[test]
    async fn test_api_key_try_from() {
        assert_eq!(ApiKey::try_from(18u16).unwrap(), ApiKey::ApiVersions);
        assert!(ApiKey::try_from(44u16).is_err());
    }

    #[test]
    async fn test_request_try_from() {
        let request_bytes = vec![
            0x00, 0x12, // ApiKey::ApiVersions (18)
            0x00, 0x01, // api_version
            0x00, 0x00, 0x00, 0x0A, // correlation_id
            0x00, 0x07, // client_id length
            0x74, 0x65, 0x73, 0x74, 0x69, 0x6E, 0x67, // client_id "testing"
            0x01, 0x02, 0x03, 0x04, // body
        ];

        let request = Request::try_from(request_bytes).unwrap();

        assert_eq!(request.api_key, ApiKey::ApiVersions);
        assert_eq!(request.api_version, 1);
        assert_eq!(request.correlation_id, 10);
        assert_eq!(request.client_id, Some("testing".to_string()));
        assert_eq!(request.body, vec![0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    async fn test_kafka_message_try_request_from_invalid_size() {
        let message_bytes = vec![
            0x00, 0x00, 0x00, 0x10, // size (16, which is incorrect)
            0x00, 0x03, // ApiKey::Metadata (3)
            0x00, 0x02, // api_version
            0x00, 0x00, 0x00, 0x0B, // correlation_id
            0x01, 0x02, 0x03, // body
        ];

        let result = Request::try_from_message(message_bytes);
        assert!(result.is_err());
    }

    #[test]
    async fn test_read_varint() {
        // Test cases: (input, expected_output)
        let test_cases = vec![
            (vec![0x00], 0),
            (vec![0x01], 1),
            (vec![0x7F], 127),
            (vec![0x80, 0x01], 128),
            (vec![0xFF, 0x01], 255),
            (vec![0x80, 0x80, 0x01], 16384),
            (vec![0xFF, 0xFF, 0x7F], 2097151),
            (vec![0x80, 0x80, 0x80, 0x80, 0x01], 268435456),
            (vec![0xFF, 0xFF, 0xFF, 0xFF, 0x0F], 4294967295),
        ];

        for (input, expected) in test_cases {
            let mut buf = &input[..];
            let result = read_varint(&mut buf).unwrap();
            assert_eq!(result, expected, "Failed for input: {:?}", input);
            assert_eq!(buf.len(), 0, "Buffer not fully consumed for input: {:?}", input);
        }
    }

    #[test]
    #[should_panic(expected = "called `Result::unwrap()` on an `Err` value: Custom { kind: UnexpectedEof, error: \"Invalid varint\" }")]
    async fn test_read_varint_incomplete() {
        let mut buf = &[0x80, 0x80][..];
        read_varint(&mut buf).unwrap();
    }
}

