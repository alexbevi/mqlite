use std::io::Cursor;

use bson::Document;
use mqlite_debug::{Component, add_counter, span};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const OP_MSG: i32 = 2013;

#[derive(Debug, Clone, PartialEq)]
pub enum PayloadSection {
    Body(Document),
    Sequence {
        identifier: String,
        documents: Vec<Document>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpMsg {
    pub request_id: i32,
    pub response_to: i32,
    pub flag_bits: u32,
    pub sections: Vec<PayloadSection>,
}

#[derive(Debug, Error)]
pub enum WireError {
    #[error("message was truncated")]
    Truncated,
    #[error("unsupported opcode {0}")]
    InvalidOpcode(i32),
    #[error("unsupported payload type {0}")]
    InvalidPayloadType(u8),
    #[error("message did not contain a body section")]
    MissingBody,
    #[error("invalid cstring in payload sequence")]
    InvalidCString,
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("bson encode error: {0}")]
    Encode(#[from] bson::ser::Error),
    #[error("bson decode error: {0}")]
    Decode(#[from] bson::de::Error),
}

impl OpMsg {
    pub fn new(request_id: i32, response_to: i32, sections: Vec<PayloadSection>) -> Self {
        Self {
            request_id,
            response_to,
            flag_bits: 0,
            sections,
        }
    }

    pub fn body(&self) -> Option<&Document> {
        self.sections.iter().find_map(|section| match section {
            PayloadSection::Body(document) => Some(document),
            PayloadSection::Sequence { .. } => None,
        })
    }

    pub fn materialize_command(&self) -> Result<Document, WireError> {
        let mut command = self.body().cloned().ok_or(WireError::MissingBody)?;
        for section in &self.sections {
            if let PayloadSection::Sequence {
                identifier,
                documents,
            } = section
            {
                command.insert(
                    identifier.clone(),
                    bson::Bson::Array(
                        documents
                            .iter()
                            .cloned()
                            .map(bson::Bson::Document)
                            .collect(),
                    ),
                );
            }
        }
        Ok(command)
    }

    pub fn encode(&self) -> Result<Vec<u8>, WireError> {
        let _span = span(Component::Wire, "op_msg_encode");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0_i32.to_le_bytes());
        bytes.extend_from_slice(&self.request_id.to_le_bytes());
        bytes.extend_from_slice(&self.response_to.to_le_bytes());
        bytes.extend_from_slice(&OP_MSG.to_le_bytes());
        bytes.extend_from_slice(&self.flag_bits.to_le_bytes());

        for section in &self.sections {
            match section {
                PayloadSection::Body(document) => {
                    bytes.push(0);
                    bytes.extend_from_slice(&bson::to_vec(document)?);
                }
                PayloadSection::Sequence {
                    identifier,
                    documents,
                } => {
                    bytes.push(1);
                    let mut payload = Vec::new();
                    payload.extend_from_slice(&0_i32.to_le_bytes());
                    payload.extend_from_slice(identifier.as_bytes());
                    payload.push(0);
                    for document in documents {
                        payload.extend_from_slice(&bson::to_vec(document)?);
                    }
                    let payload_len = payload.len() as i32;
                    payload[..4].copy_from_slice(&payload_len.to_le_bytes());
                    bytes.extend_from_slice(&payload);
                }
            }
        }

        let message_length = bytes.len() as i32;
        bytes[..4].copy_from_slice(&message_length.to_le_bytes());
        add_counter(Component::Wire, "encodedMessageBytes", bytes.len() as u64);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, WireError> {
        let _span = span(Component::Wire, "op_msg_decode");
        add_counter(Component::Wire, "decodedMessageBytes", bytes.len() as u64);
        if bytes.len() < 20 {
            return Err(WireError::Truncated);
        }

        let message_length = read_i32(bytes, 0)? as usize;
        if message_length != bytes.len() {
            return Err(WireError::Truncated);
        }

        let request_id = read_i32(bytes, 4)?;
        let response_to = read_i32(bytes, 8)?;
        let opcode = read_i32(bytes, 12)?;
        if opcode != OP_MSG {
            return Err(WireError::InvalidOpcode(opcode));
        }

        let flag_bits = read_u32(bytes, 16)?;
        let mut offset = 20;
        let mut sections = Vec::new();

        while offset < message_length {
            let payload_type = *bytes.get(offset).ok_or(WireError::Truncated)?;
            offset += 1;

            match payload_type {
                0 => {
                    let document_len = read_i32(bytes, offset)? as usize;
                    if offset + document_len > bytes.len() {
                        return Err(WireError::Truncated);
                    }
                    let document =
                        bson::from_slice::<Document>(&bytes[offset..offset + document_len])?;
                    sections.push(PayloadSection::Body(document));
                    offset += document_len;
                }
                1 => {
                    let payload_len = read_i32(bytes, offset)? as usize;
                    if offset + payload_len > bytes.len() {
                        return Err(WireError::Truncated);
                    }
                    let payload_end = offset + payload_len;
                    let identifier_start = offset + 4;
                    let identifier_end = bytes[identifier_start..payload_end]
                        .iter()
                        .position(|byte| *byte == 0)
                        .map(|position| identifier_start + position)
                        .ok_or(WireError::InvalidCString)?;
                    let identifier =
                        String::from_utf8(bytes[identifier_start..identifier_end].to_vec())
                            .map_err(|_| WireError::InvalidCString)?;
                    let mut documents = Vec::new();
                    let mut document_offset = identifier_end + 1;
                    while document_offset < payload_end {
                        let document_len = read_i32(bytes, document_offset)? as usize;
                        if document_offset + document_len > payload_end {
                            return Err(WireError::Truncated);
                        }
                        let document = bson::from_slice::<Document>(
                            &bytes[document_offset..document_offset + document_len],
                        )?;
                        documents.push(document);
                        document_offset += document_len;
                    }
                    sections.push(PayloadSection::Sequence {
                        identifier,
                        documents,
                    });
                    offset = payload_end;
                }
                other => return Err(WireError::InvalidPayloadType(other)),
            }
        }

        Ok(Self {
            request_id,
            response_to,
            flag_bits,
            sections,
        })
    }
}

pub async fn read_op_msg<R>(reader: &mut R) -> Result<OpMsg, WireError>
where
    R: AsyncRead + Unpin,
{
    let mut length_bytes = [0_u8; 4];
    reader.read_exact(&mut length_bytes).await?;
    let message_length = i32::from_le_bytes(length_bytes) as usize;
    if message_length < 20 {
        return Err(WireError::Truncated);
    }

    let mut buffer = vec![0_u8; message_length];
    buffer[..4].copy_from_slice(&length_bytes);
    reader.read_exact(&mut buffer[4..]).await?;
    OpMsg::decode(&buffer)
}

pub async fn write_op_msg<W>(writer: &mut W, message: &OpMsg) -> Result<(), WireError>
where
    W: AsyncWrite + Unpin,
{
    let bytes = message.encode()?;
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

fn read_i32(bytes: &[u8], offset: usize) -> Result<i32, WireError> {
    let slice = bytes.get(offset..offset + 4).ok_or(WireError::Truncated)?;
    Ok(i32::from_le_bytes(slice.try_into().expect("slice length")))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, WireError> {
    let slice = bytes.get(offset..offset + 4).ok_or(WireError::Truncated)?;
    Ok(u32::from_le_bytes(slice.try_into().expect("slice length")))
}

pub fn decode_document(bytes: &[u8]) -> Result<Document, WireError> {
    let _span = span(Component::Wire, "decode_document");
    add_counter(Component::Wire, "decodedDocumentBytes", bytes.len() as u64);
    let mut cursor = Cursor::new(bytes);
    Ok(Document::from_reader(&mut cursor)?)
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use pretty_assertions::assert_eq;

    use super::{OP_MSG, OpMsg, PayloadSection, WireError};

    #[test]
    fn encodes_and_decodes_round_trip() {
        let message = OpMsg {
            request_id: 42,
            response_to: 0,
            flag_bits: 0,
            sections: vec![
                PayloadSection::Body(doc! { "ping": 1, "$db": "admin" }),
                PayloadSection::Sequence {
                    identifier: "documents".to_string(),
                    documents: vec![doc! { "_id": 1 }, doc! { "_id": 2 }],
                },
            ],
        };

        let encoded = message.encode().expect("encode");
        assert_eq!(
            i32::from_le_bytes(encoded[12..16].try_into().expect("opcode")),
            OP_MSG
        );

        let decoded = OpMsg::decode(&encoded).expect("decode");
        assert_eq!(decoded, message);
    }

    #[test]
    fn materializes_document_sequences_into_command_body() {
        let message = OpMsg {
            request_id: 7,
            response_to: 0,
            flag_bits: 0,
            sections: vec![
                PayloadSection::Body(doc! { "insert": "widgets", "$db": "test" }),
                PayloadSection::Sequence {
                    identifier: "documents".to_string(),
                    documents: vec![doc! { "_id": 1 }, doc! { "_id": 2 }],
                },
            ],
        };

        let command = message.materialize_command().expect("materialize");
        assert_eq!(command.get_str("insert").expect("insert"), "widgets");
        assert_eq!(command.get_array("documents").expect("documents").len(), 2);
    }

    #[test]
    fn rejects_invalid_opcode() {
        let mut bytes = OpMsg::new(
            1,
            0,
            vec![PayloadSection::Body(doc! { "ping": 1, "$db": "admin" })],
        )
        .encode()
        .expect("encode");
        bytes[12..16].copy_from_slice(&999_i32.to_le_bytes());

        let error = OpMsg::decode(&bytes).expect_err("should reject invalid opcode");
        assert!(matches!(error, WireError::InvalidOpcode(999)));
    }
}
