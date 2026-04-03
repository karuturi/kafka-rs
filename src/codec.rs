use bytes::{Buf, BufMut, BytesMut};

pub trait Encoder {
    fn encode(&self, buf: &mut BytesMut);
}

pub trait Decoder: Sized {
    fn decode(buf: &mut BytesMut) -> anyhow::Result<Self>;
}

pub struct VarInt(pub u32);

impl Encoder for VarInt {
    fn encode(&self, buf: &mut BytesMut) {
        let mut v = self.0;
        while v >= 0x80 {
            buf.put_u8((v & 0x7F) as u8 | 0x80);
            v >>= 7;
        }
        buf.put_u8(v as u8);
    }
}

impl Decoder for VarInt {
    fn decode(buf: &mut BytesMut) -> anyhow::Result<Self> {
        let mut v = 0u32;
        let mut shift = 0;
        loop {
            if buf.is_empty() {
                anyhow::bail!("Buffer underflow while decoding VarInt");
            }
            let b = buf.get_u8();
            v |= ((b & 0x7F) as u32) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift >= 32 {
                anyhow::bail!("VarInt overflow");
            }
        }
        Ok(VarInt(v))
    }
}

pub struct CompactString(pub String);

impl Encoder for CompactString {
    fn encode(&self, buf: &mut BytesMut) {
        let len = self.0.len() as u32;
        VarInt(len + 1).encode(buf);
        buf.put_slice(self.0.as_bytes());
    }
}

impl Decoder for CompactString {
    fn decode(buf: &mut BytesMut) -> anyhow::Result<Self> {
        let len = VarInt::decode(buf)?.0;
        if len == 0 {
            anyhow::bail!("CompactString length cannot be 0 (null not supported yet)");
        }
        let str_len = (len - 1) as usize;
        if buf.remaining() < str_len {
            anyhow::bail!("Buffer underflow while decoding CompactString");
        }
        let str_bytes = buf.copy_to_bytes(str_len);
        let s = String::from_utf8(str_bytes.to_vec())?;
        Ok(CompactString(s))
    }
}

pub struct CompactBytes(pub bytes::Bytes);

impl Encoder for CompactBytes {
    fn encode(&self, _buf: &mut BytesMut) {
        todo!()
    }
}

impl Decoder for CompactBytes {
    fn decode(buf: &mut BytesMut) -> anyhow::Result<Self> {
        let len = VarInt::decode(buf)?.0;
        if len == 0 {
            anyhow::bail!("CompactBytes length cannot be 0 (null not supported yet)");
        }
        let bytes_len = (len - 1) as usize;
        if buf.remaining() < bytes_len {
            anyhow::bail!("Buffer underflow while decoding CompactBytes");
        }
        let b = buf.copy_to_bytes(bytes_len);
        Ok(CompactBytes(b))
    }
}

pub struct ProduceRequest {
    pub transactional_id: Option<CompactString>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topic_data: Vec<TopicProduceData>,
}

pub struct TopicProduceData {
    pub name: CompactString,
    pub partition_data: Vec<PartitionProduceData>,
}

pub struct PartitionProduceData {
    pub index: i32,
    pub records: Option<CompactBytes>,
}

impl Decoder for ProduceRequest {
    fn decode(buf: &mut BytesMut) -> anyhow::Result<Self> {
        // transactional_id (CompactString, nullable)
        let transactional_id = if buf.get(0).copied() == Some(0) {
            buf.advance(1);
            None
        } else {
            Some(CompactString::decode(buf)?)
        };

        let acks = buf.get_i16();
        let timeout_ms = buf.get_i32();

        // topic_data array (CompactArray)
        let topic_count = VarInt::decode(buf)?.0;
        let mut topic_data = Vec::with_capacity((topic_count - 1) as usize);
        for _ in 0..(topic_count - 1) {
            let name = CompactString::decode(buf)?;
            
            // partition_data array (CompactArray)
            let partition_count = VarInt::decode(buf)?.0;
            let mut partition_data = Vec::with_capacity((partition_count - 1) as usize);
            for _ in 0..(partition_count - 1) {
                let index = buf.get_i32();
                let records = Some(CompactBytes::decode(buf)?);
                
                // Tag buffer for PartitionProduceData
                let _tags = VarInt::decode(buf)?;
                
                partition_data.push(PartitionProduceData { index, records });
            }
            
            // Tag buffer for TopicProduceData
            let _tags = VarInt::decode(buf)?;
            
            topic_data.push(TopicProduceData { name, partition_data });
        }

        // Tag buffer for ProduceRequest
        let _tags = VarInt::decode(buf)?;

        Ok(ProduceRequest {
            transactional_id,
            acks,
            timeout_ms,
            topic_data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_encoding_decoding() {
        let values = vec![0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455];
        for v in values {
            let mut buf = BytesMut::new();
            VarInt(v).encode(&mut buf);
            let decoded = VarInt::decode(&mut buf).unwrap();
            assert_eq!(decoded.0, v, "Failed for value {}", v);
        }
    }

    #[test]
    fn test_produce_request_decoding() {
        let hex_dump = vec![
            0x00, 0x00, 0x01, 0x00, 0x00, 0x03, 0xe8, 0x02, 0x05, 0x74, 0x65, 0x73, 0x74, 0x02, 0x00, 
            0x00, 0x00, 0x00, 0x06, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00
        ];
        let mut buf = BytesMut::from(&hex_dump[..]);
        let request = ProduceRequest::decode(&mut buf).unwrap();

        assert_eq!(request.acks, 1);
        assert_eq!(request.timeout_ms, 1000);
        assert_eq!(request.topic_data.len(), 1);
        assert_eq!(request.topic_data[0].name.0, "test");
        assert_eq!(request.topic_data[0].partition_data.len(), 1);
        assert_eq!(request.topic_data[0].partition_data[0].index, 0);
        assert_eq!(request.topic_data[0].partition_data[0].records.as_ref().unwrap().0, "hello");
    }
}
