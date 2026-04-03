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
    fn test_compact_string_encoding_decoding() {
        let values = vec!["", "hello", "kafka-rs"];
        for v in values {
            let mut buf = BytesMut::new();
            CompactString(v.to_string()).encode(&mut buf);
            let decoded = CompactString::decode(&mut buf).unwrap();
            assert_eq!(decoded.0, v, "Failed for value {}", v);
        }
    }
}
