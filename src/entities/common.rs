use std::mem::size_of;
use unsigned_varint::decode;

#[derive(Debug, Clone)]
pub struct TSDBError;

pub type Result<T> = std::result::Result<T, TSDBError>;

// in this context the max varint size is 32 bits, hence the u32 return value:
// https://github.com/prometheus/prometheus/blob/main/tsdb/chunks/chunks.go#L52
pub fn get_uvarint(buf: &[u8], pos: usize) -> Result<(u32, usize)> {
    if buf.len() <= pos {
        return Ok((0, 0));
    }
    let uvarint_vec = copy_bytes(buf, 4, pos);
    match decode::u32(&uvarint_vec) {
        Ok((int, rest)) => Ok((int, 4 - rest.len())),
        Err(e) => {
            return Err(TSDBError);
        }
    }
}

pub fn copy_bytes(buf: &[u8], size: usize, pos: usize) -> Vec<u8> {
    let mut ret = vec![0; size];
    ret[..].copy_from_slice(&buf[pos..pos + size]);
    ret
}

pub fn get_checksum(buf: &[u8], pos: usize) -> Result<u32> {
    read_u32(buf, pos)
}

macro_rules! read {
    ($func:ident, $typ:ty) => {
        pub fn $func(buf: &[u8], pos: usize) -> Result<$typ> {
            let b = copy_bytes(buf, size_of::<$typ>(), pos);
            match TryInto::<[u8; size_of::<$typ>()]>::try_into(b) {
                Ok(bytes) => Ok(<$typ>::from_be_bytes(bytes)),
                Err(_) => Err(TSDBError),
            }
        }
    };
}

read!(read_u32, u32);
read!(read_u64, u64);
