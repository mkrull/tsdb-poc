use std::mem::size_of;
use unsigned_varint::decode;

#[derive(Debug, Clone)]
pub enum TSDBError {
    Default,
    CantReadSymbol,
}

pub type Result<T> = std::result::Result<T, TSDBError>;

pub fn copy_bytes(buf: &[u8], size: usize, pos: usize) -> Vec<u8> {
    let mut ret = vec![0; size];
    ret[..].copy_from_slice(&buf[pos..pos + size]);
    ret
}

pub fn slice_bytes(buf: &[u8], size: usize, pos: usize) -> &[u8] {
    &buf[pos..pos + size]
}

pub fn get_checksum(buf: &[u8], pos: usize) -> Result<u32> {
    read_u32(buf, pos)
}

macro_rules! read_varint {
    ($func:ident, $typ:ty, $ti:ident) => {
        pub fn $func(buf: &[u8], pos: usize) -> Result<($typ, usize)> {
            if buf.len() <= pos {
                return Ok((0, 0));
            }
            let uvarint_vec = copy_bytes(buf, size_of::<$typ>(), pos);
            match decode::$ti(&uvarint_vec) {
                Ok((int, rest)) => Ok((int, size_of::<$typ>() - rest.len())),
                Err(_) => {
                    return Err(TSDBError::Default);
                }
            }
        }
    };
}

read_varint!(read_varint_u32, u32, u32);
read_varint!(read_varint_u64, u64, u64);

macro_rules! read {
    ($func:ident, $typ:ty) => {
        pub fn $func(buf: &[u8], pos: usize) -> Result<$typ> {
            let b = copy_bytes(buf, size_of::<$typ>(), pos);
            match TryInto::<[u8; size_of::<$typ>()]>::try_into(b) {
                Ok(bytes) => Ok(<$typ>::from_be_bytes(bytes)),
                Err(_) => Err(TSDBError::Default),
            }
        }
    };
}

read!(read_u32, u32);
read!(read_u64, u64);
