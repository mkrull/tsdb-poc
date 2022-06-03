use std::mem::size_of;
use unsigned_varint::decode;

#[derive(Debug, Clone)]
pub enum TSDBError {
    Default,
    SymbolTableLookup,
}

pub type Result<T> = std::result::Result<T, TSDBError>;

// pub fn copy_bytes(buf: &[u8], size: usize, pos: usize) -> Vec<u8> {
//     let mut ret = vec![0; size];
//     ret[..].copy_from_slice(&buf[pos..pos + size]);
//     ret
// }

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

            let varint_vec = &buf[pos..];
            match decode::$ti(varint_vec) {
                Ok((int, rest)) => return Ok((int, varint_vec.len() - rest.len())),
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
            let b = slice_bytes(buf, size_of::<$typ>(), pos);
            match TryInto::<[u8; size_of::<$typ>()]>::try_into(b) {
                Ok(bytes) => Ok(<$typ>::from_be_bytes(bytes)),
                Err(_) => Err(TSDBError::Default),
            }
        }
    };
}

read!(read_u32, u32);
read!(read_u64, u64);

pub fn read_varint_i64(buf: &[u8], pos: usize) -> Result<(i64, usize)> {
    // those varint64s are stored as encoded uvarint64s
    let (u, size) = read_varint_u64(buf, pos)?;
    Ok((zigzag_dec(u), size))
}

// get i64 from zigzag encoded u64
// see: https://developers.google.com/protocol-buffers/docs/encoding#signed-ints
fn zigzag_dec(u: u64) -> i64 {
    (u >> 1) as i64 ^ -((u & 1) as i64)
}
