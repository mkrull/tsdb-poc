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
    get_as_num(buf, pos)
}

pub fn get_as_num(buf: &[u8], pos: usize) -> Result<u32> {
    let cs: [u8; size_of::<u32>()] = copy_bytes(buf, size_of::<u32>(), pos)
        .try_into()
        .expect("An error");
    Ok(u32::from_be_bytes(cs))
}

pub fn get_as_num64(buf: &[u8], pos: usize) -> u64 {
    let cs: [u8; 8] = copy_bytes(buf, 8, pos)
        .try_into()
        .expect("couldn't get checksum bytes");
    u64::from_be_bytes(cs)
}
