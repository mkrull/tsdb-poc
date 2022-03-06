use std::process;
use unsigned_varint::decode;

const CHECKSUM_SIZE: usize = 4;

// in this context the max varint size is 32 bits, hence the u32 return value:
// https://github.com/prometheus/prometheus/blob/main/tsdb/chunks/chunks.go#L52
pub fn get_uvarint(buf: &[u8], pos: usize) -> (u32, usize) {
    if buf.len() <= pos {
        return (0, 0);
    }
    let uvarint_vec = copy_bytes(buf, 4, pos);
    match decode::u32(&uvarint_vec) {
        Ok((int, rest)) => (int, 4 - rest.len()),
        Err(e) => {
            println!("{}", e);
            // TODO: come up with a Result type instead of exiting
            process::exit(1)
        }
    }
}

pub fn copy_bytes(buf: &[u8], size: usize, pos: usize) -> Vec<u8> {
    let mut ret = vec![0; size];
    ret[..].copy_from_slice(&buf[pos..pos + size]);
    ret
}

pub fn get_checksum(buf: &[u8], pos: usize) -> u32 {
    let cs: [u8; CHECKSUM_SIZE] = copy_bytes(buf, CHECKSUM_SIZE, pos)
        .try_into()
        .expect("couldn't get checksum bytes");
    u32::from_be_bytes(cs)
}

pub fn get_as_num(buf: &[u8], pos: usize) -> u32 {
    let cs: [u8; 4] = copy_bytes(buf, 4, pos)
        .try_into()
        .expect("couldn't get checksum bytes");
    u32::from_be_bytes(cs)
}
