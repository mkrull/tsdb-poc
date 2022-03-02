use std::process;
use unsigned_varint::decode;

// in this context the max varint size is 32 bits, hence the u32 return value:
// https://github.com/prometheus/prometheus/blob/main/tsdb/chunks/chunks.go#L52
pub fn get_uvarint(buf: &Vec<u8>, pos: usize) -> (u32, usize) {
    if buf.len() <= pos {
        return (0, 0);
    }
    let uvarint_vec = copy_bytes(buf, 4, pos);
    match decode::u32(&uvarint_vec) {
        Ok((int, rest)) => return (int, 4 - rest.len()),
        Err(e) => {
            println!("{}", e);
            // TODO: come up with a Result type instead of exiting
            process::exit(1)
        }
    }
}

pub fn copy_bytes(buf: &Vec<u8>, size: usize, pos: usize) -> Vec<u8> {
    let mut ret = vec![0; size];
    ret[..].copy_from_slice(&buf[pos..pos + size]);
    return ret;
}
