use crc::{Crc, CRC_32_ISCSI};
use std::{fs::File, io::Read, path::Path};

#[path = "common.rs"]
mod common;
use common::*;

const CASTAGNIOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const ENCODING_SIZE: usize = 1;
const CHECKSUM_SIZE: usize = 4;
const MAGIC_SIZE: usize = 4;
const VERSION_SIZE: usize = 1;

// NOTE: Format of a chunk file:
// https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/chunks.md
pub struct Chunks {
    buf: Vec<u8>,
    current_pos: usize,
}

impl Chunks {
    pub fn new(path: &Path) -> Self {
        let mut f = File::open(path).expect("Could not open file.");
        let mut buf: Vec<u8> = Vec::new();

        f.read_to_end(&mut buf).expect("Error reading into buf");

        let m = copy_bytes(&buf, MAGIC_SIZE, 0);
        println!("magic: {:x?}", m);

        let v = copy_bytes(&buf, VERSION_SIZE, 4);
        println!("version: {:x?}", v);

        Self {
            buf,
            current_pos: 8,
        }
    }
}

impl Iterator for Chunks {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let start = self.current_pos;
        let (len, size) = get_uvarint(&self.buf, self.current_pos);
        //println!("{} {} {}", len, size, self.current_pos);
        if size == 0 {
            return None;
        }
        // NOTE: sizes of segments according to:
        // https://github.com/prometheus/prometheus/blob/main/tsdb/chunks/chunks.go#L37
        //
        // len varint size
        self.current_pos += size;
        // encoding byte
        self.current_pos += ENCODING_SIZE;
        // data length
        self.current_pos += len as usize;
        // checksum bytes
        self.current_pos += CHECKSUM_SIZE;

        // verify checksum
        // the checksum is created over the encoding and data
        let data = copy_bytes(&self.buf, ENCODING_SIZE + len as usize, start + size);

        let cs: [u8; CHECKSUM_SIZE] =
            copy_bytes(&self.buf, CHECKSUM_SIZE, self.current_pos - CHECKSUM_SIZE)
                .try_into()
                .expect("couldn't get checksum bytes");
        let cs_num = u32::from_be_bytes(cs);
        let crc = CASTAGNIOLI.checksum(&data);

        if cs_num != crc {
            return None;
        }

        return Some(start);
    }
}
