use std::{fs::File, io::Read, path::Path};

#[path = "common.rs"]
mod common;
use common::*;

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

        let m = copy_bytes(&buf, 4, 0);
        println!("magic: {:x?}", m);

        let v = copy_bytes(&buf, 1, 4);
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
        self.current_pos += 1;
        // data length
        self.current_pos += len as usize;
        // checksum bytes
        self.current_pos += 4;

        // let data = copy_bytes(&self.buf, size + 1 + len as usize + 4, start);
        // println!("{:?}", data);

        return Some(start);
    }
}
