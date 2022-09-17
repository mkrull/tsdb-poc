use crc::{Crc, CRC_32_ISCSI};
use std::{fs::File, io::Read, path::Path};

use crate::common::*;

const CASTAGNIOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const ENCODING_SIZE: usize = 1;
const CHECKSUM_SIZE: usize = 4;
const MAGIC_SIZE: usize = 4;
const VERSION_SIZE: usize = 1;

// NOTE: Format of a chunk file:
// https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/chunks.md
#[derive(Debug)]
pub struct Chunks {
    buf: Vec<u8>,
    current_pos: usize,
}

impl Chunks {
    pub fn new(path: &Path) -> Self {
        let mut f = File::open(path).expect("Could not open file.");
        let mut buf: Vec<u8> = Vec::new();

        f.read_to_end(&mut buf).expect("Error reading into buf");

        let m = slice_bytes(&buf, MAGIC_SIZE, 0);
        println!("magic: {:x?}", m);

        let v = slice_bytes(&buf, VERSION_SIZE, 4);
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
        match read_varint_u32(&self.buf, self.current_pos) {
            Ok((len, size)) => {
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
                let data = slice_bytes(&self.buf, ENCODING_SIZE + len as usize, start + size);

                match get_checksum(&self.buf, self.current_pos - CHECKSUM_SIZE) {
                    Ok(cs) => {
                        let crc = CASTAGNIOLI.checksum(data);

                        if cs != crc {
                            return None;
                        }

                        Some(start)
                    }
                    Err(_) => None,
                }
            }
            Err(_) => None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn load_chunks() -> Chunks {
        let test_chunks = Path::new("testdata/testblock/chunks/000001");
        Chunks::new(test_chunks)
    }

    #[test]
    fn load_test_chunks() {
        let chunks = load_chunks();

        let expected = 37020;
        assert_eq!(expected, chunks.count());
    }
}
