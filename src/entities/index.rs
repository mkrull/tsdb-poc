use crc::{Crc, CRC_32_ISCSI};
use std::{fs::File, io::Read, path::Path, process};

#[path = "common.rs"]
mod common;
use common::*;

const CASTAGNIOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const CHECKSUM_SIZE: usize = 4;
const MAGIC_SIZE: usize = 4;
const VERSION_SIZE: usize = 1;
const NUM_SYMBOLS_SIZE: usize = 4;
const SYMBOLS_LEN_SIZE: usize = 4;

// NOTE: Format of an index file:
// https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/index.md
pub struct Index {
    buf: Vec<u8>,
    current_pos: usize,
}

impl Index {
    fn advance_pos(&mut self, n: usize) {
        self.current_pos += n;
    }

    pub fn new(path: &Path) -> Self {
        let mut f = File::open(path).expect("Could not open file.");
        let mut buf: Vec<u8> = Vec::new();

        f.read_to_end(&mut buf).expect("Error reading into buf");

        let m = copy_bytes(&buf, MAGIC_SIZE, 0);
        println!("magic: {:x?}", m);

        let v = copy_bytes(&buf, VERSION_SIZE, 4);
        println!("version: {:x?}", v);

        println!("version: {:x?}", buf);
        Self {
            buf,
            current_pos: 5,
        }
    }

    pub fn symbol_table(&mut self) -> SymbolTable {
        let len = get_as_num(&self.buf, self.current_pos);
        self.advance_pos(SYMBOLS_LEN_SIZE);
        println!("len: {}", len);

        let table_buf = copy_bytes(&self.buf, len as usize, self.current_pos);
        self.advance_pos(len as usize);

        let cs = get_checksum(&self.buf, self.current_pos);
        let crc = CASTAGNIOLI.checksum(&table_buf);

        self.advance_pos(CHECKSUM_SIZE);

        println!("{:x?}", table_buf);
        if cs != crc {
            println!("Checksum mismatch. Corrupted symbol table.");
            process::exit(1);
        }

        SymbolTable {
            num_symbols: 0,
            buf: table_buf,
            current_pos: 0,
        }
    }
}

#[derive(Debug)]
pub struct SymbolTable {
    num_symbols: usize,
    buf: Vec<u8>,
    current_pos: usize,
}
