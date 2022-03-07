use crc::{Crc, CRC_32_ISCSI};
use std::{fs::File, io::Read, path::Path, str};

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
        let v = copy_bytes(&buf, VERSION_SIZE, 4);

        println!("magic: {:x?}", m);
        println!("version: {:x?}", v);
        //println!("buf: {:x?}", buf);

        Self {
            buf,
            current_pos: 5,
        }
    }

    pub fn symbol_table(&mut self) -> common::Result<SymbolTable> {
        let len = get_as_num(&self.buf, self.current_pos)?;
        self.advance_pos(SYMBOLS_LEN_SIZE);
        println!("len: {}", len);

        let table_buf = copy_bytes(&self.buf, len as usize, self.current_pos);
        self.advance_pos(len as usize);

        let cs = get_checksum(&self.buf, self.current_pos)?;
        let crc = CASTAGNIOLI.checksum(&table_buf);

        self.advance_pos(CHECKSUM_SIZE);

        let num = get_as_num(&table_buf, 0)?;
        println!("num: {}", num);
        let data = copy_bytes(
            &table_buf,
            table_buf.len() - NUM_SYMBOLS_SIZE,
            NUM_SYMBOLS_SIZE,
        );

        //println!("{:x?}", table_buf);
        if cs != crc {
            println!("Checksum mismatch. Corrupted symbol table.");
            return Err(common::TSDBError);
        }

        Ok(SymbolTable {
            num: num as usize,
            buf: data,
            current_pos: 0,
        })
    }
}

#[derive(Debug)]
pub struct SymbolTable {
    num: usize,
    buf: Vec<u8>,
    current_pos: usize,
}

impl Iterator for SymbolTable {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        match get_uvarint(&self.buf, self.current_pos) {
            Ok((len, size)) => {
                if size == 0 {
                    return None;
                }
                self.current_pos += size;

                let data = copy_bytes(&self.buf, len as usize, self.current_pos);

                // data length
                self.current_pos += len as usize;

                match str::from_utf8(&data) {
                    Ok(s) => Some(s.to_string()),
                    Err(e) => {
                        println!("{}", e);
                        None
                    }
                }
            }
            Err(e) => None,
        }
    }
}
