use crc::{Crc, CRC_32_ISCSI};
use std::{fs::File, io::Read, mem::size_of, path::Path, process, str};

#[path = "common.rs"]
mod common;
use common::*;

const CASTAGNIOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const CHECKSUM_SIZE: usize = 4;
const TOC_ENTRY_SIZE: usize = 8;
const MAGIC_SIZE: usize = 4;
const VERSION_SIZE: usize = 1;
const NUM_SYMBOLS_SIZE: usize = 4;
const SYMBOLS_LEN_SIZE: usize = 4;
const TOC_SIZE: usize = size_of::<TOC>();

// NOTE: Format of an index file:
// https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/index.md
#[derive(Debug)]
pub struct Index {
    buf: Vec<u8>,
    toc: TOC,
}

impl Index {
    pub fn new(path: &Path) -> Self {
        let mut f = File::open(path).expect("Could not open file.");
        let mut buf: Vec<u8> = Vec::new();

        f.read_to_end(&mut buf).expect("Error reading into buf");

        let m = copy_bytes(&buf, MAGIC_SIZE, 0);
        let v = copy_bytes(&buf, VERSION_SIZE, 4);

        println!("magic: {:x?}", m);
        println!("version: {:x?}", v);

        let toc = Index::toc(&buf).expect("Could not load TOC.");

        Self { toc, buf }
    }

    fn toc(buf: &[u8]) -> Result<TOC> {
        // get table of content
        let pos = buf.len() - TOC_SIZE - CHECKSUM_SIZE;
        let toc_buf = copy_bytes(&buf, TOC_SIZE, pos);
        let cs = get_checksum(&buf, pos + TOC_SIZE)?;
        let crc = CASTAGNIOLI.checksum(&toc_buf);

        if cs != crc {
            println!("Checksum mismatch. Corrupted table of content.");
            process::exit(1);
        }

        let mut current_pos = 0;
        let symbols = read_u64(&toc_buf, current_pos)?;
        current_pos += TOC_ENTRY_SIZE;
        let series = read_u64(&toc_buf, current_pos)?;
        current_pos += TOC_ENTRY_SIZE;
        let label_index_start = read_u64(&toc_buf, current_pos)?;
        current_pos += TOC_ENTRY_SIZE;
        let label_offset_table = read_u64(&toc_buf, current_pos)?;
        current_pos += TOC_ENTRY_SIZE;
        let postings_start = read_u64(&toc_buf, current_pos)?;
        current_pos += TOC_ENTRY_SIZE;
        let postings_offset_table = read_u64(&toc_buf, current_pos)?;

        Ok(TOC {
            symbols,
            series,
            label_index_start,
            label_offset_table,
            postings_start,
            postings_offset_table,
        })
    }
}

pub fn symbol_table(i: &Index) -> Result<SymbolTable> {
    let mut curr = i.toc.symbols as usize;
    let len = read_u32(&i.buf, curr)?;
    curr += SYMBOLS_LEN_SIZE;
    println!("len: {}", len);

    let table_buf = copy_bytes(&i.buf, len as usize, curr);
    curr += len as usize;

    let cs = get_checksum(&i.buf, curr)?;
    let crc = CASTAGNIOLI.checksum(&table_buf);

    curr += CHECKSUM_SIZE;

    let num = read_u32(&table_buf, 0)?;
    println!("num: {}", num);
    let data = copy_bytes(
        &table_buf,
        table_buf.len() - NUM_SYMBOLS_SIZE,
        NUM_SYMBOLS_SIZE,
    );

    //println!("{:x?}", table_buf);
    if cs != crc {
        println!("Checksum mismatch. Corrupted symbol table.");
        return Err(TSDBError);
    }

    Ok(SymbolTable {
        num: num as usize,
        buf: data,
        current_pos: 0,
    })
}

// ┌────────────────────┬─────────────────────┐
// │ len <4b>           │ #symbols <4b>       │
// ├────────────────────┴─────────────────────┤
// │ ┌──────────────────────┬───────────────┐ │
// │ │ len(str_1) <uvarint> │ str_1 <bytes> │ │
// │ ├──────────────────────┴───────────────┤ │
// │ │                . . .                 │ │
// │ ├──────────────────────┬───────────────┤ │
// │ │ len(str_n) <uvarint> │ str_n <bytes> │ │
// │ └──────────────────────┴───────────────┘ │
// ├──────────────────────────────────────────┤
// │ CRC32 <4b>                               │
// └──────────────────────────────────────────┘
#[derive(Debug)]
pub struct SymbolTable {
    num: usize,
    buf: Vec<u8>,
    current_pos: usize,
}

impl Iterator for SymbolTable {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        match read_varint_u32(&self.buf, self.current_pos) {
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
            Err(_) => None,
        }
    }
}

// ┌──────────────────────────────────────────────────────────────────────────┐
// │ len <uvarint>                                                            │
// ├──────────────────────────────────────────────────────────────────────────┤
// │ ┌──────────────────────────────────────────────────────────────────────┐ │
// │ │                     labels count <uvarint64>                         │ │
// │ ├──────────────────────────────────────────────────────────────────────┤ │
// │ │              ┌────────────────────────────────────────────┐          │ │
// │ │              │ ref(l_i.name) <uvarint32>                  │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ ref(l_i.value) <uvarint32>                 │          │ │
// │ │              └────────────────────────────────────────────┘          │ │
// │ │                             ...                                      │ │
// │ ├──────────────────────────────────────────────────────────────────────┤ │
// │ │                     chunks count <uvarint64>                         │ │
// │ ├──────────────────────────────────────────────────────────────────────┤ │
// │ │              ┌────────────────────────────────────────────┐          │ │
// │ │              │ c_0.mint <varint64>                        │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ c_0.maxt - c_0.mint <uvarint64>            │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ ref(c_0.data) <uvarint64>                  │          │ │
// │ │              └────────────────────────────────────────────┘          │ │
// │ │              ┌────────────────────────────────────────────┐          │ │
// │ │              │ c_i.mint - c_i-1.maxt <uvarint64>          │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ c_i.maxt - c_i.mint <uvarint64>            │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ ref(c_i.data) - ref(c_i-1.data) <varint64> │          │ │
// │ │              └────────────────────────────────────────────┘          │ │
// │ │                             ...                                      │ │
// │ └──────────────────────────────────────────────────────────────────────┘ │
// ├──────────────────────────────────────────────────────────────────────────┤
// │ CRC32 <4b>                                                               │
// └──────────────────────────────────────────────────────────────────────────┘
#[derive(Debug)]
pub struct Series {
    num: usize,
    buf: Vec<u8>,
    current_pos: usize,
}

impl Iterator for Series {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        match read_varint_u32(&self.buf, self.current_pos) {
            Ok((len, size)) => {
                if size == 0 {
                    return None;
                }
                self.current_pos += size;

                let data = copy_bytes(&self.buf, len as usize, self.current_pos);

                // data length
                self.current_pos += len as usize;

                Some(data)
            }
            Err(_) => None,
        }
    }
}
// ┌─────────────────────────────────────────┐
// │ ref(symbols) <8b>                       │
// ├─────────────────────────────────────────┤
// │ ref(series) <8b>                        │
// ├─────────────────────────────────────────┤
// │ ref(label indices start) <8b>           │
// ├─────────────────────────────────────────┤
// │ ref(label offset table) <8b>            │
// ├─────────────────────────────────────────┤
// │ ref(postings start) <8b>                │
// ├─────────────────────────────────────────┤
// │ ref(postings offset table) <8b>         │
// ├─────────────────────────────────────────┤
// │ CRC32 <4b>                              │
// └─────────────────────────────────────────┘
#[derive(Debug)]
pub struct TOC {
    symbols: u64,
    series: u64,
    label_index_start: u64,
    label_offset_table: u64,
    postings_start: u64,
    postings_offset_table: u64,
}
