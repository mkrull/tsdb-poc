use crc::{Crc, CRC_32_ISCSI};
use std::{collections::HashMap, fs::File, io::Read, mem::size_of, path::Path, str};

use crate::entities::common::*;

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
        let toc_buf = slice_bytes(buf, TOC_SIZE, pos);
        let cs = get_checksum(buf, pos + TOC_SIZE)?;
        let crc = CASTAGNIOLI.checksum(toc_buf);

        if cs != crc {
            println!("Checksum mismatch. Corrupted table of content.");
            return Err(TSDBError::Default);
        }

        let mut current_pos = 0;
        let symbols = read_u64(toc_buf, current_pos)?;
        current_pos += TOC_ENTRY_SIZE;
        let series = read_u64(toc_buf, current_pos)?;
        current_pos += TOC_ENTRY_SIZE;
        let label_index_start = read_u64(toc_buf, current_pos)?;
        current_pos += TOC_ENTRY_SIZE;
        let label_offset_table = read_u64(toc_buf, current_pos)?;
        current_pos += TOC_ENTRY_SIZE;
        let postings_start = read_u64(toc_buf, current_pos)?;
        current_pos += TOC_ENTRY_SIZE;
        let postings_offset_table = read_u64(toc_buf, current_pos)?;

        Ok(TOC {
            symbols,
            series,
            label_index_start,
            label_offset_table,
            postings_start,
            postings_offset_table,
        })
    }

    pub fn read_symbol(&self, pos: usize) -> Result<String> {
        let mut p = pos;
        match read_varint_u32(&self.buf, p) {
            Ok((len, size)) => {
                if size == 0 {
                    return Err(TSDBError::Default);
                }
                p += size;

                let data = slice_bytes(&self.buf, len as usize, p);

                match str::from_utf8(data) {
                    Ok(s) => Ok(s.to_string()),
                    Err(_) => Err(TSDBError::Default),
                }
            }
            Err(_) => Err(TSDBError::CantReadSymbol),
        }
    }
}

pub fn symbol_table(i: &Index) -> Result<SymbolTable> {
    let mut curr = i.toc.symbols as usize;
    let len = read_u32(&i.buf, curr)?;
    curr += SYMBOLS_LEN_SIZE;
    println!("len: {}", len);

    let table_buf = slice_bytes(&i.buf, len as usize, curr);
    curr += len as usize;

    let cs = get_checksum(&i.buf, curr)?;
    let crc = CASTAGNIOLI.checksum(table_buf);

    let data = copy_bytes(
        table_buf,
        table_buf.len() - NUM_SYMBOLS_SIZE,
        NUM_SYMBOLS_SIZE,
    );

    //println!("{:x?}", table_buf);
    if cs != crc {
        println!("Checksum mismatch. Corrupted symbol table.");
        return Err(TSDBError::Default);
    }

    Ok(SymbolTable {
        buf: data,
        current_pos: 0,
    })
}

pub fn series(i: &Index) -> Result<Series> {
    let start = i.toc.series as usize;
    let end = i.toc.label_index_start as usize;

    // TODO: slice here, will require tying series to the lifetime of the index
    // explicitly
    let data = copy_bytes(&i.buf, end - start, start);

    Ok(Series {
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

                let data = slice_bytes(&self.buf, len as usize, self.current_pos);

                // data length
                self.current_pos += len as usize;

                match str::from_utf8(data) {
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

impl SymbolTable {}

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
    buf: Vec<u8>,
    current_pos: usize,
}

#[derive(Debug)]
pub struct SeriesItem {
    pub labels: HashMap<usize, usize>,
}

impl TryFrom<&[u8]> for SeriesItem {
    type Error = TSDBError;

    fn try_from(buf: &[u8]) -> std::result::Result<Self, Self::Error> {
        let mut pos = 0;
        let (num_labels, size) = read_varint_u64(buf, pos)?;
        pos += size;
        println!("num labels: {}", num_labels);

        let mut labels = HashMap::<usize, usize>::new();
        for _ in 0..num_labels {
            let (k, size) = read_varint_u32(buf, pos)?;
            pos += size;
            let (v, size) = read_varint_u32(buf, pos)?;
            pos += size;

            // TODO: properly resolve from symbol table
            println!("labels: {} {}", k, v);
            labels.insert(k as usize, v as usize);
        }

        Ok(SeriesItem { labels })
    }
}

#[derive(Debug)]
pub struct SeriesChunk {
    min_time: u64,
    max_time: u64,
    // TODO: data: Vec<u8>,
    data: (u64, u64),
}

impl Iterator for Series {
    type Item = SeriesItem;

    fn next(&mut self) -> Option<Self::Item> {
        // be done if we reached the end of the buffer
        println!("series pos: {}, len: {}", self.current_pos, self.buf.len());
        if self.current_pos >= self.buf.len() {
            return None;
        }
        match read_varint_u32(&self.buf, self.current_pos) {
            Ok((len, size)) => {
                println!("item len: {}, size: {}", len, size);
                if size == 0 {
                    return None;
                }
                self.current_pos += size;
                // if len is 0 keep going
                // TODO: find proper aligned pos instead of skipping single bytes
                if len == 0 {
                    return self.next();
                }
                let data = slice_bytes(&self.buf, len as usize, self.current_pos);
                println!("{:x?}", &data);
                self.current_pos += len as usize;
                match get_checksum(&self.buf, self.current_pos) {
                    Ok(cs) => {
                        let crc = CASTAGNIOLI.checksum(data);
                        if cs != crc {
                            println!("checksum mismatch");
                            return None;
                        }

                        // TODO: don't unwrap
                        let series_item = data.try_into().unwrap();
                        self.current_pos += CHECKSUM_SIZE;

                        Some(series_item)
                    }
                    Err(_) => None,
                }
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
#[derive(Debug, PartialEq)]
pub struct TOC {
    symbols: u64,
    series: u64,
    label_index_start: u64,
    postings_start: u64,
    label_offset_table: u64,
    postings_offset_table: u64,
}

#[cfg(test)]
mod test {
    use super::*;

    fn load_index() -> Index {
        let test_index = Path::new("testdata/index_format_v1/index");
        Index::new(test_index)
    }

    #[test]
    fn load_test_index() {
        let index = load_index();

        let expected = TOC {
            symbols: 5,
            series: 323,
            label_index_start: 1806,
            postings_start: 2248,
            label_offset_table: 4300,
            postings_offset_table: 4326,
        };

        assert_eq!(expected, index.toc);
    }

    #[test]
    fn load_symbol_table() {
        let index = load_index();

        // build expected vec ["0", "1", "10", ..., "foo", "meh"]
        let mut expected: Vec<String> = (0..100).map(|i| i.to_string()).collect();
        expected.sort();
        expected.push("bar".to_string());
        expected.push("baz".to_string());
        expected.push("foo".to_string());
        expected.push("meh".to_string());

        let sym_table = symbol_table(&index).unwrap();
        assert_eq!(expected, sym_table.collect::<Vec<String>>())
    }

    #[test]
    fn load_series() {
        let index = load_index();

        // expected count of series
        let expected_count = 102;
        let mut expected: HashMap<String, String> = (0..100)
            .map(|i| ("bar".to_string(), i.to_string()))
            .collect();
        expected.insert("foo".to_string(), "baz".to_string());

        let series = series(&index).unwrap();
        let mut count = 0;
        let mut got = HashMap::<String, String>::new();
        for s in series {
            count += 1;
            for (k, v) in s.labels.into_iter() {
                let key = index.read_symbol(k).unwrap();
                let val = index.read_symbol(v).unwrap();
                got.insert(key, val);
            }
        }
        assert_eq!(expected_count, count);
        assert_eq!(expected, got);
    }
}
