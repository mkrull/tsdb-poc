use clap::Parser;
use std::{fs::File, io::Read, path::Path, path::PathBuf, process};
use unsigned_varint::decode;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(short, long, parse(from_os_str), value_name = "FILE")]
    file: Option<PathBuf>,
}

// NOTE: Format of a chunk file:
// https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/chunks.md
struct Chunks {
    buf: Vec<u8>,
    current_pos: usize,
}

impl Chunks {
    fn new(path: &Path) -> Self {
        let mut f = File::open(path).expect("Could not open file.");
        let mut buf: Vec<u8> = vec![];

        f.read_to_end(&mut buf).expect("Error reading into buf");

        let m = copy_bytes(&buf, 4, 0);
        println!("{:x?}", m);

        let v = copy_bytes(&buf, 1, 4);
        println!("{:x?}", v);

        Self {
            buf: buf.clone(),
            current_pos: 8,
        }
    }
}

impl Iterator for Chunks {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let curr = self.current_pos;
        let (len, size) = get_uvarint(&self.buf, self.current_pos);
        println!("{} {} {}", len, size, self.current_pos);
        if size == 0 {
            return None;
        }
        // NOTE: sizes of segments according to:
        // https://github.com/prometheus/prometheus/blob/main/tsdb/chunks/chunks.go#L37
        //
        // len varint size
        self.current_pos += 1;
        // encoding byte
        self.current_pos += 1;
        // data length
        self.current_pos += len as usize;
        // checksum bytes
        self.current_pos += 4;

        let data = copy_bytes(&self.buf, size + 1 + len as usize + 4, curr);
        println!("{:?}", data);

        return Some((curr, self.current_pos));
    }
}

fn main() {
    let cli = Cli::parse();

    if let Some(file) = cli.file.as_deref() {
        let mut chunk_count = 0;
        let chunks = Chunks::new(file);

        for i in chunks.into_iter() {
            println!("{:?}", i);
            chunk_count += 1;
        }

        println!("{}", chunk_count);
    }
}

// in this context the max varint size is 32 bits, hence the u32 return value:
// https://github.com/prometheus/prometheus/blob/main/tsdb/chunks/chunks.go#L52
fn get_uvarint(buf: &Vec<u8>, pos: usize) -> (u32, usize) {
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

fn copy_bytes(buf: &Vec<u8>, size: usize, pos: usize) -> Vec<u8> {
    let mut ret = vec![0; size];
    ret[..].copy_from_slice(&buf[pos..pos + size]);
    return ret;
}
