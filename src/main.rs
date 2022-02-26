use std::{fs::File, io::Read};

fn main() {
    let mut f = File::open("testdata/index_format_v1/chunks/000001").expect("Could not open file.");
    let mut buf: Vec<u8> = vec![];

    f.read_to_end(&mut buf).expect("Error reading into buf");

    println!("{:?}", buf);
}
