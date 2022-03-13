use clap::Parser;
use std::{mem::size_of, path::PathBuf};

mod entities;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(short, long, parse(from_os_str), value_name = "FILE")]
    chunk_file: Option<PathBuf>,
    #[clap(short, long, parse(from_os_str), value_name = "FILE")]
    index_file: Option<PathBuf>,
}

fn main() {
    let cli = Cli::parse();

    if let Some(file) = cli.chunk_file.as_deref() {
        let mut chunk_positions: Vec<usize> = Vec::new();
        let chunks = entities::chunks::Chunks::new(file);

        for i in chunks {
            chunk_positions.push(i);
        }

        println!("Number of chunks: {}", chunk_positions.len());
    }

    if let Some(file) = cli.index_file.as_deref() {
        let index = entities::index::Index::new(file);

        println!("{:?}", &index);
        let sym_table = index.symbol_table;

        for s in sym_table {
            println!("{}", s)
        }
    }
}
