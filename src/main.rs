use clap::Parser;
use std::path::PathBuf;

pub mod entities;
use entities::chunks;
use entities::index;

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
        let chunks = chunks::Chunks::new(file);

        for i in chunks {
            chunk_positions.push(i);
        }

        println!("Number of chunks: {}", chunk_positions.len());
    }

    if let Some(file) = cli.index_file.as_deref() {
        let index = index::Index::new(file);

        let series = index::series(&index).unwrap();

        for s in series {
            println!("{:?}", s);
            for (k, v) in s.labels.into_iter() {
                let key = index.read_symbol(k).unwrap();
                let val = index.read_symbol(v).unwrap();
                println!("labels: {} -> {}", key, val);
            }
        }
    }
}
