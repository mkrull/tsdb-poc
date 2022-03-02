use clap::Parser;
use std::path::PathBuf;

mod entities;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(short, long, parse(from_os_str), value_name = "FILE")]
    file: Option<PathBuf>,
}

fn main() {
    let cli = Cli::parse();

    if let Some(file) = cli.file.as_deref() {
        let mut chunk_count = 0;
        let mut chunk_positions: Vec<usize> = vec![];
        let chunks = entities::chunks::Chunks::new(file);

        for i in chunks.into_iter() {
            chunk_positions.push(i);
            chunk_count += 1;
        }

        println!("Chunk start positions in file: {:?}", chunk_positions);
        println!("Number of chunks: {}", chunk_count);
    }
}
