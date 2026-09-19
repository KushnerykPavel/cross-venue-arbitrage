mod exporter;
mod parquet_tables;

use std::env;
use std::error::Error;
use std::io;
use std::path::PathBuf;

use exporter::{ExportRequest, export_capture};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = Arguments::parse(env::args().skip(1))?;
    let result = export_capture(ExportRequest {
        capture_directory: arguments.capture_directory,
        output_directory: arguments.output_directory,
        allow_incomplete: arguments.allow_incomplete,
    })?;
    println!(
        "Parquet dataset {} created from capture {} with {} canonical events",
        result.output_directory.display(),
        result.capture_id,
        result.canonical_event_count
    );
    for (table, rows) in result.table_rows {
        println!("{table}: {rows} rows");
    }
    Ok(())
}

struct Arguments {
    capture_directory: PathBuf,
    output_directory: PathBuf,
    allow_incomplete: bool,
}

impl Arguments {
    fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Self, io::Error> {
        let mut positional = Vec::new();
        let mut allow_incomplete = false;
        for argument in arguments {
            match argument.as_str() {
                "--allow-incomplete" => allow_incomplete = true,
                value if value.starts_with('-') => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unknown option: {value}"),
                    ));
                }
                _ => positional.push(argument),
            }
        }
        if positional.len() != 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: export-parquet <capture-directory> <new-dataset-directory> [--allow-incomplete]",
            ));
        }
        Ok(Self {
            capture_directory: PathBuf::from(&positional[0]),
            output_directory: PathBuf::from(&positional[1]),
            allow_incomplete,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_input_requires_explicit_flag() {
        let parsed = Arguments::parse([
            "/data/capture".into(),
            "/data/dataset".into(),
            "--allow-incomplete".into(),
        ])
        .unwrap();
        assert!(parsed.allow_incomplete);
        assert_eq!(parsed.output_directory, PathBuf::from("/data/dataset"));
    }
}
